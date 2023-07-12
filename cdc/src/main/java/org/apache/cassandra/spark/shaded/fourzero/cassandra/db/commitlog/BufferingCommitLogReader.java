package org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.CRC32;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.ICommitLogMarkers;
import org.apache.cassandra.spark.cdc.Marker;
import org.apache.cassandra.spark.exceptions.TransportFailureException;
import org.apache.cassandra.spark.reader.fourzero.BaseFourZeroUtils;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Mutation;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.util.CdcRandomAccessReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.util.FileDataInput;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.stats.ICdcStats;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.spark.utils.LoggerHelper;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.FBUtilities.updateChecksumInt;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

/**
 * Copied and refactored from org.apache.cassandra.db.commitlog.CommitLogReader to read from generic source not tied to java.io.File and local file system.
 */
@NotThreadSafe
public class BufferingCommitLogReader implements CommitLogReadHandler, AutoCloseable, Comparable<BufferingCommitLogReader>
{
    private static final int LEGACY_END_OF_SEGMENT_MARKER = 0;

    @VisibleForTesting
    public static final int ALL_MUTATIONS = -1;
    private final CommitLog log;
    @Nullable
    final CdcOffsetFilter offsetFilter;
    private final CRC32 checksum;
    List<PartitionUpdateWrapper> updates;
    @Nullable
    private final RangeFilter rangeFilter;
    private byte[] buffer;

    private RandomAccessReader reader;
    private CommitLogDescriptor desc = null;
    private final ReadStatusTracker statusTracker;
    private int pos = 0;
    private final ICdcStats stats;
    private final long segmentId;
    private final int messagingVersion;

    @NotNull
    private final Marker highWaterMark;

    private final LoggerHelper logger;
    @Nullable
    private final AsyncExecutor executor;
    @Nullable
    private Consumer<Marker> listener = null;
    @NotNull
    private final ICommitLogMarkers markers;

    @VisibleForTesting
    public BufferingCommitLogReader(@NotNull final CommitLog log,
                                    @Nullable final Marker highWaterMark,
                                    @NotNull final ICdcStats stats,
                                    @Nullable Consumer<Marker> listener)
    {
        this(null, log, null, ICommitLogMarkers.of(highWaterMark), 0, stats, null, false);
        this.listener = listener;
    }

    public BufferingCommitLogReader(@Nullable final CdcOffsetFilter offsetFilter,
                                    @NotNull final CommitLog log,
                                    @Nullable final RangeFilter rangeFilter,
                                    @NotNull ICommitLogMarkers markers,
                                    final int partitionId,
                                    @NotNull final ICdcStats stats,
                                    @Nullable final AsyncExecutor executor,
                                    boolean readHeader)
    {
        this.offsetFilter = offsetFilter;
        this.log = log;
        this.updates = new ArrayList<>();
        this.rangeFilter = rangeFilter;
        this.statusTracker = new ReadStatusTracker(ALL_MUTATIONS, false);
        this.checksum = new CRC32();
        this.buffer = new byte[CdcRandomAccessReader.DEFAULT_BUFFER_SIZE];
        this.reader = BufferingCommitLogReader.reader(log);
        this.markers = markers;
        this.logger = new LoggerHelper(LoggerFactory.getLogger(BufferingCommitLogReader.class),
                                       "instance", log.instance().nodeName(),
                                       "dc", log.instance().dataCenter(),
                                       "log", log.name(),
                                       "size", log.maxOffset(),
                                       "partitionId", partitionId);

        Pair<Integer, Long> pair = CommitLog.extractVersionAndSegmentId(log).orElseThrow(() -> new IllegalStateException("Could not extract segmentId from CommitLog filename"));
        this.messagingVersion = pair.getLeft();
        this.segmentId = pair.getRight();
        final Marker startMarker = markers.startMarker(log);
        this.highWaterMark = startMarker.segmentId() == segmentId ? startMarker : log.zeroMarker();
        this.stats = stats;
        this.executor = executor;

        try
        {
            if (readHeader || this.highWaterMark.position() == 0)
            {
                this.readHeader();
                if (skip(highWaterMark))
                {
                    // if we can skip this CommitLog, close immediately
                    stats.skippedCommitLogsCount(1);
                    close();
                    return;
                }
            }
            else if (shouldSkipSegmentId(highWaterMark))
            {
                stats.skippedCommitLogsCount(1);
                close();
                return;
            }
            read();
        }
        catch (Throwable t)
        {
            close();
            if (isNotFoundError(t))
            {
                return;
            }
            logger.warn("Exception reading CommitLog", t);
            throw new RuntimeException(t);
        }
    }

    public static RandomAccessReader reader(CommitLog log)
    {
        return new CdcRandomAccessReader(log);
    }

    private void readHeader() throws IOException
    {
        final long startTimeNanos = System.nanoTime();
        try
        {
            desc = CommitLogDescriptor.readHeader(reader, DatabaseDescriptor.getEncryptionContext());
        }
        catch (IOException e)
        {
            // let recover deal with it
            logger.warn("IOException reading CommitLog header", e);
            stats.commitLogHeaderReadFailureCount(1);
        }
        if (desc == null)
        {
            // don't care about whether or not the handler thinks we can continue. We can't w/out descriptor.
            // whether or not we can continue depends on whether this is the last segment
            this.handleUnrecoverableError(
            new CommitLogReadException(String.format("Could not read commit log descriptor in file %s", log.name()),
                                       CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR,
                                       false));
        }
        else
        {
            final long timeTakenToReadHeader = System.nanoTime() - startTimeNanos;
            logger.info("Read log header", "segmentId", desc.id, "compression", desc.compression,
                        "version", desc.version, "messagingVersion", desc.getMessagingVersion(),
                        "timeNanos", timeTakenToReadHeader);
            stats.commitLogHeaderReadTime(timeTakenToReadHeader);
        }
    }

    private void read()
    {
        try
        {
            readCommitLogSegment();
        }
        catch (final Throwable t)
        {
            if (isNotFoundError(t))
            {
                return;
            }
            final Throwable cause = ThrowableUtils.rootCause(t);
            logger.warn("Exception reading CommitLog", cause);
            throw new RuntimeException(cause);
        }
    }

    private boolean isNotFoundError(Throwable t)
    {
        final TransportFailureException transportEx = ThrowableUtils.rootCause(t, TransportFailureException.class);
        if (transportEx != null && transportEx.isNotFound())
        {
            // underlying commit log may have been removed before/during reading
            // this should only happen when CommitLog is old and can be removed
            logger.warn("CommitLog not found, assuming removed by underlying storage", transportEx);
            return true;
        }
        return false;
    }

    /**
     * Reads mutations from file, handing them off to handler
     *
     * @throws IOException IOException
     */
    private void readCommitLogSegment() throws IOException
    {
        final long startTimeNanos = System.nanoTime();
        SeekableCommitLogSegmentReader segmentReader;
        try
        {
            segmentReader = new SeekableCommitLogSegmentReader(segmentId, this, desc, reader, logger, false);
        }
        catch (Exception e)
        {
            this.handleUnrecoverableError(new CommitLogReadException(
            String.format("Unable to create segment reader for commit log file: %s", e),
            CommitLogReadErrorReason.UNRECOVERABLE_UNKNOWN_ERROR,
            false));
            return;
        }

        try
        {
            if (reader.getFilePointer() < highWaterMark.position())
            {
                stats.commitLogBytesSkippedOnRead(highWaterMark.position() - reader.getFilePointer());
                segmentReader.seek(highWaterMark.position());
            }

            for (CommitLogSegmentReader.SyncSegment syncSegment : segmentReader)
            {
                // Only tolerate truncationSerializationHeader if we allow in both global and segment
//                statusTracker.tolerateErrorsInSection = tolerateTruncation && syncSegment.toleratesErrorsInSection;

                statusTracker.errorContext = String.format("Next section at %d in %s", syncSegment.fileStartPosition, log.name());

                readSection(syncSegment.input, syncSegment.endPosition);

                // track the position at end of previous section after successfully reading mutations
                // so we can update highwater mark after reading
                this.pos = (int) reader.getFilePointer();

                if (listener != null)
                {
                    listener.accept(log.markerAt(segmentId, pos));
                }

                if (!statusTracker.shouldContinue())
                {
                    break;
                }
            }
        }
        // Unfortunately CommitLogSegmentReader.SegmentIterator (for-loop) cannot throw a checked exception,
        // so we check to see if a RuntimeException is wrapping an IOException.
        catch (RuntimeException re)
        {
            if (re.getCause() instanceof IOException)
            {
                throw (IOException) re.getCause();
            }
            throw re;
        }
        logger.info("Finished reading commit log", "updates", updates.size(), "timeNanos", (System.nanoTime() - startTimeNanos));
    }

    public boolean skip(@Nullable final Marker highWaterMark) throws IOException
    {
        if (shouldSkip(reader))
        {
            logger.info("Skipping playback of empty log");
            return true;
        }

        // just transform from the file name (no reading of headSkipping playback of empty log:ers) to determine version
        final long segmentIdFromFilename = CommitLogDescriptor.fromFileName(log.name()).id;

        if (segmentIdFromFilename != desc.id)
        {
            CommitLogReadException readException = new CommitLogReadException(
            String.format("Segment id mismatch (filename %d, descriptor %d) in file %s", segmentIdFromFilename, desc.id, log.name()),
            CommitLogReadErrorReason.RECOVERABLE_DESCRIPTOR_ERROR,
            false);
            return this.shouldSkipSegmentOnError(readException);
        }

        return shouldSkipSegmentId(highWaterMark);
    }

    /**
     * Peek the next 8 bytes to determine if it reaches the end of the file.
     * It should _only_ be called immediately after reading the commit log header.
     *
     * @return true to skip; otherwise, return false.
     * @throws IOException io exception
     */
    private static boolean shouldSkip(RandomAccessReader reader) throws IOException
    {
        try
        {
            reader.mark(); // mark position
            int end = reader.readInt();
            long filecrc = reader.readInt() & 0xffffffffL;
            return end == 0 && filecrc == 0;
        }
        catch (EOFException e)
        {
            // no data to read
            return true;
        }
        finally
        {
            // return to marked position before reading mutations
            reader.reset();
        }
    }

    /**
     * Any segment with id >= minPosition.segmentId is a candidate for read.
     */
    private boolean shouldSkipSegmentId(@Nullable final Marker highWaterMark)
    {
        logger.debug("Reading commit log", "version", messagingVersion, "compression", desc != null ? desc.compression : "disabled");

        if (highWaterMark != null && highWaterMark.segmentId() > segmentId)
        {
            logger.info("Skipping read of fully-flushed log", "segmentId", segmentId, "minSegmentId", highWaterMark.segmentId());
            return true;
        }
        return false;
    }

    /**
     * Reads a section of a file containing mutations
     *
     * @param reader FileDataInput / logical buffer containing commitlog mutations
     * @param end    logical numeric end of the segment being read
     */
    private void readSection(final FileDataInput reader,
                             final int end) throws IOException
    {
        final long startTimeNanos = System.nanoTime();

        while (statusTracker.shouldContinue() && reader.getFilePointer() < end && !reader.isEOF())
        {
            final int mutationStart = (int) reader.getFilePointer();
            logger.trace("Reading mutation at", "position", mutationStart);

            long claimedCRC32;
            int serializedSize;
            try
            {
                // We rely on reading serialized size == 0 (LEGACY_END_OF_SEGMENT_MARKER) to identify the end
                // of a segment, which happens naturally due to the 0 padding of the empty segment on creation.
                // However, it's possible with 2.1 era commitlogs that the last mutation ended less than 4 bytes
                // from the end of the file, which means that we'll be unable to read an a full int and instead
                // read an EOF here
                if (end - reader.getFilePointer() < 4)
                {
                    logger.trace("Not enough bytes left for another mutation in this CommitLog section, continuing");
                    statusTracker.requestTermination();
                    return;
                }

                // any of the reads may hit EOF
                serializedSize = reader.readInt();
                if (serializedSize == LEGACY_END_OF_SEGMENT_MARKER)
                {
                    logger.trace("Encountered end of segment marker at", "position", reader.getFilePointer());
                    statusTracker.requestTermination();
                    return;
                }

                // Mutation must be at LEAST 10 bytes:
                //    3 for a non-empty Keyspace
                //    3 for a Key (including the 2-byte length from writeUTF/writeWithShortLength)
                //    4 bytes for column count.
                // This prevents CRC by being fooled by special-case garbage in the file; see CASSANDRA-2128
                if (serializedSize < 10)
                {
                    if (this.shouldSkipSegmentOnError(new CommitLogReadException(
                    String.format("Invalid mutation size %d at %d in %s", serializedSize, mutationStart, statusTracker.errorContext),
                    CommitLogReadErrorReason.MUTATION_ERROR,
                    statusTracker.tolerateErrorsInSection)))
                    {
                        statusTracker.requestTermination();
                    }

                    stats.commitLogInvalidSizeMutationCount(1);
                    return;
                }

                long claimedSizeChecksum = CommitLogFormat.calculateClaimedChecksum(reader);
                checksum.reset();
                CommitLogFormat.updateChecksum(checksum, serializedSize);

                if (checksum.getValue() != claimedSizeChecksum)
                {
                    if (this.shouldSkipSegmentOnError(new CommitLogReadException(
                    String.format("Mutation size checksum failure at %d in %s", mutationStart, statusTracker.errorContext),
                    CommitLogReadErrorReason.MUTATION_ERROR,
                    statusTracker.tolerateErrorsInSection)))
                    {
                        statusTracker.requestTermination();
                    }

                    stats.mutationsChecksumMismatchCount(1);
                    return;
                }

                if (serializedSize > buffer.length)
                {
                    buffer = new byte[(int) (1.2 * serializedSize)];
                }
                reader.readFully(buffer, 0, serializedSize);

                claimedCRC32 = CommitLogFormat.calculateClaimedCRC32(reader);
            }
            catch (EOFException eof)
            {
                if (this.shouldSkipSegmentOnError(new CommitLogReadException(
                String.format("Unexpected end of segment at %d in %s", mutationStart, statusTracker.errorContext),
                CommitLogReadErrorReason.EOF,
                statusTracker.tolerateErrorsInSection)))
                {
                    statusTracker.requestTermination();
                }

                stats.commitLogSegmentUnexpectedEndErrorCount(1);
                return;
            }

            checksum.update(buffer, 0, serializedSize);
            if (claimedCRC32 != checksum.getValue())
            {
                if (this.shouldSkipSegmentOnError(new CommitLogReadException(
                String.format("Mutation checksum failure at %d in %s", mutationStart, statusTracker.errorContext),
                CommitLogReadErrorReason.MUTATION_ERROR,
                statusTracker.tolerateErrorsInSection)))
                {
                    statusTracker.requestTermination();
                }

                stats.mutationsChecksumMismatchCount(1);
                continue;
            }

            final int mutationPosition = (int) reader.getFilePointer();
            readMutationInternal(buffer, serializedSize, mutationPosition);
            statusTracker.addProcessedMutation();
        }

        stats.commitLogSegmentReadTime(System.nanoTime() - startTimeNanos);
    }

    /**
     * Deserializes and passes a Mutation to the ICommitLogReadHandler requested
     *
     * @param inputBuffer      raw byte array w/Mutation data
     * @param size             deserialized size of mutation
     * @param mutationPosition filePointer offset of end of mutation within CommitLogSegment
     */
    @VisibleForTesting
    private void readMutationInternal(final byte[] inputBuffer,
                                      final int size,
                                      final int mutationPosition) throws IOException
    {
        // For now, we need to go through the motions of deserializing the mutation to determine its size and move
        // the file pointer forward accordingly, even if we're behind the requested minPosition within this SyncSegment.

        final Mutation mutation;
        try (RebufferingInputStream bufIn = new DataInputBuffer(inputBuffer, 0, size))
        {
            mutation = Mutation.serializer.deserialize(bufIn,
                                                       messagingVersion,
                                                       DeserializationHelper.Flag.LOCAL);
        }
        catch (UnknownTableException ex)
        {
            if (ex.id == null)
            {
                return;
            }
            logger.trace("Invalid mutation", ex); // we see many unknown table exception logs when we skip over mutations from other tables
            stats.mutationsIgnoredUnknownTableCount(1);

            return;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            Path p = Files.createTempFile("mutation", "dat");

            try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(p)))
            {
                out.write(inputBuffer, 0, size);
            }

            // Checksum passed so this error can't be permissible.
            this.handleUnrecoverableError(new CommitLogReadException(
            String.format(
            "Unexpected error deserializing mutation; saved to %s.  " +
            "This may be caused by replaying a mutation against a table with the same name but incompatible schema.  " +
            "Exception follows: %s", p, t),
            CommitLogReadErrorReason.MUTATION_ERROR,
            false));

            stats.mutationsDeserializeFailedCount(1);

            return;
        }

        // fixme: Cassandra has a bug of getting the string representation of tombstoned cell of some type (date, smallint, etc.) in a multi-cell collection. The trace logging below triggers AbstractCell#toString()
        // JIRA: CASSANDRA-17695 AbstractCell#toString throws MarshalException for cell in collection
        logger.trace("Read mutation for", () -> "keyspace", mutation::getKeyspaceName, () -> "key", mutation::key,
                     () -> "mutation", () -> "{" + StringUtils.join(mutation.getPartitionUpdates().iterator(), ", ") + "}");

        stats.mutationsReadCount(1);
        stats.mutationsReadBytes(size);

        this.handleMutation(mutation, size, mutationPosition, desc);
    }

    public boolean isReadable()
    {
        return updates != null;
    }

    public void close()
    {
        if (updates == null)
        {
            return;
        }

        try
        {
            log.close();
            reader.close();
            reader = null;
            updates = null;
        }
        catch (final Throwable t)
        {
            logger.error("Unexpected exception closing reader", t);
        }
    }

    public int compareTo(@NotNull BufferingCommitLogReader o)
    {
        return Long.compare(segmentId, o.segmentId);
    }

    /**
     * Helper methods to deal with changing formats of internals of the CommitLog without polluting deserialization code.
     */
    private static class CommitLogFormat
    {
        public static long calculateClaimedChecksum(FileDataInput input) throws IOException
        {
            return input.readInt() & 0xffffffffL;
        }

        public static void updateChecksum(CRC32 checksum, int serializedSize)
        {
            updateChecksumInt(checksum, serializedSize);
        }

        public static long calculateClaimedCRC32(FileDataInput input) throws IOException
        {
            return input.readInt() & 0xffffffffL;
        }
    }

    private static class ReadStatusTracker
    {
        private int mutationsLeft;
        public String errorContext = "";
        public boolean tolerateErrorsInSection;
        private boolean error;

        public ReadStatusTracker(int mutationLimit, boolean tolerateErrorsInSection)
        {
            this.error = false;
            this.mutationsLeft = mutationLimit;
            this.tolerateErrorsInSection = tolerateErrorsInSection;
        }

        public void addProcessedMutation()
        {
            if (mutationsLeft == ALL_MUTATIONS)
            {
                return;
            }
            --mutationsLeft;
        }

        public boolean shouldContinue()
        {
            return !error && mutationsLeft != 0;
        }

        public void requestTermination()
        {
            error = true;
        }
    }

    /**
     * @return result object wrapping list of updates buffered and the final highwater marker position.
     */
    public Result result()
    {
        return new Result(this);
    }

    public static class Result
    {
        private final List<PartitionUpdateWrapper> updates;
        private final Marker marker;

        private Result(BufferingCommitLogReader reader)
        {
            this.updates = reader.updates;
            this.marker = reader.log.markerAt(reader.segmentId, reader.pos);
        }

        public List<PartitionUpdateWrapper> updates()
        {
            return updates;
        }

        public Marker marker()
        {
            return marker;
        }
    }

    // CommitLogReadHandler

    public boolean shouldSkipSegmentOnError(CommitLogReadException e)
    {
        logger.warn("CommitLog error on shouldSkipSegment", e);
        return false;
    }

    public void handleUnrecoverableError(CommitLogReadException e) throws IOException
    {
        logger.error("CommitLog unrecoverable error", e);
        statusTracker.requestTermination();
        throw e;
    }

    public void handleMutation(Mutation mutation, int size, int mutationPosition, @Nullable CommitLogDescriptor desc)
    {
        if (!mutation.trackedByCDC())
        {
            logger.debug("Ignore mutation not tracked by CDC");
            return;
        }

        mutation.getPartitionUpdates()
                .stream()
                .filter(u -> this.filter(mutationPosition, u))
                .map(u -> Pair.of(u, maxTimestamp(u)))
                .filter(this::withinTimeWindow)
                .peek(pair -> pair.getLeft().validate())
                .map(this::toCdcUpdate)
                .forEach(updates::add);
    }

    private long maxTimestamp(PartitionUpdate update)
    {
        // row deletion
        if (update.rowCount() == 1 && !update.lastRow().deletion().isLive())
        {
            return update.lastRow().deletion().time().markedForDeleteAt();
        }
        else
        {
            return update.maxTimestamp();
        }
    }

    private PartitionUpdateWrapper toCdcUpdate(final Pair<PartitionUpdate, Long> update)
    {
        return toCdcUpdate(update.getLeft(), update.getRight());
    }

    private PartitionUpdateWrapper toCdcUpdate(final PartitionUpdate update,
                                               final long maxTimestampMicros)
    {
        return new PartitionUpdateWrapper(update, maxTimestampMicros, executor);
    }

    /**
     * @param position current position in CommitLog.
     * @param update the partition update
     * @return true if this is a mutation we are looking for.
     */
    private boolean filter(int position, PartitionUpdate update)
    {
        return isCdcEnabled(update) && withinRange(position, update);
    }

    private boolean isCdcEnabled(PartitionUpdate update)
    {
        if (update.metadata().params.cdc)
        {
            return true;
        }

        String keyspace = getKeyspace(update);
        String table = getTable(update);
        logger.debug("Ignore partition update from table not tracked by CDC",
                     "keyspace", keyspace, "table", table);
        stats.untrackedChangesIgnored(keyspace, table, 1);
        return false;
    }

    private boolean withinTimeWindow(Pair<PartitionUpdate, Long> update)
    {
        PartitionUpdate pu = update.getLeft();
        long maxTimestampMicros = update.getRight();
        boolean shouldInclude = withinTimeWindow(maxTimestampMicros);
        if (!shouldInclude)
        {
            String keyspace = getKeyspace(pu);
            String table = getTable(pu);
            if (logger.isTraceEnabled())
            {
                logger.trace("Exclude the update due to out of the allowed time window.",
                             "update", "'" + pu + "'",
                             "keyspace", keyspace, "table", table,
                             "timestampMicros", maxTimestampMicros,
                             "maxAgeMicros", offsetFilter == null ? "null" : offsetFilter.maxAgeMicros());
            }
            else
            {
                logger.warn("Exclude the update due to out of the allowed time window.", null,
                            "keyspace", keyspace, "table", table,
                            "timestampMicros", maxTimestampMicros,
                            "maxAgeMicros", offsetFilter == null ? "null" : offsetFilter.maxAgeMicros());
            }
            stats.lateChangeDropped(getKeyspace(pu), getTable(pu), maxTimestampMicros);
        }
        return shouldInclude;
    }

    private boolean withinTimeWindow(long maxTimestampMicros)
    {
        if (offsetFilter == null)
        {
            return true;
        }
        return offsetFilter.overlaps(maxTimestampMicros);
    }

    /**
     * @param position current position in CommitLog.
     * @param update   a CommitLog PartitionUpdate.
     * @return true if PartitionUpdate overlaps with the Spark worker token range.
     */
    private boolean withinRange(final int position, final PartitionUpdate update)
    {
        if (rangeFilter == null)
        {
            return true;
        }

        final BigInteger token = BaseFourZeroUtils.tokenToBigInteger(update.partitionKey().getToken());

        if (!rangeFilter.skipPartition(token))
        {
            return !markers.canIgnore(log.markerAt(segmentId, position), token);
        }

        String keyspace = getKeyspace(update);
        String table = getTable(update);
        logger.debug("Ignore out of range partition update.",
                     "keyspace", keyspace, "table", table);
        stats.outOfTokenRangeChangesIgnored(keyspace, table, 1);
        return false;
    }

    private static String getKeyspace(PartitionUpdate partitionUpdate)
    {
        return partitionUpdate.metadata().keyspace;
    }

    private static String getTable(PartitionUpdate partitionUpdate)
    {
        return partitionUpdate.metadata().name;
    }
}

