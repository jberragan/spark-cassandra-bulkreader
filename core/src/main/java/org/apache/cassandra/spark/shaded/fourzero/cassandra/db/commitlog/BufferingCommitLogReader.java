package org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.exceptions.TransportFailureException;
import org.apache.cassandra.spark.reader.fourzero.FourZeroUtils;
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
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
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
    private final TableMetadata table;
    private final CommitLog log;
    @Nullable
    final CdcOffsetFilter offsetFilter;
    private final CRC32 checksum;
    List<CdcUpdate> updates;
    @Nullable
    private final SparkRangeFilter sparkRangeFilter;
    private byte[] buffer;

    private RandomAccessReader reader;
    private CommitLogDescriptor desc = null;
    private final ReadStatusTracker statusTracker;
    private int pos = 0;

    @NotNull
    private final CommitLog.Marker highWaterMark;

    private final LoggerHelper logger;

    @VisibleForTesting
    public BufferingCommitLogReader(@NotNull final TableMetadata table,
                                    @NotNull final CommitLog log,
                                    @NotNull final Watermarker watermarker)
    {
        this(table, null, log, null, watermarker.highWaterMark(log.instance()), 0);
    }

    public BufferingCommitLogReader(@NotNull final TableMetadata table,
                                    @Nullable final CdcOffsetFilter offsetFilter,
                                    @NotNull final CommitLog log,
                                    @Nullable final SparkRangeFilter sparkRangeFilter,
                                    @Nullable final CommitLog.Marker highWaterMark,
                                    final int partitionId)
    {
        this.table = table;
        this.offsetFilter = offsetFilter;
        this.log = log;
        this.updates = new ArrayList<>();
        this.sparkRangeFilter = sparkRangeFilter;
        this.statusTracker = new ReadStatusTracker(ALL_MUTATIONS, false);
        this.checksum = new CRC32();
        this.buffer = new byte[CdcRandomAccessReader.DEFAULT_BUFFER_SIZE];
        this.reader = BufferingCommitLogReader.reader(log);
        this.logger = new LoggerHelper(LoggerFactory.getLogger(BufferingCommitLogReader.class),
                                       "instance", log.instance().nodeName(),
                                       "dc", log.instance().dataCenter(),
                                       "log", log.name(),
                                       "size", log.maxOffset(),
                                       "partitionId", partitionId);

        this.highWaterMark = highWaterMark == null ? log.zeroMarker() : highWaterMark;

        try
        {
            this.readHeader();
            if (skip())
            {
                // if we can skip this CommitLog, close immediately
                close();
            }
            else
            {
                read();
            }
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

    private void readHeader()
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
            logger.info("Read log header segmentId={} compression={} version={} messagingVersion={} timeNanos={}", desc.id, desc.compression, desc.version, desc.getMessagingVersion(), (System.nanoTime() - startTimeNanos));
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
            segmentReader = new SeekableCommitLogSegmentReader(this, desc, reader, logger, false);
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
            if (desc.id == highWaterMark.segmentId() && reader.getFilePointer() < highWaterMark.position())
            {
                segmentReader.seek(highWaterMark.position());
            }

            for (CommitLogSegmentReader.SyncSegment syncSegment : segmentReader)
            {
                // Only tolerate truncationSerializationHeader if we allow in both global and segment
//                statusTracker.tolerateErrorsInSection = tolerateTruncation && syncSegment.toleratesErrorsInSection;

                statusTracker.errorContext = String.format("Next section at %d in %s", syncSegment.fileStartPosition, desc.fileName());

                readSection(syncSegment.input, syncSegment.endPosition, desc);

                // track the position at end of previous section after successfully reading mutations
                // so we can update highwater mark after reading
                this.pos = (int) reader.getFilePointer();

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
        logger.info("Finished reading commit log updates={} timeNanos={}", updates.size(), (System.nanoTime() - startTimeNanos));
    }

    public boolean skip() throws IOException
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

        return shouldSkipSegmentId();
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
    private boolean shouldSkipSegmentId()
    {
        logger.debug("Reading commit log version={} messagingVersion={} compression={}",
                     desc.version,
                     desc.getMessagingVersion(),
                     desc.compression);

        if (highWaterMark.segmentId() > desc.id)
        {
            logger.info("Skipping read of fully-flushed log segmentId={} minSegmentId={}", desc.id, highWaterMark.segmentId());
            return true;
        }
        return false;
    }

    /**
     * Reads a section of a file containing mutations
     *
     * @param reader FileDataInput / logical buffer containing commitlog mutations
     * @param end    logical numeric end of the segment being read
     * @param desc   Descriptor for CommitLog serialization
     */
    private void readSection(final FileDataInput reader,
                             final int end,
                             final CommitLogDescriptor desc) throws IOException
    {
        while (statusTracker.shouldContinue() && reader.getFilePointer() < end && !reader.isEOF())
        {
            final int mutationStart = (int) reader.getFilePointer();
            logger.trace("Reading mutation at position={}", mutationStart);

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
                    logger.trace("Encountered end of segment marker at position={}", reader.getFilePointer());
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
                continue;
            }

            final int mutationPosition = (int) reader.getFilePointer();
            readMutationInternal(buffer, serializedSize, mutationPosition, desc);
            statusTracker.addProcessedMutation();
        }
    }

    /**
     * Deserializes and passes a Mutation to the ICommitLogReadHandler requested
     *
     * @param inputBuffer      raw byte array w/Mutation data
     * @param size             deserialized size of mutation
     * @param mutationPosition filePointer offset of end of mutation within CommitLogSegment
     * @param desc             CommitLogDescriptor being worked on
     */
    @VisibleForTesting
    private void readMutationInternal(final byte[] inputBuffer,
                                      final int size,
                                      final int mutationPosition,
                                      final CommitLogDescriptor desc) throws IOException
    {
        // For now, we need to go through the motions of deserializing the mutation to determine its size and move
        // the file pointer forward accordingly, even if we're behind the requested minPosition within this SyncSegment.

        final Mutation mutation;
        try (RebufferingInputStream bufIn = new DataInputBuffer(inputBuffer, 0, size))
        {
            mutation = Mutation.serializer.deserialize(bufIn,
                                                       desc.getMessagingVersion(),
                                                       DeserializationHelper.Flag.LOCAL);
            // doublecheck that what we read is still valid for the current schema
            for (PartitionUpdate upd : mutation.getPartitionUpdates())
            {
                upd.validate();
            }
        }
        catch (UnknownTableException ex)
        {
            if (ex.id == null)
            {
                return;
            }
            logger.trace("Invalid mutation", ex); // we see many unknown table exception logs when we skip over mutations from other tables
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
            return;
        }

        logger.trace("Read mutation for keyspace={} key='{}' '{}'", mutation.getKeyspaceName(), mutation.key(),
                     "{" + StringUtils.join(mutation.getPartitionUpdates().iterator(), ", ") + "}");
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
        return Long.compare(desc.id, o.desc.id);
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
        private final List<CdcUpdate> updates;
        private final CommitLog.Marker marker;

        private Result(BufferingCommitLogReader reader)
        {
            this.updates = reader.updates;
            this.marker = reader.log.markerAt(reader.desc.id, reader.pos);
        }

        public List<CdcUpdate> updates()
        {
            return updates;
        }

        public CommitLog.Marker marker()
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

    public void handleUnrecoverableError(CommitLogReadException e)
    {
        logger.error("CommitLog unrecoverable error", e);
        statusTracker.requestTermination();
    }

    public void handleMutation(Mutation mutation, int size, int mutationPosition, CommitLogDescriptor desc)
    {
        mutation.getPartitionUpdates()
                .stream()
                .filter(this::filter)
                .map(u -> Pair.of(u, maxTimestamp(u)))
                .filter(this::withinTimeWindow)
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

    private CdcUpdate toCdcUpdate(final Pair<PartitionUpdate, Long> update)
    {
        return toCdcUpdate(update.getLeft(), update.getRight());
    }

    private CdcUpdate toCdcUpdate(final PartitionUpdate update,
                                  final long maxTimestampMicros)
    {
        return new CdcUpdate(table, update, maxTimestampMicros);
    }

    /**
     * @param update the partition update
     * @return true if this is a mutation we are looking for.
     */
    private boolean filter(PartitionUpdate update)
    {
        return isTable(update) && withinRange(update);
    }

    private boolean isTable(PartitionUpdate update)
    {
        return update.metadata().keyspace.equals(table.keyspace)
               && update.metadata().name.equals(table.name);
    }

    private boolean withinTimeWindow(Pair<PartitionUpdate, Long> update)
    {
        boolean shouldInclude = withinTimeWindow(update.getRight());
        if (!shouldInclude)
        {
            logger.info("Exclude the update due to out of the allowed time window. Update: {}", update.getLeft());
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
     * @param update a CommitLog PartitionUpdate.
     * @return true if PartitionUpdate overlaps with the Spark worker token range.
     */
    private boolean withinRange(final PartitionUpdate update)
    {
        if (sparkRangeFilter == null)
        {
            return true;
        }

        final BigInteger token = FourZeroUtils.tokenToBigInteger(update.partitionKey().getToken());
        return !sparkRangeFilter.skipPartition(update.partitionKey().getKey(), token);
    }
}
