package org.apache.cassandra.spark.reader.fourzero;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.BufferingCommitLogReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.CdcUpdate;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ListType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Cell;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.CellPath;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Row;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.spark.TaskContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

public class CdcScannerBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcScannerBuilder.class);

    // match both legacy and new version of commitlogs Ex: CommitLog-12345.log and CommitLog-4-12345.log.
    private static final Pattern COMMIT_LOG_FILE_PATTERN = Pattern.compile("CommitLog(-\\d+)*-(\\d+).log");
    private static final CompletableFuture<BufferingCommitLogReader.Result> NO_OP_FUTURE = CompletableFuture.completedFuture(null);

    final TableMetadata table;
    final Partitioner partitioner;
    final Stats stats;
    final Map<CassandraInstance, CompletableFuture<List<CdcUpdate>>> futures;
    final int minimumReplicasPerMutation;
    @Nullable
    private final SparkRangeFilter sparkRangeFilter;
    @Nullable
    private final CdcOffsetFilter offsetFilter;
    @NotNull
    final Watermarker watermarker;
    private final int partitionId;
    private final long startTimeNanos;
    @NotNull
    private final TimeProvider timeProvider;

    public CdcScannerBuilder(final TableMetadata table,
                             final Partitioner partitioner,
                             final CommitLogProvider commitLogs,
                             final Stats stats,
                             @Nullable final SparkRangeFilter sparkRangeFilter,
                             @Nullable final CdcOffsetFilter offsetFilter,
                             final int minimumReplicasPerMutation,
                             @NotNull final Watermarker jobWatermarker,
                             @NotNull final String jobId,
                             @NotNull final ExecutorService executorService,
                             @NotNull final TimeProvider timeProvider)
    {
        this.table = table;
        this.partitioner = partitioner;
        this.stats = stats;
        this.sparkRangeFilter = sparkRangeFilter;
        this.offsetFilter = offsetFilter;
        this.watermarker = jobWatermarker.instance(jobId);
        Preconditions.checkArgument(minimumReplicasPerMutation >= 1,
                                    "minimumReplicasPerMutation should be at least 1");
        this.minimumReplicasPerMutation = minimumReplicasPerMutation;
        this.startTimeNanos = System.nanoTime();
        this.timeProvider = timeProvider;

        final Map<CassandraInstance, List<CommitLog>> logs = commitLogs.logs()
                                                                       .collect(Collectors.groupingBy(CommitLog::instance, Collectors.toList()));
        final Map<CassandraInstance, CommitLog.Marker> markers = logs.keySet().stream()
                                                                     .map(watermarker::highWaterMark)
                                                                     .filter(Objects::nonNull)
                                                                     .collect(Collectors.toMap(CommitLog.Marker::instance, Function.identity()));

        this.partitionId = TaskContext.getPartitionId();
        LOGGER.info("Opening CdcScanner numInstances={} start={} maxAgeMicros={} partitionId={} listLogsTimeNanos={}",
                    logs.size(),
                    offsetFilter != null ? offsetFilter.start().getTimestampMicros() : null,
                    offsetFilter != null ? offsetFilter.maxAgeMicros() : null,
                    partitionId, System.nanoTime() - startTimeNanos
        );

        this.futures = logs.entrySet().stream()
                           .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> openInstanceAsync(e.getValue(), markers.get(e.getKey()), executorService))
                           );
    }

    private static boolean skipCommitLog(@NotNull final CommitLog log,
                                         @Nullable final CommitLog.Marker highwaterMark)
    {
        if (highwaterMark == null)
        {
            return false;
        }
        final Long segmentId = extractSegmentId(log);
        if (segmentId != null)
        {
            // only read CommitLog if greater than or equal to previously read CommitLog segmentId
            return segmentId < highwaterMark.segmentId();
        }
        return true;
    }

    @Nullable
    public static Long extractSegmentId(@NotNull final CommitLog log)
    {
        return extractSegmentId(log.name());
    }

    @Nullable
    public static Long extractSegmentId(@NotNull final String filename)
    {
        final Matcher matcher = CdcScannerBuilder.COMMIT_LOG_FILE_PATTERN.matcher(filename);
        if (matcher.matches())
        {
            try
            {
                return Long.parseLong(matcher.group(2));
            }
            catch (NumberFormatException e)
            {
                LOGGER.error("Could not parse commit log segmentId name={}", filename, e);
                return null;
            }
        }
        LOGGER.error("Could not parse commit log filename name={}", filename);
        return null; // cannot extract segment id
    }

    private CompletableFuture<List<CdcUpdate>> openInstanceAsync(@NotNull final List<CommitLog> logs,
                                                                 @Nullable final CommitLog.Marker highWaterMark,
                                                                 @NotNull final ExecutorService executorService)
    {
        // read all commit logs on instance async and combine into single future
        // if we fail to read any commit log on the instance we fail this instance
        final List<CompletableFuture<BufferingCommitLogReader.Result>> futures = logs.stream()
                                                                                     .map(log -> openReaderAsync(log, highWaterMark, executorService))
                                                                                     .collect(Collectors.toList());
        return FutureUtils.combine(futures)
                          .thenApply(result -> {
                              // update highwater mark on success
                              // if instance fails we don't update highwater mark so resume
                              // from original position on next attempt
                              result.stream().map(BufferingCommitLogReader.Result::marker)
                                    .max(CommitLog.Marker::compareTo)
                                    .ifPresent(watermarker::updateHighWaterMark);

                              // combine all updates into single list
                              return result.stream()
                                           .map(BufferingCommitLogReader.Result::updates)
                                           .flatMap(Collection::stream)
                                           .collect(Collectors.toList());
                          });
    }

    private CompletableFuture<BufferingCommitLogReader.Result> openReaderAsync(@NotNull final CommitLog log,
                                                                               @Nullable final CommitLog.Marker highWaterMark,
                                                                               @NotNull final ExecutorService executorService)
    {
        if (skipCommitLog(log, highWaterMark))
        {
            return NO_OP_FUTURE;
        }
        return CompletableFuture.supplyAsync(() -> openReader(log, highWaterMark), executorService);
    }

    @Nullable
    private BufferingCommitLogReader.Result openReader(@NotNull final CommitLog log,
                                                       @Nullable final CommitLog.Marker highWaterMark)
    {
        final long startTimeNanos = System.nanoTime();
        LOGGER.info("Opening BufferingCommitLogReader instance={} log={} high='{}' partitionId={}",
                    log.instance().nodeName(), log.name(), highWaterMark, partitionId);
        try (final BufferingCommitLogReader reader = new BufferingCommitLogReader(table, offsetFilter, log, sparkRangeFilter, highWaterMark, partitionId, stats))
        {
            if (reader.isReadable())
            {
                return reader.result();
            }
        }
        finally
        {
            final long commitLogReadTime = System.nanoTime() - startTimeNanos;
            LOGGER.info("Finished reading log on instance instance={} log={} partitionId={} timeNanos={}",
                        log.instance().nodeName(), log.name(), partitionId, commitLogReadTime);
            stats.commitLogReadTime(commitLogReadTime);
        }
        return null;
    }

    public IStreamScanner build()
    {
        // block on futures to read all CommitLog mutations and pass over to SortedStreamScanner
        final List<CdcUpdate> updates = futures.values()
                                               .stream()
                                               .map(future -> FutureUtils.await(future, (throwable -> LOGGER.warn("Failed to read instance with error", throwable))))
                                               .filter(FutureUtils.FutureResult::isSuccess)
                                               .map(FutureUtils.FutureResult::value)
                                               .filter(Objects::nonNull)
                                               .flatMap(Collection::stream)
                                               .collect(Collectors.toList());
        futures.clear();

        schedulePersist();

        stats.mutationsReadPerBatch(updates.size());

        final Collection<CdcUpdate> filtered = filterValidUpdates(updates);

        final long timeTakenToReadBatch = System.nanoTime() - startTimeNanos;
        LOGGER.info("Opened CdcScanner start={} maxAgeMicros={} partitionId={} timeNanos={}",
                    offsetFilter != null ? offsetFilter.start().getTimestampMicros() : null,
                    offsetFilter != null ? offsetFilter.maxAgeMicros() : null,
                    partitionId, timeTakenToReadBatch
        );

        stats.mutationsBatchReadTime(timeTakenToReadBatch);

        final long currentTimeMillis = System.currentTimeMillis();
        filtered
        .forEach(u -> stats.mutationReceivedLatency(currentTimeMillis -
                                                    TimeUnit.MICROSECONDS.toMillis(u.maxTimestampMicros())));

        return new SortedStreamScanner(table, partitioner, filtered, timeProvider);
    }

    /**
     * A stream scanner that is backed by a sorted collection of {@link CdcUpdate}.
     */
    private static class SortedStreamScanner extends AbstractStreamScanner
    {
        private final Queue<CdcUpdate> updates;

        SortedStreamScanner(@NotNull TableMetadata metadata,
                            @NotNull Partitioner partitionerType,
                            @NotNull Collection<CdcUpdate> updates,
                            @NotNull TimeProvider timeProvider)
        {
            super(metadata, partitionerType, timeProvider);
            this.updates = new PriorityQueue<>(CdcUpdate::compareTo);
            this.updates.addAll(updates);
        }

        @Override
        UnfilteredPartitionIterator initializePartitions()
        {
            return new UnfilteredPartitionIterator()
            {
                private CdcUpdate next;

                @Override
                public TableMetadata metadata()
                {
                    return metadata;
                }

                @Override
                public void close()
                {
                    // do nothing
                }

                @Override
                public boolean hasNext()
                {
                    if (next == null)
                    {
                        next = updates.poll();
                    }
                    return next != null;
                }

                @Override
                public UnfilteredRowIterator next()
                {
                    PartitionUpdate update = next.partitionUpdate();
                    next = null;
                    return update.unfilteredIterator();
                }
            };
        }

        @Override
        public void close()
        {
            updates.clear();
        }

        @Override
        protected void handleRowTombstone(Row row)
        {
            // prepare clustering data to be consumed the next
            columnData = new ClusteringColumnDataState(row.clustering());
            rid.setTimestamp(row.deletion().time().markedForDeleteAt());
            rid.setRowDeletion(true); // flag was reset at org.apache.cassandra.spark.sparksql.SparkCellIterator.getNext
        }

        @Override
        protected void handlePartitionTombstone(UnfilteredRowIterator partition)
        {
            rid.setPartitionKeyCopy(partition.partitionKey().getKey(),
                                    FourZeroUtils.tokenToBigInteger(partition.partitionKey().getToken()));
            rid.setTimestamp(partition.partitionLevelDeletion().markedForDeleteAt());
            rid.setPartitionDeletion(true); // flag was reset at org.apache.cassandra.spark.sparksql.SparkCellIterator.getNext
        }

        @Override
        protected void handleCellTombstone()
        {
            rid.setValueCopy(null);
        }

        @Override
        protected void handleCellTombstoneInComplex(Cell<?> cell)
        {
            if (cell.column().type instanceof ListType)
            {
                LOGGER.warn("Unable to process element deletions inside a List type. Skipping...");
                return;
            }

            CellPath path = cell.path();
            if (path.size() > 0) // size can either be 0 (EmptyCellPath) or 1 (SingleItemCellPath).
            {
                rid.addCellTombstoneInComplex(path.get(0));
            }
        }
    }

    private void schedulePersist()
    {
        // add task listener to persist Watermark on task success
        TaskContext.get().addTaskCompletionListener(context -> {
            if (context.isCompleted() && context.fetchFailed().isEmpty())
            {
                LOGGER.info("Persisting Watermark on task completion partitionId={}", partitionId);
                watermarker.persist(offsetFilter == null ? null : offsetFilter.maxAgeMicros()); // once we have read all commit logs we can persist the watermark state
            }
            else
            {
                LOGGER.warn("Not persisting Watermark due to task failure partitionId={}", partitionId, context.fetchFailed().get());
            }
        });
    }

    /**
     * Get rid of invalid updates from the updates
     *
     * @param updates, a collection of CdcUpdates
     * @return a new updates without invalid updates
     */
    private Collection<CdcUpdate> filterValidUpdates(Collection<CdcUpdate> updates)
    {
        // Only filter if it demands more than 1 replicas to compact
        if (minimumReplicasPerMutation == 1 || updates.isEmpty())
        {
            return updates;
        }

        final Map<CdcUpdate, List<CdcUpdate>> replicaCopies = updates.stream()
                                                                     .collect(Collectors.groupingBy(i -> i, Collectors.toList()));

        return replicaCopies.values()
                            .stream()
                            // discard PartitionUpdate w/o enough replicas
                            .filter(this::filter)
                            .map(u -> u.get(0)) // Dedup the valid updates to just 1 copy
                            .collect(Collectors.toList());
    }

    private boolean filter(List<CdcUpdate> updates)
    {
        return filter(updates, minimumReplicasPerMutation, watermarker, stats);
    }

    static boolean filter(List<CdcUpdate> updates,
                          int minimumReplicasPerMutation,
                          Watermarker watermarker,
                          Stats stats)
    {
        if (updates.isEmpty())
        {
            throw new IllegalStateException("Should not received empty list of updates");
        }

        final CdcUpdate update = updates.get(0);
        final PartitionUpdate partitionUpdate = update.partitionUpdate();
        final int numReplicas = updates.size() + watermarker.replicaCount(update);

        if (numReplicas < minimumReplicasPerMutation)
        {
            // insufficient replica copies to publish
            // so record replica count and handle on subsequent round
            LOGGER.warn("Ignore the partition update (partition key: {}) for this batch due to insufficient replicas received.", partitionUpdate);
            watermarker.recordReplicaCount(update, numReplicas);
            stats.insufficientReplicas(partitionUpdate, updates.size(), minimumReplicasPerMutation);
            return false;
        }

        // sufficient replica copies to publish

        if (updates.stream().anyMatch(watermarker::seenBefore))
        {
            // mutation previously marked as late
            // now we have sufficient replica copies to publish
            // so clear watermark and publish now
            LOGGER.info("Achieved consistency level for late partition update (partition key: {}).", partitionUpdate);
            watermarker.untrackReplicaCount(update);
            stats.lateMutationPublished(partitionUpdate);
            return true;
        }

        // we haven't seen this mutation before and achieved CL, so publish
        stats.publishedMutation(partitionUpdate);
        return true;
    }

    public static class CDCScanner implements ISSTableScanner
    {
        final TableMetadata tableMetadata;
        final PartitionUpdate update;
        UnfilteredRowIterator it;

        public CDCScanner(TableMetadata tableMetadata, PartitionUpdate update)
        {
            this.tableMetadata = tableMetadata;
            this.update = update;
            this.it = update.unfilteredIterator();
        }

        public long getLengthInBytes()
        {
            return 0;
        }

        public long getCompressedLengthInBytes()
        {
            return 0;
        }

        public long getCurrentPosition()
        {
            return 0;
        }

        public long getBytesScanned()
        {
            return 0;
        }

        public Set<SSTableReader> getBackingSSTables()
        {
            return Collections.emptySet();
        }

        public TableMetadata metadata()
        {
            return tableMetadata;
        }

        public void close()
        {

        }

        public boolean hasNext()
        {
            return it != null;
        }

        public UnfilteredRowIterator next()
        {
            final UnfilteredRowIterator result = it;
            this.it = null;
            return result;
        }
    }
}
