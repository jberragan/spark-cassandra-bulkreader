package org.apache.cassandra.spark.reader.fourzero;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.cdc.AbstractCdcEvent;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.ICassandraSource;
import org.apache.cassandra.spark.cdc.ICommitLogMarkers;
import org.apache.cassandra.spark.cdc.Marker;
import org.apache.cassandra.spark.cdc.RangeTombstone;
import org.apache.cassandra.spark.cdc.ValueWithMetadata;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.BufferingCommitLogReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.stats.ICdcStats;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.utils.StatsUtil.reportTimeTaken;

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

public abstract class CdcScannerBuilder<ValueType extends ValueWithMetadata,
                                       TombstoneType extends RangeTombstone<ValueType>,
                                       EventType extends AbstractCdcEvent<ValueType, TombstoneType>,
                                       ScannerType extends CdcSortedStreamScanner<ValueType, TombstoneType, EventType>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcScannerBuilder.class);

    private static final CompletableFuture<BufferingCommitLogReader.Result> NO_OP_FUTURE = CompletableFuture.completedFuture(null);
    protected final ICassandraSource cassandraSource;

    final Partitioner partitioner;
    final ICdcStats stats;
    final Map<CassandraInstance, CompletableFuture<List<PartitionUpdateWrapper>>> futures;
    final Function<String, Integer> minimumReplicasFunc;
    @Nullable
    private final RangeFilter rangeFilter;
    @NotNull
    protected final CdcOffsetFilter offsetFilter;
    @NotNull
    final Watermarker watermarker;
    protected final int partitionId;
    private final long startTimeNanos;
    @NotNull
    private final AsyncExecutor executor;
    private final boolean readCommitLogHeader;

    public CdcScannerBuilder(final int partitionId,
                             final Partitioner partitioner,
                             final ICdcStats stats,
                             @Nullable final RangeFilter rangeFilter,
                             @NotNull final CdcOffsetFilter offsetFilter,
                             final Function<String, Integer> minimumReplicasFunc,
                             @NotNull final Watermarker jobWatermarker,
                             @NotNull final String jobId,
                             @NotNull final AsyncExecutor executor,
                             boolean readCommitLogHeader,
                             @NotNull final Map<CassandraInstance, List<CommitLog>> logs,
                             final ICassandraSource cassandraSource)
    {
        this.partitioner = partitioner;
        this.stats = stats;
        this.rangeFilter = rangeFilter;
        this.offsetFilter = offsetFilter;
        this.watermarker = jobWatermarker.instance(jobId);
        this.executor = executor;
        this.readCommitLogHeader = readCommitLogHeader;
        this.minimumReplicasFunc = minimumReplicasFunc;
        this.startTimeNanos = System.nanoTime();
        this.cassandraSource = cassandraSource;

        this.partitionId = partitionId;
        LOGGER.info("Opening CdcScanner numInstances={} start={} maxAgeMicros={} partitionId={} listLogsTimeNanos={}",
                    logs.size(),
                    offsetFilter.getStartTimestampMicros(),
                    offsetFilter.maxAgeMicros(),
                    partitionId, System.nanoTime() - startTimeNanos
        );

        this.futures = logs.entrySet().stream()
                           .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> openInstanceAsync(e.getValue(), offsetFilter.markers(), executor))
                           );
    }

    private boolean skipCommitLog(@NotNull final CommitLog log,
                                  @NotNull final ICommitLogMarkers markers)
    {
        final Marker startMarker = markers.startMarker(log);
        final Long segmentId = CommitLog.extractVersionAndSegmentId(log).map(Pair::getRight).orElse(null);

        // only read CommitLog if greater than or equal to previously read CommitLog segmentId
        if (segmentId != null && segmentId >= startMarker.segmentId())
        {
            return false;
        }

        stats.skippedCommitLogsCount(1);
        return true;
    }

    private CompletableFuture<List<PartitionUpdateWrapper>> openInstanceAsync(@NotNull final List<CommitLog> logs,
                                                                              @NotNull final ICommitLogMarkers markers,
                                                                              @NotNull final AsyncExecutor executor)
    {
        // read all commit logs on instance async and combine into single future
        // if we fail to read any commit log on the instance we fail this instance
        final List<CompletableFuture<BufferingCommitLogReader.Result>> futures = logs.stream()
                                                                                     .sorted(Comparator.comparingLong(CommitLog::segmentId))
                                                                                     .map(log -> openReaderAsync(log, markers, executor))
                                                                                     .collect(Collectors.toList());
        return FutureUtils.combine(futures)
                          .thenApply(result -> {
                              // combine all updates into single list
                              return result.stream()
                                           .map(BufferingCommitLogReader.Result::updates)
                                           .flatMap(Collection::stream)
                                           .collect(Collectors.toList());
                          });
    }

    private CompletableFuture<BufferingCommitLogReader.Result> openReaderAsync(@NotNull final CommitLog log,
                                                                               @NotNull final ICommitLogMarkers markers,
                                                                               @NotNull final AsyncExecutor executor)
    {
        if (skipCommitLog(log, markers))
        {
            return NO_OP_FUTURE;
        }
        return executor.submit(() -> openReader(log, markers));
    }

    @Nullable
    private BufferingCommitLogReader.Result openReader(@NotNull final CommitLog log,
                                                       @NotNull final ICommitLogMarkers markers)
    {
        LOGGER.info("Opening BufferingCommitLogReader instance={} log={} high={} partitionId={}",
                    log.instance().nodeName(), log.name(), markers.startMarker(log), partitionId);
        return reportTimeTaken(() -> {
            try (final BufferingCommitLogReader reader = new BufferingCommitLogReader(offsetFilter, log,
                                                                                      rangeFilter, markers,
                                                                                      partitionId, stats, executor,
                                                                                      readCommitLogHeader))
            {
                if (reader.isReadable())
                {
                    BufferingCommitLogReader.Result result = reader.result();
                    LOGGER.info("Read updates from log instance={} log={} partitionId={} updates={} high={}",
                                log.instance().nodeName(), log.name(), partitionId, result.updates().size(), result.marker());
                    return result;
                }
            }
            return null;
        }, commitLogReadTime -> {
            LOGGER.info("Finished reading log on instance instance={} log={} partitionId={} timeNanos={}",
                        log.instance().nodeName(), log.name(), partitionId, commitLogReadTime);
            stats.commitLogReadTime(commitLogReadTime);
            stats.commitLogBytesFetched(log.len());
        });
    }

    public ScannerType build()
    {
        // block on futures to read all CommitLog mutations and pass over to SortedStreamScanner
        final List<PartitionUpdateWrapper> updates =
        futures.values()
               .stream()
               .map(future -> FutureUtils.await(future, throwable -> LOGGER.warn("Failed to read instance with error", throwable)))
               .filter(FutureUtils.FutureResult::isSuccess)
               .map(FutureUtils.FutureResult::value)
               .filter(Objects::nonNull)
               .flatMap(Collection::stream)
               .collect(Collectors.toList());
        futures.clear();

        schedulePersist();

        stats.mutationsReadPerBatch(updates.size());

        final long timeTakenToReadBatch = System.nanoTime() - startTimeNanos;
        LOGGER.info("Processed CdcScanner start={} maxAgeMicros={} partitionId={} timeNanos={}",
                    offsetFilter.getStartTimestampMicros(),
                    offsetFilter.maxAgeMicros(),
                    partitionId, timeTakenToReadBatch
        );
        stats.mutationsBatchReadTime(timeTakenToReadBatch);

        final Collection<PartitionUpdateWrapper> filteredUpdates = reportTimeTaken(() -> filterValidUpdates((updates)),
                                                                                   stats::mutationsFilterTime);

        final long now = System.currentTimeMillis();
        filteredUpdates.forEach(u -> stats.changeReceived(u.keyspace, u.table,
                                                          now - TimeUnit.MICROSECONDS.toMillis(u.maxTimestampMicros())));

        return buildStreamScanner(filteredUpdates);
    }

    public void schedulePersist()
    {

    }

    public abstract ScannerType buildStreamScanner(Collection<PartitionUpdateWrapper> updates);

    /**
     * Get rid of invalid updates from the updates
     *
     * @param updates, a collection of CdcUpdates
     * @return a new updates without invalid updates
     */
    private Collection<PartitionUpdateWrapper> filterValidUpdates(Collection<PartitionUpdateWrapper> updates)
    {
        if (updates.isEmpty())
        {
            return updates;
        }

        final Map<PartitionUpdateWrapper, List<PartitionUpdateWrapper>> replicaCopies = updates.stream()
                                                                                               .collect(Collectors.groupingBy(i -> i, Collectors.toList()));

        return replicaCopies.values()
                            .stream()
                            // discard PartitionUpdate w/o enough replicas
                            .filter(this::filter)
                            .map(u -> u.get(0)) // Dedup the valid updates to just 1 copy
                            .collect(Collectors.toList());
    }

    private boolean filter(List<PartitionUpdateWrapper> updates)
    {
        return filter(updates, minimumReplicasFunc, watermarker, stats);
    }

    static boolean filter(List<PartitionUpdateWrapper> updates,
                          Function<String, Integer> minimumReplicasFunc,
                          Watermarker watermarker,
                          ICdcStats stats)
    {
        if (updates.isEmpty())
        {
            throw new IllegalStateException("Should not receive empty list of updates");
        }

        final PartitionUpdateWrapper update = updates.get(0);
        final int numReplicas = updates.size() + watermarker.replicaCount(update);
        final int minimumReplicasPerMutation = minimumReplicasFunc.apply(update.keyspace);

        if (numReplicas < minimumReplicasPerMutation)
        {
            // insufficient replica copies to publish
            // so record replica count and handle on subsequent round
            LOGGER.warn("Ignore the partition update due to insufficient replicas received. required={} received={} keyspace={} table={}",
                        minimumReplicasPerMutation, numReplicas, update.keyspace, update.table);
            watermarker.recordReplicaCount(update, numReplicas);
            stats.insufficientReplicas(update.keyspace, update.table);
            return false;
        }

        // sufficient replica copies to publish

        if (updates.stream().anyMatch(watermarker::seenBefore))
        {
            // mutation previously marked as late
            // now we have sufficient replica copies to publish
            // so clear watermark and publish now
            LOGGER.info("Achieved consistency level for late partition update. required={} received={} keyspace={} table={}",
                        minimumReplicasPerMutation, numReplicas, update.keyspace, update.table);
            watermarker.untrackReplicaCount(update);
            stats.lateChangePublished(update.keyspace, update.table);
            return true;
        }

        // we haven't seen this mutation before and achieved CL, so publish
        stats.changePublished(update.keyspace, update.table);
        return true;
    }
}
