package org.apache.cassandra.spark.reader.fourzero;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.cdc.AbstractCdcEvent;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.ICassandraSource;
import org.apache.cassandra.spark.cdc.Marker;
import org.apache.cassandra.spark.cdc.RangeTombstone;
import org.apache.cassandra.spark.cdc.ValueWithMetadata;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.BufferingCommitLogReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.stats.ICdcStats;
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
    final Map<CassandraInstance, Queue<CompletableFuture<List<PartitionUpdateWrapper>>>> futures;
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
    private final ExecutorService executorService;
    private final boolean readCommitLogHeader;
    private final int cdcSubMicroBatchSize;
    private long mutationsPerMicroBatch = 0;

    public CdcScannerBuilder(final int partitionId,
                             final Partitioner partitioner,
                             final ICdcStats stats,
                             @Nullable final RangeFilter rangeFilter,
                             @NotNull final CdcOffsetFilter offsetFilter,
                             final Function<String, Integer> minimumReplicasFunc,
                             @NotNull final Watermarker jobWatermarker,
                             @NotNull final String jobId,
                             @NotNull final ExecutorService executorService,
                             boolean readCommitLogHeader,
                             @NotNull final Map<CassandraInstance, List<CommitLog>> logs,
                             final int cdcSubMicroBatchSize,
                             final ICassandraSource cassandraSource)
    {
        this.partitioner = partitioner;
        this.stats = stats;
        this.rangeFilter = rangeFilter;
        this.offsetFilter = offsetFilter;
        this.watermarker = jobWatermarker.instance(jobId);
        this.executorService = executorService;
        this.readCommitLogHeader = readCommitLogHeader;
        this.minimumReplicasFunc = minimumReplicasFunc;
        this.startTimeNanos = System.nanoTime();
        this.cdcSubMicroBatchSize = cdcSubMicroBatchSize;
        this.cassandraSource = cassandraSource;

        final Map<CassandraInstance, Marker> markers = logs.keySet().stream()
                                                           .map(offsetFilter::startMarker)
                                                           .filter(Objects::nonNull)
                                                           .collect(Collectors.toMap(Marker::instance, Function.identity()));

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
                                    e -> openInstanceAsync(e.getValue(), markers.get(e.getKey()), executorService))
                           );
    }

    private boolean skipCommitLog(@NotNull final CommitLog log,
                                  @Nullable final Marker highwaterMark)
    {
        if (highwaterMark == null)
        {
            return false;
        }

        final Long segmentId = CommitLog.extractVersionAndSegmentId(log).map(Pair::getRight).orElse(null);

        // only read CommitLog if greater than or equal to previously read CommitLog segmentId
        if (segmentId != null && segmentId >= highwaterMark.segmentId())
        {
            return false;
        }

        stats.skippedCommitLogsCount(1);
        return true;
    }

    private Queue<CompletableFuture<List<PartitionUpdateWrapper>>> openInstanceAsync(@NotNull final List<CommitLog> logs,
                                                                                     @Nullable final Marker highWaterMark,
                                                                                     @NotNull final ExecutorService executorService)
    {
        return logs.stream()
                   .sorted(Comparator.comparingLong(CommitLog::segmentId))
                   .map(log -> openReaderAsync(log, highWaterMark, executorService).thenApply(result ->
                                                                                              result == null ?
                                                                                              null : result.updates()))
                   .filter(Objects::nonNull)
                   .collect(Collectors.toCollection(ArrayDeque::new));
    }

    private CompletableFuture<BufferingCommitLogReader.Result> openReaderAsync(@NotNull final CommitLog log,
                                                                               @Nullable final Marker highWaterMark,
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
                                                       @Nullable final Marker highWaterMark)
    {
        LOGGER.info("Opening BufferingCommitLogReader instance={} log={} high={} partitionId={}",
                    log.instance().nodeName(), log.name(), highWaterMark, partitionId);
        return reportTimeTaken(() -> {
            try (final BufferingCommitLogReader reader = new BufferingCommitLogReader(offsetFilter, log,
                                                                                      rangeFilter, highWaterMark,
                                                                                      partitionId, stats, executorService,
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

    private List<PartitionUpdateWrapper> getSubBatchOfInstance(Queue<CompletableFuture<List<PartitionUpdateWrapper>>> instanceFutures)
    {
        List<CompletableFuture<List<PartitionUpdateWrapper>>> subBatchFutures = new ArrayList<>();

        for (int i = 0; i < cdcSubMicroBatchSize && !instanceFutures.isEmpty(); i++)
        {
            subBatchFutures.add(instanceFutures.remove());
        }

        return subBatchFutures
               .stream()
               .map(future -> FutureUtils.await(future, (throwable -> LOGGER.warn("Failed to read instance with error", throwable))))
               .filter(FutureUtils.FutureResult::isSuccess)
               .map(FutureUtils.FutureResult::value)
               .filter(Objects::nonNull)
               .flatMap(Collection::stream)
               .collect(Collectors.toList());
    }

    private boolean allDone()
    {
        return futures.isEmpty();
    }

    private Collection<PartitionUpdateWrapper> processSubBatch()
    {
        List<PartitionUpdateWrapper> subBatchUpdates = new ArrayList<>();

        if (allDone())
        {
            return subBatchUpdates;
        }

        Set<CassandraInstance> toRemove = new HashSet<>();
        // process sub batch of each instance
        futures.forEach((instance, queue) -> {
            subBatchUpdates.addAll(getSubBatchOfInstance(queue));
            if (queue.isEmpty())
            {
                toRemove.add(instance); // done with an instance, mark it to be removed
            }
        });
        toRemove.forEach(futures::remove);

        mutationsPerMicroBatch += subBatchUpdates.size();

        stats.subBatchesPerMicroBatchCount(1);
        stats.mutationsReadPerSubMicroBatch(subBatchUpdates.size());

        final Collection<PartitionUpdateWrapper> filteredUpdates = reportTimeTaken(() -> filterValidUpdates((subBatchUpdates)),
                                                                                   stats::mutationsFilterTime);

        LOGGER.info("Collected valid partition updates for publishing. updates={}", filteredUpdates.size());

        final long now = System.currentTimeMillis();
        for (PartitionUpdateWrapper pu : filteredUpdates)
        {
            stats.changeReceived(pu.keyspace, pu.table,
                                 now - TimeUnit.MICROSECONDS.toMillis(pu.maxTimestampMicros()));
        }

        if (futures.isEmpty())
        {
            // We are just done processing all futures, report batch level stats
            processBatchComplete();
        }

        return new ArrayList<>(filteredUpdates);
    }

    private void processBatchComplete()
    {
        schedulePersist();

        stats.mutationsReadPerBatch(mutationsPerMicroBatch);

        final long timeTakenToReadBatch = System.nanoTime() - startTimeNanos;
        LOGGER.info("Processed CdcScanner start={} maxAgeMicros={} partitionId={} timeNanos={}",
                    offsetFilter.getStartTimestampMicros(),
                    offsetFilter.maxAgeMicros(),
                    partitionId, timeTakenToReadBatch
        );
        stats.mutationsBatchReadTime(timeTakenToReadBatch);
    }

    public IStreamScanner<EventType> build()
    {
        // Wrapper to generate a CdcSortedStreamScanner for each sub batch
        return new IStreamScanner<EventType>()
        {
            private ScannerType cdcSortedStreamScanner = null;

            public void close()
            {
                if (cdcSortedStreamScanner != null)
                {
                    if (!futures.isEmpty())
                    {
                        futures.forEach((instance, instanceFutures) ->
                                        instanceFutures.forEach(fut -> fut.cancel(false)));
                    }
                    cdcSortedStreamScanner.close();
                }
            }

            public EventType data()
            {
                Preconditions.checkNotNull(cdcSortedStreamScanner);
                return cdcSortedStreamScanner.data();
            }

            public boolean next()
            {
                if (cdcSortedStreamScanner != null)
                {
                    if (cdcSortedStreamScanner.next())
                    {
                        return true;
                    }
                    // done with updates in this sub batch
                    cdcSortedStreamScanner = null;
                }

                // There can be empty commit log files, process more files if one sub batch is empty
                // Return false only when all instances are done, otherwise closes the scanner
                while (cdcSortedStreamScanner == null && !allDone())
                {
                    Collection<PartitionUpdateWrapper> updatesOfNextSubBatch = processSubBatch();
                    if (!updatesOfNextSubBatch.isEmpty())
                    {
                        cdcSortedStreamScanner = buildStreamScanner(updatesOfNextSubBatch);
                        return cdcSortedStreamScanner.next();
                    }
                }

                return false;
            }

            public void advanceToNextColumn()
            {
                if (cdcSortedStreamScanner != null)
                {
                    cdcSortedStreamScanner.advanceToNextColumn();
                }
            }
        };
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