package org.apache.cassandra.spark.reader.fourzero;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
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
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.BufferingCommitLogReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.apache.spark.TaskContext;
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

public class CdcScannerBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcScannerBuilder.class);

    private static final CompletableFuture<BufferingCommitLogReader.Result> NO_OP_FUTURE = CompletableFuture.completedFuture(null);

    final Partitioner partitioner;
    final Stats stats;
    final Map<CassandraInstance, Queue<CompletableFuture<List<PartitionUpdateWrapper>>>> futures;
    final int minimumReplicasPerMutation;
    @Nullable
    private final SparkRangeFilter sparkRangeFilter;
    @NotNull
    private final CdcOffsetFilter offsetFilter;
    @NotNull
    final Watermarker watermarker;
    private final int partitionId;
    private final long startTimeNanos;
    @NotNull
    private final ExecutorService executorService;
    private final boolean readCommitLogHeader;
    private final int cdcSubMicroBatchSize;
    private long mutationsPerMicroBatch = 0;

    public CdcScannerBuilder(final Partitioner partitioner,
                             final Stats stats,
                             @Nullable final SparkRangeFilter sparkRangeFilter,
                             @NotNull final CdcOffsetFilter offsetFilter,
                             final int minimumReplicasPerMutation,
                             @NotNull final Watermarker jobWatermarker,
                             @NotNull final String jobId,
                             @NotNull final ExecutorService executorService,
                             boolean readCommitLogHeader,
                             @NotNull final Map<CassandraInstance, List<CommitLog>> logs,
                             final int cdcSubMicroBatchSize)
    {
        this.partitioner = partitioner;
        this.stats = stats;
        this.sparkRangeFilter = sparkRangeFilter;
        this.offsetFilter = offsetFilter;
        this.watermarker = jobWatermarker.instance(jobId);
        this.executorService = executorService;
        this.readCommitLogHeader = readCommitLogHeader;
        Preconditions.checkArgument(minimumReplicasPerMutation >= 1,
                                    "minimumReplicasPerMutation should be at least 1");
        this.minimumReplicasPerMutation = minimumReplicasPerMutation;
        this.startTimeNanos = System.nanoTime();
        this.cdcSubMicroBatchSize = cdcSubMicroBatchSize;

        final Map<CassandraInstance, CommitLog.Marker> markers = logs.keySet().stream()
                                                                     .map(offsetFilter::startMarker)
                                                                     .filter(Objects::nonNull)
                                                                     .collect(Collectors.toMap(CommitLog.Marker::instance, Function.identity()));

        this.partitionId = TaskContext.getPartitionId();
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
                                  @Nullable final CommitLog.Marker highwaterMark)
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
                                                                                     @Nullable final CommitLog.Marker highWaterMark,
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
        LOGGER.info("Opening BufferingCommitLogReader instance={} log={} high='{}' partitionId={}",
                    log.instance().nodeName(), log.name(), highWaterMark, partitionId);
        return reportTimeTaken(() -> {
            try (final BufferingCommitLogReader reader = new BufferingCommitLogReader(offsetFilter, log,
                                                                                      sparkRangeFilter, highWaterMark,
                                                                                      partitionId, stats, executorService,
                                                                                      readCommitLogHeader))
            {
                if (reader.isReadable())
                {
                    return reader.result();
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

        // process sub batch of each instance
        for (Map.Entry<CassandraInstance, Queue<CompletableFuture<List<PartitionUpdateWrapper>>>> entry : futures.entrySet())
        {
            subBatchUpdates.addAll(getSubBatchOfInstance(entry.getValue()));

            if (entry.getValue().isEmpty())
            {
                futures.remove(entry.getKey()); // done with an instance
            }
        }

        mutationsPerMicroBatch += subBatchUpdates.size();

        stats.subBatchesPerMicroBatchCount(1);
        stats.mutationsReadPerSubMicroBatch(subBatchUpdates.size());

        final Collection<PartitionUpdateWrapper> filteredUpdates = reportTimeTaken(() -> filterValidUpdates((subBatchUpdates)),
                                                                                   stats::mutationsFilterTime);

        final long currentTimeMillis = System.currentTimeMillis();
        filteredUpdates
        .forEach(u -> stats.mutationReceivedLatency(currentTimeMillis -
                                                    TimeUnit.MICROSECONDS.toMillis(u.maxTimestampMicros())));

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

    public IStreamScanner<AbstractCdcEvent> build()
    {
        // Wrapper to generate a CdcSortedStreamScanner for each sub batch
        return new IStreamScanner<AbstractCdcEvent>()
        {
            private CdcSortedStreamScanner cdcSortedStreamScanner = null;

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

            public AbstractCdcEvent data()
            {
                Preconditions.checkArgument(cdcSortedStreamScanner != null);
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
                        cdcSortedStreamScanner = new CdcSortedStreamScanner(updatesOfNextSubBatch);
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

    private void schedulePersist()
    {
        // add task listener to persist Watermark on task success
        TaskContext.get().addTaskCompletionListener(context -> {
            if (context.isCompleted() && context.fetchFailed().isEmpty())
            {
                LOGGER.info("Persisting Watermark on task completion partitionId={}", partitionId);
                watermarker.persist(offsetFilter.maxAgeMicros()); // once we have read all commit logs we can persist the watermark state
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
    private Collection<PartitionUpdateWrapper> filterValidUpdates(Collection<PartitionUpdateWrapper> updates)
    {
        // Only filter if it demands more than 1 replicas to compact
        if (minimumReplicasPerMutation == 1 || updates.isEmpty())
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
        return filter(updates, minimumReplicasPerMutation, watermarker, stats);
    }

    static boolean filter(List<PartitionUpdateWrapper> updates,
                          int minimumReplicasPerMutation,
                          Watermarker watermarker,
                          Stats stats)
    {
        if (updates.isEmpty())
        {
            throw new IllegalStateException("Should not received empty list of updates");
        }

        final PartitionUpdateWrapper update = updates.get(0);
        final PartitionUpdate partitionUpdate = update.partitionUpdate();
        final int numReplicas = updates.size() + watermarker.replicaCount(update);

        if (numReplicas < minimumReplicasPerMutation)
        {
            // insufficient replica copies to publish
            // so record replica count and handle on subsequent round
            LOGGER.warn("Ignore the partition update (partition key: '{}') for this batch due to insufficient replicas received. {} required {} received.", partitionUpdate == null ? "null" : partitionUpdate.partitionKey(), minimumReplicasPerMutation, numReplicas);
            watermarker.recordReplicaCount(update, numReplicas);
            stats.insufficientReplicas(updates.size(), minimumReplicasPerMutation);
            return false;
        }

        // sufficient replica copies to publish

        if (updates.stream().anyMatch(watermarker::seenBefore))
        {
            // mutation previously marked as late
            // now we have sufficient replica copies to publish
            // so clear watermark and publish now
            LOGGER.info("Achieved consistency level for late partition update (partition key: '{}'). {} received.", partitionUpdate == null ? "null" : partitionUpdate.partitionKey(), numReplicas);
            watermarker.untrackReplicaCount(update);
            stats.lateMutationPublished();
            return true;
        }

        // we haven't seen this mutation before and achieved CL, so publish
        stats.publishedMutation();
        return true;
    }
}
