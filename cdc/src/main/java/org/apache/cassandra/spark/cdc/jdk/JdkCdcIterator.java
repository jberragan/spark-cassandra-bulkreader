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

package org.apache.cassandra.spark.cdc.jdk;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.CdcKryoRegister;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.ICassandraSource;
import org.apache.cassandra.spark.cdc.ICommitLogMarkers;
import org.apache.cassandra.spark.cdc.watermarker.InMemoryWatermarker;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.reader.fourzero.CompressionUtil;
import org.apache.cassandra.spark.sparksql.filters.CdcOffset;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.InstanceLogs;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.spark.utils.IOUtils;
import org.apache.cassandra.spark.utils.KryoUtils;
import org.apache.cassandra.spark.utils.TimeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Generic Iterator for streaming CDC events in Java.
 */
public abstract class JdkCdcIterator<StateType extends CdcState> implements AutoCloseable, IStreamScanner<JdkCdcEvent>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JdkCdcIterator.class);

    // live state

    @Nullable
    protected RangeFilter rangeFilter = null;
    protected JdkCdcScannerBuilder builder = null;
    protected JdkCdcScannerBuilder.JdkCdcSortedStreamScanner scanner = null;
    private JdkCdcEvent curr = null;

    // serializable state

    @NotNull
    ICommitLogMarkers markers;
    @NotNull
    protected final InMemoryWatermarker watermarker;
    @NotNull
    public final String jobId;
    protected final int partitionId;
    protected long epoch;
    protected long batchStartNanos = System.nanoTime();

    public JdkCdcIterator()
    {
        this(UUID.randomUUID().toString(), 0);
    }

    /**
     * @param jobId       unique global identifier for CDC streaming job. If jobId changes, all state is discarded. State should be persisted namespaced by jobId-partitionId.
     * @param partitionId unique identifier for this partition of the streaming job.
     */
    public JdkCdcIterator(@NotNull final String jobId,
                          final int partitionId)
    {
        this.jobId = jobId;
        this.partitionId = partitionId;
        this.epoch = 0;
        this.markers = ICommitLogMarkers.EMPTY;
        this.watermarker = newWatermarker(partitionId);
    }

    public JdkCdcIterator(@NotNull final String jobId,
                          final int partitionId,
                          final StateType state)
    {
        this.jobId = jobId;
        this.partitionId = partitionId;
        this.epoch = state.epoch;
        this.rangeFilter = state.range == null ? null : RangeFilter.create(state.range);
        this.markers = state.markers;
        this.watermarker = newWatermarker(partitionId);
        ((InMemoryWatermarker.PartitionWatermarker) this.watermarker.instance(jobId)).apply(state.serializationWrapper);
    }

    public JdkCdcIterator(@NotNull final String jobId,
                          final int partitionId,
                          final long epoch,
                          @Nullable RangeFilter rangeFilter,
                          @NotNull final ICommitLogMarkers markers,
                          @NotNull final InMemoryWatermarker watermarker)
    {
        this.jobId = jobId;
        this.partitionId = partitionId;
        this.epoch = epoch;
        this.rangeFilter = rangeFilter;
        this.markers = markers;
        this.watermarker = watermarker;
    }

    private static InMemoryWatermarker newWatermarker(int partitionId)
    {
        return new InMemoryWatermarker(new InMemoryWatermarker.TaskContextProvider()
        {
            public boolean hasTaskContext()
            {
                return true;
            }

            public int partitionId()
            {
                return partitionId;
            }
        });
    }

    public Watermarker watermarker()
    {
        return this.watermarker;
    }

    public String jobId()
    {
        return jobId;
    }

    public int partitionId()
    {
        return partitionId;
    }

    public long epoch()
    {
        return epoch;
    }

    @NotNull
    public ICommitLogMarkers markers()
    {
        return this.markers;
    }

    /* Abstract Methods that must be implemented */

    /**
     * @return a Cassandra Token Range that this iterator should read from. This method is called at the start of each micro-batch to permit topology changes between batches.
     * Returning null means Iterator will not apply the filter and attempt to read all available commit logs.
     */
    @Nullable
    public abstract RangeFilter rangeFilter();

    /**
     * Return a list of commit logs that should be read in the current micro-batch across a set of replicas.
     *
     * @param rangeFilter optional range filter that defines the token range to be read from. Method should return all replicas that overlap with the filter. A null filter indicates read from the entire cluster.
     * @return map of commit logs per Cassandra replica.
     */
    public abstract CommitLogProvider logs(@Nullable RangeFilter rangeFilter);

    /**
     * @return executor service for performing CommitLog i/o.
     */
    public abstract AsyncExecutor executor();

    /**
     * Optionally persist state between micro-batches, state should be stored namespaced by the jobId, partitionId and start/end tokens if RangeFilter is non-null.
     *
     * @param jobId       unique identifier for CDC streaming job.
     * @param partitionId unique identifier for this partition of the streaming job.
     * @param rangeFilter RangeFilter that provides the start-end token range for this state.
     * @param buf         ByteBuffer with the serialized Iterator state.
     */
    public abstract void persist(String jobId,
                                 int partitionId,
                                 @Nullable RangeFilter rangeFilter,
                                 ByteBuffer buf);

    /**
     * Override to supply ICassandraSource implementation to enable CDC to lookup of unfrozen lists.
     *
     * @return ICassandraSource implementation.
     */
    public ICassandraSource cassandraSource()
    {
        return (keySpace, table, columnsToFetch, primaryKeyColumns) -> null;
    }

    /**
     * @return set of keyspaces where cdc is currently enabled
     */
    public abstract Set<String> keyspaces();

    /* Optionally overridable methods for custom configuration */

    /**
     * Add optional sleep between micro-batches.
     *
     * @return duration
     */
    public Duration minDelayBetweenMicroBatches()
    {
        return Duration.ofMillis(250);
    }

    /**
     * Add optional sleep when insufficient replicas available to prevent spinning between list commit log calls.
     *
     * @return duration
     */
    public Duration sleepWhenInsufficientReplicas()
    {
        return Duration.ofSeconds(1);
    }

    public long maxAgeMicros()
    {
        return TimeUnit.MINUTES.toMicros(5);
    }

    /**
     * @param keyspace keyspace name
     * @return minimum number of replicas required to read from to achieve the consistency level
     */
    public int minimumReplicas(String keyspace)
    {
        return 2;
    }

    public Duration watermarkWindowDuration()
    {
        return Duration.ofSeconds(600);
    }

    /**
     * @return set maximum number of epochs or less than or equal to 0 to run indefinitely.
     */
    public int maxEpochs()
    {
        return -1;
    }

    /**
     * Persist state
     **/

    public void persist()
    {
        if (!persistState())
        {
            return;
        }

        try
        {
            final ByteBuffer buf = serializeStateToBytes();
            LOGGER.info("Persisting Iterator state between micro-batch partitionId={} epoch={} size={}", partitionId, epoch, buf.remaining());
            persist(jobId, partitionId, rangeFilter, buf);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return true if we should serialize and attempt to persist state between micro-batches.
     */
    public boolean persistState()
    {
        return true;
    }

    // internal methods

    protected void maybeNextBatch()
    {
        while (this.scanner == null)
        {
            nextBatch();
        }
    }

    protected void nextBatch()
    {
        Preconditions.checkArgument(this.scanner == null, "Scanner should be null before nextBatch called");
        this.rangeFilter = rangeFilter();
        final Map<CassandraInstance, List<CommitLog>> logs = logs(this.rangeFilter)
                                                             .logs()
                                                             .collect(Collectors.groupingBy(CommitLog::instance, Collectors.toList()));
        final Map<CassandraInstance, InstanceLogs> instanceLogs = logs.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new InstanceLogs(e.getValue())));

        // if insufficient replicas for any keyspace, then skip entirely otherwise we end up reading
        // all mutations (e.g. at RF=1) into the CDC state and storing until another replica comes back up.
        // This could cause state to grow indefinitely, it is better to not proceed and resume from CommitLog offset when enough replicas come back up.
        for (final String keyspace : keyspaces())
        {
            final int minReplicas = minimumReplicas(keyspace);
            if (instanceLogs.size() < minReplicas)
            {
                LOGGER.warn("Insufficient replicas available keyspace={} requiredReplicas={} availableReplicas={}", keyspace, minReplicas, instanceLogs.size());
                sleep(sleepWhenInsufficientReplicas());
                return;
            }
        }

        final CdcOffset end = new CdcOffset(TimeUtils.nowMicros() - maxAgeMicros(), instanceLogs);
        final CdcOffsetFilter offsetFilter = new CdcOffsetFilter(markers, end.allLogs(), end.getTimestampMicros(), watermarkWindowDuration());
        final ICassandraSource cassandraSource = cassandraSource();
        this.builder = new JdkCdcScannerBuilder(this.rangeFilter, offsetFilter, watermarker(), this::minimumReplicas, executor(), logs, jobId, cassandraSource);
        this.scanner = builder.build();
        this.batchStartNanos = System.nanoTime();
        this.markers = end.markers();
        this.epoch++;
    }

    protected void onFinish()
    {
        if (this.builder == null)
        {
            return;
        }

        close();

        // optionally persist Iterator state between micro-batches
        persist();

        // optionally sleep between micro-batches
        sleep(sleepMillis());
        maybeNextBatch();
    }

    protected long sleepMillis()
    {
        return minDelayBetweenMicroBatches().toMillis() - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - batchStartNanos);
    }

    private static void sleep(Duration duration)
    {
        sleep(duration.toMillis());
    }

    protected static void sleep(long sleepMillis)
    {
        if (sleepMillis <= 0)
        {
            return;
        }

        try
        {
            TimeUnit.MILLISECONDS.sleep(sleepMillis);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * CDC runs continuously unless interrupted or shutdown.
     *
     * @return if CDC is still running.
     */
    public boolean isRunning()
    {
        return true;
    }

    protected boolean epochsExceeded()
    {
        final int maxEpochs = maxEpochs();
        return maxEpochs > 0 && this.epoch >= maxEpochs;
    }

    protected boolean isFinished()
    {
        return !isRunning() || epochsExceeded();
    }

    // IStreamScanner

    public boolean next()
    {
        maybeNextBatch();
        Preconditions.checkNotNull(this.scanner, "Scanner should have been initialized");
        while (this.scanner != null && !this.scanner.next())
        {
            if (isFinished())
            {
                // cdc disabled so exit
                return false;
            }
            onFinish();
        }
        return !isFinished();
    }

    public void advanceToNextColumn()
    {
        Preconditions.checkNotNull(scanner, "next() must be called before advanceToNextColumn()");
        this.curr = scanner.data();
    }

    public JdkCdcEvent data()
    {
        Preconditions.checkNotNull(curr, "advanceToNextColumn() must be called before data()");
        return curr;
    }

    public InMemoryWatermarker.SerializationWrapper serializationWrapper()
    {
        return ((InMemoryWatermarker.PartitionWatermarker) watermarker.instance(jobId)).serializationWrapper();
    }

    public abstract StateType buildState(long epoch,
                                         @Nullable RangeFilter rangeFilter,
                                         @NotNull ICommitLogMarkers markers,
                                         @NotNull InMemoryWatermarker.SerializationWrapper serializationWrapper);

    public StateType cdcState()
    {
        return buildState(epoch, rangeFilter, markers, serializationWrapper());
    }

    /**
     * Helper method to merge two iterators for a new token range, discarding state that is outside the new token range.
     *
     * @param partitionId partition id for new iterator.
     * @param range       new token range for this iterator.
     * @param it1         previous iterator.
     * @param it2         previous iterator.
     * @param <Type>      Iterator type.
     * @return new iterator that merges the state of two previous iterators.
     */
    @SuppressWarnings("unchecked")
    public static <StateType extends CdcState, Type extends JdkCdcIterator<StateType>> Type mergeIterators(int partitionId,
                                                                                                           @Nullable Range<BigInteger> range,
                                                                                                           @NotNull final Type it1,
                                                                                                           @NotNull final Type it2)
    {
        final StateType mergedState = it1.cdcState().merge(range, it2.cdcState());
        return (Type) it1.newInstance(it2, it1.jobId, partitionId, mergedState);
    }

    /**
     * Build new iterator instance using existing in-memory state, without cloning or building new InMemoryWatermarker.
     *
     * @param other       this iterator.
     * @param jobId       job id
     * @param partitionId partition id
     * @param epoch       new epoch
     * @param rangeFilter token range filter
     * @param markers     commit log markers
     * @param watermarker watermarker
     * @param <Type>      iterator type.
     * @return cloned iterator.
     */
    public abstract <Type extends JdkCdcIterator<StateType>> JdkCdcIterator<StateType> newInstance(Type other,
                                                                                                   String jobId,
                                                                                                   int partitionId,
                                                                                                   long epoch,
                                                                                                   @Nullable RangeFilter rangeFilter,
                                                                                                   ICommitLogMarkers markers,
                                                                                                   InMemoryWatermarker watermarker);

    /**
     * Build new iterator from previously serialized state.
     *
     * @param other       this iterator.
     * @param jobId       job id
     * @param partitionId partition id
     * @param state       cdc state
     * @param <Type>      iterator type.
     * @return cloned iterator.
     */
    public abstract <Type extends JdkCdcIterator<StateType>> JdkCdcIterator<StateType> newInstance(Type other,
                                                                                                   String jobId,
                                                                                                   int partitionId,
                                                                                                   StateType state);

    // Serialization Helpers

    public abstract CdcState.Serializer<StateType> stateSerializer();

    public static <StateType extends CdcState> StateType deserializeState(CdcState.Serializer<StateType> serializer,
                                                                          Class<StateType> tClass,
                                                                          byte[] compressed)
    {
        return CdcState.deserialize(compressed, tClass, serializer);
    }

    /**
     * @return cloned JdkCdcIterator at the next epoch.
     */
    @SuppressWarnings("unchecked")
    <Type extends JdkCdcIterator<StateType>> Type nextEpoch()
    {
        return (Type) newInstance((Type) this, this.jobId, this.partitionId, epoch + 1, rangeFilter, markers, watermarker);
    }

    public CdcConsumer<StateType> toConsumer()
    {
        throw new NotImplementedException("Cdc Consumer implementation has not been added");
    }

    public ByteBuffer serializeStateToBytes() throws IOException
    {
        try (final Output out = KryoUtils.serialize(CdcKryoRegister.kryo(), cdcState(), stateSerializer()))
        {
            return CompressionUtil.INSTANCE.compress(out.getBuffer());
        }
    }

    // Closeable

    public void close()
    {
        if (scanner != null)
        {
            IOUtils.closeQuietly(this.scanner);
            this.builder = null;
            this.scanner = null;
        }
    }
}
