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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.CdcKryoRegister;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.ICassandraSource;
import org.apache.cassandra.spark.cdc.Marker;
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
public abstract class JdkCdcIterator implements AutoCloseable, IStreamScanner<JdkCdcEvent>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JdkCdcIterator.class);

    // live state

    @Nullable
    protected RangeFilter rangeFilter = null;
    private JdkCdcScannerBuilder builder = null;
    private JdkCdcScannerBuilder.JdkCdcSortedStreamScanner scanner = null;
    private JdkCdcEvent curr = null;

    // serializable state

    @NotNull
    Map<CassandraInstance, Marker> startMarkers;
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
    public JdkCdcIterator(@NotNull String jobId,
                          int partitionId)
    {
        this.jobId = jobId;
        this.partitionId = partitionId;
        this.epoch = 0;
        this.startMarkers = Collections.emptyMap();
        this.watermarker = newWatermarker(partitionId);
    }

    public JdkCdcIterator(@NotNull final String jobId,
                          final int partitionId,
                          final long epoch,
                          @Nullable Range<BigInteger> range,
                          @NotNull final Map<CassandraInstance, Marker> startMarkers,
                          @NotNull final InMemoryWatermarker.SerializationWrapper serializationWrapper)
    {
        this.jobId = jobId;
        this.partitionId = partitionId;
        this.epoch = epoch;
        this.rangeFilter = range == null ? null : RangeFilter.create(range);
        this.startMarkers = startMarkers;
        this.watermarker = newWatermarker(partitionId);
        ((InMemoryWatermarker.PartitionWatermarker) this.watermarker.instance(jobId)).apply(serializationWrapper);
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
    public Map<CassandraInstance, Marker> startMarkers()
    {
        return this.startMarkers;
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
            final ByteBuffer buf = serializeToBytes();
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
        final CdcOffsetFilter offsetFilter = new CdcOffsetFilter(startMarkers, end.allLogs(), end.getTimestampMicros(), watermarkWindowDuration());
        final ICassandraSource cassandraSource = cassandraSource();
        this.builder = new JdkCdcScannerBuilder(this.rangeFilter, offsetFilter, watermarker(), this::minimumReplicas, executor(), logs, jobId, cassandraSource);
        this.scanner = builder.build();
        this.batchStartNanos = System.nanoTime();
        this.startMarkers = end.startMarkers();
        this.epoch++;
    }

    protected void onFinish()
    {
        if (this.builder == null)
        {
            return;
        }

        IOUtils.closeQuietly(this.scanner);
        this.scanner = null;

        // optionally persist Iterator state between micro-batches
        persist();

        // optionally sleep between micro-batches
        final long sleepMillis = minDelayBetweenMicroBatches().toMillis() - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - batchStartNanos);
        sleep(sleepMillis);
        maybeNextBatch();
    }

    private static void sleep(Duration duration)
    {
        sleep(duration.toMillis());
    }

    private static void sleep(long sleepMillis)
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
        while (!this.scanner.next())
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

    // Serialization Helpers

    public abstract Serializer<? extends JdkCdcIterator> serializer();

    public static <T extends JdkCdcIterator> T deserialize(Serializer<T> serializer,
                                                           Class<T> tClass,
                                                           byte[] ar)
    {
        return KryoUtils.deserialize(CdcKryoRegister.kryo(), ar, tClass, serializer);
    }

    // Kryo

    public static abstract class Serializer<Type extends JdkCdcIterator> extends com.esotericsoftware.kryo.Serializer<Type>
    {
        public abstract Type newInstance(Kryo kryo, Input in, Class<Type> type,
                                         String jobId,
                                         int partitionId,
                                         long epoch,
                                         @Nullable Range<BigInteger> range,
                                         Map<CassandraInstance, Marker> startMarkers,
                                         InMemoryWatermarker.SerializationWrapper serializationWrapper);

        public void writeAdditionalFields(final Kryo kryo, final Output out, final Type it)
        {

        }

        @Override
        public void write(final Kryo kryo, final Output out, final Type it)
        {
            out.writeString(it.jobId);
            out.writeInt(it.partitionId);
            out.writeLong(it.epoch);

            if (it.rangeFilter != null)
            {
                final Range<BigInteger> range = it.rangeFilter.tokenRange();
                final byte[] ar1 = range.lowerEndpoint().toByteArray();
                out.writeByte(ar1.length); // Murmur3 is max 8-bytes, RandomPartitioner is max 16-bytes
                out.writeBytes(ar1);
                final byte[] ar2 = range.upperEndpoint().toByteArray();
                out.writeByte(ar2.length);
                out.writeBytes(ar2);
            }
            else
            {
                out.writeByte(-1);
            }

            // write start markers
            out.writeShort(it.startMarkers.size());
            for (final Marker marker : it.startMarkers.values())
            {
                kryo.writeObject(out, marker, Marker.SERIALIZER);
            }

            kryo.writeObject(out, ((InMemoryWatermarker.PartitionWatermarker) it.watermarker.instance(it.jobId)).serializationWrapper(), InMemoryWatermarker.SerializationWrapper.Serializer.INSTANCE);
            writeAdditionalFields(kryo, out, it);
        }

        @Override
        public Type read(Kryo kryo, Input in, Class<Type> type)
        {
            final String jobId = in.readString();
            final int partitionId = in.readInt();
            final long epoch = in.readLong();

            Range<BigInteger> range = null;
            int len = in.readByte();
            if (len > 0)
            {
                final byte[] lower = new byte[len];
                in.readBytes(lower);
                len = in.readByte();
                final byte[] upper = new byte[len];
                in.readBytes(upper);
                range = Range.closed(new BigInteger(lower), new BigInteger(upper));
            }

            final int size = in.readShort();
            final Map<CassandraInstance, Marker> startMarkers = new HashMap<>(size);
            for (int i = 0; i < size; i++)
            {
                final Marker marker = kryo.readObject(in, Marker.class, Marker.SERIALIZER);
                startMarkers.put(marker.instance(), marker);
            }

            return newInstance(kryo, in, type,
                               jobId, partitionId, epoch, range, startMarkers,
                               kryo.readObject(in, InMemoryWatermarker.SerializationWrapper.class, InMemoryWatermarker.SerializationWrapper.Serializer.INSTANCE)
            );
        }
    }

    public static <Type extends JdkCdcIterator> Type deserialize(ByteBuffer compressed,
                                                                 Class<Type> tClass,
                                                                 Serializer<Type> serializer)
    {
        try
        {
            return KryoUtils.deserialize(CdcKryoRegister.kryo(), CompressionUtil.INSTANCE.uncompress(compressed), tClass, serializer);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public ByteBuffer serializeToBytes() throws IOException
    {
        try (final Output out = KryoUtils.serialize(CdcKryoRegister.kryo(), this, serializer()))
        {
            return CompressionUtil.INSTANCE.compress(out.getBuffer());
        }
    }

    // Closeable

    public void close()
    {
        if (scanner != null)
        {
            this.scanner.close();
            this.builder = null;
            this.scanner = null;
        }
    }
}
