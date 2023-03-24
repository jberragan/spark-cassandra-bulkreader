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
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.ICassandraSource;
import org.apache.cassandra.spark.cdc.jdk.msg.CdcMessage;
import org.apache.cassandra.spark.cdc.watermarker.InMemoryWatermarker;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.reader.fourzero.CompressionUtil;
import org.apache.cassandra.spark.sparksql.filters.CdcOffset;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.utils.IOUtils;
import org.apache.cassandra.spark.utils.KryoUtils;
import org.apache.cassandra.spark.utils.TimeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Generic Iterator for streaming CDC events in Java.
 */
public abstract class JdkCdcIterator implements AutoCloseable, IStreamScanner<CdcMessage>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JdkCdcIterator.class);

    // live state

    private JdkCdcScannerBuilder builder = null;
    private IStreamScanner<JdkCdcEvent> scanner = null;
    private CdcMessage curr = null;

    // serializable state

    @NotNull
    CdcOffset start;
    @NotNull
    protected final InMemoryWatermarker watermarker;
    @NotNull
    protected final String jobId;
    protected final int partitionId;
    protected long epoch;

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
        this.start = new CdcOffset(TimeUtils.nowMicros() - maxAgeMicros());
        this.watermarker = newWatermarker(partitionId);
    }

    public JdkCdcIterator(@NotNull final String jobId,
                          final int partitionId,
                          final long epoch,
                          @NotNull final CdcOffset cdcOffset,
                          @NotNull final InMemoryWatermarker.SerializationWrapper serializationWrapper)
    {
        this.jobId = jobId;
        this.partitionId = partitionId;
        this.epoch = epoch;
        this.start = cdcOffset;
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

    public CdcOffset startOffset()
    {
        return this.start;
    }

    /* Abstract Methods that must be implemented */

    /**
     * @return a Cassandra Token Range that this iterator should read from.
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
    public abstract ExecutorService executorService();

    /**
     * Optionally persist state between micro-batches, state should be stored namespaced by the jobId and partitionId.
     *
     * @param jobId       unique identifier for CDC streaming job.
     * @param partitionId unique identifier for this partition of the streaming job.
     * @param buf         ByteBuffer with the serialized Iterator state.
     */
    public abstract void persist(String jobId, int partitionId, ByteBuffer buf);


    /**
     * Override to supply ICassandraSource implementation to enable CDC to lookup of unfrozen lists.
     *
     * @return ICassandraSource implementation.
     */
    public ICassandraSource cassandraSource()
    {
        return (keySpace, table, columnsToFetch, primaryKeyColumns) -> null;
    }

    /* Optionally overridable methods for custom configuration */

    /**
     * Add optional sleep between micro-batches.
     *
     * @return duration
     */
    public Duration sleepBetweenMicroBatches()
    {
        return Duration.ZERO;
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
            persist(jobId, partitionId, buf);
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
        if (this.scanner == null)
        {
            nextBatch();
        }
    }

    protected void nextBatch()
    {
        final RangeFilter rangeFilter = rangeFilter();
        final Map<CassandraInstance, List<CommitLog>> logs = logs(rangeFilter).logs()
                                                                              .collect(Collectors.groupingBy(CommitLog::instance, Collectors.toList()));
        final Map<CassandraInstance, CdcOffset.InstanceLogs> instanceLogs = logs.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new CdcOffset.InstanceLogs(e.getValue())));
        final CdcOffset end = new CdcOffset(TimeUtils.nowMicros() - maxAgeMicros(), instanceLogs);
        final CdcOffsetFilter offsetFilter = new CdcOffsetFilter(this.start.startMarkers(), end.allLogs(), this.start.getTimestampMicros(), watermarkWindowDuration());
        final ICassandraSource cassandraSource = cassandraSource();
        this.builder = new JdkCdcScannerBuilder(rangeFilter(), offsetFilter, watermarker(), this::minimumReplicas, executorService(), logs, jobId, cassandraSource);
        this.scanner = builder.build();
        this.start = end;
        this.epoch++;
    }

    protected void onFinish()
    {
        if (this.builder == null)
        {
            return;
        }

        final long startNanos = System.nanoTime();
        IOUtils.closeQuietly(this.scanner);

        // optionally persist Iterator state between micro-batches
        persist();

        // optionally sleep between micro-batches
        final long sleepMillis = sleepBetweenMicroBatches().toMillis() - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        if (sleepMillis > 0)
        {
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

        nextBatch();
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

    public boolean next() throws IOException
    {
        maybeNextBatch();
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

    public void advanceToNextColumn() throws IOException
    {
        Preconditions.checkNotNull(scanner, "next() must be called before advanceToNextColumn()");
        this.curr = scanner.data().toRow();
    }

    public CdcMessage data()
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
        return KryoUtils.deserialize(ar, tClass, serializer);
    }

    // Kryo

    public static abstract class Serializer<Type extends JdkCdcIterator> extends com.esotericsoftware.kryo.Serializer<Type>
    {
        public abstract Type newInstance(Kryo kryo, Input in, Class<Type> type,
                                         String jobId,
                                         int partitionId,
                                         long epoch,
                                         CdcOffset cdcOffset,
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
            kryo.writeObject(out, it.start, CdcOffset.SERIALIZER);
            kryo.writeObject(out, ((InMemoryWatermarker.PartitionWatermarker) it.watermarker.instance(it.jobId)).serializationWrapper(), InMemoryWatermarker.SerializationWrapper.Serializer.INSTANCE);
            writeAdditionalFields(kryo, out, it);
        }

        @Override
        public Type read(Kryo kryo, Input in, Class<Type> type)
        {
            return newInstance(kryo, in, type,
                               in.readString(),
                               in.readInt(),
                               in.readLong(),
                               kryo.readObject(in, CdcOffset.class, CdcOffset.SERIALIZER),
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
            return KryoUtils.deserialize(CompressionUtil.INSTANCE.uncompress(compressed), tClass, serializer);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public ByteBuffer serializeToBytes() throws IOException
    {
        try (final Output out = KryoUtils.serialize(this, serializer()))
        {
            return CompressionUtil.INSTANCE.compress(out.getBuffer());
        }
    }

    // Closeable

    public void close() throws IOException
    {
        if (scanner != null)
        {
            this.scanner.close();
            this.builder = null;
            this.scanner = null;
        }
    }
}
