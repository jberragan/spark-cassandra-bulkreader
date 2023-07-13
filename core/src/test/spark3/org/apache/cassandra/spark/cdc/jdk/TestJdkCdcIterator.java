package org.apache.cassandra.spark.cdc.jdk;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Range;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.ICommitLogMarkers;
import org.apache.cassandra.spark.cdc.Marker;
import org.apache.cassandra.spark.cdc.watermarker.InMemoryWatermarker;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TestJdkCdcIterator extends JdkCdcIterator
{
    public static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setNameFormat("cdc-io-%d").setDaemon(true).build());
    public static final AsyncExecutor ASYNC_EXECUTOR = AsyncExecutor.wrap(EXECUTOR);

    private final Path dir;

    public TestJdkCdcIterator(@NotNull String jobId,
                              int partitionId,
                              String path)
    {
        super(jobId, partitionId);
        this.dir = Paths.get(path);
    }

    public TestJdkCdcIterator(String jobId,
                              int partitionId,
                              long epoch,
                              @Nullable Range<BigInteger> range,
                              @NotNull Map<CassandraInstance, Marker> startMarkers,
                              InMemoryWatermarker.SerializationWrapper serializationWrapper,
                              String path)
    {
        this(jobId, partitionId, epoch, range, ICommitLogMarkers.of(startMarkers), serializationWrapper, path);
    }

    public TestJdkCdcIterator(String jobId,
                              int partitionId,
                              long epoch,
                              @Nullable Range<BigInteger> range,
                              @NotNull ICommitLogMarkers markers,
                              InMemoryWatermarker.SerializationWrapper serializationWrapper,
                              String path)
    {
        super(jobId, partitionId, epoch, range, markers, serializationWrapper);
        this.dir = Paths.get(path);
    }

    public TestJdkCdcIterator(String jobId,
                              int partitionId,
                              long epoch,
                              @Nullable RangeFilter rangeFilter,
                              @NotNull ICommitLogMarkers markers,
                              InMemoryWatermarker watermarker,
                              String path)
    {
        super(jobId, partitionId, epoch, rangeFilter, markers, watermarker);
        this.dir = Paths.get(path);
    }

    public TestJdkCdcIterator(Path dir)
    {
        this.dir = dir;
    }

    public void persist(String jobId,
                        int partitionId,
                        @Nullable RangeFilter rangeFilter,
                        ByteBuffer buf)
    {
        // we don't need to persist state in tests
    }

    public Set<String> keyspaces()
    {
        return Collections.emptySet();
    }

    public void close()
    {
    }

    @Nullable
    public RangeFilter rangeFilter()
    {
        return null;
    }

    public CommitLogProvider logs(@Nullable RangeFilter rangeFilter)
    {
        return () -> {
            try
            {
                try (Stream<Path> stream = Files.list(dir.resolve("commitlog")))
                {
                    return stream.filter(Files::isRegularFile)
                                 .filter(path -> path.getFileName().toString().endsWith(".log"))
                                 .map(Path::toFile)
                                 .map(LocalDataLayer.LocalCommitLog::new)
                                 .collect(Collectors.toSet())
                                 .stream()
                                 .map(l -> (LocalDataLayer.LocalCommitLog) l);
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public AsyncExecutor executor()
    {
        return ASYNC_EXECUTOR;
    }

    @Override
    public int minimumReplicas(String keyspace)
    {
        return 1;
    }

    @Override
    public boolean persistState()
    {
        return true;
    }

    public <Type extends JdkCdcIterator> JdkCdcIterator newInstance(Type other,
                                                                    String jobId,
                                                                    int partitionId,
                                                                    long epoch,
                                                                    @Nullable RangeFilter rangeFilter,
                                                                    ICommitLogMarkers markers,
                                                                    InMemoryWatermarker watermarker)
    {
        return new TestJdkCdcIterator(jobId, partitionId, epoch, rangeFilter, markers, watermarker, dir.toString());
    }

    public <Type extends JdkCdcIterator> JdkCdcIterator newInstance(Type other,
                                                                    String jobId,
                                                                    int partitionId,
                                                                    long epoch,
                                                                    @Nullable Range<BigInteger> range,
                                                                    ICommitLogMarkers mergedMarkers,
                                                                    InMemoryWatermarker.SerializationWrapper mergedSerializationWrapper)
    {
        return new TestJdkCdcIterator(jobId, partitionId, epoch, range, mergedMarkers, mergedSerializationWrapper, dir.toString());
    }

    public static Serializer<TestJdkCdcIterator> testSerializer()
    {
        return new Serializer<TestJdkCdcIterator>()
        {
            public void writeAdditionalFields(final Kryo kryo, final Output out, final TestJdkCdcIterator it)
            {
                out.writeString(it.dir.toString());
            }

            public TestJdkCdcIterator newInstance(Kryo kryo, Input in, Class<TestJdkCdcIterator> type,
                                                  String jobId,
                                                  int partitionId,
                                                  long epoch,
                                                  @Nullable Range<BigInteger> range,
                                                  @NotNull final ICommitLogMarkers markers,
                                                  InMemoryWatermarker.SerializationWrapper serializationWrapper)
            {
                return new TestJdkCdcIterator(jobId, partitionId, epoch, range, markers, serializationWrapper, in.readString());
            }
        };
    }

    @Override
    public Duration minDelayBetweenMicroBatches()
    {
        return Duration.ofMillis(1);
    }

    public Serializer<TestJdkCdcIterator> serializer()
    {
        return testSerializer();
    }

    public TestCdcConsumer toConsumer()
    {
        return new TestCdcConsumer(jobId, partitionId, event -> {
        });
    }

    public TestCdcConsumer toConsumer(Consumer<JdkCdcEvent> consumer)
    {
        return new TestCdcConsumer(jobId, partitionId, consumer);
    }

    public class TestCdcConsumer extends CdcConsumer
    {
        @NotNull
        final Consumer<JdkCdcEvent> consumer;

        public TestCdcConsumer(@NotNull String jobId,
                               final int partitionId,
                               @NotNull final Consumer<JdkCdcEvent> consumer)
        {
            super(jobId, partitionId);
            this.consumer = consumer;
        }

        public void accept(JdkCdcEvent event)
        {
            consumer.accept(event);
        }

        @Nullable
        public RangeFilter rangeFilter()
        {
            return TestJdkCdcIterator.this.rangeFilter();
        }

        public int minimumReplicas(String keyspace)
        {
            return TestJdkCdcIterator.this.minimumReplicas(keyspace);
        }

        public CommitLogProvider logs(@Nullable RangeFilter rangeFilter)
        {
            return TestJdkCdcIterator.this.logs(rangeFilter);
        }

        public AsyncExecutor executor()
        {
            return TestJdkCdcIterator.this.executor();
        }

        public void persist(String jobId, int partitionId, @Nullable RangeFilter rangeFilter, ByteBuffer buf)
        {
            TestJdkCdcIterator.this.persist(jobId, partitionId, rangeFilter, buf);
        }

        public Set<String> keyspaces()
        {
            return TestJdkCdcIterator.this.keyspaces();
        }

        public <Type extends JdkCdcIterator> JdkCdcIterator newInstance(Type other, String jobId, int partitionId, long epoch, @Nullable RangeFilter rangeFilter, ICommitLogMarkers markers, InMemoryWatermarker watermarker)
        {
            return TestJdkCdcIterator.this.newInstance(other, jobId, partitionId, epoch, rangeFilter, markers, watermarker)
                   .toConsumer();
        }

        public <Type extends JdkCdcIterator> JdkCdcIterator newInstance(Type other, String jobId, int partitionId, long epoch, @Nullable Range<BigInteger> range, ICommitLogMarkers mergedMarkers, InMemoryWatermarker.SerializationWrapper mergedSerializationWrapper)
        {
            return TestJdkCdcIterator.this.newInstance(other, jobId, partitionId, epoch, range, mergedMarkers, mergedSerializationWrapper)
                   .toConsumer();
        }

        public Serializer<TestCdcConsumer> serializer()
        {
            return testCdcSerializer();
        }

        public Duration minDelayBetweenMicroBatches()
        {
            return TestJdkCdcIterator.this.minDelayBetweenMicroBatches();
        }

        public Path dir()
        {
            return TestJdkCdcIterator.this.dir;
        }
    }

    public static Serializer<TestCdcConsumer> testCdcSerializer()
    {
        return new Serializer<>()
        {
            public void writeAdditionalFields(final Kryo kryo, final Output out, final TestCdcConsumer it)
            {
                out.writeString(it.dir().toString());
            }

            public TestCdcConsumer newInstance(Kryo kryo,
                                               Input in,
                                               Class<TestCdcConsumer> type,
                                               String jobId,
                                               int partitionId,
                                               long epoch,
                                               @Nullable Range<BigInteger> range,
                                               @NotNull final ICommitLogMarkers startMarkers,
                                               InMemoryWatermarker.SerializationWrapper serializationWrapper)
            {
                return new TestJdkCdcIterator(jobId, partitionId, epoch, range, startMarkers, serializationWrapper, in.readString())
                       .toConsumer();
            }
        };
    }
}
