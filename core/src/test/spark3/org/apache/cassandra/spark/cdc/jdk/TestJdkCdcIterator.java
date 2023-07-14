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

public class TestJdkCdcIterator extends JdkCdcIterator<TestJdkCdcIterator.TestCdcState>
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
        this(jobId, partitionId, new TestCdcState(epoch, range, markers, serializationWrapper, path));
    }

    public TestJdkCdcIterator(String jobId,
                              int partitionId,
                              long epoch,
                              @Nullable RangeFilter rangeFilter,
                              @NotNull ICommitLogMarkers markers,
                              @NotNull InMemoryWatermarker watermarker,
                              Path dir)
    {
        super(jobId, partitionId, epoch, rangeFilter, markers, watermarker);
        this.dir = dir;
    }

    public TestJdkCdcIterator(String jobId,
                              int partitionId,
                              TestCdcState state)
    {
        super(jobId, partitionId, state);
        this.dir = state.path;
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

    public <Type extends JdkCdcIterator<TestCdcState>> JdkCdcIterator<TestCdcState> newInstance(Type other,
                                                                                                String jobId,
                                                                                                int partitionId,
                                                                                                long epoch,
                                                                                                @Nullable RangeFilter rangeFilter,
                                                                                                ICommitLogMarkers markers,
                                                                                                InMemoryWatermarker watermarker)
    {
        return new TestJdkCdcIterator(jobId, partitionId, epoch, rangeFilter, markers, watermarker, dir);
    }

    public TestCdcState buildState(long epoch,
                                   @Nullable RangeFilter rangeFilter,
                                   @NotNull ICommitLogMarkers markers,
                                   @NotNull InMemoryWatermarker.SerializationWrapper serializationWrapper)
    {
        return new TestCdcState(epoch, rangeFilter, markers, serializationWrapper, dir.toString());
    }

    @Override
    public <Type extends JdkCdcIterator<TestCdcState>> JdkCdcIterator<TestCdcState> newInstance(Type other,
                                                                                                String jobId,
                                                                                                int partitionId,
                                                                                                TestCdcState state)
    {
        return new TestJdkCdcIterator(jobId, partitionId, state);
    }

    public static class TestCdcState extends CdcState
    {
        public final Path path;

        protected TestCdcState(long epoch,
                               @Nullable RangeFilter rangeFilter,
                               @NotNull ICommitLogMarkers markers,
                               @NotNull InMemoryWatermarker.SerializationWrapper serializationWrapper,
                               String path)
        {
            this(epoch, rangeFilter == null ? null : rangeFilter.tokenRange(), markers, serializationWrapper, path);
        }

        protected TestCdcState(long epoch,
                               @Nullable Range<BigInteger> range,
                               @NotNull ICommitLogMarkers markers,
                               @NotNull InMemoryWatermarker.SerializationWrapper serializationWrapper,
                               String path)
        {
            super(epoch, range, markers, serializationWrapper);
            this.path = Paths.get(path);
        }

        @SuppressWarnings("unchecked")
        public <StateType extends CdcState> StateType newInstance(StateType other,
                                                                  long epoch,
                                                                  @Nullable Range<BigInteger> range,
                                                                  ICommitLogMarkers markers,
                                                                  InMemoryWatermarker.SerializationWrapper wrapper)
        {
            return (StateType) new TestCdcState(epoch, range, markers, wrapper, this.path.toString());
        }
    }

    public static CdcState.Serializer<TestCdcState> testSerializer()
    {
        return new CdcState.Serializer<TestCdcState>()
        {
            public void writeAdditionalFields(final Kryo kryo, final Output out, final TestCdcState state)
            {
                out.writeString(state.path.toString());
            }

            public TestCdcState newInstance(Kryo kryo,
                                            Input in,
                                            Class<TestCdcState> type,
                                            long epoch,
                                            @Nullable Range<BigInteger> range,
                                            ICommitLogMarkers markers,
                                            InMemoryWatermarker.SerializationWrapper serializationWrapper)
            {
                return new TestCdcState(epoch, range, markers, serializationWrapper, in.readString());
            }
        };
    }

    @Override
    public Duration minDelayBetweenMicroBatches()
    {
        return Duration.ofMillis(1);
    }

    public CdcState.Serializer<TestCdcState> stateSerializer()
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

    public class TestCdcConsumer extends CdcConsumer<TestCdcState>
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

        public TestCdcState buildState(long epoch, @Nullable RangeFilter rangeFilter, @NotNull ICommitLogMarkers markers, @NotNull InMemoryWatermarker.SerializationWrapper serializationWrapper)
        {
            return new TestCdcState(epoch, rangeFilter, markers, serializationWrapper, TestJdkCdcIterator.this.dir.toString());
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

        public <Type extends JdkCdcIterator<TestCdcState>> JdkCdcIterator<TestCdcState> newInstance(Type other,
                                                                                                    String jobId,
                                                                                                    int partitionId,
                                                                                                    long epoch,
                                                                                                    @Nullable RangeFilter rangeFilter,
                                                                                                    ICommitLogMarkers markers,
                                                                                                    InMemoryWatermarker watermarker)
        {
            return TestJdkCdcIterator.this.newInstance(other, jobId, partitionId, epoch, rangeFilter, markers, watermarker)
                                          .toConsumer();
        }

        public <Type extends JdkCdcIterator<TestCdcState>> JdkCdcIterator<TestCdcState> newInstance(Type other,
                                                                                                    String jobId,
                                                                                                    int partitionId,
                                                                                                    TestCdcState state)
        {
            return TestJdkCdcIterator.this.newInstance(other, jobId, partitionId, state)
                                          .toConsumer();
        }

        public CdcState.Serializer<TestCdcState> stateSerializer()
        {
            return TestJdkCdcIterator.this.stateSerializer();
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
}
