package org.apache.cassandra.spark.cdc.jdk;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.FileUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.watermarker.InMemoryWatermarker;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.sparksql.filters.CdcOffset;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.jetbrains.annotations.Nullable;

public class TestJdkCdcIterator extends JdkCdcIterator
{
    public static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("cdc-io-%d").setDaemon(true).build());

    private static final String STATE_DIR = "state";
    private final Path dir;

    public TestJdkCdcIterator(String jobId,
                              int partitionId,
                              long epoch,
                              CdcOffset cdcOffset,
                              InMemoryWatermarker.SerializationWrapper serializationWrapper,
                              String path)
    {
        super(jobId, partitionId, epoch, cdcOffset, serializationWrapper);
        this.dir = Paths.get(path);
        initDir();
    }

    public TestJdkCdcIterator(Path dir)
    {
        this.dir = dir;
        initDir();
    }

    private void initDir()
    {
        try
        {
            final Path stateDir = stateDir();
            if (Files.exists(stateDir))
            {
                FileUtils.deleteDirectory(stateDir.toFile());
            }
            Files.createDirectory(stateDir);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Path stateDir()
    {
        return dir.resolve(STATE_DIR);
    }

    @Override
    public int maxEpochs()
    {
        return 200;
    }

    public void persist(String jobId, int partitionId, ByteBuffer buf)
    {
        final Path path = dir.resolve(STATE_DIR).resolve(jobId + "-" + partitionId + ".cdc");
        try (FileOutputStream fos = new FileOutputStream(path.toFile()))
        {
            try (FileChannel fc = fos.getChannel())
            {
                fc.write(buf);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void close() throws IOException
    {
        FileUtils.deleteDirectory(stateDir().toFile());
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
                return Files.list(dir.resolve("commitlog"))
                            .filter(Files::isRegularFile)
                            .filter(path -> path.getFileName().toString().endsWith(".log"))
                            .map(Path::toFile)
                            .map(LocalDataLayer.LocalCommitLog::new);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public ExecutorService executorService()
    {
        return EXECUTOR;
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
                                                  CdcOffset cdcOffset,
                                                  InMemoryWatermarker.SerializationWrapper serializationWrapper)
            {
                return new TestJdkCdcIterator(jobId, partitionId, epoch, cdcOffset, serializationWrapper, in.readString());
            }
        };
    }

    public Serializer<? extends JdkCdcIterator> serializer()
    {
        return testSerializer();
    }
}
