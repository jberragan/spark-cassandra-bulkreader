package org.apache.cassandra.spark.data;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.TableIdLookup;
import org.apache.cassandra.spark.cdc.watermarker.SparkInMemoryWatermarker;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.apache.cassandra.spark.reader.fourzero.FourZeroSchemaBuilder;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.util.CdcRandomAccessReader;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.sparksql.filters.SerializableCommitLog;
import org.apache.cassandra.spark.stats.ICdcStats;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.IOUtils;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.cassandra.spark.utils.streaming.BufferingInputStream;
import org.apache.cassandra.spark.utils.streaming.Source;
import org.apache.cassandra.spark.utils.streaming.CassandraFile;
import org.apache.cassandra.spark.utils.streaming.StreamBuffer;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;
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

/**
 * Basic DataLayer implementation to read SSTables from local file system.  Mostly used for testing.
 */
@SuppressWarnings({ "unused", "WeakerAccess" })
public class LocalDataLayer extends DataLayer implements Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalDataLayer.class);
    public static final Serializer SERIALIZER = new Serializer();
    public static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setNameFormat("file-io-%d").setDaemon(true).build());
    static final ExecutorService COMMIT_LOG_EXECUTOR = Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setNameFormat("commit-log-%d").setDaemon(true).build());
    public static final long serialVersionUID = 42L;

    private final Partitioner partitioner;
    private final CassandraVersion version;
    private final String[] paths;
    private final SparkCqlTable cqlTable;
    private final List<SchemaFeature> requestedFeatures;
    private final boolean useSSTableInputStream;
    private final String statsClass;
    private int minimumReplicasPerMutation = 1;
    private transient volatile Stats stats = null;
    private final String jobId;
    private final boolean isCdc;

    @VisibleForTesting
    public LocalDataLayer(@NotNull final CassandraVersion version,
                          @NotNull final String keyspace,
                          @NotNull final String createStmt,
                          final String... paths)
    {
        this(version, Partitioner.Murmur3Partitioner, keyspace, createStmt, Collections.emptyList(), Collections.emptySet(),
             false, false, null, paths);
    }

    public LocalDataLayer(@NotNull final CassandraVersion version,
                          @NotNull final Partitioner partitioner,
                          @NotNull final String keyspace,
                          @NotNull final String createStmt,
                          @NotNull final List<SchemaFeature> requestedFeatures,
                          @NotNull final Set<String> udts,
                          final boolean useSSTableInputStream,
                          final boolean isCdc,
                          final String statsClass,
                          final String... paths)
    {
        super();
        this.version = version;
        this.partitioner = partitioner;
        this.cqlTable = bridge().decorate(bridge().buildSchema(keyspace, createStmt, new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, ImmutableMap.of("replication_factor", 1)), partitioner, udts, null, isCdc));
        this.requestedFeatures = requestedFeatures;
        this.useSSTableInputStream = useSSTableInputStream;
        this.isCdc = isCdc;
        this.statsClass = statsClass;
        this.paths = paths;
        this.jobId = UUID.randomUUID().toString();
    }

    private LocalDataLayer(@NotNull final CassandraVersion version,
                           @NotNull final Partitioner partitioner,
                           @NotNull final String[] paths,
                           @NotNull final CqlTable cqlTable,
                           @Nullable final String statsClass,
                           @NotNull final String jobId)
    {
        this.version = version;
        this.partitioner = partitioner;
        this.paths = paths;
        this.cqlTable = bridge().decorate(cqlTable);
        this.requestedFeatures = Collections.emptyList();
        this.useSSTableInputStream = false;
        this.isCdc = false;
        this.statsClass = statsClass;
        this.jobId = jobId;
    }

    @Override
    public CassandraVersion version()
    {
        return version;
    }

    @Override
    public List<SchemaFeature> requestedFeatures()
    {
        return requestedFeatures;
    }

    @Override
    public boolean isCdc()
    {
        return isCdc;
    }

    private void loadStats()
    {
        if (this.statsClass == null || this.stats != null)
        {
            return;
        }
        synchronized (this)
        {
            if (this.stats != null)
            {
                return;
            }

            // for tests it's useful to inject a custom stats instance to collect & verify metrics
            try
            {
                final String className = statsClass.substring(0, statsClass.lastIndexOf("."));
                final String fieldName = statsClass.substring(statsClass.lastIndexOf(".") + 1);
                Class<?> c = Class.forName(className);
                final Field f = c.getDeclaredField(fieldName);
                this.stats = (Stats) f.get(null);
            }
            catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public ICdcStats cdcStats()
    {
        loadStats();
        return this.stats != null ? this.stats : super.stats();
    }

    @Override
    public Stats stats()
    {
        loadStats();
        return this.stats != null ? this.stats : super.stats();
    }

    @Override
    public int partitionCount()
    {
        return 1;
    }

    @Override
    public SparkCqlTable cqlTable()
    {
        return this.cqlTable;
    }

    public Set<CqlTable> cdcTables()
    {
        return FourZeroSchemaBuilder.cdcTables();
    }

    @Override
    public boolean isInPartition(final int partitionId, final BigInteger token, final ByteBuffer key)
    {
        return true;
    }

    @VisibleForTesting
    public LocalDataLayer withMinimumReplicasPerMutation(int minimumReplicasPerMutation)
    {
        this.minimumReplicasPerMutation = minimumReplicasPerMutation;
        return this;
    }

    @Override
    public Function<String, Integer> minimumReplicasForCdc()
    {
        return (keyspace) -> minimumReplicasPerMutation;
    }

    public String jobId()
    {
        return jobId;
    }

    @Override
    public Watermarker cdcWatermarker()
    {
        return SparkInMemoryWatermarker.INSTANCE;
    }

    @Override
    public CommitLog toLog(final int partitionId, CassandraInstance instance, SerializableCommitLog commitLog)
    {
        return new LocalCommitLog(new File(commitLog.getPath()));
    }

    @Override
    public CommitLogProvider commitLogs()
    {
        return () -> Arrays.stream(paths)
                           .map(Paths::get)
                           .flatMap(LocalDataLayer::listPath)
                           .map(Path::toFile).map(LocalCommitLog::new);
    }

    @Override
    public TableIdLookup tableIdLookup()
    {
        // no-op, because in tests the tableId in the CommitLog should match in the Schema instance in JVM.
        return (keyspace, table) -> null;
    }

    protected ExecutorService executorService()
    {
        return EXECUTOR;
    }

    public static class LocalCommitLog implements CommitLog
    {
        final long len;
        final String name, path;
        final FileSystemSource<CommitLog> source;
        final CassandraInstance instance;

        public LocalCommitLog(File file)
        {
            this.name = file.getName();
            this.path = file.getPath();
            this.len = file.length();
            this.instance = new CassandraInstance("0", "local-instance", "DC1");

            try
            {
                this.source = new FileSystemSource<CommitLog>(this, CassandraFile.FileType.COMMITLOG, file.toPath())
                {
                    @Override
                    public ExecutorService executor()
                    {
                        return COMMIT_LOG_EXECUTOR;
                    }

                    @Override
                    public long size()
                    {
                        return len;
                    }
                };
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public String name()
        {
            return name;
        }

        public String path()
        {
            return path;
        }

        public long maxOffset()
        {
            return len;
        }

        public long len()
        {
            return len;
        }

        public FileSystemSource<CommitLog> source()
        {
            return source;
        }

        public CassandraInstance instance()
        {
            return instance;
        }

        public void close() throws Exception
        {
            source.close();
        }
    }

    private static Stream<Path> listPath(Path p)
    {
        try (Stream<Path> stream = Files.list(p))
        {
            return stream
                   .filter(f -> !f.getFileName().toString().contains("_cdc.idx"))
                   .collect(Collectors.toSet())
                   .stream();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static BasicSupplier basicSupplier(@NotNull final Stream<SSTable> ssTables)
    {
        return new BasicSupplier(ssTables.collect(Collectors.toSet()));
    }

    /**
     * Builds a new {@link DataLayer} from the {@code options} map. The keys for the map
     * must be lower-cased to guarantee compatibility with maps where the keys are all
     * lower-cased.
     *
     * @param options the map with options
     * @return a new {@link DataLayer}
     */
    public static LocalDataLayer from(Map<String, String> options)
    {
        // keys need to be lower-cased to access the map
        return new LocalDataLayer(
        CassandraVersion.valueOf(options.getOrDefault(lowerCaseKey("version"), CassandraVersion.THREEZERO.toString())),
        Partitioner.valueOf(options.getOrDefault(lowerCaseKey("partitioner"), Partitioner.Murmur3Partitioner.name())),
        getOrThrow(options, lowerCaseKey("keyspace")),
        getOrThrow(options, lowerCaseKey("createStmt")),
        SchemaFeatureSet.initializeFromOptions(options),
        Arrays.stream(options.getOrDefault(lowerCaseKey("udts"), "").split("\n")).filter(StringUtils::isNotEmpty).collect(Collectors.toSet()),
        getBoolean(options, lowerCaseKey("useSSTableInputStream"), false),
        getBoolean(options, lowerCaseKey("isCdc"), false),
        options.get(lowerCaseKey("statsClass")),
        getOrThrow(options, lowerCaseKey("dirs")).split(",")
        );
    }

    /**
     * Returns the lower-cased key using {@link Locale#ROOT}.
     *
     * @param key the key
     * @return the lower-cased key using {@link Locale#ROOT}
     */
    public static String lowerCaseKey(String key)
    {
        return key == null ? null : key.toLowerCase(Locale.ROOT);
    }

    public static String getOrThrow(Map<String, String> options, String key)
    {
        return getOrThrow(options, key, () -> new RuntimeException("No " + key + " specified"));
    }

    public static int getIntOrDefault(Map<String, String> options, String key, int defaultVal)
    {
        String val = options.get(key);
        if (val == null)
        {
            return defaultVal;
        }

        try
        {
            return Integer.parseInt(val);
        }
        catch (NumberFormatException e)
        {
            LOGGER.error("Invalid number {}, Using default value {} for {}", val, defaultVal, key, e);
            return defaultVal;
        }
    }

    static String getOrThrow(Map<String, String> options, String key, Supplier<RuntimeException> throwable)
    {
        final String value = options.get(key);
        if (value == null)
        {
            throw throwable.get();
        }
        return value;
    }

    public static boolean getBoolean(Map<String, String> options, String key, boolean defaultValue)
    {
        String value = options.get(key);
        // We can't use `Boolean.parseBoolean` here, as it returns false for invalid strings.
        if (value == null)
        {
            return defaultValue;
        }
        else if (value.equalsIgnoreCase("true"))
        {
            return true;
        }
        else if (value.equalsIgnoreCase("false"))
        {
            return false;
        }
        throw new IllegalArgumentException("Key " + key + " with value " + value + " is not a boolean string.");
    }

    private static class BasicSupplier extends SSTablesSupplier
    {
        private final Set<SSTable> ssTables;

        BasicSupplier(@NotNull final Set<SSTable> ssTables)
        {
            this.ssTables = ssTables;
        }

        @Override
        public <T extends SparkSSTableReader> Set<T> openAll(final ReaderOpener<T> readerOpener)
        {
            return ssTables.stream().map(ssTable -> {
                try
                {
                    return readerOpener.openReader(ssTable, true);
                }
                catch (final IOException e)
                {
                    throw new RuntimeException(e);
                }
            }).filter(reader -> !reader.ignore()).collect(Collectors.toSet());
        }
    }

    @Override
    public SSTablesSupplier sstables(final int partitionId,
                                     @Nullable final RangeFilter rangeFilter,
                                     @NotNull final List<PartitionKeyFilter> partitionKeyFilters)
    {
        return LocalDataLayer.basicSupplier(listSSTables());
    }

    public Stream<SSTable> listSSTables()
    {
        return Arrays.stream(paths)
                     .map(Paths::get)
                     .flatMap(LocalDataLayer::list)
                     .filter(path -> path.getFileName().toString().endsWith("-" + CassandraFile.FileType.DATA.getFileSuffix()))
                     .map(Path::toString)
                     .map(FileSystemSSTable::new);
    }

    @Override
    public Partitioner partitioner()
    {
        return this.partitioner;
    }

    private static Stream<Path> list(final Path path)
    {
        try
        {
            return Files.list(path);
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public class FileSystemSSTable extends SSTable
    {
        private final String dataFilePath;

        FileSystemSSTable(final String dataFilePath)
        {
            this.dataFilePath = dataFilePath;
        }

        @Override
        protected InputStream openInputStream(final FileType fileType)
        {
            final Path filePath = resolveComponentFile(fileType);
            try
            {
                if (filePath == null)
                {
                    return null;
                }
                else if (useSSTableInputStream)
                {
                    return new BufferingInputStream<>(new FileSystemSource<>(this, fileType, filePath), stats());
                }
                return new BufferedInputStream(new FileInputStream(filePath.toFile()));
            }
            catch (final FileNotFoundException e)
            {
                return null;
            }
            catch (IOException e)
            {
                final Throwable cause = ThrowableUtils.rootCause(e);
                LOGGER.warn("IOException reading local sstable", cause);
                throw new RuntimeException(cause);
            }
        }

        public long length(FileType fileType)
        {
            return IOUtils.fileLength(resolveComponentFile(fileType));
        }

        @Override
        public boolean isMissing(final FileType fileType)
        {
            return resolveComponentFile(fileType) == null;
        }

        @Nullable
        private Path resolveComponentFile(FileType fileType)
        {
            final Path dataFilePath = Paths.get(this.dataFilePath);
            return FileType.resolveComponentFile(fileType, dataFilePath);
        }

        @Override
        public String getDataFileName()
        {
            return Paths.get(this.dataFilePath).getFileName().toString();
        }

        @Override
        public int hashCode()
        {
            return dataFilePath.hashCode();
        }

        @Override
        public boolean equals(final Object o)
        {
            return o instanceof FileSystemSSTable && dataFilePath.equals(((FileSystemSSTable) o).dataFilePath);
        }
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(7, 17)
               .append(cqlTable)
               .append(paths)
               .append(version)
               .toHashCode();
    }

    @Override
    public boolean equals(final Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }
        if (obj.getClass() != getClass())
        {
            return false;
        }

        final LocalDataLayer rhs = (LocalDataLayer) obj;
        return new EqualsBuilder()
               .append(cqlTable, rhs.cqlTable)
               .append(paths, rhs.paths)
               .append(version, rhs.version)
               .isEquals();
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<LocalDataLayer>
    {
        @Override
        public void write(final Kryo kryo, final Output out, final LocalDataLayer obj)
        {
            kryo.writeObject(out, obj.version);
            kryo.writeObject(out, obj.partitioner);
            kryo.writeObject(out, obj.paths);
            kryo.writeObject(out, obj.cqlTable);
            out.writeString(obj.statsClass);
            out.writeString(obj.jobId);
            out.writeInt(obj.minimumReplicasPerMutation);
        }

        @Override
        public LocalDataLayer read(final Kryo kryo, final Input in, final Class<LocalDataLayer> type)
        {
            return new LocalDataLayer(
            kryo.readObject(in, CassandraVersion.class),
            kryo.readObject(in, Partitioner.class),
            kryo.readObject(in, String[].class),
            kryo.readObject(in, CqlTable.class),
            in.readString(), in.readString()
            ).withMinimumReplicasPerMutation(in.readInt());
        }
    }

    static class FileSystemSource<FileType extends CassandraFile> implements Source<FileType>, AutoCloseable
    {
        private final FileType file;
        private final RandomAccessFile raf;
        private final CassandraFile.FileType fileType;
        private final long length;

        private FileSystemSource(FileType file, CassandraFile.FileType fileType, Path path) throws IOException
        {
            this.file = file;
            this.fileType = fileType;
            this.length = Files.size(path);
            this.raf = new RandomAccessFile(path.toFile(), "r");
        }

        @Override
        public long maxBufferSize()
        {
            return chunkBufferSize() * 4;
        }

        @Override
        public long chunkBufferSize()
        {
            return 16384;
        }

        @Override
        public long headerChunkSize()
        {
            return fileType == CassandraFile.FileType.COMMITLOG ? CdcRandomAccessReader.DEFAULT_BUFFER_SIZE : chunkBufferSize();
        }

        public ExecutorService executor()
        {
            return EXECUTOR;
        }

        @Override
        public void request(long start, long end, StreamConsumer consumer)
        {
            executor().submit(() -> {
                boolean close = end >= length;
                try
                {
                    // start-end range is inclusive but on the final request end == length so we need to exclude
                    int incr = close ? 0 : 1;
                    final byte[] ar = new byte[(int) (end - start + incr)];
                    if (raf.getChannel().read(ByteBuffer.wrap(ar), start) >= 0)
                    {
                        consumer.onRead(StreamBuffer.wrap(ar));
                        consumer.onEnd();
                    }
                    else
                    {
                        close = true;
                    }
                }
                catch (Throwable t)
                {
                    close = true;
                    consumer.onError(t);
                }
                finally
                {
                    if (close)
                    {
                        closeSafe();
                    }
                }
            });
        }

        public FileType file()
        {
            return file;
        }

        public CassandraFile.FileType fileType()
        {
            return fileType;
        }

        public long size()
        {
            return length;
        }

        private void closeSafe()
        {
            try
            {
                close();
            }
            catch (Exception e)
            {
                LOGGER.warn("Exception closing InputStream", e);
            }
        }

        public void close() throws Exception
        {
            if (raf != null)
            {
                raf.close();
            }
        }
    }
}
