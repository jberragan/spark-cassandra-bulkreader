package org.apache.cassandra.spark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.UnsignedBytes;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.NotImplementedException;

import org.apache.spark.sql.Column;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.fourzero.FourZero;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.SSTableTombstoneWriter;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.tools.JsonTransformer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.tools.Util;
import org.apache.cassandra.spark.utils.FilterUtils;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.quicktheories.core.Gen;
import scala.collection.JavaConversions;

import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

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
public class TestUtils
{
    static final SparkSession SPARK = SparkSession
                                              .builder()
                                              .appName("Java Test")
                                              .config("spark.master", "local")
                                              .getOrCreate();
    public static final int NUM_ROWS = 50;
    public static final int NUM_COLS = 25;
    public static final int MIN_COLLECTION_SIZE = 16;
    private static final Comparator<Object> INET_COMPARATOR = (o1, o2) -> UnsignedBytes.lexicographicalComparator().compare(((Inet4Address) o1).getAddress(), ((Inet4Address) o2).getAddress());

    public static final Comparator<Object> NESTED_COMPARATOR = new Comparator<Object>()
    {
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public int compare(Object o1, Object o2)
        {
            if (o1 instanceof Comparable && o2 instanceof Comparable)
            {
                return ((Comparable) o1).compareTo(o2);
            }
            else if (o1 instanceof Object[] && o2 instanceof Object[])
            {
                final Object[] a1 = (Object[]) o1;
                final Object[] a2 = (Object[]) o2;
                int pos = 0;
                while (pos < a1.length && pos < a2.length)
                {
                    int c = NESTED_COMPARATOR.compare(a1[pos], a2[pos]);
                    if (c != 0)
                    {
                        return c;
                    }
                    pos++;
                }
                return Integer.compare(a1.length, a2.length);
            }
            else if (o1 instanceof Map && o2 instanceof Map)
            {
                final Map<?, ?> m1 = (Map<?, ?>) o1;
                final Map<?, ?> m2 = (Map<?, ?>) o2;
                for (final Object key : m1.keySet())
                {
                    int c = NESTED_COMPARATOR.compare(m1.get(key), m2.get(key));
                    if (c != 0)
                    {
                        return c;
                    }
                }
                return Integer.compare(m1.size(), m2.size());
            }
            else if (o1 instanceof Collection && o2 instanceof Collection)
            {
                return NESTED_COMPARATOR.compare(((Collection) o1).toArray(new Object[0]), ((Collection) o2).toArray(new Object[0]));
            }
            else if (o1 instanceof Inet4Address && o2 instanceof Inet4Address)
            {
                return INET_COMPARATOR.compare(o1, o2);
            }
            throw new IllegalStateException("Unexpected comparable type: " + o1.getClass().getName());
        }
    };

    public static Object randomValue(final CqlField.CqlType type)
    {
        return type.randomValue(MIN_COLLECTION_SIZE);
    }

    public static boolean equals(final Object[] ar1, final Object[] ar2)
    {
        if (ar1 == ar2)
        {
            return true;
        }
        if (ar1 == null || ar2 == null)
        {
            return false;
        }

        final int length = ar1.length;
        if (ar2.length != length)
        {
            return false;
        }

        for (int i = 0; i < length; i++)
        {
            if (!TestUtils.equals(ar1[i], ar2[i]))
            {
                return false;
            }
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    private static boolean equals(final Object o1, final Object o2)
    {
        if ((Objects.equals(o1, o2)))
        {
            return true;
        }

        if (o1 instanceof BigDecimal && o2 instanceof BigDecimal)
        {
            return ((BigDecimal) o1).compareTo((BigDecimal) o2) == 0;
        }
        else if (o1 instanceof Collection && o2 instanceof Collection)
        {
            final Object[] a3 = ((Collection<Object>) o1).toArray(new Object[0]);
            Arrays.sort(a3, NESTED_COMPARATOR);
            final Object[] a4 = ((Collection<Object>) o2).toArray(new Object[0]);
            Arrays.sort(a4, NESTED_COMPARATOR);
            return TestUtils.equals(a3, a4);
        }
        else if (o1 instanceof Map && o2 instanceof Map)
        {
            final Object[] k1 = ((Map<Object, Object>) o1).keySet().toArray(new Object[0]);
            final Object[] k2 = ((Map<Object, Object>) o2).keySet().toArray(new Object[0]);
            if (k1[0] instanceof Comparable)
            {
                Arrays.sort(k1);
                Arrays.sort(k2);
            }
            else if (k1[0] instanceof Inet4Address) // Inet4Address is not Comparable so do byte ordering
            {
                Arrays.sort(k1, TestUtils.INET_COMPARATOR);
                Arrays.sort(k2, TestUtils.INET_COMPARATOR);
            }
            if (TestUtils.equals(k1, k2))
            {
                final Object[] v1 = new Object[k1.length];
                final Object[] v2 = new Object[k2.length];
                IntStream.range(0, k1.length).forEach(pos -> {
                    v1[pos] = ((Map<Object, Object>) o1).get(k1[pos]);
                    v2[pos] = ((Map<Object, Object>) o2).get(k2[pos]);
                });
                return TestUtils.equals(v1, v2);
            }
            return false;
        }
        else if (o1 instanceof Object[] && o2 instanceof Object[])
        {
            return TestUtils.equals((Object[]) o1, (Object[]) o2);
        }
        return false;
    }

    public static long countSSTables(final Path dir) throws IOException
    {
        return getFileType(dir, DataLayer.FileType.DATA).count();
    }

    public static Path getFirstFileType(final Path dir, final DataLayer.FileType fileType) throws IOException
    {
        return getFileType(dir, fileType).findFirst().orElseThrow(() -> new IllegalStateException(String.format("Could not find %s file", fileType.getFileSuffix())));
    }

    public static Stream<Path> getFileType(final Path dir, final DataLayer.FileType fileType) throws IOException
    {
        return Files
               .list(dir)
               .filter(path -> path.getFileName().toString().endsWith("-" + fileType.getFileSuffix()));
    }

    public static void moveFile(@NotNull final Path source, @NotNull final Path target)
    {
        try
        {
            Files.move(source, target);
        }
        catch (final IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    /**
     * Run test for all supported Cassandra versions
     *
     * @param test unit test
     */
    public static void runTest(final TestRunnable test)
    {
        qt().forAll(TestUtils.partitioners(), TestUtils.bridges())
            .checkAssert((partitioner, bridge) -> TestUtils.runTest(partitioner, bridge, test));
    }

    public static void runTest(final CassandraBridge.CassandraVersion version, final TestRunnable test)
    {
        qt().forAll(TestUtils.partitioners())
            .checkAssert((partitioner) -> TestUtils.runTest(partitioner, version, test));
    }

    public static void runTest(final Partitioner partitioner, final CassandraBridge.CassandraVersion version, final TestRunnable test)
    {
        runTest(partitioner, CassandraBridge.get(version), test);
    }

    /**
     * Create tmp directory and clean up after test
     *
     * @param bridge cassandra bridge
     * @param test   unit test
     */
    public static void runTest(final Partitioner partitioner, final CassandraBridge bridge, final TestRunnable test)
    {
        Path dir = null;
        try
        {
            dir = Files.createTempDirectory(UUID.randomUUID().toString());
            test.run(partitioner, dir, bridge);
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            if (dir != null)
            {
                try
                {
                    FileUtils.deleteDirectory(dir.toFile());
                }
                catch (final IOException ignore)
                {

                }
            }
        }
    }

    static Dataset<Row> openLocalDataset(final Partitioner partitioner,
                                         final Path dir,
                                         final String keyspace,
                                         final String createStmt,
                                         final CassandraBridge.CassandraVersion version,
                                         final Set<CqlField.CqlUdt> udts,
                                         final boolean addLastModifiedTimestampColumn,
                                         @Nullable final String statsClass,
                                         @Nullable final String filterExpression,
                                         @Nullable final String... columns)
    {
        DataFrameReader frameReader = SPARK.read().format("org.apache.cassandra.spark.sparksql.LocalDataSource")
                                           .option("keyspace", keyspace)
                                           .option("createStmt", createStmt)
                                           .option("dirs", dir.toAbsolutePath().toString())
                                           .option("version", version.toString())
                                           .option("useSSTableInputStream", true) // use in the test system to test the SSTableInputStream
                                           .option("partitioner", partitioner.name())
                                           .option("addLastModifiedTimestampColumn", addLastModifiedTimestampColumn)
                                           .option("udts", udts.stream().map(f -> f.createStmt(keyspace)).collect(Collectors.joining("\n")));
        if (statsClass != null)
        {
            frameReader = frameReader.option("statsClass", statsClass);
        }
        Dataset<Row> ds = frameReader.load();
        if (filterExpression != null)
        {
            // attach partition filter criteria
            ds = ds.filter(filterExpression);
        }
        if (columns != null)
        {
            // attach column select criteria
            final List<Column> cols = Arrays.stream(columns).map(Column::new).collect(Collectors.toList());
            ds = ds.select(JavaConversions.asScalaBuffer(cols));
        }
        return ds;
    }

    public static void writeSSTable(final CassandraBridge bridge, final Path dir, final Partitioner partitioner, final TestSchema schema, Consumer<CassandraBridge.IWriter> consumer)
    {
        writeSSTable(bridge, dir, partitioner, schema, false, consumer);
    }

    public static void writeSSTable(final CassandraBridge bridge, final Path dir, final Partitioner partitioner, final TestSchema schema, final boolean upsert, Consumer<CassandraBridge.IWriter> consumer)
    {
        bridge.writeSSTable(partitioner, schema.keyspace, dir, schema.createStmt, schema.insertStmt, schema.updateStmt, upsert, schema.udts, consumer);
    }

    public static void writeSSTable(final CassandraBridge bridge, final Path dir, final Partitioner partitioner, final TestSchema schema, Tester.Writer writer)
    {
        TestUtils.writeSSTable(bridge, dir, partitioner, schema, false, writer.consumer);
    }

    // write tombstones

    public static void writeTombstoneSSTable(final Partitioner partitioner, final CassandraBridge.CassandraVersion version, final Path dir, final String createStmt, final String deleteStmt, final Consumer<CassandraBridge.IWriter> writerConsumer)
    {
        if (version == CassandraBridge.CassandraVersion.FOURZERO)
        {
            writeFourZeroTombstoneSSTable(partitioner, dir, createStmt, deleteStmt, writerConsumer);
            return;
        }
        throw new NotImplementedException("Tombstone writer not implemented for version: " + version);
    }

    private static void writeFourZeroTombstoneSSTable(final Partitioner partitioner, final Path dir, final String createStmt, final String deleteStmt, final Consumer<CassandraBridge.IWriter> writerConsumer)
    {
        try (final SSTableTombstoneWriter writer = SSTableTombstoneWriter.builder()
                                                                         .inDirectory(dir.toFile())
                                                                         .forTable(createStmt)
                                                                         .withPartitioner(FourZero.getPartitioner(partitioner))
                                                                         .using(deleteStmt)
                                                                         .withBufferSizeInMB(128).build())
        {
            writerConsumer.accept(values -> {
                try
                {
                    writer.addRow(values);
                }
                catch (final IOException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /* sstable to json */

    public static void sstableToJson(final CassandraBridge.CassandraVersion version, final Path dataDbFile, final OutputStream out) throws FileNotFoundException
    {
        if (version == CassandraBridge.CassandraVersion.FOURZERO)
        {
            sstableToJsonFourZero(dataDbFile, out);
            return;
        }
        throw new NotImplementedException("SSTableToJson not implemented for version: " + version);
    }

    private static void sstableToJsonFourZero(final Path dataDbFile, final OutputStream out) throws FileNotFoundException
    {
        if (!Files.exists(dataDbFile))
        {
            throw new FileNotFoundException("Cannot find file " + dataDbFile.toAbsolutePath());
        }
        if (!Descriptor.isValidFile(dataDbFile.toFile()))
        {
            throw new RuntimeException("Invalid sstable file");
        }

        final Descriptor desc = Descriptor.fromFilename(dataDbFile.toAbsolutePath().toString());
        try
        {
            final TableMetadataRef metadata = TableMetadataRef.forOfflineTools(Util.metadataFromSSTable(desc));
            final SSTableReader sstable = SSTableReader.openNoValidation(desc, metadata);
            final ISSTableScanner currentScanner = sstable.getScanner();
            final Stream<UnfilteredRowIterator> partitions = iterToStream(currentScanner);
            JsonTransformer.toJson(currentScanner, partitions, false, metadata.get(), out);
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static <T> Stream<T> iterToStream(final Iterator<T> iter)
    {
        final Spliterator<T> splititer = Spliterators.spliteratorUnknownSize(iter, Spliterator.IMMUTABLE);
        return StreamSupport.stream(splititer, false);
    }

    public static ReplicationFactor simpleStrategy()
    {
        return new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, ImmutableMap.of("DC1", 3));
    }

    public static ReplicationFactor networkTopologyStrategy()
    {
        return networkTopologyStrategy(ImmutableMap.of("DC1", 3));
    }

    public static ReplicationFactor networkTopologyStrategy(final Map<String, Integer> options)
    {
        return new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, options);
    }

    /* quick theories helpers */

    public static Gen<CassandraBridge.CassandraVersion> versions()
    {
        return arbitrary().pick(new ArrayList<>(CassandraBridge.SUPPORTED_VERSIONS));
    }

    public static Gen<CassandraBridge> bridges()
    {
        return arbitrary().pick(testableVersions().stream().map(CassandraBridge::get).collect(Collectors.toList()));
    }

    static List<CassandraBridge.CassandraVersion> testableVersions()
    {
        return new ArrayList<>(Collections.singletonList(CassandraBridge.CassandraVersion.FOURZERO));
    }

    public static Gen<CqlField.NativeType> cql3Type(CassandraBridge bridge)
    {
        return arbitrary().pick(bridge.supportedTypes());
    }

    public static Gen<CqlField.SortOrder> sortOrder()
    {
        return arbitrary().enumValues(CqlField.SortOrder.class);
    }

    public static Gen<CassandraBridge.CassandraVersion> tombstoneVersions()
    {
        return arbitrary().pick(tombstoneTestableVersions());
    }

    static List<CassandraBridge.CassandraVersion> tombstoneTestableVersions()
    {
        return Collections.singletonList(CassandraBridge.CassandraVersion.FOURZERO);
    }

    public static Gen<Partitioner> partitioners()
    {
        return arbitrary().enumValues(Partitioner.class);
    }

    public static BigInteger randomBigInteger(final Partitioner partitioner)
    {
        final BigInteger range = partitioner.maxToken().subtract(partitioner.minToken());
        final int len = partitioner.maxToken().bitLength();
        BigInteger result = new BigInteger(len, RandomUtils.RANDOM);
        if (result.compareTo(partitioner.minToken()) < 0)
        {
            result = result.add(partitioner.minToken());
        }
        if (result.compareTo(range) >= 0)
        {
            result = result.mod(range).add(partitioner.minToken());
        }
        return result;
    }

    public static CassandraRing createRing(final Partitioner partitioner, int numInstances)
    {
        return createRing(partitioner, ImmutableMap.of("DC1", numInstances));
    }

    public static CassandraRing createRing(final Partitioner partitioner, final Map<String, Integer> numInstances)
    {
        final Collection<CassandraInstance> instances = numInstances.entrySet().stream().map(e -> TestUtils.createInstances(partitioner, e.getValue(), e.getKey())).flatMap(Collection::stream).collect(Collectors.toList());
        final Map<String, Integer> dcs = numInstances.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, a -> Math.min(a.getValue(), 3)));
        return new CassandraRing(partitioner, "test", new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, dcs), instances);
    }

    public static Collection<CassandraInstance> createInstances(final Partitioner partitioner, final int numInstances, final String dc)
    {
        Preconditions.checkArgument(numInstances > 0, "NumInstances must be greater than zero");
        final BigInteger split = partitioner.maxToken().subtract(partitioner.minToken()).divide(BigInteger.valueOf(numInstances));
        final Collection<CassandraInstance> instances = new ArrayList<>(numInstances);
        BigInteger token = partitioner.minToken();
        for (int i = 0; i < numInstances; i++)
        {
            instances.add(new CassandraInstance(token.toString(), "local-i" + i, dc));
            token = token.add(split);
            assertTrue(token.compareTo(partitioner.maxToken()) <= 0);
        }
        return instances;
    }

    public static Set<String> getKeys(final List<List<String>> values)
    {
        final Set<String> filterKeys = new HashSet<>();
        FilterUtils.cartesianProduct(values).forEach(keys ->
                                                     {
                                                         final String compositeKey = String.join(":", keys);
                                                         filterKeys.add(compositeKey);
                                                     });
        return filterKeys;
    }
}
