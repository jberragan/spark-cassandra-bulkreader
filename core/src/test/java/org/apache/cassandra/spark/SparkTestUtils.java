package org.apache.cassandra.spark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

import com.google.common.primitives.UnsignedBytes;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.NotImplementedException;

import org.apache.cassandra.spark.cdc.fourzero.CdcEventWriter;
import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.reader.fourzero.FourZero;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.SSTableTombstoneWriter;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.tools.JsonTransformer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.tools.Util;
import org.apache.cassandra.spark.utils.ByteBufUtils;
import org.apache.cassandra.spark.utils.FilterUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
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
public class SparkTestUtils
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

    public static boolean equals(final Object[] expected, final Object[] actual)
    {
        if (expected == actual)
        {
            return true;
        }
        if (expected == null || actual == null)
        {
            return false;
        }

        final int length = expected.length;
        if (actual.length != length)
        {
            return false;
        }

        for (int i = 0; i < length; i++)
        {
            if (!SparkTestUtils.equals(expected[i], actual[i]))
            {
                return false;
            }
        }

        return true;
    }

    public static void assertEquals(final Object expected, final Object actual)
    {
        assertTrue(String.format("Expect %s to equal to %s, but not.", expected, actual),
                   equals(expected, actual));
    }

    @SuppressWarnings("unchecked")
    public static boolean equals(final Object expected, final Object actual)
    {
        if (expected instanceof UUID && actual instanceof String) // spark does not support UUID. We compare the string values.
        {
            return expected.toString().equals(actual);
        }

        if (expected instanceof ByteBuffer && actual instanceof byte[])
        {
            return Arrays.equals(ByteBufUtils.getArray((ByteBuffer) expected), (byte[]) actual);
        }

        if (expected instanceof InetAddress && actual instanceof byte[])
        {
            return Arrays.equals(((InetAddress) expected).getAddress(), (byte[]) actual);
        }

        if (expected instanceof BigInteger && actual instanceof BigDecimal)
        {
            // compare the string values
            return expected.toString().equals(actual.toString());
        }

        if (expected instanceof Integer && actual instanceof Date)
        {
            return expected.equals((int) ((Date) actual).toLocalDate().toEpochDay());
        }

        if ((Objects.equals(expected, actual)))
        {
            return true;
        }

        if (expected instanceof BigDecimal && actual instanceof BigDecimal)
        {
            return ((BigDecimal) expected).compareTo((BigDecimal) actual) == 0;
        }
        else if (expected instanceof Collection && actual instanceof Collection)
        {
            final Object[] a3 = ((Collection<Object>) expected).toArray(new Object[0]);
            Arrays.sort(a3, NESTED_COMPARATOR);
            final Object[] a4 = ((Collection<Object>) actual).toArray(new Object[0]);
            Arrays.sort(a4, NESTED_COMPARATOR);
            return SparkTestUtils.equals(a3, a4);
        }
        else if (expected instanceof Map && actual instanceof Map)
        {
            final Object[] k1 = ((Map<Object, Object>) expected).keySet().toArray(new Object[0]);
            final Object[] k2 = ((Map<Object, Object>) actual).keySet().toArray(new Object[0]);
            if (k1[0] instanceof Comparable)
            {
                Arrays.sort(k1);
                Arrays.sort(k2);
            }
            else if (k1[0] instanceof Inet4Address) // Inet4Address is not Comparable so do byte ordering
            {
                Arrays.sort(k1, SparkTestUtils.INET_COMPARATOR);
                Arrays.sort(k2, SparkTestUtils.INET_COMPARATOR);
            }
            if (SparkTestUtils.equals(k1, k2))
            {
                final Object[] v1 = new Object[k1.length];
                final Object[] v2 = new Object[k2.length];
                IntStream.range(0, k1.length).forEach(pos -> {
                    v1[pos] = ((Map<Object, Object>) expected).get(k1[pos]);
                    v2[pos] = ((Map<Object, Object>) actual).get(k2[pos]);
                });
                return SparkTestUtils.equals(v1, v2);
            }
            return false;
        }
        else if (expected instanceof Object[] && actual instanceof Object[])
        {
            return SparkTestUtils.equals((Object[]) expected, (Object[]) actual);
        }
        return false;
    }

    public static long countSSTables(final Path dir) throws IOException
    {
        return getFileType(dir, SSTable.FileType.DATA).count();
    }

    public static Path getFirstFileType(final Path dir, final SSTable.FileType fileType) throws IOException
    {
        return getFileType(dir, fileType).findFirst().orElseThrow(() -> new IllegalStateException(String.format("Could not find %s file", fileType.getFileSuffix())));
    }

    public static Stream<Path> getFileType(final Path dir, final SSTable.FileType fileType) throws IOException
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
        qt().forAll(TestUtils.partitioners(), SparkTestUtils.bridges())
            .checkAssert((partitioner, bridge) -> SparkTestUtils.runTest(partitioner, bridge, test));
    }

    public static void runTest(final CassandraVersion version, final TestRunnable test)
    {
        qt().forAll(TestUtils.partitioners())
            .checkAssert((partitioner) -> SparkTestUtils.runTest(partitioner, version, test));
    }

    public static void runTest(final Partitioner partitioner, final CassandraVersion version, final TestRunnable test)
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

    public static StreamingQuery openStreaming(String keyspace,
                                               String createStmt,
                                               CassandraVersion version,
                                               Partitioner partitioner,
                                               Path dir,
                                               Path outputDir,
                                               Path checkpointDir,
                                               String dataSourceFQCN,
                                               @Nullable final String statsClass,
                                               CdcEventWriter cdcEventWriter)
    {
         DataStreamReader streamReader = SPARK.readStream()
                                              .format(dataSourceFQCN)
                                              .option("keyspace", keyspace)
                                              .option("createStmt", createStmt)
                                              .option("dirs", dir.toAbsolutePath().toString())
                                              .option("version", version.toString())
                                              .option("useSSTableInputStream", true) // use in the test system to test the SSTableInputStream
                                              .option("cdcSubMicroBatchSize", 3)
                                              .option("isCdc", true) // tell spark directly that it is for cdc
                                              .option("partitioner", partitioner.name())
                                              .option("udts", "");

        if (statsClass != null)
        {
            streamReader = streamReader.option("statsClass", statsClass);
        }

        final Dataset<Row> rows = streamReader.load();

        try
        {
            return rows
                   .writeStream()
                   .format("parquet")
                   .option("path", outputDir.toString())
                   .option("checkpointLocation", checkpointDir.toString())
                   .foreach(cdcEventWriter)
                   .outputMode(OutputMode.Append())
                   .start();
        }
        catch (Exception e)
        {
            // in Spark3 start() can throw a TimeoutException
            throw new RuntimeException(e);
        }
    }

    public static Dataset<Row> readCdc(Path path, StructType schema)
    {
        return SPARK.read()
                    .format("parquet")
                    .option("path", path.toString())
                    .schema(schema)
                    .load();
    }

    static Dataset<Row> openLocalDataset(final Partitioner partitioner,
                                         final Path dir,
                                         final String keyspace,
                                         final String createStmt,
                                         final CassandraVersion version,
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
                                           .option(SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP.optionName(), addLastModifiedTimestampColumn)
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
        SparkTestUtils.writeSSTable(bridge, dir, partitioner, schema, false, writer.consumer);
    }

    // write tombstones

    public static void writeTombstoneSSTable(final Partitioner partitioner, final CassandraVersion version, final Path dir, final String createStmt, final String deleteStmt, final Consumer<CassandraBridge.IWriter> writerConsumer)
    {
        if (version == CassandraVersion.FOURZERO)
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

    public static void sstableToJson(final CassandraVersion version, final Path dataDbFile, final OutputStream out) throws FileNotFoundException
    {
        if (version == CassandraVersion.FOURZERO)
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

    /* quick theories helpers */

    public static Gen<CassandraVersion> versions()
    {
        return arbitrary().pick(new ArrayList<>(CassandraBridge.SUPPORTED_VERSIONS));
    }

    public static Gen<CassandraBridge> bridges()
    {
        return arbitrary().pick(TestUtils.testableVersions().stream().map(CassandraBridge::get).collect(Collectors.toList()));
    }

    public static Gen<CqlField.NativeType> cql3Type(CassandraBridge bridge)
    {
        return arbitrary().pick(bridge.supportedTypes());
    }

    public static Gen<CqlField.SortOrder> sortOrder()
    {
        return arbitrary().enumValues(CqlField.SortOrder.class);
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

    public static boolean isNotEmpty(Path path)
    {
        return size(path) > 0;
    }

    public static long size(Path path)
    {
        try
        {
            return Files.size(path);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void clearDirectory(Path path)
    {
        clearDirectory(path, p -> {
        });
    }

    public static void clearDirectory(Path path, Consumer<Path> logger)
    {
        try (Stream<Path> walker = Files.walk(path))
        {
            walker.sorted(Comparator.reverseOrder())
                  .filter(Files::isRegularFile)
                  .forEach(f -> {
                      try
                      {
                          logger.accept(f);
                          Files.delete(f);
                      }
                      catch (IOException e)
                      {
                          throw new RuntimeException(e);
                      }
                  });
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
