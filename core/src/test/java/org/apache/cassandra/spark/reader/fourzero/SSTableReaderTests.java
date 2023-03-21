package org.apache.cassandra.spark.reader.fourzero;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.TestDataLayer;
import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.Rid;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DecoratedKey;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.AbstractRow;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Cell;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.ColumnData;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.spark.sparksql.SparkRowIterator;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.ByteBufUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.TestUtils.NUM_COLS;
import static org.apache.cassandra.spark.TestUtils.NUM_ROWS;
import static org.apache.cassandra.spark.TestUtils.countSSTables;
import static org.apache.cassandra.spark.TestUtils.getFileType;
import static org.apache.cassandra.spark.TestUtils.getFirstFileType;
import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
public class SSTableReaderTests
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableReaderTests.class);

    @Test
    public void testOpenCompressedRawInputStream()
    {
        runTest((partitioner, dir, bridge) -> {
            // write an SSTable
            final TestSchema schema = TestSchema.basic(bridge);
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    for (int j = 0; j < NUM_COLS; j++)
                    {
                        writer.write(i, j, i + j);
                    }
                }
            });
            assertEquals(1, countSSTables(dir));

            // verify we can open the CompressedRawInputStream and read through the Data.db file
            final Path dataFile = getFirstFileType(dir, DataLayer.FileType.DATA);
            final long size = Files.size(dataFile);
            assertTrue(size > 0);
            final Path compressionFile = getFirstFileType(dir, DataLayer.FileType.COMPRESSION_INFO);
            long bytesRead = 0;
            try (final InputStream dis = new BufferedInputStream(Files.newInputStream(dataFile));
                 final InputStream cis = new BufferedInputStream(Files.newInputStream(compressionFile)))
            {
                final Descriptor descriptor = Descriptor.fromFilename(new File(String.format("./%s/%s", schema.keyspace, schema.table), dataFile.getFileName().toString()));
                try (final DataInputPlus.DataInputStreamPlus in = new DataInputPlus.DataInputStreamPlus(new DataInputStream(CompressedRawInputStream.fromInputStream(dis, cis, descriptor.version.hasMaxCompressedLength()))))
                {
                    while (in.read() != -1)
                    {
                        bytesRead++;
                    }
                }
            }
            assertTrue(bytesRead > size);
        });
    }

    @Test
    public void testOpenSSTableReader()
    {
        runTest((partitioner, dir, bridge) -> {
            // write an SSTable
            final TestSchema schema = TestSchema.basic(bridge);
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    for (int j = 0; j < NUM_COLS; j++)
                    {
                        writer.write(i, j, i + j);
                    }
                }
            });
            assertEquals(1, countSSTables(dir));

            final Path dataFile = getFirstFileType(dir, DataLayer.FileType.DATA);
            final TableMetadata metaData = schema.schemaBuilder(partitioner).tableMetaData();
            final TestDataLayer dataLayer = new TestDataLayer(bridge, Collections.singletonList(dataFile));
            final FourZeroSSTableReader reader = openReader(metaData, dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find SSTable")));

            assertNotNull(reader.firstToken());
            assertNotNull(reader.lastToken());
            assertNotNull(reader.getSSTableMetadata());
            assertFalse(reader.isRepaired());
            assertEquals(NUM_ROWS * NUM_COLS, countAndValidateRows(reader));
        });
    }

    @Test
    public void testNonStandardFileName()
    {
        runTest((partitioner, directory, bridge) -> {
            final TestSchema schema = TestSchema.basic(bridge);
            TestUtils.writeSSTable(bridge, directory, partitioner, schema, writer -> writer.write(42, 43, 44));
            Files.list(directory).forEach(file -> TestUtils.moveFile(file, Paths.get(file.getParent().toString(), schema.keyspace + "-" + schema.table + "-" + file.getFileName().toString())));

            final TableMetadata metadata = schema.schemaBuilder(partitioner).tableMetaData();
            final Path file = getFirstFileType(directory, DataLayer.FileType.DATA);
            final TestDataLayer data = new TestDataLayer(bridge, Collections.singletonList(file));
            final DataLayer.SSTable table = data.listSSTables().findFirst().orElseThrow(NullPointerException::new);
            openReader(metadata, table);
        });
    }

    @Test
    public void testSSTableRange()
    {
        runTest((partitioner, dir, bridge) -> {
            // write an SSTable
            final TestSchema schema = TestSchema.basic(bridge);
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < 10; i++)
                {
                    for (int j = 0; j < 1; j++)
                    {
                        writer.write(i, j, i + j);
                    }
                }
            });
            assertEquals(1, countSSTables(dir));

            final Path dataFile = getFirstFileType(dir, DataLayer.FileType.DATA);
            final TableMetadata metaData = schema.schemaBuilder(partitioner).tableMetaData();
            final TestDataLayer dataLayer = new TestDataLayer(bridge, Collections.singletonList(dataFile));
            final SparkSSTableReader reader = openReader(metaData, dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find SSTable")));
            assertNotNull(reader.firstToken());
            assertNotNull(reader.lastToken());

            // verify primary Index.db file matches first and last
            final Path indexFile = getFirstFileType(dir, DataLayer.FileType.INDEX);
            final Pair<DecoratedKey, DecoratedKey> firstAndLast;
            try (final InputStream is = new BufferedInputStream(new FileInputStream(indexFile.toFile())))
            {
                final Pair<ByteBuffer, ByteBuffer> keys = FourZeroUtils.readPrimaryIndex(is, true, Collections.emptyList());
                firstAndLast = Pair.of(FourZero.getPartitioner(partitioner).decorateKey(keys.getLeft()), FourZero.getPartitioner(partitioner).decorateKey(keys.getRight()));
            }
            final BigInteger first = FourZeroUtils.tokenToBigInteger(firstAndLast.getLeft().getToken());
            final BigInteger last = FourZeroUtils.tokenToBigInteger(firstAndLast.getRight().getToken());
            assertEquals(first, reader.firstToken());
            assertEquals(last, reader.lastToken());

            switch (partitioner)
            {
                case Murmur3Partitioner:
                    assertFalse(SparkSSTableReader.overlaps(reader, Range.closed(Partitioner.Murmur3Partitioner.minToken(), Partitioner.Murmur3Partitioner.minToken())));
                    assertFalse(SparkSSTableReader.overlaps(reader, Range.closed(Partitioner.Murmur3Partitioner.minToken(), Partitioner.Murmur3Partitioner.minToken())));
                    assertFalse(SparkSSTableReader.overlaps(reader, Range.closed(BigInteger.valueOf(-8710962479251732708L), BigInteger.valueOf(-7686143364045646507L))));
                    assertTrue(SparkSSTableReader.overlaps(reader, Range.closed(BigInteger.valueOf(-7509452495886106294L), BigInteger.valueOf(-7509452495886106293L))));
                    assertTrue(SparkSSTableReader.overlaps(reader, Range.closed(BigInteger.valueOf(-7509452495886106293L), BigInteger.valueOf(-7509452495886106293L))));
                    assertTrue(SparkSSTableReader.overlaps(reader, Range.closed(BigInteger.valueOf(-7509452495886106293L), BigInteger.valueOf(2562047788015215502L))));
                    assertTrue(SparkSSTableReader.overlaps(reader, Range.closed(BigInteger.valueOf(-7509452495886106293L), BigInteger.valueOf(9010454139840013625L))));
                    assertTrue(SparkSSTableReader.overlaps(reader, Range.closed(BigInteger.valueOf(9010454139840013625L), BigInteger.valueOf(9010454139840013625L))));
                    assertFalse(SparkSSTableReader.overlaps(reader, Range.closed(Partitioner.Murmur3Partitioner.maxToken(), Partitioner.Murmur3Partitioner.maxToken())));
                    return;
                case RandomPartitioner:
                    assertFalse(SparkSSTableReader.overlaps(reader, Range.closed(Partitioner.RandomPartitioner.minToken(), Partitioner.RandomPartitioner.minToken())));
                    assertFalse(SparkSSTableReader.overlaps(reader, Range.closed(BigInteger.valueOf(0L), BigInteger.valueOf(500L))));
                    assertFalse(SparkSSTableReader.overlaps(reader, Range.closed(new BigInteger("18837662806270881894834867523173387677"), new BigInteger("18837662806270881894834867523173387677"))));
                    assertTrue(SparkSSTableReader.overlaps(reader, Range.closed(new BigInteger("18837662806270881894834867523173387678"), new BigInteger("18837662806270881894834867523173387678"))));
                    assertTrue(SparkSSTableReader.overlaps(reader, Range.closed(new BigInteger("18837662806270881894834867523173387679"), new BigInteger("18837662806270881894834867523173387679"))));
                    assertTrue(SparkSSTableReader.overlaps(reader, Range.closed(new BigInteger("18837662806270881894834867523173387679"), new BigInteger("137731376325982006772573399291321493164"))));
                    assertTrue(SparkSSTableReader.overlaps(reader, Range.closed(new BigInteger("137731376325982006772573399291321493164"), new BigInteger("137731376325982006772573399291321493164"))));
                    assertFalse(SparkSSTableReader.overlaps(reader, Range.closed(new BigInteger("137731376325982006772573399291321493165"), new BigInteger("137731376325982006772573399291321493165"))));
                    assertFalse(SparkSSTableReader.overlaps(reader, Range.closed(Partitioner.RandomPartitioner.maxToken(), Partitioner.RandomPartitioner.maxToken())));
                    return;
                default:
                    throw new RuntimeException("Unexpected partitioner: " + partitioner);
            }
        });
    }

    @Test
    public void testSkipNoPartitions()
    {
        runTest((partitioner, dir, bridge) -> {
            // write an SSTable
            final TestSchema schema = TestSchema.basic(bridge);
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    for (int j = 0; j < NUM_COLS; j++)
                    {
                        writer.write(i, j, i + j);
                    }
                }
            });
            assertEquals(1, countSSTables(dir));

            final Path dataFile = getFirstFileType(dir, DataLayer.FileType.DATA);
            final Path summaryFile = getFirstFileType(dir, DataLayer.FileType.SUMMARY);
            final TableMetadata metaData = schema.schemaBuilder(partitioner).tableMetaData();
            final TestDataLayer dataLayer = new TestDataLayer(bridge, Collections.singletonList(dataFile));
            SummaryDbUtils.Summary summary;
            try (final InputStream in = new BufferedInputStream(Files.newInputStream(summaryFile)))
            {
                summary = SummaryDbUtils.readSummary(in, metaData.partitioner, metaData.params.minIndexInterval, metaData.params.maxIndexInterval);
            }
            // set Spark token range equal to SSTable token range
            final Range<BigInteger> sparkTokenRange = Range.closed(FourZeroUtils.tokenToBigInteger(summary.first().getToken()), FourZeroUtils.tokenToBigInteger(summary.last().getToken()));
            final RangeFilter rangeFilter = RangeFilter.create(sparkTokenRange);
            final AtomicBoolean skipped = new AtomicBoolean(false);
            final Stats stats = new Stats()
            {
                @Override
                public void skippedPartition(ByteBuffer key, BigInteger token)
                {
                    LOGGER.error("Skipped partition when should not: " + token);
                    skipped.set(true);
                }
            };
            final FourZeroSSTableReader reader = openReader(metaData, dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find SSTable")), rangeFilter, true, stats);
            assertEquals(NUM_ROWS * NUM_COLS, countAndValidateRows(reader)); // shouldn't skip any partitions here
            assertFalse(skipped.get());
        });
    }

    @Test
    public void testSkipPartitions()
    {
        runTest((partitioner, dir, bridge) -> {
            // write an SSTable
            final TestSchema schema = TestSchema.basic(bridge);
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    for (int j = 0; j < NUM_COLS; j++)
                    {
                        writer.write(i, j, i + j);
                    }
                }
            });
            assertEquals(1, countSSTables(dir));

            final Path dataFile = getFirstFileType(dir, DataLayer.FileType.DATA);
            final TableMetadata metaData = schema.schemaBuilder(partitioner).tableMetaData();
            final TestDataLayer dataLayer = new TestDataLayer(bridge, Collections.singletonList(dataFile));
            final Range<BigInteger> sparkTokenRange;
            switch (partitioner)
            {
                case Murmur3Partitioner:
                    sparkTokenRange = Range.closed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(3074457345618258602L));
                    break;
                case RandomPartitioner:
                    sparkTokenRange = Range.closed(BigInteger.ZERO, new BigInteger("916176208424801638531839357843455255"));
                    break;
                default:
                    throw new RuntimeException("Unexpected partitioner: " + partitioner);
            }
            final RangeFilter rangeFilter = RangeFilter.create(sparkTokenRange);
            final AtomicInteger skipCount = new AtomicInteger(0);
            final AtomicBoolean pass = new AtomicBoolean(true);
            final Stats stats = new Stats()
            {
                @Override
                public void skippedPartition(ByteBuffer key, BigInteger token)
                {
                    LOGGER.info("Skipping partition: " + token);
                    skipCount.incrementAndGet();
                    if (sparkTokenRange.contains(token))
                    {
                        LOGGER.info("Should not skip partition: " + token);
                        pass.set(false);
                    }
                }
            };
            final FourZeroSSTableReader reader = openReader(metaData, dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find SSTable")), rangeFilter, false, stats);
            final int rows = countAndValidateRows(reader);
            assertTrue(skipCount.get() > 0);
            assertEquals((NUM_ROWS - skipCount.get()) * NUM_COLS, rows); // should skip out of range partitions here
            assertTrue(pass.get());
        });
    }

    @Test
    public void testSkipPartitionsCompactionScanner()
    {
        runTest((partitioner, dir, bridge) -> {
            // write an SSTable
            final TestSchema schema = TestSchema.basic(bridge);
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    for (int j = 0; j < NUM_COLS; j++)
                    {
                        writer.write(i, j, i + j);
                    }
                }
            });
            assertEquals(1, countSSTables(dir));

            final Path dataFile = getFirstFileType(dir, DataLayer.FileType.DATA);
            final TableMetadata metaData = schema.schemaBuilder(partitioner).tableMetaData();
            final Set<SparkSSTableReader> readers = new HashSet<>(1);
            final TestDataLayer dataLayer = new TestDataLayer(bridge, Collections.singletonList(dataFile), schema.buildSchema())
            {
                @Override
                public SSTablesSupplier sstables(final int partitionId,
                                                 final RangeFilter rangeFilter,
                                                 final List<PartitionKeyFilter> partitionKeyFilters)
                {
                    return new SSTablesSupplier()
                    {
                        @Override
                        public <T extends SparkSSTableReader> Set<T> openAll(ReaderOpener<T> readerOpener)
                        {
                            return (Set<T>) readers;
                        }
                    };
                }
            };
            final Range<BigInteger> sparkTokenRange;
            switch (partitioner)
            {
                case Murmur3Partitioner:
                    sparkTokenRange = Range.closed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(3074457345618258602L));
                    break;
                case RandomPartitioner:
                    sparkTokenRange = Range.closed(BigInteger.ZERO, new BigInteger("916176208424801638531839357843455255"));
                    break;
                default:
                    throw new RuntimeException("Unexpected partitioner: " + partitioner);
            }
            final RangeFilter rangeFilter = RangeFilter.create(sparkTokenRange);
            final AtomicBoolean pass = new AtomicBoolean(true);
            final AtomicInteger skipCount = new AtomicInteger(0);
            final Stats stats = new Stats()
            {
                @Override
                public void skippedPartition(ByteBuffer key, BigInteger token)
                {
                    LOGGER.info("Skipping partition: " + token);
                    skipCount.incrementAndGet();
                    if (sparkTokenRange.contains(token))
                    {
                        LOGGER.info("Should not skip partition: " + token);
                        pass.set(false);
                    }
                }
            };
            final FourZeroSSTableReader reader = openReader(metaData, dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find SSTable")), rangeFilter, false, stats);
            readers.add(reader);

            // read the SSTable end-to-end using SparkRowIterator and verify it skips the required partitions
            // and all the partitions returned are within the Spark token range.
            final SparkRowIterator it = new SparkRowIterator(0, dataLayer);
            int count = 0;
            while (it.next())
            {
                final InternalRow row = it.get();
                assertEquals(row.getInt(2), row.getInt(0) + row.getInt(1));
                final DecoratedKey key = FourZero.getPartitioner(partitioner).decorateKey((ByteBuffer) ByteBuffer.allocate(4).putInt(row.getInt(0)).flip());
                final BigInteger token = FourZeroUtils.tokenToBigInteger(key.getToken());
                assertTrue(sparkTokenRange.contains(token));
                count++;
            }
            assertTrue(skipCount.get() > 0);
            assertEquals((NUM_ROWS - skipCount.get()) * NUM_COLS, count); // should skip out of range partitions here
            assertTrue(pass.get());
        });
    }

    @Test
    public void testOpenCompactionScanner()
    {
        runTest((partitioner, dir, bridge) -> {
            // write 3 SSTables
            final TestSchema schema = TestSchema.basic(bridge);
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    for (int j = 0; j < NUM_COLS; j++)
                    {
                        writer.write(i, j, -1);
                    }
                }
            });
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    for (int j = 0; j < NUM_COLS; j++)
                    {
                        writer.write(i, j, -2);
                    }
                }
            });
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    for (int j = 0; j < NUM_COLS; j++)
                    {
                        writer.write(i, j, i + j);
                    }
                }
            });
            assertEquals(3, countSSTables(dir));

            // open CompactionStreamScanner over 3 SSTables
            final TableMetadata metaData = schema.schemaBuilder(partitioner).tableMetaData();
            final TestDataLayer dataLayer = new TestDataLayer(bridge, getFileType(dir, DataLayer.FileType.DATA).collect(Collectors.toList()));
            final Set<FourZeroSSTableReader> toCompact = dataLayer.listSSTables().map(ssTable -> openReader(metaData, ssTable)).collect(Collectors.toSet());

            int count = 0;
            try (final CompactionStreamScanner scanner = new CompactionStreamScanner(metaData, partitioner, toCompact))
            {
                // iterate through CompactionStreamScanner verifying it correctly compacts data together
                final Rid rid = scanner.data();
                while (scanner.next())
                {
                    scanner.advanceToNextColumn();

                    // extract partition key value
                    final int a = rid.getPartitionKey().asIntBuffer().get();

                    // extract clustering key value and column name
                    final ByteBuffer colBuf = rid.getColumnName();
                    final ByteBuffer clusteringKey = ByteBufUtils.readBytesWithShortLength(colBuf);
                    colBuf.get();
                    final String colName = ByteBufUtils.string(ByteBufUtils.readBytesWithShortLength(colBuf));
                    colBuf.get();
                    if (StringUtils.isEmpty(colName))
                    {
                        continue;
                    }
                    assertEquals("c", colName);
                    final int b = clusteringKey.asIntBuffer().get();

                    // extract value column
                    final int c = rid.getValue().asIntBuffer().get();

                    // verify CompactionIterator compacts 3 sstables to use last values written
                    assertEquals(c, a + b);
                    count++;
                }
            }
            assertEquals(NUM_ROWS * NUM_COLS, count);
        });
    }

    @Test
    public void testFiltersDoNotMatch()
    {
        runTest((partitioner, dir, bridge) -> {
            // write an SSTable
            final TestSchema schema = TestSchema.basic(bridge);
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    for (int j = 0; j < NUM_COLS; j++)
                    {
                        writer.write(i, j, i + j);
                    }
                }
            });
            assertEquals(1, countSSTables(dir));

            final Path dataFile = getFirstFileType(dir, DataLayer.FileType.DATA);
            final TableMetadata metaData = new FourZeroSchemaBuilder(schema.createStmt, schema.keyspace, new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, ImmutableMap.of("replication_factor", 1)), partitioner).tableMetaData();
            final TestDataLayer dataLayer = new TestDataLayer(bridge, Collections.singletonList(dataFile));

            final BigInteger token = BigInteger.valueOf(9010454139840013626L);
            final RangeFilter outsideRange = RangeFilter.create(Range.closed(token, token));

            final AtomicBoolean pass = new AtomicBoolean(true);
            final AtomicInteger skipCount = new AtomicInteger(0);
            final Stats stats = new Stats()
            {
                @Override
                public void skippedSSTable(@Nullable final RangeFilter rangeFilter,
                                           @NotNull final List<PartitionKeyFilter> partitionKeyFilters,
                                           @NotNull BigInteger firstToken,
                                           @NotNull BigInteger lastToken)
                {
                    skipCount.incrementAndGet();
                    if (rangeFilter == null || partitionKeyFilters.size() != 0)
                    {
                        pass.set(false);
                    }
                }
            };
            final FourZeroSSTableReader reader = openReader(metaData, dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find SSTable")), outsideRange, true, stats);
            assertTrue(reader.ignore());
            assertEquals(1, skipCount.get());
            assertTrue(pass.get());
        });
    }

    @Test
    public void testFilterKeyMissingInIndex()
    {
        runTest((partitioner, dir, bridge) -> {
            // write an SSTable
            final TestSchema schema = TestSchema.basic(bridge);
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    for (int j = 0; j < NUM_COLS; j++)
                    {
                        writer.write(i, j, i + j);
                    }
                }
            });
            assertEquals(1, countSSTables(dir));

            final Path dataFile = getFirstFileType(dir, DataLayer.FileType.DATA);
            final TableMetadata metaData = new FourZeroSchemaBuilder(schema.createStmt, schema.keyspace, new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, ImmutableMap.of("replication_factor", 1)), partitioner).tableMetaData();
            final TestDataLayer dataLayer = new TestDataLayer(bridge, Collections.singletonList(dataFile));

            final ByteBuffer key1 = Int32Type.instance.fromString("51");
            final BigInteger token1 = bridge.hash(partitioner, key1);
            final PartitionKeyFilter keyNotInSSTable1 = PartitionKeyFilter.create(key1, token1);
            final ByteBuffer key2 = Int32Type.instance.fromString("90");
            final BigInteger token2 = bridge.hash(partitioner, key2);
            final PartitionKeyFilter keyNotInSSTable2 = PartitionKeyFilter.create(key2, token2);
            final List<PartitionKeyFilter> partitionKeyFilters = Arrays.asList(keyNotInSSTable1, keyNotInSSTable2);

            final AtomicBoolean pass = new AtomicBoolean(true);
            final AtomicInteger skipCount = new AtomicInteger(0);
            final Stats stats = new Stats()
            {
                @Override
                public void skippedSSTable(@Nullable final RangeFilter rangeFilter,
                                           @NotNull final List<PartitionKeyFilter> partitionKeyFilters,
                                           @NotNull BigInteger firstToken,
                                           @NotNull BigInteger lastToken)
                {
                    pass.set(false);
                }

                @Override
                public void missingInIndex()
                {
                    skipCount.incrementAndGet();
                    if (partitionKeyFilters.size() != 2)
                    {
                        pass.set(false);
                    }
                }
            };
            final FourZeroSSTableReader reader = openReader(metaData, dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find SSTable")), partitionKeyFilters, true, stats);
            assertTrue(reader.ignore());
            assertEquals(1, skipCount.get());
            assertTrue(pass.get());
        });
    }

    @Test
    public void testPartialFilterMatch()
    {
        runTest((partitioner, dir, bridge) -> {
            // write an SSTable
            final TestSchema schema = TestSchema.basic(bridge);
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    for (int j = 0; j < NUM_COLS; j++)
                    {
                        writer.write(i, j, i + j);
                    }
                }
            });
            assertEquals(1, countSSTables(dir));

            final Path dataFile = getFirstFileType(dir, DataLayer.FileType.DATA);
            final TableMetadata metaData = new FourZeroSchemaBuilder(schema.createStmt, schema.keyspace, new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, ImmutableMap.of("replication_factor", 1)), partitioner).tableMetaData();
            final TestDataLayer dataLayer = new TestDataLayer(bridge, Collections.singletonList(dataFile));

            final ByteBuffer key1 = Int32Type.instance.fromString("0");
            final BigInteger token1 = bridge.hash(partitioner, key1);
            final PartitionKeyFilter keyInSSTable = PartitionKeyFilter.create(key1, token1);
            final RangeFilter rangeFilter = RangeFilter.create(Range.closed(token1, token1));

            final ByteBuffer key2 = Int32Type.instance.fromString("55");
            final BigInteger token2 = bridge.hash(partitioner, key2);
            final PartitionKeyFilter keyNotInSSTable = PartitionKeyFilter.create(key2, token2);
            final List<PartitionKeyFilter> partitionKeyFilters = Arrays.asList(keyInSSTable, keyNotInSSTable);

            final AtomicBoolean pass = new AtomicBoolean(true);
            final AtomicInteger skipCount = new AtomicInteger(0);
            final Stats stats = new Stats()
            {
                @Override
                public void skippedPartition(ByteBuffer key, BigInteger token)
                {
                    LOGGER.info("Skipping partition: " + token);
                    skipCount.incrementAndGet();
                    if (partitionKeyFilters.stream().anyMatch(filter -> filter.matches(key)))
                    {
                        LOGGER.info("Should not skip partition: " + token);
                        pass.set(false);
                    }
                }
            };
            final FourZeroSSTableReader reader = openReader(metaData, dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find SSTable")), rangeFilter, partitionKeyFilters, false, stats);
            final int rows = countAndValidateRows(reader);
            assertTrue(skipCount.get() > 0);
            assertEquals(NUM_COLS, rows);
            assertEquals((NUM_ROWS - skipCount.get()) * NUM_COLS, rows); // should skip partitions not matching filters
            assertTrue(pass.get());
        });
    }

    private static FourZeroSSTableReader openReader(final TableMetadata metaData, final DataLayer.SSTable ssTable)
    {
        return openReader(metaData, ssTable, null, new ArrayList<>(), true, Stats.DoNothingStats.INSTANCE);
    }

    private static FourZeroSSTableReader openReader(final TableMetadata metaData,
                                                    final DataLayer.SSTable ssTable,
                                                    final RangeFilter rangeFilter)
    {
        return openReader(metaData, ssTable, rangeFilter, Collections.emptyList(), true, Stats.DoNothingStats.INSTANCE);
    }

    private static FourZeroSSTableReader openReader(final TableMetadata metaData,
                                                    final DataLayer.SSTable ssTable,
                                                    final RangeFilter rangeFilter,
                                                    final boolean readIndexOffset,
                                                    final Stats stats)
    {
        return openReader(metaData, ssTable, rangeFilter, new ArrayList<>(), readIndexOffset, stats);
    }

    private static FourZeroSSTableReader openReader(final TableMetadata metaData,
                                                    final DataLayer.SSTable ssTable,
                                                    final List<PartitionKeyFilter> partitionKeyFilters,
                                                    final boolean readIndexOffset,
                                                    final Stats stats)
    {
        return openReader(metaData, ssTable, null, partitionKeyFilters, readIndexOffset, stats);
    }

    private static FourZeroSSTableReader openReader(final TableMetadata metaData,
                                                    final DataLayer.SSTable ssTable,
                                                    final RangeFilter rangeFilter,
                                                    final List<PartitionKeyFilter> partitionKeyFilters,
                                                    final boolean readIndexOffset,
                                                    final Stats stats)
    {
        try
        {
            return FourZeroSSTableReader.builder(metaData, ssTable)
                                        .withRangeFilter(rangeFilter)
                                        .withPartitionKeyFilters(partitionKeyFilters)
                                        .withReadIndexOffset(readIndexOffset)
                                        .withStats(stats)
                                        .build();
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static int countAndValidateRows(@NotNull final FourZeroSSTableReader reader)
    {
        final ISSTableScanner scanner = reader.scanner();
        int count = 0;
        while (scanner.hasNext())
        {
            final UnfilteredRowIterator it = scanner.next();
            while (it.hasNext())
            {
                final BufferDecoratedKey key = (BufferDecoratedKey) it.partitionKey();
                final int a = key.getKey().asIntBuffer().get();
                final Unfiltered unfiltered = it.next();
                assertTrue(unfiltered.isRow());
                final AbstractRow row = (AbstractRow) unfiltered;
                final int b = row.clustering().bufferAt(0).asIntBuffer().get();
                for (final ColumnData data : row)
                {
                    final Cell cell = (Cell) data;
                    final int c = cell.buffer().getInt();
                    assertEquals(c, a + b);
                    count++;
                }
            }
        }
        return count;
    }

    @Test
    public void testConstructFilename()
    {
        // Standard SS table data file name
        assertEquals(new File("./keyspace/table/na-1-big-Data.db"), FourZeroSSTableReader.constructFilename("keyspace", "table", "na-1-big-Data.db"));

        // Non-standard SS table data file name
        assertEquals(new File("./keyspace/table/na-1-big-Data.db"), FourZeroSSTableReader.constructFilename("keyspace", "table", "keyspace-table-na-1-big-Data.db"));

        // Malformed SS table data file names
        assertEquals(new File("./keyspace/table/keyspace-table-qwerty-na-1-big-Data.db"), FourZeroSSTableReader.constructFilename("keyspace", "table", "keyspace-table-qwerty-na-1-big-Data.db"));
        assertEquals(new File("./keyspace/table/keyspace-qwerty-na-1-big-Data.db"), FourZeroSSTableReader.constructFilename("keyspace", "table", "keyspace-qwerty-na-1-big-Data.db"));
        assertEquals(new File("./keyspace/table/qwerty-table-na-1-big-Data.db"), FourZeroSSTableReader.constructFilename("keyspace", "table", "qwerty-table-na-1-big-Data.db"));
        assertEquals(new File("./keyspace/table/keyspace-na-1-big-Data.db"), FourZeroSSTableReader.constructFilename("keyspace", "table", "keyspace-na-1-big-Data.db"));
        assertEquals(new File("./keyspace/table/table-na-1-big-Data.db"), FourZeroSSTableReader.constructFilename("keyspace", "table", "table-na-1-big-Data.db"));
        assertEquals(new File("./keyspace/table/qwerty"), FourZeroSSTableReader.constructFilename("keyspace", "table", "qwerty"));
    }

    @Test
    public void testExtractRangeSparkFilter()
    {
        final Optional<Range<BigInteger>> r1 = FourZeroSSTableReader.extractRange(RangeFilter.create(Range.closed(BigInteger.valueOf(5L), BigInteger.valueOf(500L))), Collections.emptyList());
        assertTrue(r1.isPresent());
        assertEquals(BigInteger.valueOf(5L), r1.get().lowerEndpoint());
        assertEquals(BigInteger.valueOf(500L), r1.get().upperEndpoint());

        final Optional<Range<BigInteger>> r2 = FourZeroSSTableReader.extractRange(RangeFilter.create(Range.closed(BigInteger.valueOf(-10000L), BigInteger.valueOf(29593L))), Collections.emptyList());
        assertTrue(r2.isPresent());
        assertEquals(BigInteger.valueOf(-10000L), r2.get().lowerEndpoint());
        assertEquals(BigInteger.valueOf(29593L), r2.get().upperEndpoint());

        assertFalse(FourZeroSSTableReader.extractRange(null, Collections.emptyList()).isPresent());
    }

    @Test
    public void testExtractRangePartitionKeyFilters()
    {
        final List<ByteBuffer> keys = new ArrayList<>();
        for (int i = 0; i < 1000; i++)
        {
            keys.add((ByteBuffer) ByteBuffer.allocate(4).putInt(i).flip());
        }

        final List<PartitionKeyFilter> partitionKeyFilters = keys.stream().map(b -> {
            final BigInteger token = FourZeroUtils.tokenToBigInteger(Murmur3Partitioner.instance.getToken(b).getToken());
            return PartitionKeyFilter.create(b, token);
        }).collect(Collectors.toList());

        final Range<BigInteger> sparkRange = Range.closed(new BigInteger("0"), new BigInteger("2305843009213693952"));
        final RangeFilter rangeFilter = RangeFilter.create(sparkRange);
        final List<PartitionKeyFilter> inRangePartitionKeyFilters = partitionKeyFilters.stream().filter(t -> sparkRange.contains(t.token())).collect(Collectors.toList());
        assertTrue(inRangePartitionKeyFilters.size() > 1);

        final Optional<Range<BigInteger>> range = FourZeroSSTableReader.extractRange(rangeFilter, inRangePartitionKeyFilters);
        assertTrue(range.isPresent());
        assertNotEquals(sparkRange, range.get());
        assertTrue(sparkRange.lowerEndpoint().compareTo(range.get().lowerEndpoint()) < 0);
        assertTrue(sparkRange.upperEndpoint().compareTo(range.get().upperEndpoint()) > 0);
    }

    // incremental repair

    @Test
    public void testIncrementalRepair()
    {
        runTest((partitioner, dir, bridge) -> {
            final TestSchema schema = TestSchema.basic(bridge);
            final int numSSTables = 4;
            final int numRepaired = 2;
            final int numUnRepaired = numSSTables - numRepaired;

            // write some SSTables
            for (int a = 0; a < numSSTables; a++)
            {
                final int pos = a * NUM_ROWS;
                TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                    for (int i = pos; i < pos + NUM_ROWS; i++)
                    {
                        for (int j = 0; j < NUM_COLS; j++)
                        {
                            writer.write(i, j, i + j);
                        }
                    }
                });
            }
            assertEquals(numSSTables, countSSTables(dir));

            final TableMetadata metaData = new FourZeroSchemaBuilder(schema.createStmt, schema.keyspace, new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, ImmutableMap.of("replication_factor", 1)), partitioner).tableMetaData();
            final TestDataLayer dataLayer = new TestDataLayer(bridge, getFileType(dir, DataLayer.FileType.DATA).collect(Collectors.toList()));

            final AtomicInteger skipCount = new AtomicInteger(0);
            final Stats stats = new Stats()
            {
                @Override
                public void skippedRepairedSSTable(DataLayer.SSTable ssTable, long repairedAt)
                {
                    skipCount.incrementAndGet();
                }
            };

            // mark some SSTables as repaired
            final Map<DataLayer.SSTable, Boolean> isRepaired = dataLayer.listSSTables().collect(Collectors.toMap(Function.identity(), a -> false));
            int count = 0;
            for (final DataLayer.SSTable ssTable : isRepaired.keySet())
            {
                if (count < numRepaired)
                {
                    isRepaired.put(ssTable, true);
                    count++;
                }
            }

            final List<FourZeroSSTableReader> primaryReaders = dataLayer.listSSTables()
                                                                        .map(ssTable -> openIncrementalReader(metaData, ssTable, stats, true, isRepaired.get(ssTable)))
                                                                        .filter(reader -> !reader.ignore())
                                                                        .collect(Collectors.toList());
            final List<FourZeroSSTableReader> nonPrimaryReaders = dataLayer.listSSTables()
                                                                           .map(ssTable -> openIncrementalReader(metaData, ssTable, stats, false, isRepaired.get(ssTable)))
                                                                           .filter(reader -> !reader.ignore())
                                                                           .collect(Collectors.toList());

            // primary repair replica should read all sstables
            assertEquals(numSSTables, primaryReaders.size());

            // non-primary repair replica should only read unrepaired sstables
            assertEquals(numUnRepaired, nonPrimaryReaders.size());
            for (final FourZeroSSTableReader reader : nonPrimaryReaders)
            {
                assertFalse(isRepaired.get(reader.sstable()));
            }
            assertEquals(numUnRepaired, skipCount.get());


            final Set<FourZeroSSTableReader> toCompact = Stream
            .concat(primaryReaders.stream().filter(r -> isRepaired.get(r.sstable())),
                    nonPrimaryReaders.stream())
            .collect(Collectors.toSet());
            assertEquals(numSSTables, toCompact.size());

            int rowCount = 0;
            boolean[] found = new boolean[numSSTables * NUM_ROWS];
            try (final CompactionStreamScanner scanner = new CompactionStreamScanner(metaData, partitioner, toCompact))
            {
                // iterate through CompactionScanner and verify we have all the partition keys we are looking for
                final Rid rid = scanner.data();
                while (scanner.next())
                {
                    scanner.advanceToNextColumn();
                    final int a = rid.getPartitionKey().asIntBuffer().get();
                    found[a] = true;
                    // extract clustering key value and column name
                    final ByteBuffer colBuf = rid.getColumnName();
                    final ByteBuffer clusteringKey = ByteBufUtils.readBytesWithShortLength(colBuf);
                    colBuf.get();
                    final String colName = ByteBufUtils.string(ByteBufUtils.readBytesWithShortLength(colBuf));
                    colBuf.get();
                    if (StringUtils.isEmpty(colName))
                    {
                        continue;
                    }
                    assertEquals("c", colName);
                    final int b = clusteringKey.asIntBuffer().get();

                    // extract value column
                    final int c = rid.getValue().asIntBuffer().get();

                    assertEquals(c, a + b);
                    rowCount++;
                }
            }
            assertEquals(numSSTables * NUM_ROWS * NUM_COLS, rowCount);
            for (final boolean b : found)
            {
                assertTrue(b);
            }
        });
    }

    @Test
    public void testPartitionKeyFilter()
    {
        runTest((partitioner, dir, bridge) -> {
            final TestSchema schema = TestSchema.builder().withPartitionKey("a", bridge.text())
                                                .withClusteringKey("b", bridge.aInt())
                                                .withColumn("c", bridge.aInt())
                                                .withColumn("d", bridge.text())
                                                .build();
            final CqlTable cqlTable = schema.buildSchema();
            final int numSSTables = 24;
            final String partitionKeyStr = (String) bridge.text().randomValue(1024);
            final Pair<ByteBuffer, BigInteger> partitionKey = bridge.getPartitionKey(cqlTable, partitioner, Collections.singletonList(partitionKeyStr));
            final PartitionKeyFilter partitionKeyFilter = PartitionKeyFilter.create(partitionKey.getLeft(), partitionKey.getRight());
            final RangeFilter rangeFilter = RangeFilter.create(Range.closed(partitioner.minToken(), partitioner.maxToken()));
            final Integer[] expectedC = new Integer[NUM_COLS];
            final String[] expectedD = new String[NUM_COLS];

            // write some SSTables
            for (int i = 0; i < numSSTables; i++)
            {
                final boolean isLastSSTable = i == numSSTables - 1;
                TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                    if (isLastSSTable)
                    {
                        // write partition key in last sstable only
                        for (int b = 0; b < NUM_COLS; b++)
                        {
                            expectedC[b] = (int) bridge.aInt().randomValue(1024);
                            expectedD[b] = (String) bridge.text().randomValue(1024);
                            writer.write(partitionKeyStr, b, expectedC[b], expectedD[b]);
                        }
                    }

                    for (int j = 0; j < 2; j++)
                    {
                        for (int k = 0; k < NUM_COLS; k++)
                        {
                            String key = null;
                            while (key == null || key.equals(partitionKeyStr))
                            {
                                key = (String) bridge.text().randomValue(1024);
                            }
                            writer.write(key,
                                         j,
                                         bridge.aInt().randomValue(1024),
                                         bridge.text().randomValue(1024));
                        }
                    }
                });
            }

            final TableMetadata metaData = new FourZeroSchemaBuilder(schema.createStmt, schema.keyspace, new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, ImmutableMap.of("replication_factor", 1)), partitioner).tableMetaData();
            final TestDataLayer dataLayer = new TestDataLayer(bridge, getFileType(dir, DataLayer.FileType.DATA).collect(Collectors.toList()));
            final List<DataLayer.SSTable> ssTables = dataLayer.listSSTables().collect(Collectors.toList());
            assertEquals(numSSTables, ssTables.size());

            final Set<String> keys = new HashSet<>();
            for (final DataLayer.SSTable ssTable : ssTables)
            {
                final FourZeroSSTableReader reader = readerBuilder(metaData, ssTable, Stats.DoNothingStats.INSTANCE, true, false)
                                                     .withPartitionKeyFilter(partitionKeyFilter)
                                                     .withRangeFilter(rangeFilter)
                .build();
                if (reader.ignore())
                {
                    continue;
                }

                final ISSTableScanner scanner = reader.scanner();
                int colCount = 0;
                while (scanner.hasNext())
                {
                    final UnfilteredRowIterator it = scanner.next();
                    it.partitionKey().getKey().mark();
                    final String key = UTF8Serializer.instance.deserialize(it.partitionKey().getKey());
                    it.partitionKey().getKey().reset();
                    keys.add(key);
                    while (it.hasNext())
                    {
                        it.next();
                        colCount++;
                    }
                }
                assertEquals(NUM_COLS, colCount);
            }
            assertEquals(1, keys.size());
            assertEquals(partitionKeyStr, keys.stream().findFirst().orElseThrow(() -> new RuntimeException("No partition keys returned")));
        });
    }

    private static FourZeroSSTableReader.Builder readerBuilder(final TableMetadata metadata,
                                                               final DataLayer.SSTable ssTable,
                                                               final Stats stats,
                                                               final boolean isRepairPrimary,
                                                               final boolean isRepaired)
    {
        return FourZeroSSTableReader.builder(metadata, ssTable)
                                    .withReadIndexOffset(true)
                                    .withStats(stats)
                                    .isRepairPrimary(isRepairPrimary)
                                    .withIsRepairedFunction((statsMetadata -> isRepaired));
    }

    private static FourZeroSSTableReader openIncrementalReader(final TableMetadata metadata,
                                                               final DataLayer.SSTable ssTable,
                                                               final Stats stats,
                                                               final boolean isRepairPrimary,
                                                               final boolean isRepaired)
    {
        try
        {
            return readerBuilder(metadata, ssTable, stats, isRepairPrimary, isRepaired)
            .useIncrementalRepair(true)
            .build();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

}
