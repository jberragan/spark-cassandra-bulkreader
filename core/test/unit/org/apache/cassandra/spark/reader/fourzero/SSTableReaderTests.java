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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

import org.apache.cassandra.spark.stats.Stats;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.TestDataLayer;
import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
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
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.sparksql.CustomFilter;
import org.apache.cassandra.spark.sparksql.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.SparkRangeFilter;
import org.apache.cassandra.spark.sparksql.SparkRowIterator;
import org.apache.cassandra.spark.utils.ByteBufUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.spark.TestUtils.NUM_COLS;
import static org.apache.cassandra.spark.TestUtils.NUM_ROWS;
import static org.apache.cassandra.spark.TestUtils.countSSTables;
import static org.apache.cassandra.spark.TestUtils.getFileType;
import static org.apache.cassandra.spark.TestUtils.getFirstFileType;
import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
                    throw new RuntimeException("Unexpected partitioner: " + partitioner.toString());
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
            Pair<DecoratedKey, DecoratedKey> keys;
            try (final InputStream in = new BufferedInputStream(Files.newInputStream(summaryFile)))
            {
                keys = FourZeroUtils.readSummary(in, metaData.partitioner, metaData.params.minIndexInterval, metaData.params.maxIndexInterval);
            }
            // set Spark token range equal to SSTable token range
            final Range<BigInteger> sparkTokenRange = Range.closed(FourZeroUtils.tokenToBigInteger(keys.getLeft().getToken()), FourZeroUtils.tokenToBigInteger(keys.getRight().getToken()));
            final SparkRangeFilter rangeFilter = SparkRangeFilter.create(sparkTokenRange);
            final AtomicBoolean skipped = new AtomicBoolean(false);
            final Stats stats = new Stats()
            {
                @Override
                public void skipedPartition(ByteBuffer key, BigInteger token)
                {
                    LOGGER.error("Skipped partition when should not: " + token);
                    skipped.set(true);
                }
            };
            final FourZeroSSTableReader reader = openReader(metaData, dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find SSTable")), Collections.singletonList(rangeFilter), stats);
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
                    throw new RuntimeException("Unexpected partitioner: " + partitioner.toString());
            }
            final SparkRangeFilter rangeFilter = SparkRangeFilter.create(sparkTokenRange);
            final AtomicInteger skipCount = new AtomicInteger(0);
            final AtomicBoolean pass = new AtomicBoolean(true);
            final Stats stats = new Stats()
            {
                @Override
                public void skipedPartition(ByteBuffer key, BigInteger token)
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
            final FourZeroSSTableReader reader = openReader(metaData, dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find SSTable")), Collections.singletonList(rangeFilter), stats);
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
                public SSTablesSupplier sstables(final List<CustomFilter> filters)
                {
                    return new SSTablesSupplier()
                    {
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
                    throw new RuntimeException("Unexpected partitioner: " + partitioner.toString());
            }
            final SparkRangeFilter rangeFilter = SparkRangeFilter.create(sparkTokenRange);
            final AtomicBoolean pass = new AtomicBoolean(true);
            final AtomicInteger skipCount = new AtomicInteger(0);
            final Stats stats = new Stats()
            {
                @Override
                public void skipedPartition(ByteBuffer key, BigInteger token)
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
            final FourZeroSSTableReader reader = openReader(metaData, dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find SSTable")), Collections.singletonList(rangeFilter), stats);
            readers.add(reader);

            // read the SSTable end-to-end using SparkRowIterator and verify it skips the required partitions
            // and all the partitions returned are within the Spark token range.
            final SparkRowIterator it = new SparkRowIterator(dataLayer);
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
                final Rid rid = scanner.getRid();
                while (scanner.hasNext())
                {
                    scanner.next();

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
            final SparkRangeFilter outsideRange = SparkRangeFilter.create(Range.closed(token, token));
            final List<CustomFilter> filters = Collections.singletonList(outsideRange);

            final AtomicBoolean pass = new AtomicBoolean(true);
            final AtomicInteger skipCount = new AtomicInteger(0);
            final Stats stats = new Stats()
            {
                @Override
                public void skipedSSTable(List<CustomFilter> filters, BigInteger firstToken, BigInteger lastToken)
                {
                    skipCount.incrementAndGet();
                    if (filters.size() != 1)
                    {
                        pass.set(false);
                    }
                }
            };
            final FourZeroSSTableReader reader = openReader(metaData, dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find SSTable")), filters, stats);
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
            final List<CustomFilter> filters = Arrays.asList(keyNotInSSTable1, keyNotInSSTable2);

            final AtomicBoolean pass = new AtomicBoolean(true);
            final AtomicInteger skipCount = new AtomicInteger(0);
            final Stats stats = new Stats()
            {
                @Override
                public void skipedSSTable(List<CustomFilter> filters, BigInteger firstToken, BigInteger lastToken)
                {
                    pass.set(false);
                }

                @Override
                public void missingInIndex()
                {
                    skipCount.incrementAndGet();
                    if (filters.size() != 2)
                    {
                        pass.set(false);
                    }
                }
            };
            final FourZeroSSTableReader reader = openReader(metaData, dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find SSTable")), filters, stats);
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
            final SparkRangeFilter rangeFilter = SparkRangeFilter.create(Range.closed(token1, token1));

            final ByteBuffer key2 = Int32Type.instance.fromString("55");
            final BigInteger token2 = bridge.hash(partitioner, key2);
            final PartitionKeyFilter keyNotInSSTable = PartitionKeyFilter.create(key2, token2);
            final List<CustomFilter> filters = Arrays.asList(rangeFilter, keyInSSTable, keyNotInSSTable);

            final AtomicBoolean pass = new AtomicBoolean(true);
            final AtomicInteger skipCount = new AtomicInteger(0);
            final Stats stats = new Stats()
            {
                @Override
                public void skipedPartition(ByteBuffer key, BigInteger token)
                {
                    LOGGER.info("Skipping partition: " + token);
                    skipCount.incrementAndGet();
                    if (filters.stream().anyMatch(filter -> !filter.skipPartition(key, token)))
                    {
                        LOGGER.info("Should not skip partition: " + token);
                        pass.set(false);
                    }
                }
            };
            final FourZeroSSTableReader reader = openReader(metaData, dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find SSTable")), filters, stats);
            final int rows = countAndValidateRows(reader);
            assertTrue(skipCount.get() > 0);
            assertEquals(NUM_COLS, rows);
            assertEquals((NUM_ROWS - skipCount.get()) * NUM_COLS, rows); // should skip partitions not matching filters
            assertTrue(pass.get());
        });
    }

    private static FourZeroSSTableReader openReader(final TableMetadata metaData, final DataLayer.SSTable ssTable)
    {
        return openReader(metaData, ssTable, new ArrayList<>(), Stats.DoNothingStats.INSTANCE);
    }

    private static FourZeroSSTableReader openReader(final TableMetadata metaData, final DataLayer.SSTable ssTable, final List<CustomFilter> filters)
    {
        return openReader(metaData, ssTable, filters, Stats.DoNothingStats.INSTANCE);
    }

    private static FourZeroSSTableReader openReader(final TableMetadata metaData, final DataLayer.SSTable ssTable, final List<CustomFilter> filters, final Stats stats)
    {
        try
        {
            return new FourZeroSSTableReader(metaData, ssTable, filters, stats);
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static int countAndValidateRows(@NotNull final FourZeroSSTableReader reader)
    {
        final ISSTableScanner scanner = reader.getScanner();
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
                final int b = row.clustering().get(0).asIntBuffer().get();
                for (final ColumnData data : row)
                {
                    final Cell cell = (Cell) data;
                    final int c = cell.value().getInt();
                    assertEquals(c, a + b);
                    count++;
                }
            }
        }
        return count;
    }
}
