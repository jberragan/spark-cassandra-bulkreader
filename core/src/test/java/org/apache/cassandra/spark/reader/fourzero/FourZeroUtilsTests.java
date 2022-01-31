package org.apache.cassandra.spark.reader.fourzero;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Range;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DecoratedKey;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.SerializationHeader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.IPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.spark.sparksql.filters.CustomFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;

import static org.apache.cassandra.spark.TestUtils.NUM_COLS;
import static org.apache.cassandra.spark.TestUtils.NUM_ROWS;
import static org.apache.cassandra.spark.TestUtils.countSSTables;
import static org.apache.cassandra.spark.TestUtils.getFirstFileType;
import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
public class FourZeroUtilsTests
{
    @Test
    public void testReadStatsMetaData()
    {
        runTest((partitioner, dir, bridge) -> {
                    // write an SSTable
                    final TestSchema schema = TestSchema.basic(bridge);
                    final long nowMicros = System.currentTimeMillis() * 1000;
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
                    final Descriptor descriptor = Descriptor.fromFilename(new File(String.format("./%s/%s", schema.keyspace, schema.table), dataFile.getFileName().toString()));
                    final Path statsFile = getFirstFileType(dir, DataLayer.FileType.STATISTICS);

                    // deserialize stats meta data and verify components match expected values
                    final Map<MetadataType, MetadataComponent> componentMap;
                    try (InputStream in = new BufferedInputStream(Files.newInputStream(statsFile)))
                    {
                        componentMap = FourZeroUtils.deserializeStatsMetadata(in, EnumSet.allOf(MetadataType.class), descriptor);
                    }
                    assertNotNull(componentMap);
                    assertFalse(componentMap.isEmpty());

                    final ValidationMetadata validationMetadata = (ValidationMetadata) componentMap.get(MetadataType.VALIDATION);
                    assertEquals(FourZeroSchemaBuilder.SHADED_PACKAGE_NAME + "dht." + partitioner.name(), validationMetadata.partitioner);

                    final CompactionMetadata compactionMetadata = (CompactionMetadata) componentMap.get(MetadataType.COMPACTION);
                    assertNotNull(compactionMetadata);

                    final StatsMetadata statsMetadata = (StatsMetadata) componentMap.get(MetadataType.STATS);
                    assertEquals(NUM_ROWS * NUM_COLS, statsMetadata.totalRows);
                    assertEquals(0L, statsMetadata.repairedAt);
                    final long tolerance = TimeUnit.MICROSECONDS.convert(10, TimeUnit.SECONDS); // want to avoid test flakiness but timestamps should be in same ball park
                    assertTrue(Math.abs(statsMetadata.maxTimestamp - nowMicros) < tolerance);
                    assertTrue(Math.abs(statsMetadata.minTimestamp - nowMicros) < tolerance);

                    final SerializationHeader.Component header = (SerializationHeader.Component) componentMap.get(MetadataType.HEADER);
                    assertNotNull(header);
                    assertEquals(FourZeroSchemaBuilder.SHADED_PACKAGE_NAME + "db.marshal.Int32Type", header.getKeyType().toString());
                    final List<AbstractType<?>> clusteringTypes = header.getClusteringTypes();
                    assertEquals(1, clusteringTypes.size());
                    assertEquals(FourZeroSchemaBuilder.SHADED_PACKAGE_NAME + "db.marshal.Int32Type", clusteringTypes.get(0).toString());
                    assertTrue(header.getStaticColumns().isEmpty());
                    final List<AbstractType<?>> regulars = new ArrayList<>(header.getRegularColumns().values());
                    assertEquals(1, regulars.size());
                    assertEquals(FourZeroSchemaBuilder.SHADED_PACKAGE_NAME + "db.marshal.Int32Type", regulars.get(0).toString());
                }
        );
    }

    @Test
    public void testReadFirstLastPartitionKey()
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

                    // read Summary.db file for first and last partition keys from Summary.db
                    final Path summaryFile = getFirstFileType(dir, DataLayer.FileType.SUMMARY);
                    final SummaryDbUtils.Summary summaryKeys;
                    try (final InputStream in = new BufferedInputStream(Files.newInputStream(summaryFile)))
                    {
                        summaryKeys = SummaryDbUtils.readSummary(in, Murmur3Partitioner.instance, 128, 2048);
                    }
                    assertNotNull(summaryKeys);
                    assertNotNull(summaryKeys.first());
                    assertNotNull(summaryKeys.last());

                    // read Primary Index.db file for first and last partition keys from Summary.db
                    final Path indexFile = getFirstFileType(dir, DataLayer.FileType.INDEX);
                    final Pair<DecoratedKey, DecoratedKey> indexKeys;
                    try (final InputStream in = new BufferedInputStream(Files.newInputStream(indexFile)))
                    {
                        final Pair<ByteBuffer, ByteBuffer> keys = FourZeroUtils.readPrimaryIndex(in, true, Collections.emptyList());
                        indexKeys = Pair.of(Murmur3Partitioner.instance.decorateKey(keys.getLeft()), Murmur3Partitioner.instance.decorateKey(keys.getRight()));
                    }
                    assertNotNull(indexKeys);
                    assertEquals(indexKeys.getLeft(), summaryKeys.first());
                    assertEquals(indexKeys.getRight(), summaryKeys.last());
                }
        );
    }

    @Test
    public void testSearchInBloomFilter()
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

                    final ByteBuffer key1 = Int32Type.instance.fromString("1");
                    final BigInteger token1 = bridge.hash(partitioner, key1);
                    final PartitionKeyFilter keyInSSTable = PartitionKeyFilter.create(key1, token1);

                    final ByteBuffer key2 = Int32Type.instance.fromString("51");
                    final BigInteger token2 = bridge.hash(partitioner, key2);
                    final SparkRangeFilter rangeFilter = SparkRangeFilter.create(Range.closed(token1, token2));

                    // read Filter.db file
                    final Path filterFile = getFirstFileType(dir, DataLayer.FileType.FILTER);
                    final Descriptor descriptor = Descriptor.fromFilename(filterFile.toFile().getName());
                    IPartitioner iPartitioner;
                    switch (partitioner)
                    {
                        case Murmur3Partitioner:
                            iPartitioner = Murmur3Partitioner.instance;
                            break;
                        case RandomPartitioner:
                            iPartitioner = RandomPartitioner.instance;
                            break;
                        default:
                            throw new RuntimeException("Unexpected partitioner: " + partitioner.toString());
                    }

                    try (InputStream indexStream = new FileInputStream(filterFile.toString()))
                    {
                        final DataLayer.SSTable ssTable = mock(DataLayer.SSTable.class);
                        when(ssTable.openFilterStream()).thenReturn(indexStream);
                        final List<CustomFilter> filters = FourZeroUtils.filterKeyInBloomFilter(ssTable, iPartitioner, descriptor, Arrays.asList(keyInSSTable, rangeFilter));
                        assertEquals(1, filters.size());
                        assertEquals(keyInSSTable, filters.get(0));
                    }
                }
        );
    }

    @Test
    public void testSearchInIndexEmptyFilters()
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

                    final Path indexFile = getFirstFileType(dir, DataLayer.FileType.INDEX);
                    try (InputStream indexStream = new FileInputStream(indexFile.toString()))
                    {
                        final DataLayer.SSTable ssTable = mock(DataLayer.SSTable.class);
                        when(ssTable.openPrimaryIndexStream()).thenReturn(indexStream);
                        assertFalse(FourZeroUtils.anyFilterKeyInIndex(ssTable, Collections.emptyList()));
                    }
                }
        );
    }

    @Test
    public void testSearchInIndexKeyNotFound()
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

                    final ByteBuffer key2 = Int32Type.instance.fromString("51");
                    final BigInteger token2 = bridge.hash(partitioner, key2);
                    final PartitionKeyFilter keyNotInSSTable = PartitionKeyFilter.create(key2, token2);

                    final SparkRangeFilter rangeFilter = SparkRangeFilter.create(Range.closed(token2, token2));

                    final Path indexFile = getFirstFileType(dir, DataLayer.FileType.INDEX);
                    try (InputStream indexStream = new FileInputStream(indexFile.toString()))
                    {
                        final DataLayer.SSTable ssTable = mock(DataLayer.SSTable.class);
                        when(ssTable.openPrimaryIndexStream()).thenReturn(indexStream);
                        assertFalse(FourZeroUtils.anyFilterKeyInIndex(ssTable, Arrays.asList(keyNotInSSTable, rangeFilter)));
                    }
                }
        );
    }

    @Test
    public void testSearchInIndexKeyFound()
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

                    final ByteBuffer key1 = Int32Type.instance.fromString("19");
                    final BigInteger token1 = bridge.hash(partitioner, key1);
                    final PartitionKeyFilter keyInSSTable = PartitionKeyFilter.create(key1, token1);

                    final SparkRangeFilter rangeFilter = SparkRangeFilter.create(Range.closed(token1, token1));

                    final Path indexFile = getFirstFileType(dir, DataLayer.FileType.INDEX);
                    try (InputStream indexStream = new FileInputStream(indexFile.toString()))
                    {
                        final DataLayer.SSTable ssTable = mock(DataLayer.SSTable.class);
                        when(ssTable.openPrimaryIndexStream()).thenReturn(indexStream);
                        assertTrue(FourZeroUtils.anyFilterKeyInIndex(ssTable, Arrays.asList(rangeFilter, keyInSSTable)));
                    }
                }
        );
    }

}
