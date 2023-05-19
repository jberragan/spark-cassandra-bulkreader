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

package org.apache.cassandra.spark.cdc;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.spark.SparkTestUtils;
import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.cdc.jdk.TestJdkCdcIterator;
import org.apache.cassandra.spark.cdc.jdk.msg.CdcMessage;
import org.apache.cassandra.spark.cdc.jdk.msg.Column;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SparkCqlField;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.reader.fourzero.FourZeroSchemaBuilder;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.BufferingCommitLogReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.CellPath;
import org.apache.cassandra.spark.stats.CdcStats;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.cdc.CdcTester.DEFAULT_NUM_ROWS;
import static org.apache.cassandra.spark.cdc.CdcTester.LOG;
import static org.apache.cassandra.spark.cdc.CdcTester.assertCqlTypeEquals;
import static org.apache.cassandra.spark.cdc.CdcTester.testWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.quicktheories.QuickTheory.qt;

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

public class CdcTests extends VersionRunner
{
    private static final int TTL = 42;

    @ClassRule
    public static TemporaryFolder DIR = new TemporaryFolder();

    @BeforeClass
    public static void setup()
    {
        CdcTester.setup(DIR);
    }

    @AfterClass
    public static void tearDown()
    {
        CdcTester.tearDown();
    }

    public CdcTests(CassandraVersion version)
    {
        super(version);
    }

    public static final TestStats STATS = new TestStats();

    @Test
    public void testSinglePartitionKey()
    {
        qt().forAll(SparkTestUtils.cql3Type(bridge))
            .checkAssert(type -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", type))
                .withCdcEventChecker((testRows, events) -> {
                    for (SparkCdcEvent event : events)
                    {
                        assertEquals(1, event.getPartitionKeys().size());
                        assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                        assertNull(event.getClusteringKeys());
                        assertNull(event.getStaticColumns());
                        assertEquals(Arrays.asList("c1", "c2"),
                                     event.getValueColumns().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList()));
                        assertNull(event.getTtl());
                    }
                })
                .run();
            });
    }

    // Test mutations that partially update are correctly reflected in the cdc event.
    @Test
    public void testUpdatePartialColumns()
    {
        final Set<Integer> ttlRowIdx = new HashSet<>();
        final Random rnd = new Random(1);
        qt().forAll(SparkTestUtils.cql3Type(bridge))
            .checkAssert(type -> {
                ttlRowIdx.clear();
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", type))
                .clearWriters()
                .withAddLastModificationTime(true)
                .withWriter((tester, rows, writer) -> {
                    long time = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        TestSchema.TestRow testRow = Tester.newUniqueRow(tester.schema, rows);
                        // mark c1 as not updated / unset
                        testRow = testRow.copy("c1", CassandraBridge.UNSET_MARKER);
                        if (rnd.nextDouble() > 0.5)
                        {
                            testRow.setTTL(TTL);
                            ttlRowIdx.add(i);
                        }
                        writer.accept(testRow, time++);
                    }
                })
                .withCdcEventChecker((testRows, events) -> {
                    int i = 0;
                    for (SparkCdcEvent event : events)
                    {
                        assertEquals(1, event.getPartitionKeys().size());
                        assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                        assertNull(event.getClusteringKeys());
                        assertNull(event.getStaticColumns());
                        assertEquals("c1 should be absent",
                                     Arrays.asList("c2"),
                                     event.getValueColumns().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList()));

                        if (ttlRowIdx.contains(i))
                        {
                            assertNotNull(event.getTtl());
                            assertEquals(TTL, event.getTtl().ttlInSec);
                        }
                        else
                        {
                            assertNull(event.getTtl());
                        }
                        i++;
                    }
                })
                .run();
            });
    }

    @Test
    public void testMultipleWritesToSameKeyInBatch()
    {
        // The test writes different groups of mutations.
        // Each group of mutations write to the same key with the different timestamp.
        // For CDC, it only deduplicate and emit the replicated mutations, i.e. they have the same writetime.
        qt().forAll(SparkTestUtils.cql3Type(bridge))
            .checkAssert(type -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", type))
                .clearWriters()
                .withNumRows(1000)
                .withExpectedNumRows(2000)
                .withAddLastModificationTime(true)
                .withWriter((tester, rows, writer) -> {
                    // write initial values
                    long timestamp = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        writer.accept(Tester.newUniqueRow(tester.schema, rows), timestamp++);
                    }

                    // overwrite with new mutations at later timestamp
                    for (TestSchema.TestRow row : rows.values())
                    {
                        final TestSchema.TestRow newUniqueRow = Tester.newUniqueRow(tester.schema, rows);
                        for (final CqlField field : tester.cqlTable.valueColumns())
                        {
                            // update value columns
                            row = row.copy(field.pos(), newUniqueRow.get(field.pos()));
                        }
                        row.setTTL(TTL);
                        writer.accept(row, timestamp++);
                    }
                })
                .withCdcEventChecker((testRows, events) -> {
                    assertEquals("Each PK should get 2 mutations", testRows.size() * 2, events.size());
                    long ts = -1L;
                    int partitions = testRows.size();
                    int i = 0;
                    for (SparkCdcEvent event : events)
                    {
                        if (ts == -1L)
                        {
                            ts = event.getTimestamp(TimeUnit.MICROSECONDS);
                        }
                        else
                        {
                            long lastTs = ts;
                            ts = event.getTimestamp(TimeUnit.MICROSECONDS);
                            assertTrue("Writetime should be monotonic increasing",
                                       lastTs < ts);
                        }
                        if (i >= partitions) // the rows in the second batch has ttl specified.
                        {
                            assertNotNull(event.getTtl());
                            assertEquals(TTL, event.getTtl().ttlInSec);
                        }
                        i++;
                    }
                })
                .run();
            });
    }

    @Test
    public void testCompactOnlyWithEnoughReplicas()
    {
        qt().forAll(SparkTestUtils.cql3Type(bridge))
            .checkAssert(type -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", type))
                .withDataSource(RequireTwoReplicasLocalDataSource.class.getName())
                .withNumRows(1000)
                .withExpectedNumRows(999) // expect 1 less row
                .withAddLastModificationTime(true)
                .clearWriters()
                .withWriter((tester, rows, writer) -> {
                    // write initial values
                    final long timestamp = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                    Map<Long, TestSchema.TestRow> genRows = new LinkedHashMap<>();
                    IntStream.range(0, tester.numRows)
                             .forEach(i -> genRows.put(timestamp + i, Tester.newUniqueRow(tester.schema, rows)));
                    genRows.forEach((time, row) -> writer.accept(row, time));

                    // Write the same values again, with the first value skipped.
                    // All values except the first one have 2 copies
                    // The test is using RequireTwoReplicasCompactionDataSource,
                    // so the output should not contain the first value
                    for (long i = 1; i < tester.numRows; i++)
                    {
                        long time = timestamp + i;
                        writer.accept(genRows.get(time), time);
                    }
                })
                .withCdcEventChecker((testRows, events) -> {
                    int uniqueCount = events.stream()
                                            .map(e -> e.getTimestamp(TimeUnit.MICROSECONDS))
                                            .collect(Collectors.toSet()).size();
                    assertEquals("Output rows should have distinct lastModified timestamps",
                                 events.size(), uniqueCount);
                    assertEquals("There should be exact one less row in the output.",
                                 events.size() + 1, testRows.size());
                })
                .run();
            });
    }

    @Test
    public void testCompositePartitionKey()
    {
        qt().forAll(SparkTestUtils.cql3Type(bridge))
            .checkAssert(t -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk1", bridge.uuid())
                                                .withPartitionKey("pk2", t)
                                                .withPartitionKey("pk3", bridge.timestamp())
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", bridge.text()))
                .withCdcEventChecker((testRows, events) -> {
                    for (SparkCdcEvent event : events)
                    {
                        assertEquals(3, event.getPartitionKeys().size());
                        assertEquals(Arrays.asList("pk1", "pk2", "pk3"),
                                     event.getPartitionKeys().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList()));
                        assertNull(event.getClusteringKeys());
                        assertNull(event.getStaticColumns());
                        assertEquals(Arrays.asList("c1", "c2"),
                                     event.getValueColumns().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList()));
                        assertNull(event.getTtl());
                    }
                })
                .run();
            });
    }

    @Test
    public void testClusteringKey()
    {
        qt().forAll(SparkTestUtils.cql3Type(bridge))
            .checkAssert(t -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withClusteringKey("ck", t)
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", bridge.text()))
                .withCdcEventChecker((testRows, events) -> {
                    for (SparkCdcEvent event : events)
                    {
                        assertEquals(1, event.getPartitionKeys().size());
                        assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                        assertEquals(1, event.getClusteringKeys().size());
                        assertEquals("ck", event.getClusteringKeys().get(0).columnName);
                        assertCqlTypeEquals(t.cqlName(), event.getClusteringKeys().get(0).columnType);
                        assertNull(event.getStaticColumns());
                        assertEquals(Arrays.asList("c1", "c2"),
                                     event.getValueColumns().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList()));
                        assertNull(event.getTtl());
                    }
                })
                .run();
            });
    }

    @Test
    public void testMultipleClusteringKeys()
    {
        qt().withExamples(50).forAll(SparkTestUtils.cql3Type(bridge), SparkTestUtils.cql3Type(bridge), SparkTestUtils.cql3Type(bridge))
            .checkAssert((t1, t2, t3) -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withClusteringKey("ck1", t1)
                                                .withClusteringKey("ck2", t2)
                                                .withClusteringKey("ck3", t3)
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", bridge.text()))
                .withCdcEventChecker((testRows, events) -> {
                    for (SparkCdcEvent event : events)
                    {
                        assertEquals(1, event.getPartitionKeys().size());
                        assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                        assertEquals(Arrays.asList("ck1", "ck2", "ck3"),
                                     event.getClusteringKeys().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList()));
                        assertCqlTypeEquals(t1.cqlName(), event.getClusteringKeys().get(0).columnType);
                        assertCqlTypeEquals(t2.cqlName(), event.getClusteringKeys().get(1).columnType);
                        assertCqlTypeEquals(t3.cqlName(), event.getClusteringKeys().get(2).columnType);
                        assertNull(event.getStaticColumns());
                        assertEquals(Arrays.asList("c1", "c2"),
                                     event.getValueColumns().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList()));
                        assertNull(event.getTtl());
                    }
                })
                .run();
            });
    }

    @Test
    public void testSet()
    {
        qt().forAll(SparkTestUtils.cql3Type(bridge))
            .checkAssert(t -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", bridge.set(t)))
                .withCdcEventChecker((testRows, events) -> {
                    for (SparkCdcEvent event : events)
                    {
                        assertEquals(1, event.getPartitionKeys().size());
                        assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                        assertNull(event.getClusteringKeys());
                        assertNull(event.getStaticColumns());
                        assertEquals(Arrays.asList("c1", "c2"),
                                     event.getValueColumns().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList()));
                        SparkValueWithMetadata setValue = event.getValueColumns().get(1);
                        String setType = setValue.columnType;
                        assertTrue(setType.startsWith("set<"));
                        assertCqlTypeEquals(t.cqlName(),
                                            setType.substring(4, setType.length() - 1)); // extract the type in set<>
                        Object v = bridge.parseType(setType).deserializeToJava(setValue.getValue());
                        assertTrue(v instanceof Set);
                        Set set = (Set) v;
                        assertTrue(set.size() > 0);
                        assertNull(event.getTtl());
                    }
                })
                .run();
            });
    }

    @Test
    public void testSkipPersistOnSparkFailure()
    {
        qt().forAll(SparkTestUtils.cql3Type(bridge))
                .checkAssert(t -> {
                    testWith(bridge, DIR, TestSchema.builder()
                            .withPartitionKey("pk", bridge.uuid())
                            .withColumn("c1", bridge.bigint()))
                            .withDataSource(SpyWatermarkerDataSource.class.getName())
                            .shouldCdcEventWriterFailOnProcessing()
                            .withNumRows(1)
                            .withExpectedNumRows(0)
                            .withCdcEventChecker((testRows, events) -> {
                                verify(SpyWatermarkerDataSource.SpyWaterMarkerDataLayer.inMemoryWatermarker, never()).persist(anyLong());
                            })
                            .run();
                });
    }

    @Test
    public void testList()
    {
        qt().forAll(SparkTestUtils.cql3Type(bridge))
            .checkAssert(t -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", bridge.list(bridge.aInt())))
                .withDataSource(UnfrozenListDataSource.class.getName())
                .withCdcEventChecker((testRows, events) -> {
                    for (SparkCdcEvent event : events)
                    {
                        assertEquals(1, event.getPartitionKeys().size());
                        assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                        assertNull(event.getClusteringKeys());
                        assertNull(event.getStaticColumns());
                        assertEquals(Arrays.asList("c1", "c2"),
                                     event.getValueColumns().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList()));
                        SparkValueWithMetadata listValue = event.getValueColumns().get(1);
                        String listType = listValue.columnType;
                        assertTrue(listType.startsWith("list<"));
                        assertCqlTypeEquals(bridge.aInt().cqlName(),
                                            listType.substring(5, listType.length() - 1)); // extract the type in list<>
                        Object v = bridge.parseType(listType).deserializeToJava(listValue.getValue());
                        assertTrue(v instanceof List);
                        List list = (List) v;
                        assertEquals(Arrays.asList(1, 2, 3, 4), list);
                        assertNull(event.getTtl());
                    }
                })
                .run();
            });
    }

    @Test
    public void testMap()
    {
        qt().withExamples(50).forAll(SparkTestUtils.cql3Type(bridge), SparkTestUtils.cql3Type(bridge))
            .checkAssert((t1, t2) -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", bridge.map(t1, t2)))
                .withCdcEventChecker((testRows, events) -> {
                    for (SparkCdcEvent event : events)
                    {
                        assertEquals(1, event.getPartitionKeys().size());
                        assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                        assertNull(event.getClusteringKeys());
                        assertNull(event.getStaticColumns());
                        assertEquals(Arrays.asList("c1", "c2"),
                                     event.getValueColumns().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList()));
                        SparkValueWithMetadata mapValue = event.getValueColumns().get(1);
                        String mapType = mapValue.columnType;
                        assertTrue(mapType.startsWith("map<"));
                        int commaIndex = mapType.indexOf(',');
                        assertCqlTypeEquals(t1.cqlName(),
                                            // extract the key type in map<>
                                            mapType.substring(4, commaIndex)); // extract the key type in map<>
                        assertCqlTypeEquals(t2.cqlName(),
                                            // extract the value type in map<>; +2 to exclude , and the following space
                                            mapType.substring(commaIndex + 2, mapType.length() - 1));
                        Object v = bridge.parseType(mapType).deserializeToJava(mapValue.getValue());
                        assertTrue(v instanceof Map);
                        Map map = (Map) v;
                        assertTrue(map.size() > 0);
                        assertNull(event.getTtl());
                    }
                })
                .run();
            });
    }

    @Test
    public void testUpdateFlag()
    {
        qt().withExamples(10)
            .forAll(SparkTestUtils.cql3Type(bridge))
            .checkAssert(type -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withColumn("c1", bridge.aInt())
                                                .withColumn("c2", type))
                .clearWriters()
                .withNumRows(1000)
                .withWriter((tester, rows, writer) -> {
                    final int halfway = tester.numRows / 2;
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        TestSchema.TestRow testRow = Tester.newUniqueRow(tester.schema, rows);
                        testRow = testRow.copy("c1", i);
                        if (i >= halfway)
                        {
                            testRow.fromUpdate();
                        }
                        testRow.setTTL(TTL);
                        writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                    }
                })
                .withCdcEventChecker((testRows, events) -> {
                    int halfway = events.size() / 2;
                    for (SparkCdcEvent event : events)
                    {
                        assertEquals(1, event.getPartitionKeys().size());
                        assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                        assertNull(event.getClusteringKeys());
                        assertNull(event.getStaticColumns());
                        assertEquals(Arrays.asList("c1", "c2"),
                                     event.getValueColumns().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList()));
                        ByteBuffer c1Bb = event.getValueColumns().get(0).getValue();
                        int i = (Integer) bridge.aInt().deserialize(c1Bb);
                        SparkCdcEvent.Kind expectedKind = i >= halfway
                                                          ? SparkCdcEvent.Kind.UPDATE
                                                          : SparkCdcEvent.Kind.INSERT;
                        assertEquals(expectedKind, event.kind);
                        assertEquals(TTL, event.getTtl().ttlInSec);
                    }
                })
                .run();
            });
    }

    // commit log reader

    @Test
    public void testReaderWatermarking() throws IOException
    {
        final TestSchema schema = TestSchema.builder()
                                            .withPartitionKey("pk", bridge.bigint())
                                            .withColumn("c1", bridge.bigint())
                                            .withColumn("c2", bridge.bigint())
                                            .withCdc(true)
                                            .build();
        final CqlTable cqlTable = schema.buildSchema();
        new FourZeroSchemaBuilder(cqlTable, Partitioner.Murmur3Partitioner, null, true); // init Schema instance
        final int numRows = 1000;

        // write some rows to a CommitLog
        final Set<Long> keys = new HashSet<>(numRows);
        for (int i = 0; i < numRows; i++)
        {
            TestSchema.TestRow row = schema.randomRow();
            while (keys.contains(row.getLong("pk")))
            {
                row = schema.randomRow();
            }
            keys.add(row.getLong("pk"));
            bridge.log(cqlTable, LOG, row, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        }
        LOG.sync();

        final List<Marker> markers = Collections.synchronizedList(new ArrayList<>());
        final File logFile = Files.list(CdcTests.DIR.getRoot().toPath().resolve("cdc"))
                                  .max((o1, o2) -> {
                                      try
                                      {
                                          return Long.compare(Files.size(o1), Files.size(o2));
                                      }
                                      catch (IOException e)
                                      {
                                          throw new RuntimeException(e);
                                      }
                                  }).orElseThrow(() -> new RuntimeException("No log files found")).toFile();

        // read entire commit log and verify correct
        Consumer<Marker> listener = markers::add;
        final Set<Long> allRows = readLog(null, keys, logFile, listener);
        assertEquals(numRows, allRows.size());

        // re-read commit log from each watermark position
        // and verify subset of partitions are read
        int foundRows = allRows.size();
        allRows.clear();
        final List<Marker> allMarkers = new ArrayList<>(markers);
        Marker prevMarker = null;
        for (final Marker marker : allMarkers)
        {
            final Set<Long> result = readLog(marker, keys, logFile, null);
            assertTrue(result.size() < foundRows);
            foundRows = result.size();
            if (prevMarker != null)
            {
                assertTrue(prevMarker.compareTo(marker) < 0);
                assertTrue(prevMarker.position < marker.position);
            }
            prevMarker = marker;

            if (marker.equals(allMarkers.get(allMarkers.size() - 1)))
            {
                // last marker should return 0 updates
                // and be at the end of the file
                assertTrue(result.isEmpty());
            }
            else
            {
                assertFalse(result.isEmpty());
            }
        }
    }

    @Test
    public void testCdcStats()
    {
        qt().withExamples(1)
            .forAll(SparkTestUtils.cql3Type(bridge), SparkTestUtils.cql3Type(bridge), SparkTestUtils.cql3Type(bridge))
            .checkAssert((t1, t2, t3) -> {
                STATS.reset();
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withClusteringKey("ck1", t1)
                                                .withClusteringKey("ck2", t2)
                                                .withClusteringKey("ck3", t3)
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", bridge.text()))
                .withStatsClass(CdcTests.class.getName() + ".STATS")
                .withCdcEventChecker((testRows, events) -> {
                    int rowCount = events.size();
                    assertTrue(STATS.getStats(TestStats.TEST_CDC_TIME_TAKEN_TO_READ_BATCH).size() > 0); // atleast 1 batch
                    assertTrue(STATS.getStats(TestStats.TEST_CDC_COMMIT_LOG_READ_TIME).size() >=
                               STATS.getStats(TestStats.TEST_CDC_TIME_TAKEN_TO_READ_BATCH).size()); // atleast one log file per batch
                    assertEquals(rowCount, STATS.getCounterValue(TestStats.TEST_CDC_MUTATIONS_READ_COUNT)); // as many mutations as rows
                    assertEquals(rowCount, STATS.getStats(TestStats.TEST_CDC_MUTATIONS_READ_BYTES).size());
                    assertEquals(rowCount, STATS.getStats(TestStats.TEST_CDC_MUTATION_RECEIVED_LATENCY).size());
                    assertEquals(rowCount, STATS.getStats(TestStats.TEST_CDC_MUTATION_PRODUCED_LATENCY).size());

                    long totalMutations = STATS.getStats(TestStats.TEST_CDC_MUTATIONS_READ_PER_BATCH).stream().reduce(Long::sum).orElse(0L);
                    assertEquals(rowCount, totalMutations);

                    // Should read commit log headers - but might be skipped when seek to highwaterMark
                    assertTrue(STATS.getStats(TestStats.TEST_CDC_COMMIT_LOG_HEADER_READ_TIME).size() > 0);
                    assertTrue(STATS.getStats(TestStats.TEST_CDC_COMMIT_LOG_HEADER_READ_TIME).size() <= STATS.getStats(TestStats.TEST_CDC_COMMIT_LOG_READ_TIME).size());

                    assertEquals(STATS.getStats(TestStats.TEST_CDC_COMMIT_LOG_READ_TIME).size(),
                                 STATS.getStats(TestStats.TEST_CDC_COMMIT_LOG_BYTES_FETCHED).size());
                    assertTrue(STATS.getCounterValue(TestStats.TEST_CDC_SKIPPED_COMMIT_LOGS_COUNT) > 0);
                    assertTrue(STATS.getStats(TestStats.TEST_CDC_COMMIT_LOG_SEGMENT_READ_TIME).size() > 0);
                })
                .run();
            });
    }

    @Test
    public void testMultiTable()
    {
        final TestSchema.Builder tableBuilder1 = TestSchema.builder()
                                                           .withPartitionKey("pk", bridge.uuid())
                                                           .withClusteringKey("ck1", bridge.text())
                                                           .withColumn("c1", bridge.bigint())
                                                           .withColumn("c2", bridge.text())
                                                           .withCdc(true);
        final TestSchema.Builder tableBuilder2 = TestSchema.builder()
                                                           .withPartitionKey("a", bridge.aInt())
                                                           .withPartitionKey("b", bridge.timeuuid())
                                                           .withClusteringKey("c", bridge.text())
                                                           .withClusteringKey("d", bridge.bigint())
                                                           .withColumn("e", bridge.map(bridge.aInt(), bridge.text()))
                                                           .withCdc(true);
        final TestSchema.Builder tableBuilder3 = TestSchema.builder()
                                                           .withPartitionKey("c1", bridge.text())
                                                           .withClusteringKey("c2", bridge.aInt())
                                                           .withColumn("c3", bridge.set(bridge.bigint()))
                                                           .withCdc(false);
        final TestSchema schema2 = tableBuilder2.build();
        final TestSchema schema3 = tableBuilder3.build();
        final CqlTable cqlTable2 = schema2.buildSchema();
        final CqlTable cqlTable3 = schema3.buildSchema();
        schema2.schemaBuilder(Partitioner.Murmur3Partitioner, new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 1, "DC3", 5)));
        schema2.setCassandraVersion(version);
        schema3.schemaBuilder(Partitioner.Murmur3Partitioner, new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 1, "DC3", 5)));
        schema3.setCassandraVersion(version);
        final int numRows = DEFAULT_NUM_ROWS;

        final AtomicReference<TestSchema> schema1Holder = new AtomicReference<>();
        final CdcTester.Builder testBuilder = CdcTester.builder(bridge, tableBuilder1, DIR.getRoot().toPath())
                                                       .clearWriters()
                                                       .withWriter((tester, rows, writer) -> {
                                                           for (int i = 0; i < numRows; i++)
                                                           {
                                                               writer.accept(Tester.newUniqueRow(tester.schema, rows), TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                                                           }
                                                       })
                                                       .withWriter(new CdcTester.CdcWriter()
                                                       {
                                                           public void write(CdcTester tester, Map<String, TestSchema.TestRow> rows, BiConsumer<TestSchema.TestRow, Long> writer)
                                                           {
                                                               final Map<String, TestSchema.TestRow> prevRows = new HashMap<>(numRows);
                                                               for (int i = 0; i < numRows; i++)
                                                               {
                                                                   writer.accept(Tester.newUniqueRow(schema2, prevRows), TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                                                               }
                                                           }

                                                           public CqlTable cqlTable(CdcTester tester)
                                                           {
                                                               return cqlTable2;
                                                           }
                                                       })
                                                       .withWriter(new CdcTester.CdcWriter()
                                                       {
                                                           public void write(CdcTester tester, Map<String, TestSchema.TestRow> rows, BiConsumer<TestSchema.TestRow, Long> writer)
                                                           {
                                                               final Map<String, TestSchema.TestRow> prevRows = new HashMap<>(numRows);
                                                               for (int i = 0; i < numRows; i++)
                                                               {
                                                                   writer.accept(Tester.newUniqueRow(schema3, prevRows), TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                                                               }
                                                           }

                                                           public CqlTable cqlTable(CdcTester tester)
                                                           {
                                                               return cqlTable3;
                                                           }
                                                       })
                                                       .withExpectedNumRows(numRows * 2)
                                                       .withCdcEventChecker((testRows, events) -> {
                                                           final TestSchema schema1 = schema1Holder.get();
                                                           assertEquals(numRows * 2, events.size());
                                                           assertEquals(numRows, events.stream()
                                                                                       .filter(f -> f.keyspace.equals(schema1.keyspace))
                                                                                       .filter(f -> f.table.equals(schema1.table)).count());
                                                           assertEquals(numRows, events.stream()
                                                                                       .filter(f -> f.keyspace.equals(schema2.keyspace))
                                                                                       .filter(f -> f.table.equals(schema2.table)).count());
                                                           assertEquals(0, events.stream()
                                                                                 .filter(f -> f.keyspace.equals(schema3.keyspace))
                                                                                 .filter(f -> f.table.equals(schema3.table)).count());
                                                       });
        final CdcTester cdcTester = testBuilder.build();
        schema1Holder.set(cdcTester.schema);
        cdcTester.run();
    }

    private Set<Long> readLog(@Nullable final Marker highWaterMark,
                              Set<Long> keys,
                              File logFile,
                              @Nullable Consumer<Marker> listener)
    {
        final Set<Long> result = new HashSet<>();

        try (final LocalDataLayer.LocalCommitLog log = new LocalDataLayer.LocalCommitLog(logFile))
        {
            try (final BufferingCommitLogReader reader = new BufferingCommitLogReader(log, highWaterMark,
                                                                                      CdcStats.DoNothingCdcStats.INSTANCE,
                                                                                      listener))
            {
                for (final PartitionUpdateWrapper update : reader.result().updates())
                {
                    final long key = Objects.requireNonNull(update.partitionKey()).getKey().getLong();
                    assertFalse(result.contains(key));
                    result.add(key);
                    assertTrue(keys.contains(key));
                }
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return result;
    }

    // tombstone tests

    @Test
    public void testCellDeletion()
    {
        // The test write cell-level tombstones,
        // i.e. deleting one or more columns in a row, for cdc job to aggregate.
        qt().forAll(SparkTestUtils.cql3Type(bridge))
            .checkAssert(type -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", type)
                                                .withColumn("c3", bridge.list(type)))
                .clearWriters()
                .withWriter((tester, rows, writer) -> {
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        TestSchema.TestRow testRow = Tester.newUniqueRow(tester.schema, rows);
                        testRow = testRow.copy("c1", CassandraBridge.UNSET_MARKER); // mark c1 as not updated / unset
                        testRow = testRow.copy("c2", null); // delete c2
                        testRow = testRow.copy("c3", null); // delete c3
                        writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                    }
                })
                .withCdcEventChecker((testRows, events) -> {
                    for (SparkCdcEvent event : events)
                    {
                        assertEquals(AbstractCdcEvent.Kind.DELETE, event.kind);
                        assertEquals(1, event.getPartitionKeys().size());
                        assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                        assertNull(event.getClusteringKeys());
                        assertNull(event.getStaticColumns());
                        assertEquals(Arrays.asList("c2", "c3"), // c1 is not updated
                                     event.getValueColumns().stream()
                                          .map(v -> v.columnName)
                                          .collect(Collectors.toList()));
                        SparkValueWithMetadata c2 = event.getValueColumns().get(0);
                        assertCqlTypeEquals(type.cqlName(), c2.columnType);
                        assertNull(event.getTtl());
                    }
                })
                .run();
            });
    }

    @Test
    public void testRowDeletionWithClusteringKeyAndStatic()
    {
        testRowDeletion(true, // has static
                        true, // has clustering key?
                        type -> TestSchema.builder()
                                          .withPartitionKey("pk", bridge.uuid())
                                          .withClusteringKey("ck", bridge.bigint())
                                          .withStaticColumn("sc", bridge.bigint())
                                          .withColumn("c1", type)
                                          .withColumn("c2", bridge.bigint()));
    }

    @Test
    public void testRowDeletionWithClusteringKeyNoStatic()
    {
        testRowDeletion(false, // has static
                        true, // has clustering key?
                        type -> TestSchema.builder()
                                          .withPartitionKey("pk", bridge.uuid())
                                          .withClusteringKey("ck", bridge.bigint())
                                          .withColumn("c1", type)
                                          .withColumn("c2", bridge.bigint()));
    }

    @Test
    public void testRowDeletionSimpleSchema()
    {
        testRowDeletion(false, // has static
                        false, // has clustering key?
                        type -> TestSchema.builder()
                                          .withPartitionKey("pk", bridge.uuid())
                                          .withColumn("c1", type)
                                          .withColumn("c2", bridge.bigint()));
    }

    private void testRowDeletion(boolean hasStatic, boolean hasClustering, Function<SparkCqlField.SparkCqlType, TestSchema.Builder> schemaBuilder)
    {
        // The test write row-level tombstones
        // The expected output should include the values of all primary keys but all other columns should be null,
        // i.e. [pk.., ck.., null..]. The bitset should indicate that only the primary keys are present.
        // This kind of output means the entire row is deleted
        final Set<Integer> rowDeletionIndices = new HashSet<>();
        final Random rnd = new Random(1);
        final long minTimestamp = System.currentTimeMillis();
        final int numRows = 1000;
        qt().forAll(SparkTestUtils.cql3Type(bridge))
            .checkAssert(type -> {
                testWith(bridge, DIR, schemaBuilder.apply(type))
                .withAddLastModificationTime(true)
                .clearWriters()
                .withNumRows(numRows)
                .withWriter((tester, rows, writer) -> {
                    rowDeletionIndices.clear();
                    long timestamp = minTimestamp;
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        TestSchema.TestRow testRow = Tester.newUniqueRow(tester.schema, rows);
                        if (rnd.nextDouble() < 0.5)
                        {
                            testRow.delete();
                            rowDeletionIndices.add(i);
                        }
                        timestamp += 1;
                        writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(timestamp));
                    }
                })
                .withCdcEventChecker((testRows, events) -> {
                    for (int i = 0; i < events.size(); i++)
                    {
                        SparkCdcEvent event = events.get(i);
                        long lmtInMillis = event.getTimestamp(TimeUnit.MILLISECONDS);
                        assertTrue("Last modification time should have a lower bound of " + minTimestamp,
                                   lmtInMillis >= minTimestamp);
                        assertEquals("Regardless of being row deletion or not, the partition key must present",
                                     1, event.getPartitionKeys().size());
                        if (hasClustering) // and ck to be set.
                        {
                            assertEquals(1, event.getClusteringKeys().size());
                        }
                        else
                        {
                            assertNull(event.getClusteringKeys());
                        }

                        if (rowDeletionIndices.contains(i)) // verify row deletion
                        {
                            assertNull("None primary key columns should be null", event.getStaticColumns());
                            assertNull("None primary key columns should be null", event.getValueColumns());
                            assertEquals(AbstractCdcEvent.Kind.ROW_DELETE, event.kind);
                        }
                        else // verify update
                        {
                            if (hasStatic)
                            {
                                assertNotNull(event.getStaticColumns());
                            }
                            else
                            {
                                assertNull(event.getStaticColumns());
                            }
                            assertNotNull(event.getValueColumns());
                            assertEquals(AbstractCdcEvent.Kind.INSERT, event.kind);
                        }
                    }
                })
                .run();
            });
    }

    @Test
    public void testPartitionDeletionWithStaticColumn()
    {
        testPartitionDeletion(true, // has static columns
                              true, // has clustering key
                              1, // partition key columns
                              type -> TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withClusteringKey("ck", bridge.bigint())
                                                .withStaticColumn("sc", bridge.bigint())
                                                .withColumn("c1", type)
                                                .withColumn("c2", bridge.bigint()));
    }

    @Test
    public void testPartitionDeletionWithCompositePK()
    {
        testPartitionDeletion(false, // has static columns
                              true, // has clustering key
                              2, // partition key columns
                              type -> TestSchema.builder()
                                                .withPartitionKey("pk1", bridge.uuid())
                                                .withPartitionKey("pk2", type)
                                                .withClusteringKey("ck", bridge.bigint())
                                                .withColumn("c1", type)
                                                .withColumn("c2", bridge.bigint()));
    }

    @Test
    public void testPartitionDeletionWithoutCK()
    {
        testPartitionDeletion(false, // has static columns
                              false, // has clustering key
                              3, // partition key columns
                              type -> TestSchema.builder()
                                                .withPartitionKey("pk1", bridge.uuid())
                                                .withPartitionKey("pk2", type)
                                                .withPartitionKey("pk3", bridge.bigint())
                                                .withColumn("c1", type)
                                                .withColumn("c2", bridge.bigint()));
    }

    // At most can have 1 clustering key when `hasClustering` is true.
    private void testPartitionDeletion(boolean hasStatic, boolean hasClustering, int partitionKeys, Function<SparkCqlField.SparkCqlType, TestSchema.Builder> schemaBuilder)
    {
        // The test write partition-level tombstones
        // The expected output should include the values of all partition keys but all other columns should be null,
        // i.e. [pk.., null..]. The bitset should indicate that only the partition keys are present.
        // This kind of output means the entire partition is deleted
        final Set<Integer> partitionDeletionIndices = new HashSet<>();
        final List<List<Object>> validationPk = new ArrayList<>(); // pk of the partition deletions
        final Random rnd = new Random(1);
        final long minTimestamp = System.currentTimeMillis();
        final int numRows = 1000;
        qt().forAll(SparkTestUtils.cql3Type(bridge))
            .checkAssert(type -> {
                testWith(bridge, DIR, schemaBuilder.apply(type))
                .withAddLastModificationTime(true)
                .clearWriters()
                .withNumRows(numRows)
                .withWriter((tester, rows, writer) -> {
                    partitionDeletionIndices.clear();
                    validationPk.clear();
                    long timestamp = minTimestamp;
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        TestSchema.TestRow testRow;
                        if (rnd.nextDouble() < 0.5)
                        {
                            testRow = Tester.newUniquePartitionDeletion(tester.schema, rows);
                            List<Object> pk = new ArrayList<>(partitionKeys);
                            for (int j = 0; j < partitionKeys; j++)
                            {
                                pk.add(testRow.get(j));
                            }
                            validationPk.add(pk);
                            partitionDeletionIndices.add(i);
                        }
                        else
                        {
                            testRow = Tester.newUniqueRow(tester.schema, rows);
                        }
                        timestamp += 1;
                        writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(timestamp));
                    }
                })
                .withCdcEventChecker((testRows, events) -> {
                    for (int i = 0, pkValidationIdx = 0; i < events.size(); i++)
                    {
                        SparkCdcEvent event = events.get(i);
                        long lmtInMillis = event.getTimestamp(TimeUnit.MILLISECONDS);
                        assertTrue("Last modification time should have a lower bound of " + minTimestamp,
                                   lmtInMillis >= minTimestamp);
                        assertEquals("Regardless of being row deletion or not, the partition key must present",
                                     partitionKeys, event.getPartitionKeys().size());

                        if (partitionDeletionIndices.contains(i)) // verify partition deletion
                        {
                            assertEquals(AbstractCdcEvent.Kind.PARTITION_DELETE, event.kind);
                            assertNull("Partition deletion has no clustering keys", event.getClusteringKeys());

                            assertNull(event.getStaticColumns());
                            assertNull(event.getValueColumns());

                            List<Object> testPKs = event.getPartitionKeys().stream()
                                                        .map(v -> {
                                                            CqlField.CqlType cqlType = v.getCqlType(bridge::parseType);
                                                            return cqlType.deserializeToJava(v.getValue());
                                                        })
                                                        .collect(Collectors.toList());

                            List<Object> expectedPK = validationPk.get(pkValidationIdx++);
                            assertTrue("Partition deletion should indicate the correct partition at row" + i +
                                       ". Expected: " + expectedPK + ", actual: " + testPKs,
                                       SparkTestUtils.equals(expectedPK.toArray(), testPKs.toArray()));
                        }
                        else // verify update
                        {
                            assertEquals(AbstractCdcEvent.Kind.INSERT, event.kind);
                            if (hasClustering)
                            {
                                assertNotNull(event.getClusteringKeys());
                            }
                            else
                            {
                                assertNull(event.getClusteringKeys());
                            }
                            if (hasStatic)
                            {
                                assertNotNull(event.getStaticColumns());
                            }
                            else
                            {
                                assertNull(event.getStaticColumns());
                            }
                            assertNotNull(event.getValueColumns());
                        }
                    }
                })
                .run();
            });
    }

    @Test
    public void testElementDeletionInMap()
    {
        final String name = "m";
        testElementDeletionInCollection(1, 2, /* numOfColumns */
                                        Arrays.asList(name),
                                        type -> TestSchema.builder()
                                                          .withPartitionKey("pk", bridge.uuid())
                                                          .withColumn(name, bridge.map(type, type)));
    }

    @Test
    public void testElementDeletionInSet()
    {
        final String name = "s";
        testElementDeletionInCollection(1, 2, /* numOfColumns */
                                        Arrays.asList(name),
                                        type -> TestSchema.builder()
                                                          .withPartitionKey("pk", bridge.uuid())
                                                          .withColumn(name, bridge.set(type)));
    }

    @Test
    public void testElementDeletionsInMultipleColumns()
    {
        testElementDeletionInCollection(1, 4, /* numOfColumns */
                                        Arrays.asList("c1", "c2", "c3"),
                                        type -> TestSchema.builder()
                                                          .withPartitionKey("pk", bridge.uuid())
                                                          .withColumn("c1", bridge.set(type))
                                                          .withColumn("c2", bridge.set(type))
                                                          .withColumn("c3", bridge.set(type)));
    }

    // validate that cell deletions in a complex data can be correctly encoded.
    private void testElementDeletionInCollection(int numOfPKs, int numOfColumns, List<String> collectionColumnNames, Function<SparkCqlField.SparkCqlType, TestSchema.Builder> schemaBuilder)
    {
        // key: row# that has deletion; value: the deleted cell key/path in the collection
        final Map<Integer, byte[]> elementDeletionIndices = new HashMap<>();
        final Random rnd = new Random(1);
        final long minTimestamp = System.currentTimeMillis();
        final int numRows = 1000;
        qt().forAll(SparkTestUtils.cql3Type(bridge))
            .checkAssert(type -> {
                testWith(bridge, DIR, schemaBuilder.apply(type))
                .withAddLastModificationTime(true)
                .clearWriters()
                .withNumRows(numRows)
                .withWriter((tester, rows, writer) -> {
                    elementDeletionIndices.clear();
                    long timestamp = minTimestamp;
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        int ignoredSize = 10;
                        TestSchema.TestRow testRow;
                        if (rnd.nextDouble() < 0.5)
                        {
                            // NOTE: it is a little hacky. For simplicity, all collections in the row
                            // has the SAME entry being deleted.
                            ByteBuffer key = type.serialize(type.randomValue(ignoredSize));
                            testRow = Tester.newUniqueRow(tester.schema, rows);
                            for (String name : collectionColumnNames)
                            {
                                testRow = testRow.copy(name,
                                                       CassandraBridge.CollectionElement.deleted(CellPath.create(key)));
                            }
                            elementDeletionIndices.put(i, key.array());
                        }
                        else
                        {
                            testRow = Tester.newUniqueRow(tester.schema, rows);
                        }
                        timestamp += 1;
                        writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(timestamp));
                    }
                })
                .withCdcEventChecker((testRows, events) -> {
                    for (int i = 0; i < events.size(); i++)
                    {
                        SparkCdcEvent event = events.get(i);
                        long lmtInMillis = event.getTimestamp(TimeUnit.MILLISECONDS);
                        assertTrue("Last modification time should have a lower bound of " + minTimestamp,
                                   lmtInMillis >= minTimestamp);
                        assertEquals("Regardless of being row deletion or not, the partition key must present",
                                     numOfPKs, event.getPartitionKeys().size());
                        assertNull(event.getClusteringKeys());
                        assertNull(event.getStaticColumns());

                        if (elementDeletionIndices.containsKey(i)) // verify deletion
                        {
                            assertEquals(AbstractCdcEvent.Kind.COMPLEX_ELEMENT_DELETE, event.kind);
                            Map<String, List<ByteBuffer>> cellTombstonesPerCol = event.getTombstonedCellsInComplex();
                            assertNotNull(cellTombstonesPerCol);
                            Map<String, SparkValueWithMetadata> valueColMap = event.getValueColumns()
                                                                                   .stream()
                                                                                   .collect(Collectors.toMap(v -> v.columnName, Function.identity()));
                            for (String name : collectionColumnNames)
                            {
                                assertNull("Collection column's value should be null since only deletion applies",
                                           valueColMap.get(name).getValue());
                                assertNotNull(cellTombstonesPerCol.get(name));
                                List<ByteBuffer> deletedCellKeys = cellTombstonesPerCol.get(name);
                                assertEquals(1, deletedCellKeys.size());
                                assert deletedCellKeys.get(0).hasArray();
                                byte[] keyBytesRead = deletedCellKeys.get(0).array();
                                assertArrayEquals("The key encoded should be the same",
                                                  elementDeletionIndices.get(i), keyBytesRead);
                            }
                        }
                        else // verify update
                        {
                            assertEquals(AbstractCdcEvent.Kind.INSERT, event.kind);
                            assertNotNull(event.getValueColumns());
                        }
                    }
                })
                .run();
            });
    }

    @Test
    public void testRangeDeletions()
    {
        testRangeDeletions(false, // has static
                           1, // num of partition key columns
                           2, // num of clustering key columns
                           true, // openEnd
                           type -> TestSchema.builder()
                                             .withPartitionKey("pk1", bridge.uuid())
                                             .withClusteringKey("ck1", type)
                                             .withClusteringKey("ck2", bridge.bigint())
                                             .withColumn("c1", type));
        testRangeDeletions(false, // has static
                           1, // num of partition key columns
                           2, // num of clustering key columns
                           false, // openEnd
                           type -> TestSchema.builder()
                                             .withPartitionKey("pk1", bridge.uuid())
                                             .withClusteringKey("ck1", type)
                                             .withClusteringKey("ck2", bridge.bigint())
                                             .withColumn("c1", type));
    }

    @Test
    public void testRangeDeletionsWithStatic()
    {
        testRangeDeletions(true, // has static
                           1, // num of partition key columns
                           2, // num of clustering key columns
                           true, // openEnd
                           type -> TestSchema.builder()
                                             .withPartitionKey("pk1", bridge.uuid())
                                             .withClusteringKey("ck1", bridge.ascii())
                                             .withClusteringKey("ck2", bridge.bigint())
                                             .withStaticColumn("s1", bridge.uuid())
                                             .withColumn("c1", type));
        testRangeDeletions(true, // has static
                           1, // num of partition key columns
                           2, // num of clustering key columns
                           false, // openEnd
                           type -> TestSchema.builder()
                                             .withPartitionKey("pk1", bridge.uuid())
                                             .withClusteringKey("ck1", bridge.ascii())
                                             .withClusteringKey("ck2", bridge.bigint())
                                             .withStaticColumn("s1", bridge.uuid())
                                             .withColumn("c1", type));
    }

    // validate that range deletions can be correctly encoded.
    private void testRangeDeletions(boolean hasStatic, int numOfPartitionKeys, int numOfClusteringKeys, boolean withOpenEnd, Function<SparkCqlField.SparkCqlType, TestSchema.Builder> schemaBuilder)
    {
        Preconditions.checkArgument(numOfClusteringKeys > 0, "Range deletion test won't run without having clustering keys!");
        // key: row# that has deletion; value: the deleted cell key/path in the collection
        final Map<Integer, TestSchema.TestRow> rangeTombstones = new HashMap<>();
        final Random rnd = new Random(1);
        final long minTimestamp = System.currentTimeMillis();
        final int numRows = 1000;
        qt().forAll(SparkTestUtils.cql3Type(bridge))
            .checkAssert(type -> {
                testWith(bridge, DIR, schemaBuilder.apply(type))
                .withAddLastModificationTime(true)
                .clearWriters()
                .withNumRows(numRows)
                .withWriter((tester, rows, writer) -> {
                    long timestamp = minTimestamp;
                    rangeTombstones.clear();
                    for (int i = 0; i < tester.numRows; i++)
                    {
                        TestSchema.TestRow testRow;
                        if (rnd.nextDouble() < 0.5)
                        {
                            testRow = Tester.newUniqueRow(tester.schema, rows);
                            Object[] baseBound = testRow.rawValues(numOfPartitionKeys, numOfPartitionKeys + numOfClusteringKeys);
                            // create a new bound that has the last CK value different from the base bound
                            Object[] newBound = new Object[baseBound.length];
                            System.arraycopy(baseBound, 0, newBound, 0, baseBound.length);
                            TestSchema.TestRow newRow = Tester.newUniqueRow(tester.schema, rows);
                            int lastCK = newBound.length - 1;
                            newBound[lastCK] = newRow.get(numOfPartitionKeys + numOfClusteringKeys - 1);
                            Object[] open, close;
                            // the field's corresponding java type should be comparable... (ugly :()
                            if (((Comparable<Object>) baseBound[lastCK]).compareTo(newBound[lastCK]) < 0) // for queries like WHERE ck > 1 AND ck < 2
                            {
                                open = baseBound;
                                close = newBound;
                            }
                            else
                            {
                                open = newBound;
                                close = baseBound;
                            }
                            if (withOpenEnd) // for queries like WHERE ck > 1
                            {
                                close[lastCK] = null;
                            }
                            testRow.setRangeTombstones(Arrays.asList(
                            new CassandraBridge.RangeTombstoneData(new CassandraBridge.RangeTombstoneData.Bound(open, true), new CassandraBridge.RangeTombstoneData.Bound(close, true))));
                            rangeTombstones.put(i, testRow);
                        }
                        else
                        {
                            testRow = Tester.newUniqueRow(tester.schema, rows);
                        }
                        timestamp += 1;
                        writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(timestamp));
                    }
                })
                .withCdcEventChecker((testRows, events) -> {
                    for (int i = 0; i < events.size(); i++)
                    {
                        SparkCdcEvent event = events.get(i);
                        long lmtInMillis = event.getTimestamp(TimeUnit.MILLISECONDS);
                        assertTrue("Last modification time should have a lower bound of " + minTimestamp,
                                   lmtInMillis >= minTimestamp);
                        assertEquals("Regardless of being row deletion or not, the partition key must present",
                                     numOfPartitionKeys, event.getPartitionKeys().size());

                        if (rangeTombstones.containsKey(i)) // verify deletion
                        {
                            assertEquals(AbstractCdcEvent.Kind.RANGE_DELETE, event.kind);
                            // the bounds are added in its dedicated column.
                            assertNull("Clustering keys should be absent for range deletion",
                                       event.getClusteringKeys());
                            assertNull(event.getStaticColumns());
                            List<SparkRangeTombstone> rangeTombstoneList = event.getRangeTombstoneList();
                            assertNotNull(rangeTombstoneList);
                            assertEquals("There should be 1 range tombstone",
                                         1, rangeTombstoneList.size());
                            TestSchema.TestRow sourceRow = rangeTombstones.get(i);
                            CassandraBridge.RangeTombstoneData expectedRT = sourceRow.rangeTombstones().get(0);
                            SparkRangeTombstone rt = rangeTombstoneList.get(0);
                            assertEquals(expectedRT.open.inclusive, rt.startInclusive);
                            assertEquals(expectedRT.close.inclusive, rt.endInclusive);
                            assertEquals(numOfClusteringKeys, rt.getStartBound().size());
                            assertEquals(withOpenEnd ? numOfClusteringKeys - 1 : numOfClusteringKeys,
                                         rt.getEndBound().size());
                            Object[] startBoundVals = rt.getStartBound().stream()
                                                        .map(v -> v.getCqlType(bridge::parseType)
                                                                   .deserializeToJava(v.getValue()))
                                                        .toArray();
                            SparkTestUtils.assertEquals(expectedRT.open.values, startBoundVals);

                            Object[] endBoundVals = rt.getEndBound().stream()
                                                      .map(v -> v.getCqlType(bridge::parseType)
                                                                 .deserializeToJava(v.getValue()))
                                                      .toArray();
                            // The range bound in mutation does not encode the null value.
                            // We need to get rid of the null in the test value array
                            Object[] expectedCloseVals = withOpenEnd
                                                         ? new Object[numOfClusteringKeys - 1]
                                                         : expectedRT.close.values;
                            System.arraycopy(expectedRT.close.values, 0,
                                             expectedCloseVals, 0, expectedCloseVals.length);
                            SparkTestUtils.assertEquals(expectedCloseVals, endBoundVals);
                        }
                        else // verify update
                        {
                            assertEquals(AbstractCdcEvent.Kind.INSERT, event.kind);
                            assertNotNull(event.getClusteringKeys());
                            if (hasStatic)
                            {
                                assertNotNull(event.getStaticColumns());
                            }
                            else
                            {
                                assertNull(event.getStaticColumns());
                            }
                            assertNotNull(event.getValueColumns());
                        }
                    }
                })
                .run();
            });
    }

    // jdk cdc iterator tests

    @Test
    public void testInserts()
    {
        runJdkTest(bridge, TestSchema.builder()
                                     .withPartitionKey("a", bridge.timeuuid())
                                     .withPartitionKey("b", bridge.text())
                                     .withClusteringKey("c", bridge.timestamp())
                                     .withColumn("d", bridge.map(bridge.text(), bridge.aInt())),
                   (schema, i, rows) -> {
                       final TestSchema.TestRow row = schema.randomRow();
                       rows.put(row.getKey(), row);
                       return row;
                   },
                   (msg, rows, nowMicros) -> {
                       assertEquals(msg.operationType(), AbstractCdcEvent.Kind.INSERT);
                       assertEquals(msg.lastModifiedTimeMicros(), nowMicros);
                       final String key = msg.column("a").value().toString() + ":" +
                                          msg.column("b").value().toString() + ":" +
                                          msg.column("c").value().toString();
                       assertTrue(rows.containsKey(key));
                       final TestSchema.TestRow testRow = rows.get(key);
                       final Map<String, Integer> expected = (Map<String, Integer>) testRow.get(3);
                       final Column col = msg.valueColumns().get(0);
                       assertEquals("d", col.name());
                       assertEquals("map<text, int>", col.type().cqlName());
                       final Map<String, Integer> actual = (Map<String, Integer>) col.value();
                       assertEquals(expected, actual);
                   });
    }

    @Test
    public void testPartitionDelete()
    {
        runJdkTest(bridge,
                   TestSchema.builder()
                             .withPartitionKey("a", bridge.timeuuid())
                             .withPartitionKey("b", bridge.aInt())
                             .withClusteringKey("c", bridge.bigint())
                             .withColumn("d", bridge.text()),
                   (schema, i, rows) -> {
                       final TestSchema.TestRow row = schema.randomPartitionDelete();
                       rows.put(row.get(0) + ":" + row.get(1), row); // partition delete so just the partition keys
                       return row;
                   },
                   (msg, rows, nowMicros) -> {
                       assertEquals(msg.operationType(), AbstractCdcEvent.Kind.PARTITION_DELETE);
                       assertEquals(msg.lastModifiedTimeMicros(), nowMicros);
                       final String key = msg.column("a").value().toString() + ":" + msg.column("b").value().toString();
                       assertTrue(rows.containsKey(key));
                       assertEquals(2, msg.partitionKeys().size());
                       assertEquals(0, msg.clusteringKeys().size());
                       assertEquals(0, msg.staticColumns().size());
                       assertEquals(0, msg.valueColumns().size());
                   });
    }

    @Test
    public void testRowDelete()
    {
        runJdkTest(bridge,
                   TestSchema.builder()
                             .withPartitionKey("a", bridge.timeuuid())
                             .withPartitionKey("b", bridge.aInt())
                             .withClusteringKey("c", bridge.bigint())
                             .withColumn("d", bridge.text()),
                   (schema, i, rows) -> {
                       final TestSchema.TestRow row = schema.randomRow();
                       row.delete();
                       rows.put(row.getKey(), row);
                       return row;
                   },
                   (msg, rows, nowMicros) -> {
                       assertEquals(msg.operationType(), AbstractCdcEvent.Kind.ROW_DELETE);
                       assertEquals(msg.lastModifiedTimeMicros(), nowMicros);
                       final String key = msg.column("a").value().toString() + ":" +
                                          msg.column("b").value().toString() + ":" +
                                          msg.column("c").value().toString();
                       assertTrue(rows.containsKey(key));
                       assertEquals(2, msg.partitionKeys().size());
                       assertEquals(1, msg.clusteringKeys().size());
                       assertEquals(0, msg.staticColumns().size());
                       assertEquals(0, msg.valueColumns().size());
                   });
    }

    @Test
    public void testUpdate()
    {
        runJdkTest(bridge,
                   TestSchema.builder()
                             .withPartitionKey("a", bridge.timeuuid())
                             .withPartitionKey("b", bridge.aInt())
                             .withClusteringKey("c", bridge.bigint())
                             .withColumn("d", bridge.text()),
                   (schema, i, rows) -> {
                       final TestSchema.TestRow row = schema.randomRow();
                       row.fromUpdate();
                       rows.put(row.getKey(), row);
                       return row;
                   },
                   (msg, rows, nowMicros) -> {
                       assertEquals(msg.operationType(), AbstractCdcEvent.Kind.UPDATE);
                       assertEquals(msg.lastModifiedTimeMicros(), nowMicros);
                       final String key = msg.column("a").value().toString() + ":" +
                                          msg.column("b").value().toString() + ":" +
                                          msg.column("c").value().toString();
                       assertTrue(rows.containsKey(key));
                       assertEquals(2, msg.partitionKeys().size());
                       assertEquals(1, msg.clusteringKeys().size());
                       assertEquals(0, msg.staticColumns().size());
                       assertEquals(1, msg.valueColumns().size());
                       final TestSchema.TestRow row = rows.get(key);
                       final String expected = (String) row.get(3);
                       assertEquals(expected, msg.valueColumns().get(0).value().toString());
                   }
        );
    }

    @Test
    public void testRangeTombstone()
    {
        runJdkTest(bridge,
                   TestSchema.builder()
                             .withPartitionKey("a", bridge.uuid())
                             .withClusteringKey("b", bridge.aInt())
                             .withClusteringKey("c", bridge.aInt())
                             .withColumn("d", bridge.text()),
                   (schema, i, rows) -> {
                       final TestSchema.TestRow testRow = Tester.newUniqueRow(schema, rows);
                       final int start = RandomUtils.randomPositiveInt(1024);
                       final int end = start + RandomUtils.randomPositiveInt(100000);
                       testRow.setRangeTombstones(Collections.singletonList(
                                                  new CassandraBridge.RangeTombstoneData(new CassandraBridge.RangeTombstoneData.Bound(new Integer[]{ start, start + RandomUtils.randomPositiveInt(100) }, true),
                                                                                         new CassandraBridge.RangeTombstoneData.Bound(new Integer[]{ end, end + RandomUtils.randomPositiveInt(100) }, true)))
                       );
                       rows.put(testRow.get(0).toString(), testRow);
                       return testRow;
                   },
                   (msg, rows, nowMicros) -> {
                       assertEquals(msg.operationType(), AbstractCdcEvent.Kind.RANGE_DELETE);
                       final List<org.apache.cassandra.spark.cdc.jdk.msg.RangeTombstone> tombstones = msg.rangeTombstones();
                       final TestSchema.TestRow row = rows.get(msg.column("a").value().toString());
                       assertEquals(1, tombstones.size());
                       final org.apache.cassandra.spark.cdc.jdk.msg.RangeTombstone tombstone = tombstones.get(0);
                       assertTrue(tombstone.startInclusive);
                       assertTrue(tombstone.endInclusive);
                       assertEquals(2, tombstone.startBound().size());
                       assertEquals(2, tombstone.endBound().size());
                       assertEquals("b", tombstone.startBound().get(0).name());
                       assertEquals("c", tombstone.startBound().get(1).name());
                       assertEquals("b", tombstone.endBound().get(0).name());
                       assertEquals("c", tombstone.endBound().get(1).name());
                       final CassandraBridge.RangeTombstoneData expected = row.rangeTombstones().get(0);
                       assertEquals(expected.open.values[0], tombstone.startBound().get(0).value());
                       assertEquals(expected.open.values[1], tombstone.startBound().get(1).value());
                       assertEquals(expected.close.values[0], tombstone.endBound().get(0).value());
                       assertEquals(expected.close.values[1], tombstone.endBound().get(1).value());
                       assertEquals(expected.open.inclusive, tombstone.startInclusive);
                       assertEquals(expected.close.inclusive, tombstone.endInclusive);
                   }
        );
    }

    @Test
    public void testSetDeletion()
    {
        final Map<String, String> deletedValues = new HashMap<>(SparkTestUtils.NUM_ROWS);
        runJdkTest(bridge,
                   TestSchema.builder()
                             .withPartitionKey("a", bridge.uuid())
                             .withColumn("b", bridge.set(bridge.text())),
                   (schema, i, rows) -> {
                       TestSchema.TestRow testRow = Tester.newUniqueRow(schema, rows);
                       final String deletedValue = (String) bridge.text().randomValue(4);
                       final ByteBuffer key = bridge.text().serialize(deletedValue);
                       testRow = testRow.copy("b", CassandraBridge.CollectionElement.deleted(CellPath.create(key)));
                       deletedValues.put(testRow.get(0).toString(), deletedValue);
                       return testRow;
                   },
                   (msg, rows, nowMicros) -> {
                       assertEquals(msg.operationType(), AbstractCdcEvent.Kind.COMPLEX_ELEMENT_DELETE);
                       final String expected = deletedValues.get(Objects.requireNonNull(msg.partitionKeys().get(0).value()).toString());
                       assertNotNull(msg.getComplexCellDeletion());
                       assertEquals(expected, msg.getComplexCellDeletion().get("b").get(0).toString());
                   }
        );
    }

    @Test
    public void testMapDeletion()
    {
        final Map<String, String> deletedValues = new HashMap<>(SparkTestUtils.NUM_ROWS);
        runJdkTest(bridge,
                   TestSchema.builder()
                             .withPartitionKey("a", bridge.uuid())
                             .withColumn("b", bridge.map(bridge.text(), bridge.aInt())),
                   (schema, i, rows) -> {
                       TestSchema.TestRow testRow = Tester.newUniqueRow(schema, rows);
                       final String deletedValue = (String) bridge.text().randomValue(4);
                       final ByteBuffer key = bridge.text().serialize(deletedValue);
                       testRow = testRow.copy("b", CassandraBridge.CollectionElement.deleted(CellPath.create(key)));
                       deletedValues.put(testRow.get(0).toString(), deletedValue);
                       return testRow;
                   },
                   (msg, rows, nowMicros) -> {
                       assertEquals(msg.operationType(), AbstractCdcEvent.Kind.COMPLEX_ELEMENT_DELETE);
                       final String expected = deletedValues.get(Objects.requireNonNull(msg.partitionKeys().get(0).value()).toString());
                       assertNotNull(msg.getComplexCellDeletion());
                       assertEquals(expected, msg.getComplexCellDeletion().get("b").get(0).toString());
                   }
        );
    }

    public interface RowGenerator
    {
        TestSchema.TestRow newRow(TestSchema schema, int i, Map<String, TestSchema.TestRow> rows);
    }

    public interface TestVerifier
    {
        void verify(CdcMessage msg, Map<String, TestSchema.TestRow> rows, long nowMicros);
    }

    private static void resetJdkTest()
    {
        CdcTester.tearDown();
        LOG.start();
    }

    private static void runJdkTest(CassandraBridge bridge,
                                   TestSchema.Builder schemaBuilder,
                                   RowGenerator rowGenerator,
                                   TestVerifier verify)
    {
        final long nowMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        final int numRows = SparkTestUtils.NUM_ROWS;
        final TestSchema schema = schemaBuilder
                                  .withCdc(true)
                                  .build();
        final CqlTable cqlTable = schema.buildSchema();
        schema.schemaBuilder(Partitioner.Murmur3Partitioner);
        schema.setCassandraVersion(CassandraVersion.FOURZERO);

        try
        {
            final Map<String, TestSchema.TestRow> rows = new HashMap<>(numRows);
            for (int i = 0; i < numRows; i++)
            {
                final TestSchema.TestRow row = rowGenerator.newRow(schema, i, rows);
                bridge.log(cqlTable, LOG, row, nowMicros);
            }
            LOG.sync();

            int count = 0;
            final long start = System.currentTimeMillis();
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
            try (final TestJdkCdcIterator cdc = new TestJdkCdcIterator(DIR.getRoot().toPath()))
            {
                while (count < numRows && cdc.next())
                {
                    cdc.advanceToNextColumn();
                    verify.verify(cdc.data().toRow(), rows, nowMicros);
                    count++;
                    if (CdcTester.maybeTimeout(start, numRows, count, cdc.jobId))
                    {
                        break;
                    }
                }
                assertEquals(numRows, count);
            }
        }
        finally
        {
            resetJdkTest();
        }
    }
}
