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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.fourzero.FourZeroSchemaBuilder;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.BufferingCommitLogReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.stats.Stats;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.cdc.CdcTester.LOG;
import static org.apache.cassandra.spark.cdc.CdcTester.assertCqlTypeEquals;
import static org.apache.cassandra.spark.cdc.CdcTester.testWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
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

    public CdcTests(CassandraBridge.CassandraVersion version)
    {
        super(version);
    }

    public static final TestStats STATS = new TestStats();

    @Test
    public void testSinglePartitionKey()
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", type))
                .withCdcEventChecker((testRows, events) -> {
                    for (AbstractCdcEvent event : events)
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
        qt().forAll(TestUtils.cql3Type(bridge))
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
                    for (AbstractCdcEvent event : events)
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
        qt().forAll(TestUtils.cql3Type(bridge))
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
                        for (final CqlField field : tester.cqlSchema.valueColumns())
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
                    for (AbstractCdcEvent event : events)
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
        qt().forAll(TestUtils.cql3Type(bridge))
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
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(t -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk1", bridge.uuid())
                                                .withPartitionKey("pk2", t)
                                                .withPartitionKey("pk3", bridge.timestamp())
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", bridge.text()))
                    .withCdcEventChecker((testRows, events) -> {
                        for (AbstractCdcEvent event : events)
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
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(t ->  {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withClusteringKey("ck", t)
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", bridge.text()))
                    .withCdcEventChecker((testRows, events) -> {
                        for (AbstractCdcEvent event : events)
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
        qt().withExamples(50).forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((t1, t2, t3) -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withClusteringKey("ck1", t1)
                                                .withClusteringKey("ck2", t2)
                                                .withClusteringKey("ck3", t3)
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", bridge.text()))
                    .withCdcEventChecker((testRows, events) -> {
                        for (AbstractCdcEvent event : events)
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
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(t -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", bridge.set(t)))
                    .withCdcEventChecker((testRows, events) -> {
                        for (AbstractCdcEvent event : events)
                        {
                            assertEquals(1, event.getPartitionKeys().size());
                            assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                            assertNull(event.getClusteringKeys());
                            assertNull(event.getStaticColumns());
                            assertEquals(Arrays.asList("c1", "c2"),
                                         event.getValueColumns().stream()
                                              .map(v -> v.columnName)
                                              .collect(Collectors.toList()));
                            String setType = event.getValueColumns().get(1).columnType;
                            assertTrue(setType.startsWith("set<"));
                            assertCqlTypeEquals(t.cqlName(),
                                                setType.substring(4, setType.length() - 1)); // extract the type in set<>
                            assertNull(event.getTtl());
                        }
                    })
                    .run();
            });
    }

    @Test
    public void testList()
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(t -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", bridge.list(t)))
                    .withCdcEventChecker((testRows, events) -> {
                        for (AbstractCdcEvent event : events)
                        {
                            assertEquals(1, event.getPartitionKeys().size());
                            assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                            assertNull(event.getClusteringKeys());
                            assertNull(event.getStaticColumns());
                            assertEquals(Arrays.asList("c1", "c2"),
                                         event.getValueColumns().stream()
                                              .map(v -> v.columnName)
                                              .collect(Collectors.toList()));
                            String listType = event.getValueColumns().get(1).columnType;
                            assertTrue(listType.startsWith("list<"));
                            assertCqlTypeEquals(t.cqlName(),
                                                listType.substring(5, listType.length() - 1)); // extract the type in list<>
                            assertNull(event.getTtl());
                        }
                    })
                    .run();
            });
    }

    @Test
    public void testMap()
    {
        qt().withExamples(50).forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((t1, t2) -> {
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", bridge.map(t1, t2)))
                    .withCdcEventChecker((testRows, events) -> {
                        for (AbstractCdcEvent event : events)
                        {
                            assertEquals(1, event.getPartitionKeys().size());
                            assertEquals("pk", event.getPartitionKeys().get(0).columnName);
                            assertNull(event.getClusteringKeys());
                            assertNull(event.getStaticColumns());
                            assertEquals(Arrays.asList("c1", "c2"),
                                         event.getValueColumns().stream()
                                              .map(v -> v.columnName)
                                              .collect(Collectors.toList()));
                            String mapType = event.getValueColumns().get(1).columnType;
                            assertTrue(mapType.startsWith("map<"));
                            int commaIndex = mapType.indexOf(',');
                            assertCqlTypeEquals(t1.cqlName(),
                                                // extract the key type in map<>
                                                mapType.substring(4, commaIndex)); // extract the key type in map<>
                            assertCqlTypeEquals(t2.cqlName(),
                                                // extract the value type in map<>; +2 to exclude , and the following space
                                                mapType.substring(commaIndex + 2, mapType.length() - 1));
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
            .forAll(TestUtils.cql3Type(bridge))
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
                        for (AbstractCdcEvent event : events)
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
                            AbstractCdcEvent.Kind expectedKind = i >= halfway
                                                                 ? AbstractCdcEvent.Kind.UPDATE
                                                                 : AbstractCdcEvent.Kind.INSERT;
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
        final CqlSchema cqlSchema = schema.buildSchema();
        final FourZeroSchemaBuilder schemaBuilder = new FourZeroSchemaBuilder(cqlSchema, Partitioner.Murmur3Partitioner);
        final TableMetadata metadata = schemaBuilder.tableMetaData();
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
            bridge.log(cqlSchema, LOG, row, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        }
        LOG.sync();

        final List<CommitLog.Marker> markers = Collections.synchronizedList(new ArrayList<>());
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
        Consumer<CommitLog.Marker> listener = markers::add;
        final Set<Long> allRows = readLog(metadata, null, keys, logFile, listener);
        assertEquals(numRows, allRows.size());

        // re-read commit log from each watermark position
        // and verify subset of partitions are read
        int foundRows = allRows.size();
        allRows.clear();
        final List<CommitLog.Marker> allMarkers = new ArrayList<>(markers);
        CommitLog.Marker prevMarker = null;
        for (final CommitLog.Marker marker : allMarkers)
        {
            final Set<Long> result = readLog(metadata, marker, keys, logFile, null);
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
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((t1, t2, t3) -> {
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
                        assertTrue(STATS.getStats(TestStats.TEST_CDC_COMMIT_LOG_BYTES_SKIPPED).size() > 0);
                    })
                    .run();
            });
    }

    private Set<Long> readLog(TableMetadata metadata,
                              @Nullable final CommitLog.Marker highWaterMark,
                              Set<Long> keys,
                              File logFile,
                              @Nullable Consumer<CommitLog.Marker> listener)
    {
        final Set<Long> result = new HashSet<>();

        try (final LocalDataLayer.LocalCommitLog log = new LocalDataLayer.LocalCommitLog(logFile))
        {
            try (final BufferingCommitLogReader reader = new BufferingCommitLogReader(metadata, log, highWaterMark,
                                                                                      Stats.DoNothingStats.INSTANCE,
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
}
