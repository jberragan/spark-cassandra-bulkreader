package org.apache.cassandra.spark.cdc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.fourzero.FourZeroSchemaBuilder;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.BufferingCommitLogReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.CdcUpdate;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.Nullable;
import scala.collection.mutable.WrappedArray;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
            .checkAssert(type ->
                         CdcTester.builder(bridge, DIR, TestSchema.builder()
                                                                  .withPartitionKey("pk", bridge.uuid())
                                                                  .withColumn("c1", bridge.bigint())
                                                                  .withColumn("c2", type))
                                  .withRowChecker(sparkRows -> {
                                      for (Row row : sparkRows)
                                      {
                                          byte[] updatedFieldsIndicator = (byte[]) row.get(3);
                                          BitSet bs = BitSet.valueOf(updatedFieldsIndicator);
                                          BitSet expected = new BitSet(3);
                                          expected.set(0, 3); // expecting all columns to be set
                                          assertEquals(expected, bs);
                                          assertRowHasNoTTL(row);
                                      }
                                  })
                                  .run());
    }

    @Test
    public void testUpdatedFieldsIndicator()
    {
        final Set<Integer> ttlRowIdx = new HashSet<>();
        final Random rnd = new Random(1);
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         CdcTester.builder(bridge, DIR, TestSchema.builder()
                                                                  .withPartitionKey("pk", bridge.uuid())
                                                                  .withColumn("c1", bridge.bigint())
                                                                  .withColumn("c2", type))
                                  .clearWriters()
                                  .withAddLastModificationTime(true)
                                  .withWriter((tester, rows, writer) -> {
                                      ttlRowIdx.clear();
                                      long time = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                                      for (int i = 0; i < tester.numRows; i++)
                                      {
                                          TestSchema.TestRow testRow = Tester.newUniqueRow(tester.schema, rows);
                                          testRow = testRow.copy("c1", CassandraBridge.UNSET_MARKER); // mark c1 as not updated / unset
                                          if (rnd.nextDouble() > 0.5)
                                          {
                                              testRow.setTTL(TTL);
                                              ttlRowIdx.add(i);
                                          }
                                          writer.accept(testRow, time++);
                                      }
                                  })
                                  .withSparkRowTestRowsChecker((actualRow, sparkRows) -> {
                                      int i = 0;
                                      for (Row row : sparkRows)
                                      {
                                          byte[] updatedFieldsIndicator = (byte[]) row.get(4);
                                          BitSet bs = BitSet.valueOf(updatedFieldsIndicator);
                                          BitSet expected = new BitSet(3);
                                          expected.set(0); // expecting pk to be set
                                          expected.set(2); // and c2 to be set.
                                          assertEquals(expected, bs);
                                          assertNull("c1 should be null", row.get(1));
                                          if (ttlRowIdx.contains(i))
                                          {
                                              assertRowHasTTL(row);
                                          }
                                          else
                                          {
                                              assertRowHasNoTTL(row);
                                          }
                                          i++;
                                      }
                                  })
                                  .run());
    }

    @Test
    public void testMultipleWritesToSameKeyInBatch()
    {
        // The test writes different groups of mutations.
        // Each group of mutations write to the same key with the different timestamp.
        // For CDC, it only deduplicate and emit the replicated mutations, i.e. they have the same writetime.
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type -> CdcTester.builder(bridge, DIR, TestSchema.builder()
                                                                          .withPartitionKey("pk", bridge.uuid())
                                                                          .withColumn("c1", bridge.bigint())
                                                                          .withColumn("c2", type))
                                          .clearWriters()
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
                                          .withChecker((testRows, actualRows) -> {
                                              int partitions = testRows.size();
                                              int mutations = actualRows.size();
                                              assertEquals("Each PK should get 2 mutations", partitions * 2, mutations);
                                          })
                                          .withSparkRowTestRowsChecker((testRows, sparkRows) -> {
                                              long ts = -1L;
                                              int partitions = testRows.size();
                                              int i = 0;
                                              for (Row row : sparkRows)
                                              {
                                                  if (ts == -1L)
                                                  {
                                                      ts = getMicros(row.getTimestamp(3));
                                                  }
                                                  else
                                                  {
                                                      long lastTs = ts;
                                                      ts = getMicros(row.getTimestamp(3));
                                                      assertTrue("Writetime should be monotonic increasing",
                                                                 lastTs < ts);
                                                  }
                                                  if (i >= partitions) // the rows in the second batch has ttl specified.
                                                  {
                                                      assertRowHasTTL(row);
                                                  }
                                                  i++;
                                              }
                                          })
                                          .run());
    }

    private long getMicros(java.sql.Timestamp timestamp)
    {
        long millis = timestamp.getTime();
        int nanos = timestamp.getNanos();
        return TimeUnit.MILLISECONDS.toMicros(millis) + TimeUnit.NANOSECONDS.toMicros(nanos);
    }

    @Test
    public void testCompactOnlyWithEnoughReplicas()
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         CdcTester.builder(bridge, DIR, TestSchema.builder()
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
                                      final long timestamp = System.currentTimeMillis();
                                      Map<Long, TestSchema.TestRow> genRows = new HashMap<>();
                                      IntStream.range(0, tester.numRows)
                                               .forEach(i -> genRows.put(timestamp + i, Tester.newUniqueRow(tester.schema, rows)));
                                      genRows.forEach((key, value) -> writer.accept(value, TimeUnit.MILLISECONDS.toMicros(key)));

                                      // Write the same values again, with the first value skipped.
                                      // All values except the first one have 2 copies
                                      // The test is using RequireTwoReplicasCompactionDataSource,
                                      // so the output should not contain the first value
                                      for (long i = 1; i < tester.numRows; i++)
                                      {
                                          writer.accept(genRows.get(timestamp + i), TimeUnit.MILLISECONDS.toMicros(timestamp + i));
                                      }
                                  })
                                  .withRowChecker(rows -> {
                                      int size = rows.size();
                                      // the timestamp column is added at column 4
                                      int uniqueTsCount = rows.stream().map(r -> r.getTimestamp(3).getTime())
                                                              .collect(Collectors.toSet())
                                                              .size();
                                      Assert.assertEquals("Output rows should have distinct lastModified timestamps", size, uniqueTsCount);
                                  })
                                  .withChecker((testRows, actualRows) -> {
                                      Assert.assertEquals("There should be exact one row less in the output.",
                                                          actualRows.size() + 1, testRows.size());
                                      boolean allContains = true;
                                      TestSchema.TestRow unexpectedRow = null;
                                      for (TestSchema.TestRow row : actualRows)
                                      {
                                          if (!testRows.containsValue(row))
                                          {
                                              allContains = false;
                                              unexpectedRow = row;
                                              break;
                                          }
                                      }
                                      if (!allContains && unexpectedRow != null)
                                      {
                                          Assert.fail("Found an unexpected row from the output: " + unexpectedRow);
                                      }
                                  })
                                  .run());
    }

    @Test
    public void testCompositePartitionKey()
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(t ->
                         CdcTester.builder(bridge, DIR, TestSchema.builder()
                                                                  .withPartitionKey("pk1", bridge.uuid())
                                                                  .withPartitionKey("pk2", t)
                                                                  .withPartitionKey("pk3", bridge.timestamp())
                                                                  .withColumn("c1", bridge.bigint())
                                                                  .withColumn("c2", bridge.text()))
                                  .run()
            );
    }

    @Test
    public void testClusteringKey()
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(t ->
                         CdcTester.builder(bridge, DIR, TestSchema.builder()
                                                                  .withPartitionKey("pk", bridge.uuid())
                                                                  .withPartitionKey("ck", t)
                                                                  .withColumn("c1", bridge.bigint())
                                                                  .withColumn("c2", bridge.text()))
                                  .run()
            );
    }

    @Test
    public void testMultipleClusteringKeys()
    {
        qt().withExamples(50).forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((t1, t2, t3) ->
                         CdcTester.builder(bridge, DIR, TestSchema.builder()
                                                                  .withPartitionKey("pk", bridge.uuid())
                                                                  .withClusteringKey("ck1", t1)
                                                                  .withClusteringKey("ck2", t2)
                                                                  .withClusteringKey("ck3", t3)
                                                                  .withColumn("c1", bridge.bigint())
                                                                  .withColumn("c2", bridge.text()))
                                  .run()
            );
    }

    @Test
    public void testSet()
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(t ->
                         CdcTester.builder(bridge, DIR, TestSchema.builder()
                                                                  .withPartitionKey("pk", bridge.uuid())
                                                                  .withColumn("c1", bridge.bigint())
                                                                  .withColumn("c2", bridge.set(t))
                                  )
                                  .run());
    }

    @Test
    public void testList()
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(t ->
                         CdcTester.builder(bridge, DIR, TestSchema.builder()
                                                                  .withPartitionKey("pk", bridge.uuid())
                                                                  .withColumn("c1", bridge.bigint())
                                                                  .withColumn("c2", bridge.list(t))
                                  )
                                  .run());
    }

    @Test
    public void testMap()
    {
        //todo
        qt().withExamples(1).forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((t1, t2) ->
                         CdcTester.builder(bridge, DIR, TestSchema.builder()
                                                                  .withPartitionKey("pk", bridge.uuid())
                                                                  .withColumn("c1", bridge.bigint())
                                                                  .withColumn("c2", bridge.map(t1, t2))
                                  )
                                  .run());
    }

    @Test
    public void testUpdateFlag()
    {
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         CdcTester.builder(bridge, DIR, TestSchema.builder()
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
                                  .withRowChecker(sparkRows -> {
                                      final int len = sparkRows.size();
                                      final int halfway = len / 2;
                                      for (Row row : sparkRows)
                                      {
                                          final int i = row.getInt(1);
                                          final boolean isUpdate = row.getBoolean(4);
                                          assertEquals(isUpdate, i >= halfway);
                                          assertRowHasTTL(row);
                                      }
                                  })
                                  .run());
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
            bridge.log(cqlSchema, CdcTester.LOG, row, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        }
        CdcTester.LOG.sync();

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
        qt().withExamples(1).forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((t1, t2, t3) ->
                         CdcTester.builder(bridge, DIR, TestSchema.builder()
                                                                  .withPartitionKey("pk", bridge.uuid())
                                                                  .withClusteringKey("ck1", t1)
                                                                  .withClusteringKey("ck2", t2)
                                                                  .withClusteringKey("ck3", t3)
                                                                  .withColumn("c1", bridge.bigint())
                                                                  .withColumn("c2", bridge.text()))
                                  .withStatsClass(CdcTests.class.getName() + ".STATS")
                                  .withRowChecker(rows -> {
                                      int rowCount = rows.size();
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

                                      STATS.reset();
                                  })
                                  .run()
            );
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
            try (final BufferingCommitLogReader reader = new BufferingCommitLogReader(metadata, log, highWaterMark, Stats.DoNothingStats.INSTANCE, listener))
            {
                for (final CdcUpdate update : reader.result().updates())
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

    private static void assertRowHasTTL(Row row)
    {
        WrappedArray<Integer> ttl = row.getAs(SchemaFeatureSet.TTL.optionName());
        assertEquals(TTL, (int) ttl.apply(0));
    }

    private static void assertRowHasNoTTL(Row row)
    {
        assertNull(row.getAs(SchemaFeatureSet.TTL.optionName()));
    }
}
