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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.fourzero.FourZeroSchemaBuilder;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.BufferingCommitLogReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.CdcUpdate;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.Nullable;

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
    @ClassRule
    public static TemporaryFolder DIR = new TemporaryFolder();

    @BeforeClass
    public static void setup()
    {
        CdcTester.setup();
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
                                      }
                                  })
                                  .run());
    }

    @Test
    public void testUpdatedFieldsIndicator()
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         CdcTester.builder(bridge, DIR, TestSchema.builder()
                                                                  .withPartitionKey("pk", bridge.uuid())
                                                                  .withColumn("c1", bridge.bigint())
                                                                  .withColumn("c2", type))
                                  .clearWriters()
                                  .withAddLastModificationTime(true)
                                  .withWriter((tester, rows, writer) -> {
                                      for (int i = 0; i < tester.numRows; i++)
                                      {
                                          TestSchema.TestRow testRow = Tester.newUniqueRow(tester.schema, rows);
                                          testRow = testRow.copy("c1", CassandraBridge.UNSET_MARKER); // mark c1 as not updated / unset
                                          writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                                      }
                                  })
                                  .withRowChecker(sparkRows -> {
                                      for (Row row : sparkRows)
                                      {
                                          byte[] updatedFieldsIndicator = (byte[]) row.get(4);
                                          BitSet bs = BitSet.valueOf(updatedFieldsIndicator);
                                          BitSet expected = new BitSet(3);
                                          expected.set(0); // expecting pk to be set
                                          expected.set(2); // and c2 to be set.
                                          assertEquals(expected, bs);
                                          assertNull("c1 should be null", row.get(1));
                                      }
                                  })
                                  .run());
    }

    @Test
    public void testCellDeletion()
    {
        // The test write cell-level tombstones,
        // i.e. deleting one or more columns in a row, for cdc job to aggregate.
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type -> CdcTester.builder(bridge, DIR, TestSchema.builder()
                                                                          .withPartitionKey("pk", bridge.uuid())
                                                                          .withColumn("c1", bridge.bigint())
                                                                          .withColumn("c2", type))
                                          .clearWriters()
                                          .withWriter((tester, rows, writer) -> {
                                              for (int i = 0; i < tester.numRows; i++)
                                              {
                                                  TestSchema.TestRow testRow = Tester.newUniqueRow(tester.schema, rows);
                                                  testRow = testRow.copy("c1", CassandraBridge.UNSET_MARKER); // mark c1 as not updated / unset
                                                  testRow = testRow.copy("c2", null); // delete c2
                                                  writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
                                              }
                                          })
                                          .withRowChecker(sparkRows -> {
                                              for (Row row : sparkRows)
                                              {
                                                  byte[] updatedFieldsIndicator = (byte[]) row.get(3);
                                                  BitSet bs = BitSet.valueOf(updatedFieldsIndicator);
                                                  BitSet expected = new BitSet(3);
                                                  expected.set(0); // expecting pk to be set
                                                  expected.set(2); // and c2 to be set.
                                                  assertEquals(expected, bs);
                                                  assertNotNull("pk should not be null", row.get(0)); // pk should be set
                                                  assertNull("c1 should be null", row.get(1)); // null due to unset
                                                  assertNull("c2 should be null", row.get(2)); // null due to deletion
                                              }
                                          })
                                          .run());
    }

    @Test
    public void testRowDeletionWithClusteringKeyAndStatic()
    {
        testRowDeletion(5, // number of columns
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
        testRowDeletion(4, // number of columns
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
        testRowDeletion(3, // number of columns
                        false, // has clustering key?
                        type -> TestSchema.builder()
                                          .withPartitionKey("pk", bridge.uuid())
                                          .withColumn("c1", type)
                                          .withColumn("c2", bridge.bigint()));
    }

    private void testRowDeletion(int numOfColumns, boolean hasClustering, Function<CqlField.NativeType, TestSchema.Builder> schemaBuilder)
    {
        // The test write row-level tombstones
        // The expected output should include the values of all primary keys but all other columns should be null,
        // i.e. [pk.., ck.., null..]. The bitset should indicate that only the primary keys are present.
        // This kind of output means the entire row is deleted
        final Set<Integer> rowDeletionIndices = new HashSet<>();
        final Random rnd = new Random(1);
        final long minTimestamp = System.currentTimeMillis();
        final int numRows = 1000;
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type -> CdcTester.builder(bridge, DIR, schemaBuilder.apply(type))
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
                                          // Disable checker on the test row. The check is done in the row checker below.
                                          .withChecker((testRows, actualRows) -> {
                                          })
                                          .withRowChecker(sparkRows -> {
                                              assertEquals("Unexpected number of rows in output", numRows, sparkRows.size());
                                              for (int i = 0; i < sparkRows.size(); i++)
                                              {
                                                  Row row = sparkRows.get(i);
                                                  long lmtInMillis = row.getTimestamp(numOfColumns).getTime();
                                                  assertTrue("Last modification time should have a lower bound of " + minTimestamp,
                                                             lmtInMillis >= minTimestamp);
                                                  byte[] updatedFieldsIndicator = (byte[]) row.get(numOfColumns + 1); // indicator column
                                                  BitSet bs = BitSet.valueOf(updatedFieldsIndicator);
                                                  BitSet expected = new BitSet(numOfColumns);
                                                  if (rowDeletionIndices.contains(i)) // verify row deletion
                                                  {
                                                      expected.set(0); // expecting pk
                                                      if (hasClustering) // and ck to be set.
                                                      {
                                                          expected.set(1);
                                                      }
                                                      assertEquals("row" + i + " should only have the primary keys to be flagged.", expected, bs);
                                                      assertNotNull("pk should not be null", row.get(0)); // pk should be set
                                                      if (hasClustering)
                                                      {
                                                          assertNotNull("ck should not be null", row.get(1)); // ck should be set
                                                      }
                                                      for (int colIndex = hasClustering ? 2 : 1; colIndex < numOfColumns; colIndex++)
                                                      {
                                                          // null due to row deletion
                                                          assertNull("None primary key columns should be null", row.get(colIndex));
                                                      }
                                                  }
                                                  else // verify update
                                                  {
                                                      for (int colIndex = 0; colIndex < numOfColumns; colIndex++)
                                                      {
                                                          expected.set(colIndex);
                                                          assertNotNull("All column values should exist for full row update",
                                                                        row.get(colIndex));
                                                      }
                                                      assertEquals("row" + i + " should have all columns set", expected, bs);
                                                  }
                                              }
                                          })
                                          .run());
    }

    @Test
    public void testPartitionDeletionWithStaticColumn()
    {
        testPartitionDeletion(5, // num of columns
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
        testPartitionDeletion(5, // num of columns
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
        testPartitionDeletion(5, // num of columns
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
    private void testPartitionDeletion(int numOfColumns, boolean hasClustering, int partitionKeys, Function<CqlField.NativeType, TestSchema.Builder> schemaBuilder)
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
        qt().withExamples(1) // todo: remove it
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type -> CdcTester.builder(bridge, DIR, schemaBuilder.apply(type))
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
                                          // Disable checker on the test row. The check is done in the row checker below.
                                          .withChecker((testRows, actualRows) -> {
                                          })
                                          .withRowChecker(sparkRows -> {
                                              assertEquals("Unexpected number of rows in output", numRows, sparkRows.size());
                                              for (int i = 0, pkValidationIdx = 0; i < sparkRows.size(); i++)
                                              {
                                                  Row row = sparkRows.get(i);
                                                  long lmtInMillis = row.getTimestamp(numOfColumns).getTime();
                                                  assertTrue("Last modification time should have a lower bound of " + minTimestamp,
                                                             lmtInMillis >= minTimestamp);
                                                  byte[] updatedFieldsIndicator = (byte[]) row.get(numOfColumns + 1); // indicator column
                                                  BitSet bs = BitSet.valueOf(updatedFieldsIndicator);
                                                  BitSet expected = new BitSet(numOfColumns);
                                                  if (partitionDeletionIndices.contains(i)) // verify partition deletion
                                                  {
                                                      List<Object> pk = new ArrayList<>(partitionKeys);
                                                      // expecting partition keys
                                                      for (int j = 0; j < partitionKeys; j++)
                                                      {
                                                          expected.set(j);
                                                          assertNotNull("partition key should not be null", row.get(j));
                                                          pk.add(row.get(j));
                                                      }
                                                      assertEquals("row" + i + " should only have only the partition keys to be flagged.", expected, bs);
                                                      assertEquals("Partition deletion should indicate the correct partition at row" + i,
                                                                   validationPk.get(pkValidationIdx++).toString(), pk.toString()); // compare the string value of PKs.
                                                      if (hasClustering)
                                                      {
                                                          assertNull("ck should be null at row" + i, row.get(partitionKeys)); // ck should be set
                                                      }
                                                      for (int colIndex = partitionKeys; colIndex < numOfColumns; colIndex++)
                                                      {
                                                          // null due to partition deletion
                                                          assertNull("None partition key columns should be null at row" + 1, row.get(colIndex));
                                                      }
                                                  }
                                                  else // verify update
                                                  {
                                                      for (int colIndex = 0; colIndex < numOfColumns; colIndex++)
                                                      {
                                                          expected.set(colIndex);
                                                          assertNotNull("All column values should exist for full row update",
                                                                        row.get(colIndex));
                                                      }
                                                      assertEquals("row" + i + " should have all columns set", expected, bs);
                                                  }
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
                                                  writer.accept(row, timestamp++);
                                              }
                                          })
                                          .withChecker((testRows, actualRows) -> {
                                              int partitions = testRows.size();
                                              int mutations = actualRows.size();
                                              assertEquals("Each PK should get 2 mutations", partitions * 2, mutations);
                                          })
                                          .withRowChecker(sparkRows -> {
                                              long ts = -1L;
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
                                          if (i >= halfway) {
                                              testRow.fromUpdate();
                                          }
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

        final AtomicReference<CommitLog.Marker> currentMarker = new AtomicReference<>();
        final List<CommitLog.Marker> markers = Collections.synchronizedList(new ArrayList<>());
        final Watermarker watermarker = new Watermarker()
        {
            public Watermarker instance(String jobId)
            {
                return this;
            }

            public void recordReplicaCount(CdcUpdate update, int numReplicas)
            {

            }

            public int replicaCount(CdcUpdate update)
            {
                return 0;
            }

            public void untrackReplicaCount(CdcUpdate update)
            {

            }

            public boolean seenBefore(CdcUpdate update)
            {
                return false;
            }

            public void updateHighWaterMark(CommitLog.Marker marker)
            {
                markers.add(marker);
            }

            @Nullable
            public CommitLog.Marker highWaterMark(CassandraInstance instance)
            {
                final CommitLog.Marker marker = currentMarker.get();
                return marker == null ? instance.zeroMarker() : marker;
            }

            public void persist(@Nullable Long maxAgeMicros)
            {

            }

            public void clear()
            {
                markers.clear();
            }
        };
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
        final Set<Long> allRows = readLog(metadata, watermarker, keys, logFile);
        assertEquals(numRows, allRows.size());

        // re-read commit log from each watermark position
        // and verify subset of partitions are read
        int foundRows = allRows.size();
        allRows.clear();
        final List<CommitLog.Marker> allMarkers = new ArrayList<>(markers);
        CommitLog.Marker prevMarker = null;
        for (final CommitLog.Marker marker : allMarkers)
        {
            currentMarker.set(marker);
            final Set<Long> result = readLog(metadata, watermarker, keys, logFile);
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

    private Set<Long> readLog(TableMetadata metadata,
                              Watermarker watermarker,
                              Set<Long> keys,
                              File logFile)
    {
        final Set<Long> result = new HashSet<>();
        try (final LocalDataLayer.LocalCommitLog log = new LocalDataLayer.LocalCommitLog(logFile))
        {
            try (final BufferingCommitLogReader reader = new BufferingCommitLogReader(metadata, log, watermarker))
            {
                for (final CdcUpdate update : reader.result().updates())
                {
                    final long key = update.partitionKey().getKey().getLong();
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
