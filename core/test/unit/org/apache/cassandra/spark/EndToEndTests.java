package org.apache.cassandra.spark;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Test;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlUdt;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.TupleValue;
import org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.UDTValue;
import org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.UserTypeHelper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.mutable.WrappedArray;

import static org.apache.cassandra.spark.data.CqlField.NativeCql3Type.ASCII;
import static org.apache.cassandra.spark.data.CqlField.NativeCql3Type.BIGINT;
import static org.apache.cassandra.spark.data.CqlField.NativeCql3Type.BOOLEAN;
import static org.apache.cassandra.spark.data.CqlField.NativeCql3Type.DECIMAL;
import static org.apache.cassandra.spark.data.CqlField.NativeCql3Type.FLOAT;
import static org.apache.cassandra.spark.data.CqlField.NativeCql3Type.INET;
import static org.apache.cassandra.spark.data.CqlField.NativeCql3Type.INT;
import static org.apache.cassandra.spark.data.CqlField.NativeCql3Type.TEXT;
import static org.apache.cassandra.spark.data.CqlField.NativeCql3Type.TIMESTAMP;
import static org.apache.cassandra.spark.data.CqlField.NativeCql3Type.TIMEUUID;
import static org.apache.cassandra.spark.data.CqlField.NativeCql3Type.TINYINT;
import static org.apache.cassandra.spark.data.CqlField.NativeCql3Type.UUID;
import static org.apache.cassandra.spark.data.CqlField.NativeCql3Type.VARCHAR;
import static org.apache.cassandra.spark.data.CqlField.NativeCql3Type.VARINT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.characters;
import static org.quicktheories.generators.SourceDSL.integers;

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
 * End-to-end tests that write random data to multiple SSTables, reads the data back into Spark and verifies the rows in Spark match the expected.
 * Uses QuickTheories to test many combinations of field data types and clustering key sort order.
 * Uses custom SSTableTombstoneWriter to write SSTables with tombstones to verify Spark bulk reader correctly purges tombstoned data.
 */
public class EndToEndTests
{
    /* partition key tests */

    @Test
    public void testSinglePartitionKey()
    {
        Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withColumn("c1", BIGINT).withColumn("c2", TEXT).build())
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withSumField("c1")
              .run();
    }

    @Test
    public void testOnlyPartitionKeys()
    {
        // special case where schema is only partition keys
        Tester.builder(TestSchema.builder().withPartitionKey("a", UUID).build())
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
        Tester.builder(TestSchema.builder().withPartitionKey("a", UUID).withPartitionKey("b", BIGINT).build())
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @Test
    public void testOnlyPartitionClusteringKeys()
    {
        Tester.builder(TestSchema.builder().withPartitionKey("a", UUID).withClusteringKey("b", BIGINT).withClusteringKey("c", TEXT).build())
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @Test
    public void testMultiplePartitionKeys()
    {
        Tester.builder(TestSchema.builder().withPartitionKey("a", UUID).withPartitionKey("b", BIGINT).withColumn("c", TEXT).withColumn("d", BIGINT).build())
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withSumField("d")
              .run();
    }

    /* clustering key tests */

    @Test
    public void testBasicSingleClusteringKey()
    {
        Tester.builder(TestSchema.builder().withPartitionKey("a", BIGINT).withClusteringKey("b", BIGINT).withColumn("c", BIGINT).build())
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withSumField("c")
              .run();
    }

    @Test
    public void testSingleClusteringKeyOrderBy()
    {
        qt().forAll(TestUtils.cql3Type(), TestUtils.sortOrder())
            .checkAssert((clusteringKeyType, sortOrder) ->
                         Tester.builder(TestSchema.builder().withPartitionKey("a", BIGINT).withClusteringKey("b", clusteringKeyType).withColumn("c", BIGINT).withSortOrder(sortOrder).build())
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run());
    }

    @Test
    public void testMultipleClusteringKeys()
    {
        Tester.builder(TestSchema.builder().withPartitionKey("a", UUID).withClusteringKey("b", INT).withClusteringKey("c", TEXT).withColumn("d", TEXT).withColumn("e", BIGINT).build())
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withSumField("e")
              .run();
    }

    @Test
    public void testManyClusteringKeys()
    {
        Tester.builder(TestSchema.builder().withPartitionKey("a", UUID)
                                 .withClusteringKey("b", TIMESTAMP).withClusteringKey("c", TEXT).withClusteringKey("d", UUID).withClusteringKey("e", FLOAT)
                                 .withColumn("f", TEXT).withColumn("g", BIGINT).build())
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .withSumField("g")
              .run();
    }

    /* data type tests */

    @Test
    public void testAllDataTypesPartitionKey()
    {
        // test partition key can be read for all data types
        qt().forAll(TestUtils.cql3Type())
            .checkAssert((partitionKeyType) -> {
                // boolean or empty types have limited cardinality
                final int numRows = TestUtils.getCardinality(partitionKeyType, 10);
                Tester.builder(TestSchema.builder().withPartitionKey("a", partitionKeyType).withColumn("b", CqlField.NativeCql3Type.BIGINT).build())
                      .withNumRandomSSTables(1)
                      .withNumRandomRows(numRows)
                      .withExpectedRowCountPerSSTable(numRows)
                      .run();
            });
    }

    @Test
    public void testAllDataTypesValueColumn()
    {
        // test value column can be read for all data types
        qt().forAll(TestUtils.cql3Type())
            .checkAssert((valueType) -> Tester.builder(TestSchema.builder().withPartitionKey("a", BIGINT).withColumn("b", valueType).build())
                                              .withNumRandomSSTables(1)
                                              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                                              .run());
    }

    /* compaction */

    @Test
    public void testMultipleSSTablesCompaction()
    {
        final AtomicLong startTotal = new AtomicLong(0);
        final AtomicLong newTotal = new AtomicLong(0);
        final Map<UUID, Long> col1 = new HashMap<>(Tester.DEFAULT_NUM_ROWS);
        final Map<UUID, String> col2 = new HashMap<>(Tester.DEFAULT_NUM_ROWS);
        Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withColumn("c1", BIGINT).withColumn("c2", TEXT).build())
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int i = 0; i < Tester.DEFAULT_NUM_ROWS; i++)
                  {
                      final UUID pk = java.util.UUID.randomUUID();
                      final long c1 = TestUtils.RANDOM.nextInt(10000000);
                      final String c2 = java.util.UUID.randomUUID().toString();
                      startTotal.addAndGet(c1);
                      col1.put(pk, c1);
                      col2.put(pk, c2);
                      writer.write(pk, c1, c2);
                  }
              })
              // overwrite c1 with new value greater than previous
              .withSSTableWriter(writer -> {
                  for (final UUID pk : col1.keySet())
                  {
                      final long newBalance = (long) TestUtils.RANDOM.nextInt(10000000) + col1.get(pk);
                      assertTrue(newBalance > col1.get(pk));
                      newTotal.addAndGet(newBalance);
                      col1.put(pk, newBalance);
                      writer.write(pk, newBalance, col2.get(pk));
                  }
              })
              .withCheck(ds -> {
                  assertTrue(startTotal.get() < newTotal.get());
                  long sum = 0;
                  int count = 0;
                  for (final Row row : ds.collectAsList())
                  {
                      final UUID pk = java.util.UUID.fromString(row.getString(0));
                      assertEquals(row.getLong(1), col1.get(pk).longValue());
                      assertEquals(row.getString(2), col2.get(pk));
                      sum += (long) row.get(1);
                      count++;
                  }
                  assertEquals(Tester.DEFAULT_NUM_ROWS, count);
                  assertEquals(newTotal.get(), sum);
              })
              .withReset(() -> {
                  startTotal.set(0);
                  newTotal.set(0);
                  col1.clear();
                  col2.clear();
              });
    }

    @Test
    public void testCompaction()
    {
        final int numRowsCols = 20;
        final AtomicInteger total = new AtomicInteger(0);
        Tester.builder(TestSchema.builder().withPartitionKey("a", INT).withClusteringKey("b", INT).withColumn("c", INT).build())
              // don't write random data
              .dontWriteRandomData()
              // write some SSTables deterministically
              .withSSTableWriter((writer) -> {
                  for (int i = 0; i < numRowsCols; i++)
                  {
                      for (int j = 0; j < numRowsCols; j++)
                      {
                          writer.write(i, j, 0);
                      }
                  }
              })
              .withSSTableWriter((writer) -> {
                  for (int i = 0; i < numRowsCols; i++)
                  {
                      for (int j = 0; j < numRowsCols; j++)
                      {
                          writer.write(i, j, 1);
                      }
                  }
              })
              .withSSTableWriter((writer) -> {
                  for (int i = 0; i < numRowsCols; i++)
                  {
                      for (int j = 0; j < numRowsCols; j++)
                      {
                          final int num = j * 500;
                          total.addAndGet(num);
                          writer.write(i, j, num);
                      }
                  }
              })
              .withReadListener(row -> {
                  // we should have compacted the sstables to remove duplicate data and tombstones
                  assert (row.getInteger("b") * 500) == row.getInteger("c");
              })
              // verify sums to correct total
              .withCheck(ds -> assertEquals(total.get(), ds.groupBy().sum("c").first().getLong(0)))
              .withCheck(ds -> assertEquals(numRowsCols * numRowsCols, ds.groupBy().count().first().getLong(0)))
              .withReset(() -> total.set(0))
              .run();
    }

    @Test
    public void testSingleClusteringKey()
    {
        final AtomicLong total = new AtomicLong(0);
        final Map<Integer, MutableLong> testSum = new HashMap<>();
        final Set<Integer> clusteringKeys = new HashSet<>(Arrays.asList(0, 1, 2, 3));
        for (final int clusteringKey : clusteringKeys)
        {
            testSum.put(clusteringKey, new MutableLong(0));
        }

        Tester.builder(TestSchema.builder().withPartitionKey("a", UUID).withClusteringKey("b", INT).withColumn("c", BIGINT).withColumn("d", TEXT).build())
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int i = 0; i < Tester.DEFAULT_NUM_ROWS; i++)
                  {
                      for (final int clusteringKey : clusteringKeys)
                      {
                          final UUID accountId = java.util.UUID.randomUUID();
                          final long balance = TestUtils.RANDOM.nextInt(10000000);
                          total.addAndGet(balance);
                          final String name = java.util.UUID.randomUUID().toString().substring(0, 8);
                          testSum.get(clusteringKey).add(balance);
                          writer.write(accountId, clusteringKey, balance, name);
                      }
                  }
              })
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS * clusteringKeys.size())
              .withCheck(ds -> {
                  assertEquals(total.get(), testSum.values().stream().mapToLong(MutableLong::getValue).sum());
                  long sum = 0;
                  int count = 0;
                  for (final Row row : ds.collectAsList())
                  {
                      assertNotNull(row.getString(0));
                      final long balance = row.getLong(2);
                      assertNotNull(row.getString(3));
                      sum += balance;
                      count++;
                  }
                  assertEquals(total.get(), sum);
                  assertEquals(Tester.DEFAULT_NUM_ROWS * clusteringKeys.size(), count);
              })
              .withCheck(ds -> {
                  // test basic group by matches expected
                  for (final Row row : ds.groupBy("b").sum("c").collectAsList())
                  {
                      assertEquals(testSum.get(row.getInt(0)).getValue().longValue(), row.getLong(1));
                  }
              })
              .withReset(() -> {
                  total.set(0);
                  for (final int clusteringKey : clusteringKeys)
                  {
                      testSum.put(clusteringKey, new MutableLong(0));
                  }
              })
              .run();
    }

    /* static columns */

    @Test
    public void testOnlyStaticColumn()
    {
        Tester.builder(TestSchema.builder().withPartitionKey("a", UUID).withClusteringKey("b", BIGINT).withStaticColumn("c", INT).build())
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    @SuppressWarnings("UnstableApiUsage") // use of guava Uninterruptibles
    @Test
    public void testStaticColumn()
    {
        final int numRows = 100, numCols = 20;
        Tester.builder(TestSchema.builder().withPartitionKey("a", INT)
                                 .withClusteringKey("b", INT)
                                 .withStaticColumn("c", INT)
                                 .withColumn("d", TEXT).build())
              // don't write random data
              .dontWriteRandomData()
              // write some SSTables deterministically
              .withSSTableWriter((writer) -> {
                  for (int i = 0; i < numRows; i++)
                  {
                      for (int j = 0; j < numCols; j++)
                      {
                          // we need to sleep here to prevent timestamp conflicts confusing the static column value
                          if (j == numCols - 1)
                          {
                              Uninterruptibles.sleepUninterruptibly(2, TimeUnit.MILLISECONDS);
                          }
                          writer.write(i, j, i * j, java.util.UUID.randomUUID().toString());
                      }
                  }
              })
              .withSSTableWriter((writer) -> {
                  for (int i = 0; i < numRows; i++)
                  {
                      for (int j = numCols; j < numCols * 2; j++)
                      {
                          // we need to sleep here to prevent timestamp conflicts confusing the static column value
                          if (j == numCols * 2 - 1)
                          {
                              Uninterruptibles.sleepUninterruptibly(2, TimeUnit.MILLISECONDS);
                          }

                          writer.write(i, j, i * j, java.util.UUID.randomUUID().toString());
                      }
                  }
              })
              .withReadListener(row -> {
                  // static column should be the last value written
                  assert (row.getInteger("c") == (row.getInteger("a") * (numCols * 2 - 1)));
              })
              // verify row count is correct
              .withCheck(ds -> assertEquals(numRows * numCols * 2, ds.count()))
              .run();
    }

    @Test
    public void testMultipleSSTableCompacted()
    {
        final TestSchema schema = TestSchema.builder().withPartitionKey("a", UUID)
                                            .withClusteringKey("b", INT).withClusteringKey("c", TEXT)
                                            .withColumn("d", TEXT).withColumn("e", BIGINT).build();
        final AtomicLong total = new AtomicLong(0);
        final Map<UUID, TestSchema.TestRow> rows = new HashMap<>(Tester.DEFAULT_NUM_ROWS);
        Tester.builder(schema)
              // don't write random data
              .dontWriteRandomData()
              // write some SSTables with random data
              .withSSTableWriter((writer) -> {
                  for (int i = 0; i < Tester.DEFAULT_NUM_ROWS; i++)
                  {
                      final TestSchema.TestRow testRow = schema.randomRow();
                      rows.put(testRow.getUUID("a"), testRow);
                      writer.write(testRow.allValues());
                  }
              })
              // overwrite rows/cells multiple times in different sstables and ensure compaction compacts together correctly
              .withSSTableWriter(writer -> {
                  for (final TestSchema.TestRow testRow : ImmutableSet.copyOf(rows.values()))
                  {
                      // update rows with new values
                      final TestSchema.TestRow newTestRow = testRow.set("e", TestUtils.RANDOM.nextLong()).set("d", java.util.UUID.randomUUID().toString().substring(0, 10));
                      rows.put(testRow.getUUID("a"), newTestRow);
                      writer.write(newTestRow.allValues());
                  }
              })
              .withSSTableWriter(writer -> {
                  for (final TestSchema.TestRow testRow : ImmutableSet.copyOf(rows.values()))
                  {
                      // update rows with new values - this should be the final values seen by Spark
                      final TestSchema.TestRow newTestRow = testRow.set("e", TestUtils.RANDOM.nextLong()).set("d", java.util.UUID.randomUUID().toString().substring(0, 10));
                      rows.put(testRow.getUUID("a"), newTestRow);
                      total.addAndGet(newTestRow.getLong("e"));
                      writer.write(newTestRow.allValues());
                  }
              })
              // verify rows returned by Spark match expected
              .withReadListener(actualRow -> assertTrue(rows.containsKey(actualRow.getUUID("a"))))
              .withReadListener(actualRow -> assertEquals(rows.get(actualRow.getUUID("a")), actualRow))
              .withReadListener(actualRow -> assertEquals(rows.get(actualRow.getUUID("a")).getLong("e"), actualRow.getLong("e")))
              // verify Spark aggregations match expected
              .withCheck(ds -> assertEquals(total.get(), ds.groupBy().sum("e").first().getLong(0)))
              .withCheck(ds -> assertEquals(rows.size(), ds.groupBy().count().first().getLong(0)))
              .withReset(() -> {
                  total.set(0);
                  rows.clear();
              })
              .run();
    }

    /* Tombstone tests */

    @Test
    public void testPartitionTombstoneInt()
    {
        final int numRows = 100, numCols = 10;
        qt().withExamples(20).forAll(integers().between(0, numRows - 2))
            .checkAssert(deleteRangeStart -> {
                assert (deleteRangeStart >= 0 && deleteRangeStart < numRows);
                final int deleteRangeEnd = deleteRangeStart + TestUtils.RANDOM.nextInt(numRows - deleteRangeStart - 1) + 1;
                assert (deleteRangeEnd > deleteRangeStart && deleteRangeEnd < numRows);

                Tester.builder(TestSchema.basicBuilder().withDeleteFields("a =").build())
                      .withVersions(TestUtils.tombstoneTestableVersions())
                      .dontWriteRandomData()
                      .withSSTableWriter(writer -> {
                          for (int i = 0; i < numRows; i++)
                          {
                              for (int j = 0; j < numCols; j++)
                              {
                                  writer.write(i, j, j);
                              }
                          }
                      })
                      .withTombstoneWriter(writer -> {
                          for (int i = deleteRangeStart; i < deleteRangeEnd; i++)
                          {
                              writer.write(i);
                          }
                      })
                      .dontCheckNumSSTables()
                      .withCheck(ds -> {
                          int count = 0;
                          for (final Row row : ds.collectAsList())
                          {
                              final int value = row.getInt(0);
                              assertTrue(value >= 0 && value < numRows);
                              assertTrue(value < deleteRangeStart || value >= deleteRangeEnd);
                              count++;
                          }
                          assertEquals((numRows - (deleteRangeEnd - deleteRangeStart)) * numCols, count);
                      })
                      .run();
            });
    }

    @Test
    public void testRowTombstoneInt()
    {
        final int numRows = 100, numCols = 10;
        qt().withExamples(20).forAll(integers().between(0, numCols - 1))
            .checkAssert(colNum ->
                         Tester.builder(TestSchema.basicBuilder().withDeleteFields("a =", "b =").build())
                               .withVersions(TestUtils.tombstoneTestableVersions())
                               .dontWriteRandomData()
                               .withSSTableWriter(writer -> {
                                   for (int i = 0; i < numRows; i++)
                                   {
                                       for (int j = 0; j < numCols; j++)
                                       {
                                           writer.write(i, j, j);
                                       }
                                   }
                               })
                               .withTombstoneWriter(writer -> {
                                   for (int i = 0; i < numRows; i++)
                                   {
                                       writer.write(i, colNum);
                                   }
                               })
                               .dontCheckNumSSTables()
                               .withCheck(ds -> {
                                   int count = 0;
                                   for (final Row row : ds.collectAsList())
                                   {
                                       final int value = row.getInt(0);
                                       assertTrue(row.getInt(0) >= 0 && value < numRows);
                                       assertTrue(row.getInt(1) != colNum);
                                       assertEquals(row.get(1), row.get(2));
                                       count++;
                                   }
                                   assertEquals(numRows * (numCols - 1), count);
                               })
                               .run());
    }

    @Test
    public void testRangeTombstoneInt()
    {
        final int numRows = 10, numCols = 128;
        qt().withExamples(10).forAll(integers().between(0, numCols - 1))
            .checkAssert(startBound -> {
                assertTrue(startBound < numCols);
                final int endBound = startBound + TestUtils.RANDOM.nextInt(numCols - startBound);
                assertTrue(endBound >= startBound && endBound <= numCols);
                final int numTombstones = endBound - startBound;

                Tester.builder(TestSchema.basicBuilder().withDeleteFields("a =", "b >=", "b <").build())
                      .withVersions(TestUtils.tombstoneTestableVersions())
                      .dontWriteRandomData()
                      .withSSTableWriter(writer -> {
                          for (int i = 0; i < numRows; i++)
                          {
                              for (int j = 0; j < numCols; j++)
                              {
                                  writer.write(i, j, j);
                              }
                          }
                      })
                      .withTombstoneWriter(writer -> {
                          for (int i = 0; i < numRows; i++)
                          {
                              writer.write(i, startBound, endBound);
                          }
                      })
                      .dontCheckNumSSTables()
                      .withCheck(ds -> {
                          int count = 0;
                          for (final Row row : ds.collectAsList())
                          {
                              // verify row values exist within correct range with range tombstoned values removed
                              final int value = row.getInt(1);
                              assertEquals(value, row.getInt(2));
                              assertTrue(value <= numCols);
                              assertTrue(value < startBound || value >= endBound);
                              count++;
                          }
                          assertEquals(numRows * (numCols - numTombstones), count);
                      })
                      .run();
            });
    }

    @Test
    public void testRangeTombstoneString()
    {
        final int numRows = 10, numCols = 128;
        qt().withExamples(10).forAll(characters().ascii())
            .checkAssert(startBound -> {
                assertTrue(startBound <= numCols);
                final char endBound = (char) (startBound + TestUtils.RANDOM.nextInt(numCols - startBound));
                assertTrue(endBound >= startBound && endBound <= numCols);
                final int numTombstones = endBound - startBound;

                Tester.builder(TestSchema.builder().withPartitionKey("a", INT).withClusteringKey("b", TEXT).withColumn("c", INT).withDeleteFields("a =", "b >=", "b <").build())
                      .withVersions(TestUtils.tombstoneTestableVersions())
                      .dontWriteRandomData()
                      .withSSTableWriter(writer -> {
                          for (int i = 0; i < numRows; i++)
                          {
                              for (int j = 0; j < numCols; j++)
                              {
                                  final String value = String.valueOf((char) j);
                                  writer.write(i, value, j);
                              }
                          }
                      })
                      .withTombstoneWriter(writer -> {
                          for (int i = 0; i < numRows; i++)
                          {
                              writer.write(i, startBound.toString(), Character.toString(endBound));
                          }
                      })
                      .dontCheckNumSSTables()
                      .withCheck(ds -> {
                          int count = 0;
                          for (final Row row : ds.collectAsList())
                          {
                              // verify row values exist within correct range with range tombstoned values removed
                              final char c = row.getString(1).charAt(0);
                              assertTrue(c <= numCols);
                              assertTrue(c < startBound || c >= endBound);
                              count++;
                          }
                          assertEquals(numRows * (numCols - numTombstones), count);
                      })
                      .run();
            });
    }

    /* Partial rows: test reading rows with missing columns */

    @Test
    public void testPartialRow()
    {
        final Map<UUID, UUID> rows = new HashMap<>();
        Tester.builder(TestSchema.builder().withPartitionKey("a", UUID)
                                 .withColumn("b", TEXT).withColumn("c", UUID).withColumn("d", INT).withColumn("e", UUID).withColumn("f", INT)
                                 .withInsertFields("a", "c", "e") // override insert statement to only insert some columns
                                 .build())
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int i = 0; i < Tester.DEFAULT_NUM_ROWS; i++)
                  {
                      final UUID key = java.util.UUID.randomUUID(), value = java.util.UUID.randomUUID();
                      rows.put(key, value);
                      writer.write(key, value, value);
                  }
              })
              .withCheck((ds) -> {
                  for (final Row row : ds.collectAsList())
                  {
                      assertEquals(6, row.size());
                      final UUID key = java.util.UUID.fromString(row.getString(0));
                      final UUID value1 = java.util.UUID.fromString(row.getString(2));
                      final UUID value2 = java.util.UUID.fromString(row.getString(4));
                      assertTrue(rows.containsKey(key));
                      assertEquals(rows.get(key), value1);
                      assertEquals(value2, value1);
                      assertNull(row.get(1));
                      assertNull(row.get(3));
                      assertNull(row.get(5));
                  }
              })
              .withReset(rows::clear)
              .run();
    }

    @Test
    public void testPartialRowClusteringKeys()
    {
        final Map<String, String> rows = new HashMap<>();
        Tester.builder(TestSchema.builder().withPartitionKey("a", UUID)
                                 .withClusteringKey("b", UUID).withClusteringKey("c", UUID)
                                 .withColumn("d", TEXT).withColumn("e", UUID).withColumn("f", INT).withColumn("g", UUID).withColumn("h", INT)
                                 .withInsertFields("a", "b", "c", "e", "g") // override insert statement to only insert some columns
                                 .build())
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int i = 0; i < Tester.DEFAULT_NUM_ROWS; i++)
                  {
                      final UUID a = java.util.UUID.randomUUID(), b = java.util.UUID.randomUUID(), c = java.util.UUID.randomUUID(), e = java.util.UUID.randomUUID(), g = java.util.UUID.randomUUID();
                      final String key = a + ":" + b + ":" + c;
                      final String value = e + ":" + g;
                      rows.put(key, value);
                      writer.write(a, b, c, e, g);
                  }
              })
              .withCheck((ds) -> {
                  for (final Row row : ds.collectAsList())
                  {
                      assertEquals(8, row.size());
                      final String a = row.getString(0), b = row.getString(1), c = row.getString(2), e = row.getString(4), g = row.getString(6);
                      final String key = a + ":" + b + ":" + c;
                      final String value = e + ":" + g;
                      assertTrue(rows.containsKey(key));
                      assertEquals(rows.get(key), value);
                      assertNull(row.get(3));
                      assertNull(row.get(5));
                      assertNull(row.get(7));
                  }
              })
              .withReset(rows::clear)
              .run();
    }

    // collections

    @Test
    public void testSet()
    {
        qt().forAll(TestUtils.cql3Type())
            .checkAssert((type) ->
                         Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withColumn("a", CqlField.set(type)).build())
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run());
    }

    @Test
    public void testList()
    {
        qt().forAll(TestUtils.cql3Type())
            .checkAssert((type) ->
                         Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withColumn("a", CqlField.list(type)).build())
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run());
    }

    @Test
    public void testMap()
    {
        qt().withExamples(50) // limit number of tests otherwise n x n tests takes too long
            .forAll(TestUtils.cql3Type(), TestUtils.cql3Type())
            .checkAssert((keyType, valueType) -> {
                Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withColumn("a", CqlField.map(keyType, valueType)).build())
                      .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                      .run();
            });
    }

    @Test
    public void testClusteringKeySet()
    {
        Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withClusteringKey("id", INT).withColumn("a", CqlField.set(TEXT)).build())
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run();
    }

    // filters

    @Test
    public void testSinglePartitionKeyFilter()
    {
        final int numRows = 10;
        Tester.builder(TestSchema.builder().withPartitionKey("a", INT).withColumn("b", INT).build())
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int i = 0; i < numRows; i++)
                  {
                      writer.write(i, i + 1);
                  }
              })
              .withFilter("a=1")
              .withCheck((ds) -> {
                  for (final Row row : ds.collectAsList())
                  {
                      final int a = row.getInt(0);
                      assertEquals(1, a);
                  }
              })
              .run();
    }

    @Test
    public void testMultiplePartitionKeyFilter()
    {
        final int numRows = 10, numCols = 5;
        final Set<String> keys = TestUtils.getKeys(Arrays.asList(Arrays.asList("2", "3"), Arrays.asList("2", "3", "4")));
        Tester.builder(TestSchema.builder().withPartitionKey("a", INT).withPartitionKey("b", INT).withColumn("c", INT).build())
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int i = 0; i < numRows; i++)
                  {
                      for (int j = 0; j < numCols; j++)
                      {
                          writer.write(i, i + 1, j);
                      }
                  }
              })
              .withFilter("a in (2, 3) and b in (2, 3, 4)")
              .withCheck((ds) -> {
                  for (final Row row : ds.collectAsList())
                  {
                      final int a = row.getInt(0), b = row.getInt(1);
                      final String key = a + ":" + b;
                      assertTrue(keys.contains(key));
                  }
              })
              .run();
    }

    @Test
    public void testFiltersDoNotMatch()
    {
        final int numRows = 10;
        Tester.builder(TestSchema.builder().withPartitionKey("a", INT).withColumn("b", INT).build())
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (int i = 0; i < numRows; i++)
                  {
                      writer.write(i, i + 1);
                  }
              })
              .withFilter("a=11")
              .withCheck((ds) -> assertTrue(ds.collectAsList().isEmpty()))
              .run();
    }

    // frozen collections

    @Test
    public void testFrozenSet()
    {
        // pk -> a frozen<set<?>>
        qt().forAll(TestUtils.cql3Type())
            .checkAssert((type) ->
                         Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withColumn("a", CqlField.set(type).frozen()).build())
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run()
            );
    }

    @Test
    public void testFrozenList()
    {
        // pk -> a frozen<list<?>>
        qt().forAll(TestUtils.cql3Type())
            .checkAssert((type) ->
                         Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withColumn("a", CqlField.list(type).frozen()).build())
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run()
            );
    }

    @Test
    public void testFrozenMap()
    {
        // pk -> a frozen<map<?, ?>>
        qt().withExamples(50) // limit number of tests otherwise n x n tests takes too long
            .forAll(TestUtils.cql3Type(), TestUtils.cql3Type())
            .checkAssert((keyType, valueType) ->
                         Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withColumn("a", CqlField.map(keyType, valueType).frozen()).build())
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run());
    }

    @Test
    public void testNestedMapSet()
    {
        // pk -> a map<text, frozen<set<text>>>
        Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withColumn("a", CqlField.map(TEXT, CqlField.set(TEXT).frozen())).build())
              .withNumRandomRows(32)
              .withExpectedRowCountPerSSTable(32)
              .run();
    }

    @Test
    public void testNestedMapList()
    {
        // pk -> a map<text, frozen<list<text>>>
        Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withColumn("a", CqlField.map(TEXT, CqlField.list(TEXT).frozen())).build())
              .withNumRandomRows(32)
              .withExpectedRowCountPerSSTable(32)
              .run();
    }

    @Test
    public void testNestedMapMap()
    {
        // pk -> a map<text, frozen<map<bigint, varchar>>>
        Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withColumn("a", CqlField.map(TEXT, CqlField.map(BIGINT, VARCHAR).frozen())).build())
              .withNumRandomRows(32)
              .withExpectedRowCountPerSSTable(32)
              .dontCheckNumSSTables()
              .run();
    }

    @Test
    public void testFrozenNestedMapMap()
    {
        // pk -> a frozen<map<text, <map<int, timestamp>>>
        Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withColumn("a", CqlField.map(TEXT, CqlField.map(INT, TIMESTAMP)).frozen()).build())
              .withNumRandomRows(32)
              .withExpectedRowCountPerSSTable(32)
              .dontCheckNumSSTables()
              .run();
    }

    @Test
    public void testUdtNativeTypes()
    {
        // pk -> a testudt<b text, c type, d int>
        qt().forAll(TestUtils.cql3Type()).checkAssert((type) -> Tester.builder(
        TestSchema.builder()
                  .withPartitionKey("pk", UUID)
                  .withColumn("a", CqlUdt.builder("keyspace", "testudt")
                                         .withField("b", TEXT)
                                         .withField("c", type)
                                         .withField("d", INT).build())
                  .build()).run()
        );
    }

    @Test
    public void testUdtInnerSet()
    {
        // pk -> a testudt<b text, c frozen<type>, d int>
        qt().forAll(TestUtils.cql3Type())
            .checkAssert((type) ->
                         Tester.builder(
                         TestSchema.builder()
                                   .withPartitionKey("pk", UUID)
                                   .withColumn("a", CqlUdt.builder("keyspace", "testudt")
                                                          .withField("b", TEXT)
                                                          .withField("c", CqlField.set(type).frozen())
                                                          .withField("d", INT).build())
                                   .build()).run()
            );
    }

    @Test
    public void testUdtInnerList()
    {
        // pk -> a testudt<b bigint, c frozen<list<type>>, d boolean>
        qt().forAll(TestUtils.cql3Type())
            .checkAssert((type) ->
                         Tester.builder(
                         TestSchema.builder()
                                   .withPartitionKey("pk", UUID)
                                   .withColumn("a", CqlUdt.builder("keyspace", "testudt")
                                                          .withField("b", BIGINT)
                                                          .withField("c", CqlField.list(type).frozen())
                                                          .withField("d", BOOLEAN).build())
                                   .build()).run()
            );
    }

    @Test
    public void testUdtInnerMap()
    {
        // pk -> a testudt<b float, c frozen<set<uuid>>, d frozen<map<type1, type2>>, e boolean>
        qt().withExamples(50)
            .forAll(TestUtils.cql3Type(), TestUtils.cql3Type())
            .checkAssert((type1, type2) ->
                         Tester.builder(
                         TestSchema.builder()
                                   .withPartitionKey("pk", UUID)
                                   .withColumn("a", CqlUdt.builder("keyspace", "testudt")
                                                          .withField("b", FLOAT)
                                                          .withField("c", CqlField.set(UUID).frozen())
                                                          .withField("d", CqlField.map(type1, type2).frozen())
                                                          .withField("e", BOOLEAN)
                                                          .build())
                                   .build()).run()
            );
    }

    @Test
    public void testMultipleUdts()
    {
        // pk -> col1 udt1<a float, b frozen<set<uuid>>, c frozen<set<type>>, d boolean>, col2 udt2<a text, b bigint, g varchar>, col3 udt3<int, type, ascii>
        qt()
        .forAll(TestUtils.cql3Type())
        .checkAssert((type) ->
                     Tester.builder(
                     TestSchema.builder()
                               .withPartitionKey("pk", UUID)
                               .withColumn("col1", CqlUdt.builder("keyspace", "udt1")
                                                         .withField("a", FLOAT)
                                                         .withField("b", CqlField.set(UUID).frozen())
                                                         .withField("c", CqlField.set(type).frozen())
                                                         .withField("d", BOOLEAN)
                                                         .build())
                               .withColumn("col2", CqlUdt.builder("keyspace", "udt2")
                                                         .withField("a", TEXT)
                                                         .withField("b", BIGINT)
                                                         .withField("g", VARCHAR)
                                                         .build())
                               .withColumn("col3", CqlUdt.builder("keyspace", "udt3")
                                                         .withField("a", INT)
                                                         .withField("b", CqlField.list(type).frozen())
                                                         .withField("c", ASCII)
                                                         .build())
                               .build()).run()
        );
    }

    @Test
    public void testNestedUdt()
    {
        // pk -> a test_udt<b float, c frozen<set<uuid>>, d frozen<nested_udt<x int, y type, z int>>, e boolean>
        qt()
        .forAll(TestUtils.cql3Type())
        .checkAssert((type) ->
                     Tester.builder(
                     TestSchema.builder()
                               .withPartitionKey("pk", UUID)
                               .withColumn("a", CqlUdt.builder("keyspace", "test_udt")
                                                      .withField("b", FLOAT)
                                                      .withField("c", CqlField.set(UUID).frozen())
                                                      .withField("d", CqlUdt.builder("keyspace", "nested_udt")
                                                                            .withField("x", INT)
                                                                            .withField("y", type)
                                                                            .withField("z", INT)
                                                                            .build().frozen())
                                                      .withField("e", BOOLEAN)
                                                      .build())
                               .build())
                           .run()
        );
    }

    // tuples

    @Test
    public void testBasicTuple()
    {
        // pk -> a tuple<int, type1, bigint, type2>
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(), TestUtils.cql3Type())
            .checkAssert((type1, type2) ->
                         Tester.builder(
                         TestSchema.builder()
                                   .withPartitionKey("pk", UUID)
                                   .withColumn("a", CqlField.tuple(INT, type1, BIGINT, type2))
                                   .build())
                               .run()
            );
    }

    @Test
    public void testTupleWithClusteringKey()
    {
        // pk -> col1 type1 -> a tuple<int, type2, bigint>
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(), TestUtils.cql3Type())
            .checkAssert((type1, type2) ->
                         Tester.builder(
                         TestSchema.builder()
                                   .withPartitionKey("pk", UUID)
                                   .withClusteringKey("col1", type1)
                                   .withColumn("a", CqlField.tuple(INT, type2, BIGINT))
                                   .build())
                               .run()
            );
    }

    @Test
    public void testNestedTuples()
    {
        // pk -> a tuple<varchar, tuple<int, type1, float, varchar, tuple<bigint, boolean, type2>>, timeuuid>
        // test tuples nested within tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(), TestUtils.cql3Type())
            .checkAssert((type1, type2) ->
                         Tester.builder(
                         TestSchema.builder()
                                   .withPartitionKey("pk", UUID)
                                   .withColumn("a", CqlField.tuple(VARCHAR, CqlField.tuple(INT, type1, FLOAT, VARCHAR, CqlField.tuple(BIGINT, BOOLEAN, type2)), TIMEUUID))
                                   .build())
                               .run()
            );
    }

    @Test
    public void testTupleSet()
    {
        // pk -> a tuple<varchar, tuple<int, varchar, float, varchar, set<type>>, timeuuid>
        // test set nested within tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type())
            .checkAssert((type) ->
                         Tester.builder(
                         TestSchema.builder()
                                   .withPartitionKey("pk", UUID)
                                   .withColumn("a", CqlField.tuple(VARCHAR, CqlField.tuple(INT, VARCHAR, FLOAT, VARCHAR, CqlField.set(type)), TIMEUUID))
                                   .build())
                               .run()
            );
    }

    @Test
    public void testTupleList()
    {
        // pk -> a tuple<varchar, tuple<int, varchar, float, varchar, list<type>>, timeuuid>
        // test list nested within tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type())
            .checkAssert((type) ->
                         Tester.builder(
                         TestSchema.builder()
                                   .withPartitionKey("pk", UUID)
                                   .withColumn("a", CqlField.tuple(VARCHAR, CqlField.tuple(INT, VARCHAR, FLOAT, VARCHAR, CqlField.list(type)), TIMEUUID))
                                   .build())
                               .run()
            );
    }

    @Test
    public void testTupleMap()
    {
        // pk -> a tuple<varchar, tuple<int, varchar, float, varchar, map<type1, type2>>, timeuuid>
        // test map nested within tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(), TestUtils.cql3Type())
            .checkAssert((type1, type2) ->
                         Tester.builder(
                         TestSchema.builder()
                                   .withPartitionKey("pk", UUID)
                                   .withColumn("a", CqlField.tuple(VARCHAR, CqlField.tuple(INT, VARCHAR, FLOAT, VARCHAR, CqlField.map(type1, type2)), TIMEUUID))
                                   .build())
                               .run()
            );
    }

    @Test
    public void testMapTuple()
    {
        // pk -> a map<timeuuid, frozen<tuple<boolean, type, timestamp>>>
        // test tuple nested within map
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type())
            .checkAssert((type) ->
                         Tester.builder(
                         TestSchema.builder()
                                   .withPartitionKey("pk", UUID)
                                   .withColumn("a", CqlField.map(TIMEUUID, CqlField.tuple(BOOLEAN, type, TIMESTAMP).frozen()))
                                   .build())
                               .run()
            );
    }

    @Test
    public void testSetTuple()
    {
//         pk -> a set<frozen<tuple<type, float, text>>>
//         test tuple nested within set
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type())
            .checkAssert((type) ->
                         Tester.builder(
                         TestSchema.builder()
                                   .withPartitionKey("pk", UUID)
                                   .withColumn("a", CqlField.set(CqlField.tuple(type, FLOAT, TEXT).frozen()))
                                   .build())
                               .run()
            );
    }

    @Test
    public void testListTuple()
    {
        // pk -> a list<frozen<tuple<int, inet, decimal, type>>>
        // test tuple nested within map
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type())
            .checkAssert((type) ->
                         Tester.builder(
                         TestSchema.builder()
                                   .withPartitionKey("pk", UUID)
                                   .withColumn("a", CqlField.list(CqlField.tuple(INT, INET, DECIMAL, type).frozen()))
                                   .build())
                               .run()
            );
    }


    @Test
    public void testTupleUDT()
    {
        // pk -> a tuple<varchar, frozen<nested_udt<x int, y type, z int>>, timeuuid>
        // test tuple with inner UDT
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type())
            .checkAssert((type) ->
                         Tester.builder(
                         TestSchema.builder()
                                   .withPartitionKey("pk", UUID)
                                   .withColumn("a", CqlField.tuple(VARCHAR, CqlUdt.builder("keyspace", "nested_udt")
                                                                                  .withField("x", INT)
                                                                                  .withField("y", type)
                                                                                  .withField("z", INT)
                                                                                  .build().frozen(), TIMEUUID))
                                   .build())
                               .run()
            );
    }

    @Test
    public void testUDTTuple()
    {
        // pk -> a nested_udt<x text, y tuple<int, float, type, timestamp>, z ascii>
        // test UDT with inner tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type())
            .checkAssert((type) ->
                         Tester.builder(
                         TestSchema.builder()
                                   .withPartitionKey("pk", UUID)
                                   .withColumn("a", CqlUdt.builder("keyspace", "nested_udt")
                                                          .withField("x", TEXT)
                                                          .withField("y", CqlField.tuple(INT, FLOAT, type, TIMESTAMP))
                                                          .withField("z", ASCII)
                                                          .build())
                                   .build())
                               .run()
            );
    }

    @Test
    public void testTupleClusteringKey()
    {
        qt()
        .forAll(TestUtils.cql3Type())
        .checkAssert((type) ->
                     Tester.builder(
                     TestSchema.builder()
                               .withPartitionKey("pk", UUID)
                               .withClusteringKey("ck", CqlField.tuple(INT, TEXT, type, FLOAT))
                               .withColumn("a", TEXT)
                               .withColumn("b", INT)
                               .withColumn("c", ASCII)
                               .build())
                           .run()
        );
    }

    @Test
    public void testUdtClusteringKey()
    {
        qt()
        .forAll(TestUtils.cql3Type())
        .checkAssert((type) ->
                     Tester.builder(
                     TestSchema.builder()
                               .withPartitionKey("pk", UUID)
                               .withClusteringKey("ck", CqlUdt.builder("keyspace", "udt1")
                                                              .withField("a", TEXT)
                                                              .withField("b", type)
                                                              .withField("c", INT)
                                                              .build().frozen())
                               .withColumn("a", TEXT)
                               .withColumn("b", INT)
                               .withColumn("c", ASCII)
                               .build())
                           .run()
        );
    }

    @Test
    public void testComplexSchema()
    {
        final String keyspace = "complex_schema2";
        final CqlUdt udt1 = CqlUdt.builder(keyspace, "udt1")
                                  .withField("time", BIGINT)
                                  .withField("\"time_offset_minutes\"", INT)
                                  .build();
        final CqlUdt udt2 = CqlUdt.builder(keyspace, "udt2")
                                  .withField("\"version\"", TEXT)
                                  .withField("\"id\"", TEXT)
                                  .withField("platform", TEXT)
                                  .withField("time_range", TEXT)
                                  .build();
        final CqlUdt udt3 = CqlUdt.builder(keyspace, "udt3")
                                  .withField("field", TEXT)
                                  .withField("\"time_with_zone\"", udt1)
                                  .build();
        final CqlUdt udt4 = CqlUdt.builder(keyspace, "udt4")
                                  .withField("\"first_seen\"", udt3.frozen())
                                  .withField("\"last_seen\"", udt3.frozen())
                                  .withField("\"first_transaction\"", udt3.frozen())
                                  .withField("\"last_transaction\"", udt3.frozen())
                                  .withField("\"first_listening\"", udt3.frozen())
                                  .withField("\"last_listening\"", udt3.frozen())
                                  .withField("\"first_reading\"", udt3.frozen())
                                  .withField("\"last_reading\"", udt3.frozen())
                                  .withField("\"output_event\"", TEXT)
                                  .withField("\"event_history\"", CqlField.map(BIGINT, CqlField.map(TEXT, BOOLEAN).frozen()).frozen())
                                  .build();

        Tester.builder(TestSchema.builder()
                                 .withKeyspace(keyspace)
                                 .withTable("complex_table")
                                 .withPartitionKey("\"consumerId\"", TEXT)
                                 .withClusteringKey("dimensions", udt2.frozen())
                                 .withColumn("fields", udt4.frozen())
                                 .withColumn("first_transition_time", udt1.frozen())
                                 .withColumn("last_transition_time", udt1.frozen())
                                 .withColumn("prev_state_id", TEXT)
                                 .withColumn("state_id", TEXT)
                                 .build())
              .run();
    }

    @Test
    public void testNestedFrozenUDT()
    {
        // "(a bigint PRIMARY KEY, b <map<int, frozen<testudt>>>)"
        // testudt(a text, b bigint, c int)
        final String keyspace = "nested_frozen_udt";
        final CqlUdt testudt = CqlUdt.builder(keyspace, "testudt")
                                     .withField("a", TEXT)
                                     .withField("b", BIGINT)
                                     .withField("c", INT)
                                     .build();
        Tester.builder(TestSchema.builder()
                                 .withKeyspace(keyspace)
                                 .withPartitionKey("a", BIGINT)
                                 .withColumn("b", CqlField.map(INT, testudt.frozen()))
                                 .build())
              .run();
    }

    @Test
    public void testDeepNestedUDT()
    {
        final String keyspace = "deep_nested_frozen_udt";
        final CqlUdt udt1 = CqlUdt.builder(keyspace, "udt1")
                                  .withField("a", TEXT)
                                  .withField("b", INT)
                                  .withField("c", BIGINT).build();
        final CqlUdt udt2 = CqlUdt.builder(keyspace, "udt2")
                                  .withField("a", INT)
                                  .withField("b", CqlField.set(UUID))
                                  .build();
        final CqlUdt udt3 = CqlUdt.builder(keyspace, "udt3")
                                  .withField("a", INT)
                                  .withField("b", CqlField.set(UUID))
                                  .build();
        final CqlUdt udt4 = CqlUdt.builder(keyspace, "udt4")
                                  .withField("a", TEXT)
                                  .withField("b", TEXT)
                                  .withField("c", UUID)
                                  .withField("d", CqlField.list(CqlField.tuple(TEXT, BIGINT).frozen()).frozen())
                                  .build();
        final CqlUdt udt5 = CqlUdt.builder(keyspace, "udt5")
                                  .withField("a", TEXT)
                                  .withField("b", TEXT)
                                  .withField("c", BIGINT)
                                  .withField("d", CqlField.set(udt4.frozen()))
                                  .build();
        final CqlUdt udt6 = CqlUdt.builder(keyspace, "udt6")
                                  .withField("a", TEXT)
                                  .withField("b", TEXT)
                                  .withField("c", INT)
                                  .withField("d", INT)
                                  .build();
        final CqlUdt udt7 = CqlUdt.builder(keyspace, "udt7")
                                  .withField("a", TEXT)
                                  .withField("b", UUID)
                                  .withField("c", BOOLEAN)
                                  .withField("d", BOOLEAN)
                                  .withField("e", BOOLEAN)
                                  .withField("f", BIGINT)
                                  .withField("g", BIGINT)
                                  .build();

        final CqlUdt udt8 = CqlUdt.builder(keyspace, "udt8")
                                  .withField("a", TEXT)
                                  .withField("b", BOOLEAN)
                                  .withField("c", BOOLEAN)
                                  .withField("d", BOOLEAN)
                                  .withField("e", BIGINT)
                                  .withField("f", BIGINT)
                                  .withField("g", UUID)
                                  .withField("h", BIGINT)
                                  .withField("i", UUID)
                                  .withField("j", UUID)
                                  .withField("k", UUID)
                                  .withField("l", UUID)
                                  .withField("m", INT)
                                  .withField("n", TIMESTAMP)
                                  .withField("o", TEXT)
                                  .build();

        Tester.builder(
        TestSchema.builder()
                  .withKeyspace(keyspace)
                  .withTable("info")
                  .withPartitionKey("pk", UUID)
                  .withClusteringKey("ck", UUID)
                  .withColumn("a", udt3.frozen())
                  .withColumn("b", udt2.frozen())
                  .withColumn("c", CqlField.set(CqlField.tuple(udt1, TEXT).frozen()))
                  .withColumn("d", CqlField.set(CqlField.tuple(BIGINT, TEXT).frozen()))
                  .withColumn("e", CqlField.set(CqlField.tuple(udt2, TEXT).frozen()))
                  .withColumn("f", CqlField.set(udt7.frozen()))
                  .withColumn("g", CqlField.map(INT, CqlField.set(TEXT).frozen()))
                  .withColumn("h", CqlField.set(TINYINT))
                  .withColumn("i", CqlField.map(TEXT, udt6.frozen()))
                  .withColumn("j", CqlField.map(TEXT, CqlField.map(TEXT, TEXT).frozen()))
                  .withColumn("k", CqlField.list(CqlField.tuple(TEXT, TEXT, TEXT).frozen()))
                  .withColumn("l", CqlField.list(udt5.frozen()))
                  .withColumn("m", udt8.frozen())
                  .withMinCollectionSize(4)
                  .build())
              .withNumRandomRows(50)
              .withNumRandomSSTables(2)
              .run();
    }


    // big decimal/integer tests

    @Test
    public void testBigDecimal()
    {
        Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withColumn("c1", DECIMAL).withColumn("c2", TEXT).build())
              .run();
    }

    @Test
    public void testBigInteger()
    {
        Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withColumn("c1", VARINT).withColumn("c2", TEXT).build())
              .run();
    }

    @Test
    public void testUdtFieldOrdering()
    {
        final String keyspace = "udt_field_ordering";
        final CqlUdt udt1 = CqlUdt.builder(keyspace, "udt1")
                                  .withField("c", TEXT)
                                  .withField("b", UUID)
                                  .withField("a", BOOLEAN)
                                  .build();
        Tester.builder(TestSchema.builder()
                                 .withKeyspace(keyspace)
                                 .withPartitionKey("pk", UUID)
                                 .withColumn("a", CqlField.set(udt1.frozen()))
                                 .build()
        ).run();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUdtTupleInnerNulls()
    {
        final String keyspace = "udt_inner_nulls";
        final CqlUdt udt1 = CqlUdt.builder(keyspace, "udt1")
                                  .withField("a", UUID)
                                  .withField("b", TEXT)
                                  .build();
        final CqlField.CqlTuple tuple = CqlField.tuple(BIGINT, TEXT, INT);

        final int numRows = 50;
        final Map<UUID, Set<UDTValue>> aValues = new LinkedHashMap<>(numRows);
        final Map<UUID, TupleValue> bValues = new LinkedHashMap<>(numRows);
        for (int i = 0; i < numRows; i++)
        {
            final java.util.UUID pk = java.util.UUID.randomUUID();
            final Set<UDTValue> value = new HashSet<>(numRows);
            for (int j = 0; j < numRows; j++)
            {
                final Map<String, Object> udtValue = new HashMap<>(2);
                udtValue.put("a", java.util.UUID.randomUUID());
                udtValue.put("b", j >= 25 ? java.util.UUID.randomUUID().toString() : null);
                value.add(UserTypeHelper.toUserTypeValue(CassandraBridge.CassandraVersion.THREEZERO, udt1, udtValue));
            }
            aValues.put(pk, value);
            bValues.put(pk, UserTypeHelper.toTupleValue(CassandraBridge.CassandraVersion.THREEZERO, tuple, new Object[]{TestUtils.RANDOM.nextLong(), i > 25 ? null : java.util.UUID.randomUUID().toString(), TestUtils.RANDOM.nextInt()}));
        }

        Tester.builder(TestSchema.builder()
                                 .withKeyspace(keyspace)
                                 .withPartitionKey("pk", UUID)
                                 .withColumn("a", CqlField.set(udt1.frozen()))
                                 .withColumn("b", tuple)
                                 .build()
        ).withSSTableWriter(writer -> {
            for (final UUID pk : aValues.keySet())
            {
                writer.write(pk, aValues.get(pk), bValues.get(pk));
            }
        }).withCheck(ds -> {
            for (final Row row : ds.collectAsList()) {
                final java.util.UUID pk = java.util.UUID.fromString(row.getString(0));
                final WrappedArray<Object> ar1 = (WrappedArray<Object>) row.get(1);
                final Set<UDTValue> expectedA = aValues.get(pk);
                assertNotNull(expectedA);
                assertEquals(ar1.length(), expectedA.size());
                for (final Row innerRow : (Row[]) ar1.array()) {
                    final java.util.UUID a = java.util.UUID.fromString(innerRow.getString(0));
                    final String b = innerRow.getString(1);
                    final Map<String, Object> udtValue = new HashMap<>(2);
                    udtValue.put("a", a);
                    udtValue.put("b", b);
                    assertTrue(expectedA.contains(UserTypeHelper.toUserTypeValue(CassandraBridge.CassandraVersion.THREEZERO, udt1, udtValue)));
                }

                final Row innerTuple = (Row) row.get(2);
                final TupleValue expectedB = bValues.get(pk);
                assertEquals(expectedB.getLong(0), innerTuple.getLong(0));
                assertEquals(expectedB.getString(1), innerTuple.getString(1));
                assertEquals(expectedB.getInt(2), innerTuple.getInt(2));
            }
        })
              .dontWriteRandomData().run();
    }

    // complex clustering keys

    @Test
    public void testUdtsWithNulls() {
        final String keyspace = "udt_with_nulls";
        final Map<Long, Map<String, String>> udts = new HashMap<>(Tester.DEFAULT_NUM_ROWS);
        final CqlUdt udtType = CqlUdt.builder(keyspace, "udt1").withField("a", TEXT).withField("b", TEXT).withField("c", TEXT).build();
        Tester.builder(TestSchema.builder().withKeyspace(keyspace).withPartitionKey("pk", BIGINT)
                .withClusteringKey("ck", udtType.frozen())
                .withColumn("col1", TEXT).withColumn("col2", TIMESTAMP).withColumn("col3", INT)
                .build())
                .dontWriteRandomData()
                .withSSTableWriter(writer -> {
                    final int midPoint = Tester.DEFAULT_NUM_ROWS / 2;
                    for (long pk = 0; pk < Tester.DEFAULT_NUM_ROWS; pk++) {
                        final Map<String, String> udt = ImmutableMap.of(pk < midPoint ? "a" : "b", TestUtils.randomValue(TEXT).toString(), "c", TestUtils.randomValue(TEXT).toString());
                        udts.put(pk, udt);
                        writer.write(pk, UserTypeHelper.toUserTypeValue(CassandraBridge.CassandraVersion.THREEZERO, udtType, udt), TestUtils.randomValue(TEXT), TestUtils.randomValue(TIMESTAMP), TestUtils.randomValue(INT));
                    }
                })
                .withCheck(ds -> {
                    final Map<Long, Row> rows = ds.collectAsList().stream().collect(Collectors.toMap(r -> r.getLong(0), r -> r.getStruct(1)));
                    assertEquals(rows.size(), udts.size());
                    for (final Map.Entry<Long, Row> pk : rows.entrySet()) {
                        assertEquals(udts.get(pk.getKey()).get("a"), pk.getValue().getString(0));
                        assertEquals(udts.get(pk.getKey()).get("b"), pk.getValue().getString(1));
                        assertEquals(udts.get(pk.getKey()).get("c"), pk.getValue().getString(2));
                    }
                })
                .run();
    }

    @Test
    public void testMapClusteringKey()
    {
        Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID).withClusteringKey("ck", CqlField.map(BIGINT, TEXT).frozen())
                .withColumn("c1", TEXT).withColumn("c2", TEXT).withColumn("c3", TEXT).build())
                .withNumRandomRows(5)
                .run();
    }

    @Test
    public void testListClusteringKey()
    {
        Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID)
                .withClusteringKey("ck", CqlField.list(BIGINT).frozen())
                .withColumn("c1", TEXT).withColumn("c2", TEXT).withColumn("c3", TEXT)
                .build()).run();
    }

    @Test
    public void testSetClusteringKey()
    {
        Tester.builder(TestSchema.builder().withPartitionKey("pk", UUID)
                .withClusteringKey("ck", CqlField.set(FLOAT).frozen())
                .withColumn("c1", TEXT).withColumn("c2", TEXT).withColumn("c3", TEXT)
                .build()).run();
    }

    @Test
    public void testUdTClusteringKey()
    {
        final String keyspace = "udt_clustering_key";
        Tester.builder(TestSchema.builder()
                .withKeyspace(keyspace).withPartitionKey("pk", UUID)
                .withClusteringKey("ck", CqlUdt.builder(keyspace, "udt1").withField("a", TEXT).withField("b", FLOAT).withField("c", BIGINT).build().frozen())
                .withColumn("c1", TEXT).withColumn("c2", TEXT).withColumn("c3", TEXT).build())
                .run();
    }
}
