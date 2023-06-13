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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.spark.SparkTestUtils;
import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.SparkCqlField;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.CassandraBridge.RangeTombstoneData;
import org.apache.cassandra.spark.reader.CassandraBridge.RangeTombstoneData.Bound;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.CellPath;

import static org.apache.cassandra.spark.cdc.CdcTester.assertCqlTypeEquals;
import static org.apache.cassandra.spark.cdc.CdcTester.testWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;

public class CdcTombstoneTests extends VersionRunner
{
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

    public CdcTombstoneTests(CassandraVersion version)
    {
        super(version);
    }

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
                                    new RangeTombstoneData(new Bound(open, true), new Bound(close, true))));
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
                                RangeTombstoneData expectedRT = sourceRow.rangeTombstones().get(0);
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
}
