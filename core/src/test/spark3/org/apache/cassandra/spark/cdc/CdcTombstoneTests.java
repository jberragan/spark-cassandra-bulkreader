package org.apache.cassandra.spark.cdc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.CassandraBridge.RangeTombstone;
import org.apache.cassandra.spark.reader.CassandraBridge.RangeTombstone.Bound;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.CellPath;
import org.apache.spark.sql.Row;
import scala.collection.mutable.WrappedArray;

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

    public CdcTombstoneTests(CassandraBridge.CassandraVersion version)
    {
        super(version);
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
                                          .withRowChecker(sparkRows -> {
                                              for (Row row : sparkRows)
                                              {
                                                  byte[] updatedFieldsIndicator = (byte[]) row.get(4);
                                                  BitSet bs = BitSet.valueOf(updatedFieldsIndicator);
                                                  BitSet expected = new BitSet(4);
                                                  expected.set(0); // expecting pk to be set
                                                  expected.set(2); // and c2 to be set.
                                                  expected.set(3); // and c3 to be set.
                                                  assertEquals(expected, bs);
                                                  assertNotNull("pk should not be null", row.get(0)); // pk should be set
                                                  assertNull("c1 should be null", row.get(1)); // null due to unset
                                                  assertNull("c2 should be null", row.get(2)); // null due to deletion
                                                  assertNull("c3 should be null", row.get(3)); // null due to deletion
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
        qt()//.forAll(arbitrary().pick(bridge.blob()))
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
                                          .withChecker((testRows, actualRows) -> {})
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
                                                      List<Object> expectedPK = validationPk.get(pkValidationIdx++);
                                                      assertTrue("Partition deletion should indicate the correct partition at row" + i +
                                                                 ". Expected: " + expectedPK + ", actual: " + pk,
                                                                 TestUtils.equals(expectedPK.toArray(), pk.toArray()));
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
    public void testElementDeletionInMap()
    {
        final String name = "m";
        testElementDeletionInCollection(2, /* numOfColumns */
                                        Arrays.asList(name),
                                        type -> TestSchema.builder()
                                                          .withPartitionKey("pk", bridge.uuid())
                                                          .withColumn(name, bridge.map(type, type)));
    }

    @Test
    public void testElementDeletionInSet()
    {
        final String name = "s";
        testElementDeletionInCollection(2, /* numOfColumns */
                                        Arrays.asList(name),
                                        type -> TestSchema.builder()
                                                          .withPartitionKey("pk", bridge.uuid())
                                                          .withColumn(name, bridge.set(type)));
    }

    @Test
    public void testElementDeletionsInMultipleColumns()
    {
        testElementDeletionInCollection(4, /* numOfColumns */
                                        Arrays.asList("c1", "c2", "c3"),
                                        type -> TestSchema.builder()
                                                          .withPartitionKey("pk", bridge.uuid())
                                                          .withColumn("c1", bridge.set(type))
                                                          .withColumn("c2", bridge.set(type))
                                                          .withColumn("c3", bridge.set(type)));
    }

    // validate that cell deletions in a complex data can be correctly encoded.
    private void testElementDeletionInCollection(int numOfColumns, List<String> collectionColumnNames, Function<CqlField.NativeType, TestSchema.Builder> schemaBuilder)
    {
        // key: row# that has deletion; value: the deleted cell key/path in the collection
        final Map<Integer, byte[]> elementDeletionIndices = new HashMap<>();
        final Random rnd = new Random(1);
        final long minTimestamp = System.currentTimeMillis();
        final int numRows = 1000;
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type -> CdcTester.builder(bridge, DIR, schemaBuilder.apply(type))
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
                                          // Disable checker on the test row. The check is done in the sparkrow checker below.
                                          .withChecker((testRows, actualRows) -> {})
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
                                                  for (int colIndex = 0; colIndex < numOfColumns; colIndex++)
                                                  {
                                                      expected.set(colIndex);
                                                  }
                                                  if (elementDeletionIndices.containsKey(i)) // verify deletion
                                                  {
                                                      Map<Object, Object> cellTombstonesPerCol = row.getJavaMap(numOfColumns + 3); // cell deletion in complex
                                                      assertNotNull(cellTombstonesPerCol);
                                                      for (String name : collectionColumnNames)
                                                      {
                                                          assertNull("Collection column should be null after deletion",
                                                                     row.get(row.fieldIndex(name)));

                                                          assertNotNull(cellTombstonesPerCol.get(name));
                                                          WrappedArray<?> deletedCellKeys = (WrappedArray<?>) cellTombstonesPerCol.get(name);
                                                          assertEquals(1, deletedCellKeys.length());
                                                          byte[] keyBytesRead = (byte[]) deletedCellKeys.apply(0);
                                                          assertArrayEquals("The key encoded should be the same",
                                                                            elementDeletionIndices.get(i), keyBytesRead);
                                                      }
                                                  }
                                                  else // verify update
                                                  {
                                                      for (int colIndex = 0; colIndex < numOfColumns; colIndex++)
                                                      {
                                                          assertNotNull("All column values should exist for full row update",
                                                                        row.get(colIndex));
                                                      }
                                                      assertNull("the cell deletion map should be absent",
                                                                 row.get(numOfColumns + 3));
                                                  }
                                                  assertEquals("row" + i + " should have all columns set", expected, bs);
                                              }
                                          })
                                          .run());
    }

    @Test
    public void testRangeDeletions()
    {
        testRangeDeletions(4, // num of columns
                              1, // num of partition key columns
                              2, // num of clustering key columns
                              type -> TestSchema.builder()
                                                .withPartitionKey("pk1", bridge.uuid())
                                                .withClusteringKey("ck1", type)
                                                .withClusteringKey("ck2", bridge.bigint())
                                                .withColumn("c1", type));
    }

    @Test
    public void testRangeDeletionsWithStatic()
    {
        testRangeDeletions(5, // num of columns
                           1, // num of partition key columns
                           2, // num of clustering key columns
                           type -> TestSchema.builder()
                                             .withPartitionKey("pk1", bridge.uuid())
                                             .withClusteringKey("ck1", bridge.ascii())
                                             .withClusteringKey("ck2", bridge.bigint())
                                             .withStaticColumn("s1", bridge.uuid())
                                             .withColumn("c1", type));
    }

    // validate that range deletions can be correctly encoded.
    private void testRangeDeletions(int numOfColumns, int numOfPartitionKeys, int numOfClusteringKeys, Function<CqlField.NativeType, TestSchema.Builder> schemaBuilder)
    {
        Preconditions.checkArgument(numOfClusteringKeys > 0, "Range deletion test won't run without having clustering keys!");
        // key: row# that has deletion; value: the deleted cell key/path in the collection
        final Map<Integer, TestSchema.TestRow> rangeTombestones = new HashMap<>();
        final Random rnd = new Random(1);
        final long minTimestamp = System.currentTimeMillis();
        final int numRows = 1000;
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(
                type ->
                    CdcTester.builder(bridge, DIR, schemaBuilder.apply(type))
                             .withAddLastModificationTime(true)
                             .clearWriters()
                             .withNumRows(numRows)
                             .withWriter((tester, rows, writer) -> {
                                 long timestamp = minTimestamp;
                                 rangeTombestones.clear();
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
                                         // the field's cooresponding java type should be comparable... (ugly :()
                                         if (((Comparable<Object>) baseBound[lastCK]).compareTo(newBound[lastCK]) < 0)
                                         {
                                             open = baseBound;
                                             close = newBound;
                                         }
                                         else
                                         {
                                             open = newBound;
                                             close = baseBound;
                                         }
                                         testRow.setRangeTombstones(Arrays.asList(
                                             new RangeTombstone(new Bound(open, true), new Bound(close, true))));
                                         rangeTombestones.put(i, testRow);
                                     }
                                     else
                                     {
                                         testRow = Tester.newUniqueRow(tester.schema, rows);
                                     }
                                     timestamp += 1;
                                     writer.accept(testRow, TimeUnit.MILLISECONDS.toMicros(timestamp));
                                 }
                             })
                             // Disable checker on the test row. The check is done in the sparkrow checker below.
                             .withChecker((testRows, actualRows) -> {})
                             .withSparkRowTestRowsChecker((testRows, sparkRows) -> {
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
                                     if (rangeTombestones.containsKey(i)) // verify deletion
                                     {
                                         for (int colIdx = 0; colIdx < numOfColumns; colIdx++)
                                         {
                                             if (colIdx < numOfPartitionKeys)
                                             {
                                                 assertNotNull("All partition keys should exist for range tombstone",
                                                               row.get(colIdx));
                                                 expected.set(colIdx);
                                             }
                                             else
                                             {
                                                 assertNull("Non-partition key columns should be null",
                                                            row.get(colIdx));
                                             }
                                             Object rtColumn = row.get(numOfColumns + 4); // range deletion column
                                             assertNotNull(rtColumn);
                                             WrappedArray<?> tombstones = (WrappedArray<?>) rtColumn;
                                             assertEquals("There should be 1 range tombstone", 1, tombstones.length());
                                             TestSchema.TestRow sourceRow = rangeTombestones.get(i);
                                             RangeTombstone expectedRT = sourceRow.rangeTombstones().get(0);
                                             Row rt = (Row) tombstones.apply(0);
                                             assertEquals("Range tombstone should have 4 fields", 4, rt.length());
                                             assertEquals(expectedRT.open.inclusive, rt.getAs("StartInclusive"));
                                             assertEquals(expectedRT.close.inclusive, rt.getAs("EndInclusive"));
                                             Row open = rt.getAs("Start");
                                             assertEquals(numOfClusteringKeys, open.length());
                                             Row close = rt.getAs("End");
                                             assertEquals(numOfClusteringKeys, close.length());
                                             for (int ckIdx = 0; ckIdx < numOfClusteringKeys; ckIdx++)
                                             {
                                                 TestUtils.assertEquals(expectedRT.open.values[ckIdx], open.get(ckIdx));
                                                 TestUtils.assertEquals(expectedRT.close.values[ckIdx], close.get(ckIdx));
                                             }
                                         }
                                     }
                                     else // verify update
                                     {
                                         for (int colIndex = 0; colIndex < numOfColumns; colIndex++)
                                         {
                                             assertNotNull("All column values should exist for full row update",
                                                           row.get(colIndex));
                                             expected.set(colIndex);
                                         }
                                         assertNull("the cell deletion map should be absent",
                                                    row.get(numOfColumns + 3));
                                     }
                                     assertEquals("row" + i + " should have the expected columns set", expected, bs);
                                 }
                             })
                             .run());
    }
}
