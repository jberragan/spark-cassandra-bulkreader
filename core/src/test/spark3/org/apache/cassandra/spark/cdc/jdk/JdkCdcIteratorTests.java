package org.apache.cassandra.spark.cdc.jdk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.cdc.AbstractCdcEvent;
import org.apache.cassandra.spark.cdc.FourZeroCommitLog;
import org.apache.cassandra.spark.cdc.jdk.msg.CdcMessage;
import org.apache.cassandra.spark.cdc.jdk.msg.Column;
import org.apache.cassandra.spark.cdc.jdk.msg.RangeTombstone;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.fourzero.FourZero;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.CellPath;
import org.apache.cassandra.spark.utils.RandomUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class JdkCdcIteratorTests
{
    public static final CassandraBridge BRIDGE = CassandraBridge.get(CassandraBridge.CassandraVersion.FOURZERO);

    @ClassRule
    public static TemporaryFolder DIR = new TemporaryFolder();

    @BeforeClass
    public static void setup()
    {
        JdkCdcIteratorTests.setup(DIR);
    }

    @AfterClass
    public static void end()
    {
        tearDown();
    }

    public static void tearDown()
    {
        try
        {
            LOG.stop();
        }
        finally
        {
            LOG.clear();
        }
    }

    public static FourZeroCommitLog LOG;

    public static void setup(TemporaryFolder testFolder)
    {
        setup(testFolder, 32);
    }

    public static void setup(TemporaryFolder testFolder, final int commitLogSegmentSize)
    {
        FourZero.setup();
        FourZero.setCommitLogPath(testFolder.getRoot().toPath());
        FourZero.setCDC(testFolder.getRoot().toPath(), commitLogSegmentSize);
        LOG = new FourZeroCommitLog(testFolder.getRoot());
    }

    @Test
    public void testInserts()
    {
        runTest(TestSchema.builder()
                          .withPartitionKey("a", BRIDGE.timeuuid())
                          .withPartitionKey("b", BRIDGE.text())
                          .withClusteringKey("c", BRIDGE.timestamp())
                          .withColumn("d", BRIDGE.map(BRIDGE.text(), BRIDGE.aInt())),
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
        runTest(
        TestSchema.builder()
                  .withPartitionKey("a", BRIDGE.timeuuid())
                  .withPartitionKey("b", BRIDGE.aInt())
                  .withClusteringKey("c", BRIDGE.bigint())
                  .withColumn("d", BRIDGE.text()),
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
        runTest(
        TestSchema.builder()
                  .withPartitionKey("a", BRIDGE.timeuuid())
                  .withPartitionKey("b", BRIDGE.aInt())
                  .withClusteringKey("c", BRIDGE.bigint())
                  .withColumn("d", BRIDGE.text()),
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
        runTest(
        TestSchema.builder()
                  .withPartitionKey("a", BRIDGE.timeuuid())
                  .withPartitionKey("b", BRIDGE.aInt())
                  .withClusteringKey("c", BRIDGE.bigint())
                  .withColumn("d", BRIDGE.text()),
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
        runTest(
        TestSchema.builder()
                  .withPartitionKey("a", BRIDGE.uuid())
                  .withClusteringKey("b", BRIDGE.aInt())
                  .withClusteringKey("c", BRIDGE.aInt())
                  .withColumn("d", BRIDGE.text()),
        (schema, i, rows) -> {
            final TestSchema.TestRow testRow = Tester.newUniqueRow(schema, rows);
            final int start = RandomUtils.randomPositiveInt(1024);
            final int end = start + RandomUtils.randomPositiveInt(100000);
            testRow.setRangeTombstones(ImmutableList.of(
                                       new CassandraBridge.RangeTombstoneData(new CassandraBridge.RangeTombstoneData.Bound(new Integer[]{ start, start + RandomUtils.randomPositiveInt(100) }, true),
                                                                              new CassandraBridge.RangeTombstoneData.Bound(new Integer[]{ end, end + RandomUtils.randomPositiveInt(100) }, true)))
            );
            rows.put(testRow.get(0).toString(), testRow);
            return testRow;
        },
        (msg, rows, nowMicros) -> {
            assertEquals(msg.operationType(), AbstractCdcEvent.Kind.RANGE_DELETE);
            final List<RangeTombstone> tombstones = msg.rangeTombstones();
            final TestSchema.TestRow row = rows.get(msg.column("a").value().toString());
            assertEquals(1, tombstones.size());
            final RangeTombstone tombstone = tombstones.get(0);
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
        final Map<String, String> deletedValues = new HashMap<>(TestUtils.NUM_ROWS);
        runTest(
        TestSchema.builder()
                  .withPartitionKey("a", BRIDGE.uuid())
                  .withColumn("b", BRIDGE.set(BRIDGE.text())),
        (schema, i, rows) -> {
            TestSchema.TestRow testRow = Tester.newUniqueRow(schema, rows);
            final String deletedValue = (String) BRIDGE.text().randomValue(4);
            final ByteBuffer key = BRIDGE.text().serialize(deletedValue);
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
        final Map<String, String> deletedValues = new HashMap<>(TestUtils.NUM_ROWS);
        runTest(
        TestSchema.builder()
                  .withPartitionKey("a", BRIDGE.uuid())
                  .withColumn("b", BRIDGE.map(BRIDGE.text(), BRIDGE.aInt())),
        (schema, i, rows) -> {
            TestSchema.TestRow testRow = Tester.newUniqueRow(schema, rows);
            final String deletedValue = (String) BRIDGE.text().randomValue(4);
            final ByteBuffer key = BRIDGE.text().serialize(deletedValue);
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

    private static void runTest(TestSchema.Builder schemaBuilder,
                                RowGenerator rowGenerator,
                                TestVerifier verify)
    {
        LOG.start();
        final long nowMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        final int numRows = TestUtils.NUM_ROWS;
        final TestSchema schema = schemaBuilder
                                  .withCdc(true)
                                  .build();
        final CqlTable cqlTable = schema.buildSchema();
        schema.schemaBuilder(Partitioner.Murmur3Partitioner);
        schema.setCassandraVersion(CassandraBridge.CassandraVersion.FOURZERO);

        try
        {
            final Map<String, TestSchema.TestRow> rows = new HashMap<>(numRows);
            for (int i = 0; i < numRows; i++)
            {
                final TestSchema.TestRow row = rowGenerator.newRow(schema, i, rows);
                BRIDGE.log(cqlTable, LOG, row, nowMicros);
            }
            LOG.sync();

            int count = 0;
            try (final TestJdkCdcIterator cdc = new TestJdkCdcIterator(DIR.getRoot().toPath()))
            {
                while (cdc.next())
                {
                    cdc.advanceToNextColumn();
                    verify.verify(cdc.data(), rows, nowMicros);
                    count++;
                }
                assertEquals(numRows, count);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        finally
        {
            tearDown();
        }
    }
}
