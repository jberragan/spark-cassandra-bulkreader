package org.apache.cassandra.spark.reader.fourzero;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.BytesType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Schema;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.UTF8Serializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SchemaUtilsTest
{
    static final CassandraBridge BRIDGE = CassandraBridge.get(CassandraBridge.CassandraVersion.FOURZERO);

    @Test
    public void testEnableCdc()
    {
        Schema schema = Schema.instance;
        final TestSchema testSchema = TestSchema.builder().withPartitionKey("a", BRIDGE.aInt())
                                                .withClusteringKey("b", BRIDGE.text())
                                                .withColumn("c", BRIDGE.timeuuid())
                                                .build();
        final CqlTable cqlTable = testSchema.buildSchema();
        assertFalse(SchemaUtils.has(schema, cqlTable));
        assertFalse(SchemaUtils.cdcEnabledTables(schema).containsKey(cqlTable.keyspace()));
        new FourZeroSchemaBuilder(cqlTable.createStmt(), cqlTable.keyspace(), new ReplicationFactor(ImmutableMap.of("class", "NetworkTopologyStrategy", "DC1", "3", "dc2", "5")));
        assertTrue(SchemaUtils.cdcEnabledTables(schema).containsKey(cqlTable.keyspace()));
        assertTrue(SchemaUtils.has(schema, cqlTable));
        assertTrue(SchemaUtils.cdcEnabledTables(schema).get(cqlTable.keyspace()).isEmpty());
        SchemaUtils.enableCdc(schema, cqlTable);
        final Map<String, Set<String>> cdcTables = SchemaUtils.cdcEnabledTables(schema);
        assertTrue(cdcTables.containsKey(cqlTable.keyspace()));
        assertEquals(1, cdcTables.get(cqlTable.keyspace()).size());
        assertTrue(cdcTables.get(cqlTable.keyspace()).contains(cqlTable.table()));
    }

    @Test
    public void testSchemaTest()
    {
        Schema schema = Schema.instance;
        final TestSchema testSchema1 = TestSchema.builder()
                                                 .withPartitionKey("a", BRIDGE.aInt())
                                                 .withClusteringKey("b", BRIDGE.text())
                                                 .withColumn("c", BRIDGE.timeuuid())
                                                 .build();
        final CqlTable cqlTable1 = testSchema1.buildSchema();
        new FourZeroSchemaBuilder(cqlTable1, Partitioner.Murmur3Partitioner, null, true);
        assertTrue(SchemaUtils.has(schema, cqlTable1));

        final TestSchema testSchema2 = TestSchema.builder()
                                                 .withKeyspace(cqlTable1.keyspace())
                                                 .withTable(cqlTable1.table())
                                                 .withPartitionKey("a", BRIDGE.aInt())
                                                 .withClusteringKey("b", BRIDGE.text())
                                                 .withColumn("c", BRIDGE.timeuuid())
                                                 .withColumn("d", BRIDGE.blob())
                                                 .withColumn("e", BRIDGE.timeuuid())
                                                 .build();
        final CqlTable cqlTable2 = testSchema2.buildSchema();

        assertColNotExists(cqlTable1, "d");
        assertColNotExists(cqlTable1, "e");

        SchemaUtils.maybeUpdateSchema(schema, Partitioner.Murmur3Partitioner, cqlTable2, null, true);
        assertTrue(SchemaUtils.has(schema, cqlTable2));
        assertColExists(cqlTable2, "d", BytesType.class);
        assertColExists(cqlTable2, "e", TimeUUIDType.class);

        final TestSchema testSchema3 = TestSchema.builder()
                                                 .withKeyspace(cqlTable1.keyspace())
                                                 .withTable(cqlTable1.table())
                                                 .withPartitionKey("a", BRIDGE.aInt())
                                                 .withClusteringKey("b", BRIDGE.text())
                                                 .withColumn("d", BRIDGE.blob())
                                                 .withColumn("e", BRIDGE.timeuuid())
                                                 .build();
        final CqlTable cqlTable3 = testSchema3.buildSchema();
        assertTrue(SchemaUtils.has(schema, cqlTable3));
        SchemaUtils.maybeUpdateSchema(schema, Partitioner.Murmur3Partitioner, cqlTable3, null, true);
        assertColNotExists(cqlTable3, "c");
        assertColExists(cqlTable3, "d", BytesType.class);
        assertColExists(cqlTable3, "e", TimeUUIDType.class);
    }

    private static <T> void assertColExists(CqlTable cqlTable,
                                            String name,
                                            Class<T> tClass)
    {
        assertCol(cqlTable.keyspace(), cqlTable.table(), name, tClass);
    }

    private static <T> void assertColNotExists(CqlTable cqlTable,
                                               String name)
    {
        assertCol(cqlTable.keyspace(), cqlTable.table(), name, null);
    }

    private static <T> void assertCol(String keyspace,
                                      String table,
                                      String name,
                                      Class<T> tClass)
    {
        final TableMetadata tb = SchemaUtils.getKeyspaceMetadata(Schema.instance, keyspace).orElseThrow(RuntimeException::new).getTableOrViewNullable(table);
        final ColumnMetadata col = Objects.requireNonNull(tb).getColumn(UTF8Serializer.instance.serialize(name));
        if (tClass != null)
        {
            assertNotNull(col);
            assertEquals(col.type.getClass().getName(), tClass.getName());
        }
        else
        {
            assertNull(col);
        }
    }
}
