package org.apache.cassandra.spark.reader.fourzero;

import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.reader.CassandraBridge;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SchemaUtilsTest
{
    static final CassandraBridge BRIDGE = CassandraBridge.get(CassandraBridge.CassandraVersion.FOURZERO);

    @Test
    public void testEnableCdc()
    {
        final TestSchema testSchema = TestSchema.builder().withPartitionKey("a", BRIDGE.aInt())
                                                .withClusteringKey("b", BRIDGE.text())
                                                .withColumn("c", BRIDGE.timeuuid())
                                                .build();
        final CqlTable cqlTable = testSchema.buildSchema();
        assertFalse(SchemaUtils.has(cqlTable));
        assertFalse(SchemaUtils.cdcEnabledTables().containsKey(cqlTable.keyspace()));
        new FourZeroSchemaBuilder(cqlTable.createStmt(), cqlTable.keyspace(), new ReplicationFactor(Map.of("class", "NetworkTopologyStrategy", "DC1", "3", "dc2", "5")));
        assertTrue(SchemaUtils.cdcEnabledTables().containsKey(cqlTable.keyspace()));
        assertTrue(SchemaUtils.has(cqlTable));
        assertTrue(SchemaUtils.cdcEnabledTables().get(cqlTable.keyspace()).isEmpty());
        SchemaUtils.enableCdc(cqlTable);
        final Map<String, Set<String>> cdcTables = SchemaUtils.cdcEnabledTables();
        assertEquals(1, cdcTables.size());
        assertTrue(cdcTables.containsKey(cqlTable.keyspace()));
        assertEquals(1, cdcTables.get(cqlTable.keyspace()).size());
        assertTrue(cdcTables.get(cqlTable.keyspace()).contains(cqlTable.table()));
    }
}
