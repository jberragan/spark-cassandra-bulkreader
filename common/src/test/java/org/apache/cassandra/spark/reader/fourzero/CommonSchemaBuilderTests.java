package org.apache.cassandra.spark.reader.fourzero;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.fourzero.FourZeroTypes;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class CommonSchemaBuilderTests
{
    @BeforeClass
    public static void setup()
    {
        FourZeroTypes.setup();
    }

    public static final String SCHEMA_TXT = "CREATE TABLE backup_test.sbr_test (\n" +
                                            "    account_id uuid PRIMARY KEY,\n" +
                                            "    balance bigint,\n" +
                                            "    name text\n" +
                                            ") WITH bloom_filter_fp_chance = 0.1\n" +
                                            "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
                                            "    AND comment = 'Created by: jberragan'\n" +
                                            "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}\n" +
                                            "    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
                                            "    AND crc_check_chance = 1.0\n" +
                                            "    AND default_time_to_live = 0\n" +
                                            "    AND gc_grace_seconds = 864000\n" +
                                            "    AND max_index_interval = 2048\n" +
                                            "    AND memtable_flush_period_in_ms = 0\n" +
                                            "    AND min_index_interval = 128\n;";

    @Test
    public void testBuild()
    {
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final CqlTable schema = new FourZeroSchemaBuilder(SCHEMA_TXT, "backup_test", rf).build();
        final List<CqlField> fields = schema.fields();
        assertNotNull(fields);
        assertEquals(3, fields.size());
        assertEquals("account_id", fields.get(0).name());
        assertEquals("balance", fields.get(1).name());
        assertEquals("name", fields.get(2).name());
        assertEquals(SSTable.OSS_PACKAGE_NAME.matcher(SCHEMA_TXT).replaceAll(SSTable.SHADED_PACKAGE_NAME), schema.createStmt());
        assertEquals(3, schema.replicationFactor().getOptions().get("DC1").intValue());
        assertEquals(3, schema.replicationFactor().getOptions().get("DC2").intValue());
        assertNull(schema.replicationFactor().getOptions().get("DC3"));
        assertEquals(1, schema.numPartitionKeys());
        assertEquals(0, schema.numClusteringKeys());
        assertEquals(0, schema.numStaticColumns());
        assertEquals(2, schema.numValueColumns());
    }

    @Test
    public void testEquality()
    {
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final CqlTable schema1 = new FourZeroSchemaBuilder(SCHEMA_TXT, "backup_test", rf).build();
        final CqlTable schema2 = new FourZeroSchemaBuilder(SCHEMA_TXT, "backup_test", rf).build();
        assertNotSame(schema1, schema2);
        assertNotEquals(null, schema2);
        assertNotEquals(null, schema1);
        assertNotEquals(new ArrayList<>(), schema1);
        assertEquals(schema1, schema1);
        assertEquals(schema1, schema2);
        assertEquals(schema1.hashCode(), schema2.hashCode());
    }

    @Test
    public void testSameKeyspace()
    {
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final CqlTable schema1 = new FourZeroSchemaBuilder(SCHEMA_TXT, "backup_test", rf).build();
        final CqlTable schema2 = new FourZeroSchemaBuilder(SCHEMA_TXT.replace("sbr_test", "sbr_test2"), "backup_test", rf).build();
        assertNotSame(schema1, schema2);
        assertEquals("sbr_test2", schema2.table());
        assertEquals("sbr_test", schema1.table());
    }

    @Test
    public void testUUID()
    {
        assertEquals(1, new FourZeroTypes().getTimeUUID().version());
    }

    @Test
    public void getCompactionClass()
    {
        FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.compaction.LeveledCompactionStrategy", "LeveledCompactionStrategy");
    }

    @Test
    public void testFourZeroTypes()
    {
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.Murmur3Partitioner", "Murmur3Partitioner"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.RandomPartitioner", "RandomPartitioner"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractCompositeType", "AbstractCompositeType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType", "AbstractType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AsciiType", "AsciiType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.BooleanType", "BooleanType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.BytesType", "BytesType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ByteType", "ByteType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.CollectionType", "CollectionType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.CompositeType", "CompositeType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.CounterColumnType", "CounterColumnType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.DateType", "DateType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.DecimalType", "DecimalType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.DoubleType", "DoubleType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.DurationType", "DurationType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.DynamicCompositeType", "DynamicCompositeType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.EmptyType", "EmptyType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.FloatType", "FloatType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.FrozenType", "FrozenType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.InetAddressType", "InetAddressType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.Int32Type", "Int32Type"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.IntegerType", "IntegerType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.LexicalUUIDType", "LexicalUUIDType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ListType", "ListType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.LongType", "LongType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.MapType", "MapType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.NumberType", "NumberType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.PartitionerDefinedOrder", "PartitionerDefinedOrder"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ReversedType", "ReversedType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.SetType", "SetType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ShortType", "ShortType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.SimpleDateType", "SimpleDateType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TemporalType", "TemporalType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TimestampType", "TimestampType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TimeType", "TimeType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TimeUUIDType", "TimeUUIDType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TupleType", "TupleType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TypeParser", "TypeParser"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UserType", "UserType"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UTF8Type", "UTF8Type"));
        assertNotNull(FBUtilities.classForName("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UUIDType", "UUIDType"));
    }

    @Test
    public void testConvertToShadedPackages()
    {
        assertEquals("", FourZeroSchemaBuilder.convertToShadedPackages(""));
        assertEquals("string", FourZeroSchemaBuilder.convertToShadedPackages("string"));
        assertEquals("prefixorg.apache.cassandra.suffix", FourZeroSchemaBuilder.convertToShadedPackages("prefixorg.apache.cassandra.suffix"));
        assertEquals("prefix org.apache.cassandra.spark.shaded.suffix", FourZeroSchemaBuilder.convertToShadedPackages("prefix org.apache.cassandra.spark.shaded.suffix"));
        assertEquals("prefix org.apache.cassandra.spark.shaded.fourzero.cassandra.suffix", FourZeroSchemaBuilder.convertToShadedPackages("prefix org.apache.cassandra.suffix"));
        assertEquals("prefix org.apache.cassandra.spark.shaded.fourzero.cassandra.infix org.apache.cassandra.spark.shaded.fourzero.cassandra.suffix", FourZeroSchemaBuilder.convertToShadedPackages("prefix org.apache.cassandra.infix org.apache.cassandra.suffix"));

        assertEquals("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UUIDType", FourZeroSchemaBuilder.convertToShadedPackages("org.apache.cassandra.db.marshal.UUIDType"));
        assertEquals("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UUIDType", FourZeroSchemaBuilder.convertToShadedPackages("org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UUIDType"));
        assertEquals("org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.RandomPartitioner", FourZeroSchemaBuilder.convertToShadedPackages("org.apache.cassandra.dht.RandomPartitioner"));
        assertEquals("\"org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UUIDType\"", FourZeroSchemaBuilder.convertToShadedPackages("\"org.apache.cassandra.db.marshal.UUIDType\""));
        assertEquals("'org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UUIDType'", FourZeroSchemaBuilder.convertToShadedPackages("'org.apache.cassandra.db.marshal.UUIDType'"));
        assertEquals("abcorg.apache.cassandra.db.marshal.UUIDType", FourZeroSchemaBuilder.convertToShadedPackages("abcorg.apache.cassandra.db.marshal.UUIDType"));
    }

    @Test
    public void testShadedPackageNames()
    {
        final String converted = FourZeroSchemaBuilder.convertToShadedPackages(SCHEMA_TXT);

        assertEquals("CREATE TABLE backup_test.sbr_test (\n" +
                     "    account_id uuid PRIMARY KEY,\n" +
                     "    balance bigint,\n" +
                     "    name text\n" +
                     ") WITH bloom_filter_fp_chance = 0.1\n" +
                     "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
                     "    AND comment = 'Created by: jberragan'\n" +
                     "    AND compaction = {'class': 'org.apache.cassandra.spark.shaded.fourzero.cassandra.db.compaction.LeveledCompactionStrategy'}\n" +
                     "    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.spark.shaded.fourzero.cassandra.io.compress.LZ4Compressor'}\n" +
                     "    AND crc_check_chance = 1.0\n" +
                     "    AND default_time_to_live = 0\n" +
                     "    AND gc_grace_seconds = 864000\n" +
                     "    AND max_index_interval = 2048\n" +
                     "    AND memtable_flush_period_in_ms = 0\n" +
                     "    AND min_index_interval = 128\n;", converted);
    }

    @Test
    public void testCollections()
    {
        final String create_stmt = "CREATE TABLE backup_test.collection_test (account_id uuid PRIMARY KEY, balance bigint, names set<text>);";
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final CqlTable schema = new FourZeroSchemaBuilder(create_stmt, "backup_test", rf).build();
        assertEquals(schema.getField("names").type().internalType(), CqlField.CqlType.InternalType.Set);
    }


    @Test
    public void testSetClusteringKey()
    {
        final String create_stmt = "CREATE TABLE backup_test.sbr_test_set_ck (pk uuid, ck frozen<set<text>>, PRIMARY KEY (pk, ck));";
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        new FourZeroSchemaBuilder(create_stmt, "backup_test", rf).build();
    }

    @Test
    public void testListClusteringKey()
    {
        final String create_stmt = "CREATE TABLE backup_test.sbr_test_list_ck (pk uuid, ck frozen<list<bigint>>, PRIMARY KEY (pk, ck));";
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        new FourZeroSchemaBuilder(create_stmt, "backup_test", rf).build();
    }

    @Test
    public void testMapClusteringKey()
    {
        final String create_stmt = "CREATE TABLE backup_test.sbr_test_map_ck (pk uuid, ck frozen<map<uuid, timestamp>>, PRIMARY KEY (pk, ck));";
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        new FourZeroSchemaBuilder(create_stmt, "backup_test", rf).build();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNativeUnsupportedColumnMetaData()
    {
        final String create_stmt = "CREATE TABLE backup_test.sbr_test (account_id uuid PRIMARY KEY, transactions counter);";
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        new FourZeroSchemaBuilder(create_stmt, "backup_test", rf).build();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedInnerType()
    {
        final String create_stmt = "CREATE TABLE backup_test.sbr_test (account_id uuid PRIMARY KEY, transactions frozen<map<text, duration>>);";
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        new FourZeroSchemaBuilder(create_stmt, "backup_test", rf).build();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedUdt()
    {
        final String create_stmt = "CREATE TABLE backup_test.sbr_test (account_id uuid PRIMARY KEY, transactions testudt);";
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        new FourZeroSchemaBuilder(create_stmt, "backup_test", rf, Partitioner.Murmur3Partitioner, toSet("CREATE TYPE backup_test.testudt(birthday timestamp, count bigint, length duration);"), null).build();
    }

    @Test
    public void testNestedUDTs()
    {
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final String keyspace = "nested_udt_schema";
        final FourZeroSchemaBuilder builder = new FourZeroSchemaBuilder("CREATE TABLE " + keyspace + ".udt_test (\n" +
                                                                        "    a uuid PRIMARY KEY,\n" +
                                                                        "    b bigint,\n" +
                                                                        "    c a_udt,\n" +
                                                                        "    d text\n" +
                                                                        ");", "test_keyspace", rf, Partitioner.Murmur3Partitioner,
                                                                        toSet("CREATE TYPE " + keyspace + ".a_udt (col1 bigint, col2 text, col3 frozen<map<uuid, b_udt>>);",
                                                                              "CREATE TYPE " + keyspace + ".b_udt (col1 timeuuid, col2 text, col3 frozen<set<c_udt>>);",
                                                                              "CREATE TYPE " + keyspace + ".c_udt (col1 float, col2 uuid, col3 int);"), null);
        final CqlTable schema = builder.build();
        assertEquals(3, schema.udts().size());
    }

    private static Set<String> toSet(final String... strs)
    {
        return new HashSet<>(Arrays.asList(strs));
    }
}
