package org.apache.cassandra.spark.reader.fourzero;


import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.CqlUdt;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.FBUtilities;
import org.jetbrains.annotations.Nullable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
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

public class SchemaBuilderTests
{
    static
    {
        FourZero.setup();
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
        final CqlSchema schema = new SchemaBuilder(SCHEMA_TXT, "backup_test", rf).build();
        final List<CqlField> fields = schema.fields();
        assertNotNull(fields);
        assertEquals(3, fields.size());
        assertEquals("account_id", fields.get(0).name());
        assertEquals("balance", fields.get(1).name());
        assertEquals("name", fields.get(2).name());
        assertEquals(SCHEMA_TXT.replace(SchemaBuilder.OSS_PACKAGE_NAME, SchemaBuilder.SHADED_PACKAGE_NAME), schema.createStmt());
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
        final CqlSchema schema1 = new SchemaBuilder(SCHEMA_TXT, "backup_test", rf).build();
        final CqlSchema schema2 = new SchemaBuilder(SCHEMA_TXT, "backup_test", rf).build();
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
        final CqlSchema schema1 = new SchemaBuilder(SCHEMA_TXT, "backup_test", rf).build();
        final CqlSchema schema2 = new SchemaBuilder(SCHEMA_TXT.replace("sbr_test", "sbr_test2"), "backup_test", rf).build();
        assertNotSame(schema1, schema2);
        assertEquals("sbr_test2", schema2.table());
        assertEquals("sbr_test", schema1.table());
    }

    @Test
    public void testHasher()
    {
        assertEquals(BigInteger.valueOf(6747049197585865300L), new FourZero().hash(Partitioner.Murmur3Partitioner, (ByteBuffer) ByteBuffer.allocate(8).putLong(992393994949L).flip()));
        assertEquals(BigInteger.valueOf(7071430368280192841L), new FourZero().hash(Partitioner.Murmur3Partitioner, (ByteBuffer) ByteBuffer.allocate(4).putInt(999).flip()));
        assertEquals(new BigInteger("28812675363873787366858706534556752548"), new FourZero().hash(Partitioner.RandomPartitioner, (ByteBuffer) ByteBuffer.allocate(8).putLong(34828288292L).flip()));
        assertEquals(new BigInteger("154860613751552680515987154638148676974"), new FourZero().hash(Partitioner.RandomPartitioner, (ByteBuffer) ByteBuffer.allocate(4).putInt(1929239).flip()));
    }

    @Test
    public void testUUID()
    {
        assertEquals(1, new FourZero().getTimeUUID().version());
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
    public void testShadedPackageNames()
    {
        final String converted = SchemaBuilder.convertToShadedPackages(SCHEMA_TXT);

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
        final CqlSchema schema = new SchemaBuilder(create_stmt, "backup_test", rf).build();
        assertEquals(schema.getField("names").type().internalType(), CqlField.CqlType.InternalType.Set);
    }

    @Test
    public void testSetClusteringKey()
    {
        final String create_stmt = "CREATE TABLE backup_test.sbr_test_set_ck (pk uuid, ck frozen<set<text>>, PRIMARY KEY (pk, ck));";
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        new SchemaBuilder(create_stmt, "backup_test", rf).build();
    }

    @Test
    public void testListClusteringKey()
    {
        final String create_stmt = "CREATE TABLE backup_test.sbr_test_list_ck (pk uuid, ck frozen<list<bigint>>, PRIMARY KEY (pk, ck));";
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        new SchemaBuilder(create_stmt, "backup_test", rf).build();
    }

    @Test
    public void testMapClusteringKey()
    {
        final String create_stmt = "CREATE TABLE backup_test.sbr_test_map_ck (pk uuid, ck frozen<map<uuid, timestamp>>, PRIMARY KEY (pk, ck));";
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        new SchemaBuilder(create_stmt, "backup_test", rf).build();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNativeUnsupportedColumnMetaData()
    {
        final String create_stmt = "CREATE TABLE backup_test.sbr_test (account_id uuid PRIMARY KEY, transactions counter);";
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        new SchemaBuilder(create_stmt, "backup_test", rf).build();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedInnerType()
    {
        final String create_stmt = "CREATE TABLE backup_test.sbr_test (account_id uuid PRIMARY KEY, transactions frozen<map<text, duration>>);";
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        new SchemaBuilder(create_stmt, "backup_test", rf).build();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedUdt()
    {
        final String create_stmt = "CREATE TABLE backup_test.sbr_test (account_id uuid PRIMARY KEY, transactions testudt);";
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        new SchemaBuilder(create_stmt, "backup_test", rf, Partitioner.Murmur3Partitioner, toSet("CREATE TYPE backup_test.testudt(birthday timestamp, count bigint, length duration);")).build();
    }

    @Test
    public void testCollectionMatcher()
    {
        qt().forAll(TestUtils.cql3Type()).checkAssert(type -> testMatcher("set<%s>", "set", type));
        qt().forAll(TestUtils.cql3Type()).checkAssert(type -> testMatcher("list<%s>", "list", type));
        qt().forAll(TestUtils.cql3Type(), TestUtils.cql3Type()).checkAssert((type1, type2) -> {
            testMatcher("map<%s,%s>", "map", type1, type2);
            testMatcher("map<%s , %s>", "map", type1, type2);
        });
        qt().forAll(TestUtils.cql3Type()).checkAssert(type -> testMatcher(type.cqlName(), null, null));
        qt().forAll(TestUtils.cql3Type(), TestUtils.cql3Type()).checkAssert((type1, type2) -> {
            testMatcher("tuple<%s,%s>", "tuple", type1, type2);
            testMatcher("tuple<%s , %s>", "tuple", type1, type2);
        });
    }

    private static void testMatcher(final String pattern, final String collection, final CqlField.NativeCql3Type dataType1)
    {
        testMatcher(pattern, collection, dataType1, null);
    }

    private static void testMatcher(final String pattern, final String collection, final CqlField.NativeCql3Type dataType1, final CqlField.NativeCql3Type dataType2)
    {
        final boolean isMap = dataType2 != null;
        final String str;
        if (dataType1 == null && dataType2 == null)
        {
            str = pattern;
        }
        else if (dataType2 == null)
        {
            str = String.format(pattern, dataType1);
        }
        else
        {
            str = String.format(pattern, dataType1, dataType2);
        }

        final Matcher matcher = CqlField.COLLECTIONS_PATTERN.matcher(str);
        assertEquals(collection != null && dataType1 != null, matcher.matches());
        if (matcher.matches())
        {
            assertNotNull(collection);
            assertNotNull(dataType1);
            assertEquals(collection, matcher.group(1));
            final String[] types = CqlField.splitInnerTypes(matcher.group(2));
            assertEquals(dataType1, CqlField.NativeCql3Type.valueOf(types[0].toUpperCase()));
            if (isMap)
            {
                assertEquals(dataType2, CqlField.NativeCql3Type.valueOf(types[1].toUpperCase()));
            }
        }
        else
        {
            // raw CQL3 data type
            CqlField.NativeCql3Type.valueOf(pattern.toUpperCase());
        }
    }


    @Test
    public void testFrozenMatcher()
    {
        qt().forAll(TestUtils.cql3Type()).checkAssert(type -> testFrozen("frozen<set<%s>>", CqlField.CqlSet.class, type));
        qt().forAll(TestUtils.cql3Type()).checkAssert(type -> testFrozen("frozen<list<%s>>", CqlField.CqlList.class, type));
        qt().forAll(TestUtils.cql3Type(), TestUtils.cql3Type()).checkAssert((type1, type2) -> {
            testFrozen("frozen<map<%s,%s>>", CqlField.CqlMap.class, type1, type2);
            testFrozen("frozen<map<%s , %s>>", CqlField.CqlMap.class, type1, type2);
        });
    }

    @Test
    public void testNestedFrozenSet()
    {
        final String patternStr = "map<text, frozen<set<bigint>>>";
        final CqlField.CqlType type = CqlField.parseType(patternStr);
        assertNotNull(type);
        assertTrue(type instanceof CqlField.CqlMap);
        final CqlField.CqlMap map = (CqlField.CqlMap) type;
        assertTrue(map.keyType() instanceof CqlField.NativeCql3Type);
        assertTrue(map.valueType() instanceof CqlField.CqlFrozen);
        final CqlField.NativeCql3Type key = (CqlField.NativeCql3Type) map.keyType();
        assertSame(key, CqlField.NativeCql3Type.TEXT);
        final CqlField.CqlFrozen value = (CqlField.CqlFrozen) map.valueType();
        final CqlField.CqlSet inner = (CqlField.CqlSet) value.inner();
        assertSame(inner.type(), CqlField.NativeCql3Type.BIGINT);
    }

    @Test
    public void testNestedFrozenMap()
    {
        final String patternStr = "map<text, frozen<map<bigint, text>>>";
        final CqlField.CqlType type = CqlField.parseType(patternStr);
        assertNotNull(type);
        assertTrue(type instanceof CqlField.CqlMap);
        final CqlField.CqlMap map = (CqlField.CqlMap) type;
        assertTrue(map.keyType() instanceof CqlField.NativeCql3Type);
        assertTrue(map.valueType() instanceof CqlField.CqlFrozen);
        final CqlField.NativeCql3Type key = (CqlField.NativeCql3Type) map.keyType();
        assertSame(key, CqlField.NativeCql3Type.TEXT);
        final CqlField.CqlFrozen value = (CqlField.CqlFrozen) map.valueType();
        final CqlField.CqlMap inner = (CqlField.CqlMap) value.inner();
        assertSame(inner.keyType(), CqlField.NativeCql3Type.BIGINT);
        assertSame(inner.valueType(), CqlField.NativeCql3Type.TEXT);
    }

    private static void testFrozen(final String pattern, final Class<? extends CqlField.CqlCollection> collectionType, final CqlField.CqlType innerType)
    {
        testFrozen(pattern, collectionType, innerType, null);
    }

    private static void testFrozen(final String pattern, final Class<? extends CqlField.CqlCollection> collectionType,
                                   final CqlField.CqlType innerType, @Nullable final CqlField.CqlType innerType2)
    {
        final String patternStr = innerType2 == null ? String.format(pattern, innerType) : String.format(pattern, innerType, innerType2);
        final CqlField.CqlType type = CqlField.parseType(patternStr);
        assertNotNull(type);
        assertTrue(type instanceof CqlField.CqlFrozen);
        final CqlField.CqlFrozen frozen = (CqlField.CqlFrozen) type;
        final CqlField.CqlCollection inner = (CqlField.CqlCollection) frozen.inner();
        assertNotNull(inner);
        assertTrue(collectionType.isInstance(inner));
        assertEquals(innerType, inner.type());
        if (innerType2 != null)
        {
            final CqlField.CqlMap map = (CqlField.CqlMap) inner;
            assertEquals(innerType2, map.valueType());
        }
    }

    /* user defined types */

    @Test
    public void testUdts()
    {
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final String keyspace = "udt_keyspace";
        final String udtName = "udt_name";
        final SchemaBuilder builder = new SchemaBuilder("CREATE TABLE " + keyspace + ".udt_test (\n" +
                                                        "    account_id uuid PRIMARY KEY,\n" +
                                                        "    balance bigint,\n" +
                                                        "    info " + udtName + ",\n" +
                                                        "    name text\n" +
                                                        ");", keyspace, rf, Partitioner.Murmur3Partitioner,
                                                        toSet("CREATE TYPE " + keyspace + "." + udtName + " (\n" +
                                                              "  birthday timestamp,\n" +
                                                              "  nationality text,\n" +
                                                              "  weight float,\n" +
                                                              "  height int\n" +
                                                              ");"));
        final CqlSchema schema = builder.build();
        assertEquals(1, schema.udts().size());
        final CqlUdt udt = schema.udts().stream().findFirst().get();
        assertEquals(udtName, udt.name());
        final List<CqlField> udtFields = udt.fields();
        assertEquals(4, udtFields.size());
        assertEquals(CqlField.NativeCql3Type.TIMESTAMP, udtFields.get(0).type());
        assertEquals(CqlField.NativeCql3Type.TEXT, udtFields.get(1).type());
        assertEquals(CqlField.NativeCql3Type.FLOAT, udtFields.get(2).type());
        assertEquals(CqlField.NativeCql3Type.INT, udtFields.get(3).type());

        final List<CqlField> fields = schema.fields();
        assertEquals(CqlField.NativeCql3Type.UUID, fields.get(0).type());
        assertEquals(CqlField.NativeCql3Type.BIGINT, fields.get(1).type());
        assertEquals(CqlField.CqlType.InternalType.Udt, fields.get(2).type().internalType());
        assertEquals(CqlField.NativeCql3Type.TEXT, fields.get(3).type());

        final CqlUdt udtField = (CqlUdt) fields.get(2).type();
        assertEquals(CqlField.NativeCql3Type.TIMESTAMP, udtField.field(0).type());
        assertEquals(CqlField.NativeCql3Type.TEXT, udtField.field(1).type());
        assertEquals(CqlField.NativeCql3Type.FLOAT, udtField.field(2).type());
        assertEquals(CqlField.NativeCql3Type.INT, udtField.field(3).type());
    }

    @Test
    public void testCollectionUdts()
    {
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final String keyspace = "collection_keyspace";
        final String udtName = "basic_info";
        final SchemaBuilder builder = new SchemaBuilder("CREATE TABLE " + keyspace + "." + udtName + " (\n" +
                                                        "    account_id uuid PRIMARY KEY,\n" +
                                                        "    balance bigint,\n" +
                                                        "    info frozen<map<text, " + udtName + ">>,\n" +
                                                        "    name text\n" +
                                                        ");", "test_keyspace", rf, Partitioner.Murmur3Partitioner,
                                                        toSet("CREATE TYPE " + keyspace + "." + udtName + " (\n" +
                                                              "  birthday timestamp,\n" +
                                                              "  nationality text,\n" +
                                                              "  weight float,\n" +
                                                              "  height int\n" +
                                                              ");"));
        final CqlSchema schema = builder.build();
        final List<CqlField> fields = schema.fields();
        assertEquals(CqlField.NativeCql3Type.UUID, fields.get(0).type());
        assertEquals(CqlField.NativeCql3Type.BIGINT, fields.get(1).type());
        assertEquals(CqlField.CqlType.InternalType.Frozen, fields.get(2).type().internalType());
        assertEquals(CqlField.NativeCql3Type.TEXT, fields.get(3).type());

        final CqlField.CqlMap mapField = (CqlField.CqlMap) ((CqlField.CqlFrozen) fields.get(2).type()).inner();
        assertEquals(CqlField.NativeCql3Type.TEXT, mapField.keyType());
        final CqlField.CqlFrozen valueType = (CqlField.CqlFrozen) mapField.valueType();
        final CqlUdt udtField = (CqlUdt) valueType.inner();
        assertEquals(CqlField.NativeCql3Type.TIMESTAMP, udtField.field(0).type());
        assertEquals(CqlField.NativeCql3Type.TEXT, udtField.field(1).type());
        assertEquals(CqlField.NativeCql3Type.FLOAT, udtField.field(2).type());
        assertEquals(CqlField.NativeCql3Type.INT, udtField.field(3).type());
    }

    @Test
    public void testParseUdt()
    {
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final SchemaBuilder builder = new SchemaBuilder(SCHEMA_TXT, "test_keyspace", rf, Partitioner.Murmur3Partitioner,
                                                        toSet("CREATE TYPE test_keyspace.tuple_test (a int, b bigint, c blob, d text)"));
        final CqlSchema schema = builder.build();
        assertEquals(1, schema.udts().size());
        final CqlUdt udt = schema.udts().stream().findFirst().get();
        assertEquals("tuple_test", udt.name());
        final List<CqlField> fields = udt.fields();
        assertEquals(4, fields.size());
        assertEquals(CqlField.NativeCql3Type.INT, fields.get(0).type());
        assertEquals(CqlField.NativeCql3Type.BIGINT, fields.get(1).type());
        assertEquals(CqlField.NativeCql3Type.BLOB, fields.get(2).type());
        assertEquals(CqlField.NativeCql3Type.TEXT, fields.get(3).type());
    }

    @Test
    public void testParseTuple()
    {
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final SchemaBuilder builder = new SchemaBuilder("CREATE TABLE tuple_keyspace.tuple_test (\n" +
                                                        "    account_id uuid PRIMARY KEY,\n" +
                                                        "    balance bigint,\n" +
                                                        "    info tuple<bigint, text, float, boolean>," +
                                                        "    name text\n" +
                                                        ")", "tuple_keyspace", rf, Partitioner.Murmur3Partitioner);
        final CqlSchema schema = builder.build();
        final List<CqlField> fields = schema.fields();
        assertEquals(4, fields.size());
        assertEquals(CqlField.NativeCql3Type.UUID, fields.get(0).type());
        assertEquals(CqlField.NativeCql3Type.BIGINT, fields.get(1).type());
        assertEquals(CqlField.NativeCql3Type.TEXT, fields.get(3).type());

        assertEquals(CqlField.CqlType.InternalType.Frozen, fields.get(2).type().internalType());
        final CqlField.CqlTuple tuple = (CqlField.CqlTuple) ((CqlField.CqlFrozen) fields.get(2).type()).inner();
        assertEquals(CqlField.NativeCql3Type.BIGINT, tuple.type(0));
        assertEquals(CqlField.NativeCql3Type.TEXT, tuple.type(1));
        assertEquals(CqlField.NativeCql3Type.FLOAT, tuple.type(2));
        assertEquals(CqlField.NativeCql3Type.BOOLEAN, tuple.type(3));
    }

    @Test
    public void testComplexSchema()
    {
        final String keyspace = "complex_schema1";
        final String type1 = "CREATE TYPE " + keyspace + ".field_with_timestamp (\n" +
                             "    field text,\n" +
                             "    \"timeWithZone\" frozen<" + keyspace + ".analytics_time_with_zone>\n" +
                             ");";
        final String type2 = "CREATE TYPE " + keyspace + ".first_last_seen_fields_v1 (\n" +
                             "    \"firstSeen\" frozen<" + keyspace + ".field_with_timestamp>,\n" +
                             "    \"lastSeen\" frozen<" + keyspace + ".field_with_timestamp>,\n" +
                             "    \"firstTransaction\" frozen<" + keyspace + ".field_with_timestamp>,\n" +
                             "    \"lastTransaction\" frozen<" + keyspace + ".field_with_timestamp>,\n" +
                             "    \"firstListening\" frozen<" + keyspace + ".field_with_timestamp>,\n" +
                             "    \"lastListening\" frozen<" + keyspace + ".field_with_timestamp>,\n" +
                             "    \"firstReading\" frozen<" + keyspace + ".field_with_timestamp>,\n" +
                             "    \"lastReading\" frozen<" + keyspace + ".field_with_timestamp>,\n" +
                             "    \"outputEvent\" text,\n" +
                             "    \"eventHistory\" frozen<map<bigint, frozen<map<text, boolean>>>>\n" +
                             ");";
        final String type3 = "CREATE TYPE " + keyspace + ".analytics_time_with_zone (\n" +
                             "    time bigint,\n" +
                             "    \"timezoneOffsetMinutes\" int\n" +
                             ");";
        final String type4 = "CREATE TYPE " + keyspace + ".first_last_seen_dimensions_v1 (\n" +
                             "    \"osMajorVersion\" text,\n" +
                             "    \"storeFrontId\" text,\n" +
                             "    platform text,\n" +
                             "    time_range text\n" +
                             ");";
        final String table = "CREATE TABLE " + keyspace + ".books_ltd_v3 (\n" +
                             "    \"consumerId\" text,\n" +
                             "    dimensions frozen<" + keyspace + ".first_last_seen_dimensions_v1>,\n" +
                             "    fields frozen<" + keyspace + ".first_last_seen_fields_v1>,\n" +
                             "    first_transition_time frozen<" + keyspace + ".analytics_time_with_zone>,\n" +
                             "    last_transition_time frozen<" + keyspace + ".analytics_time_with_zone>,\n" +
                             "    prev_state_id text,\n" +
                             "    state_id text,\n" +
                             "    PRIMARY KEY (\"consumerId\", dimensions)\n" +
                             ") WITH CLUSTERING ORDER BY (dimensions ASC);";
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final SchemaBuilder builder = new SchemaBuilder(table, keyspace, rf, Partitioner.Murmur3Partitioner, toSet(type1, type2, type3, type4));
        final CqlSchema schema = builder.build();
        assertEquals("books_ltd_v3", schema.table());
        assertEquals(keyspace, schema.keyspace());
        assertEquals(7, schema.fields().size());
        assertEquals(1, schema.partitionKeys().size());
        assertEquals(1, schema.clusteringKeys().size());

        final List<CqlField> fields = schema.fields();
        assertEquals(7, fields.size());
        assertEquals("consumerId", fields.get(0).name());
        assertEquals(CqlField.NativeCql3Type.TEXT, fields.get(0).type());
        final CqlField clusteringKey = fields.get(1);
        assertEquals("dimensions", clusteringKey.name());
        assertEquals(CqlField.CqlType.InternalType.Frozen, clusteringKey.type().internalType());

        final CqlUdt clusteringUDT = (CqlUdt) ((CqlField.CqlFrozen) clusteringKey.type()).inner();
        assertEquals("first_last_seen_dimensions_v1", clusteringUDT.name());
        assertEquals(keyspace, clusteringUDT.keyspace());
        assertEquals("osMajorVersion", clusteringUDT.field(0).name());
        assertEquals(CqlField.NativeCql3Type.TEXT, clusteringUDT.field(0).type());
        assertEquals("storeFrontId", clusteringUDT.field(1).name());
        assertEquals(CqlField.NativeCql3Type.TEXT, clusteringUDT.field(1).type());
        assertEquals("platform", clusteringUDT.field(2).name());
        assertEquals(CqlField.NativeCql3Type.TEXT, clusteringUDT.field(2).type());
        assertEquals("time_range", clusteringUDT.field(3).name());
        assertEquals(CqlField.NativeCql3Type.TEXT, clusteringUDT.field(3).type());

        final CqlUdt fieldsUDT = (CqlUdt) ((CqlField.CqlFrozen) fields.get(2).type()).inner();
        assertEquals("first_last_seen_fields_v1", fieldsUDT.name());
        assertEquals("firstSeen", fieldsUDT.field(0).name());
        assertEquals("field_with_timestamp", ((CqlUdt) ((CqlField.CqlFrozen) fieldsUDT.field(0).type()).inner()).name());
        assertEquals("lastSeen", fieldsUDT.field(1).name());
        assertEquals("field_with_timestamp", ((CqlUdt) ((CqlField.CqlFrozen) fieldsUDT.field(1).type()).inner()).name());
        assertEquals("firstTransaction", fieldsUDT.field(2).name());
        assertEquals("field_with_timestamp", ((CqlUdt) ((CqlField.CqlFrozen) fieldsUDT.field(2).type()).inner()).name());
        assertEquals("lastTransaction", fieldsUDT.field(3).name());
        assertEquals("field_with_timestamp", ((CqlUdt) ((CqlField.CqlFrozen) fieldsUDT.field(3).type()).inner()).name());
        assertEquals("firstListening", fieldsUDT.field(4).name());
        assertEquals("field_with_timestamp", ((CqlUdt) ((CqlField.CqlFrozen) fieldsUDT.field(4).type()).inner()).name());
        assertEquals("lastListening", fieldsUDT.field(5).name());
        assertEquals("field_with_timestamp", ((CqlUdt) ((CqlField.CqlFrozen) fieldsUDT.field(5).type()).inner()).name());
        assertEquals("firstReading", fieldsUDT.field(6).name());
        assertEquals("field_with_timestamp", ((CqlUdt) ((CqlField.CqlFrozen) fieldsUDT.field(6).type()).inner()).name());
        assertEquals("lastReading", fieldsUDT.field(7).name());
        assertEquals("field_with_timestamp", ((CqlUdt) ((CqlField.CqlFrozen) fieldsUDT.field(7).type()).inner()).name());
        assertEquals("outputEvent", fieldsUDT.field(8).name());
        assertEquals(CqlField.NativeCql3Type.TEXT, fieldsUDT.field(8).type());
        assertEquals("eventHistory", fieldsUDT.field(9).name());
        assertEquals(CqlField.NativeCql3Type.BIGINT, ((CqlField.CqlMap) ((CqlField.CqlFrozen) fieldsUDT.field(9).type()).inner()).keyType());
        assertEquals(CqlField.CqlType.InternalType.Frozen, ((CqlField.CqlMap) ((CqlField.CqlFrozen) fieldsUDT.field(9).type()).inner()).valueType().internalType());
    }

    @Test
    public void testNestedUDTs()
    {
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final String keyspace = "nested_udt_schema";
        final SchemaBuilder builder = new SchemaBuilder("CREATE TABLE " + keyspace + ".udt_test (\n" +
                                                        "    a uuid PRIMARY KEY,\n" +
                                                        "    b bigint,\n" +
                                                        "    c a_udt,\n" +
                                                        "    d text\n" +
                                                        ");", "test_keyspace", rf, Partitioner.Murmur3Partitioner,
                                                        toSet("CREATE TYPE " + keyspace + ".a_udt (col1 bigint, col2 text, col3 frozen<map<uuid, b_udt>>);",
                                                              "CREATE TYPE " + keyspace + ".b_udt (col1 timeuuid, col2 text, col3 frozen<set<c_udt>>);",
                                                              "CREATE TYPE " + keyspace + ".c_udt (col1 float, col2 uuid, col3 int);"));
        final CqlSchema schema = builder.build();
        assertEquals(3, schema.udts().size());
    }

    private static Set<String> toSet(final String... strs)
    {
        return new HashSet<>(Arrays.asList(strs));
    }
}
