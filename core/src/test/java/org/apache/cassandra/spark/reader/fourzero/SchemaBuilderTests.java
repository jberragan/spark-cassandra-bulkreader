package org.apache.cassandra.spark.reader.fourzero;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.spark.SparkTestUtils;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SparkCqlField;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.jetbrains.annotations.Nullable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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

public class SchemaBuilderTests extends VersionRunner
{
    static
    {
        FourZero.setup();
    }

    public SchemaBuilderTests(CassandraVersion version)
    {
        super(version);
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
    public void testCollectionMatcher()
    {
        qt().forAll(SparkTestUtils.cql3Type(bridge)).checkAssert(type -> testMatcher("set<%s>", "set", type));
        qt().forAll(SparkTestUtils.cql3Type(bridge)).checkAssert(type -> testMatcher("list<%s>", "list", type));
        qt().forAll(SparkTestUtils.cql3Type(bridge), SparkTestUtils.cql3Type(bridge)).checkAssert((type1, type2) -> {
            testMatcher("map<%s,%s>", "map", type1, type2);
            testMatcher("map<%s , %s>", "map", type1, type2);
        });
        qt().forAll(SparkTestUtils.cql3Type(bridge)).checkAssert(type -> testMatcher(type.cqlName(), null, null));
        qt().forAll(SparkTestUtils.cql3Type(bridge), SparkTestUtils.cql3Type(bridge)).checkAssert((type1, type2) -> {
            testMatcher("tuple<%s,%s>", "tuple", type1, type2);
            testMatcher("tuple<%s , %s>", "tuple", type1, type2);
        });
    }

    private void testMatcher(final String pattern, final String collection, final SparkCqlField.SparkCqlType dataType1)
    {
        testMatcher(pattern, collection, dataType1, null);
    }

    private void testMatcher(final String pattern, final String collection, final SparkCqlField.SparkCqlType dataType1, final SparkCqlField.SparkCqlType dataType2)
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

        final Matcher matcher = CassandraTypes.COLLECTIONS_PATTERN.matcher(str);
        assertEquals(collection != null && dataType1 != null, matcher.matches());
        if (matcher.matches())
        {
            assertNotNull(collection);
            assertNotNull(dataType1);
            assertEquals(collection, matcher.group(1));
            final String[] types = CassandraTypes.splitInnerTypes(matcher.group(2));
            assertEquals(dataType1, bridge.nativeType(types[0].toUpperCase()));
            if (isMap)
            {
                assertEquals(dataType2, bridge.nativeType(types[1].toUpperCase()));
            }
        }
        else
        {
            // raw CQL3 data type
            bridge.nativeType(pattern.toUpperCase());
        }
    }


    @Test
    public void testFrozenMatcher()
    {
        qt().forAll(SparkTestUtils.cql3Type(bridge)).checkAssert(type -> testFrozen("frozen<set<%s>>", CqlField.CqlSet.class, type));
        qt().forAll(SparkTestUtils.cql3Type(bridge)).checkAssert(type -> testFrozen("frozen<list<%s>>", CqlField.CqlList.class, type));
        qt().forAll(SparkTestUtils.cql3Type(bridge), SparkTestUtils.cql3Type(bridge)).checkAssert((type1, type2) -> {
            testFrozen("frozen<map<%s,%s>>", CqlField.CqlMap.class, type1, type2);
            testFrozen("frozen<map<%s , %s>>", CqlField.CqlMap.class, type1, type2);
        });
    }

    @Test
    public void testNestedFrozenSet()
    {
        final String patternStr = "map<text, frozen<set<bigint>>>";
        final CqlField.CqlType type = bridge.parseType(patternStr);
        assertNotNull(type);
        assertTrue(type instanceof CqlField.CqlMap);
        final CqlField.CqlMap map = (CqlField.CqlMap) type;
        assertTrue(map.keyType() instanceof CqlField.NativeType);
        assertTrue(map.valueType() instanceof CqlField.CqlFrozen);
        final CqlField.NativeType key = (CqlField.NativeType) map.keyType();
        assertSame(key, bridge.text());
        final CqlField.CqlFrozen value = (CqlField.CqlFrozen) map.valueType();
        final CqlField.CqlSet inner = (CqlField.CqlSet) value.inner();
        assertSame(inner.type(), bridge.bigint());
    }

    @Test
    public void testNestedFrozenMap()
    {
        final String patternStr = "map<text, frozen<map<bigint, text>>>";
        final CqlField.CqlType type = bridge.parseType(patternStr);
        assertNotNull(type);
        assertTrue(type instanceof CqlField.CqlMap);
        final CqlField.CqlMap map = (CqlField.CqlMap) type;
        assertTrue(map.keyType() instanceof CqlField.NativeType);
        assertTrue(map.valueType() instanceof CqlField.CqlFrozen);
        final CqlField.NativeType key = (CqlField.NativeType) map.keyType();
        assertSame(key, bridge.text());
        final CqlField.CqlFrozen value = (CqlField.CqlFrozen) map.valueType();
        final CqlField.CqlMap inner = (CqlField.CqlMap) value.inner();
        assertSame(inner.keyType(), bridge.bigint());
        assertSame(inner.valueType(), bridge.text());
    }

    private void testFrozen(final String pattern, final Class<? extends CqlField.CqlCollection> collectionType, final CqlField.CqlType innerType)
    {
        testFrozen(pattern, collectionType, innerType, null);
    }

    private void testFrozen(final String pattern, final Class<? extends CqlField.CqlCollection> collectionType,
                            final CqlField.CqlType innerType, @Nullable final CqlField.CqlType innerType2)
    {
        final String patternStr = innerType2 == null ? String.format(pattern, innerType) : String.format(pattern, innerType, innerType2);
        final CqlField.CqlType type = bridge.parseType(patternStr);
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
        final CqlTable schema = bridge.buildSchema(keyspace, "CREATE TABLE " + keyspace + ".udt_test (\n" +
                                                             "    account_id uuid PRIMARY KEY,\n" +
                                                             "    balance bigint,\n" +
                                                             "    info " + udtName + ",\n" +
                                                             "    name text\n" +
                                                             ");", rf, Partitioner.Murmur3Partitioner,
                                                   toSet("CREATE TYPE " + keyspace + "." + udtName + " (\n" +
                                                         "  birthday timestamp,\n" +
                                                         "  nationality text,\n" +
                                                         "  weight float,\n" +
                                                         "  height int\n" +
                                                         ");"));
        assertEquals(1, schema.udts().size());
        final CqlField.CqlUdt udt = schema.udts().stream().findFirst().get();
        assertEquals(udtName, udt.name());
        final List<CqlField> udtFields = udt.fields();
        assertEquals(4, udtFields.size());
        assertEquals(bridge.timestamp(), udtFields.get(0).type());
        assertEquals(bridge.text(), udtFields.get(1).type());
        assertEquals(bridge.aFloat(), udtFields.get(2).type());
        assertEquals(bridge.aInt(), udtFields.get(3).type());

        final List<CqlField> fields = schema.fields();
        assertEquals(bridge.uuid(), fields.get(0).type());
        assertEquals(bridge.bigint(), fields.get(1).type());
        assertEquals(CqlField.CqlType.InternalType.Udt, fields.get(2).type().internalType());
        assertEquals(bridge.text(), fields.get(3).type());

        final CqlField.CqlUdt udtField = (CqlField.CqlUdt) fields.get(2).type();
        assertEquals(bridge.timestamp(), udtField.field(0).type());
        assertEquals(bridge.text(), udtField.field(1).type());
        assertEquals(bridge.aFloat(), udtField.field(2).type());
        assertEquals(bridge.aInt(), udtField.field(3).type());
    }

    @Test
    public void testCollectionUdts()
    {
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final String keyspace = "collection_keyspace";
        final String udtName = "basic_info";
        final CqlTable schema = bridge.buildSchema("test_keyspace", "CREATE TABLE " + keyspace + "." + udtName + " (\n" +
                                                                    "    account_id uuid PRIMARY KEY,\n" +
                                                                    "    balance bigint,\n" +
                                                                    "    info frozen<map<text, " + udtName + ">>,\n" +
                                                                    "    name text\n" +
                                                                    ");", rf, Partitioner.Murmur3Partitioner,
                                                   toSet("CREATE TYPE " + keyspace + "." + udtName + " (\n" +
                                                         "  birthday timestamp,\n" +
                                                         "  nationality text,\n" +
                                                         "  weight float,\n" +
                                                         "  height int\n" +
                                                         ");"));
        final List<CqlField> fields = schema.fields();
        assertEquals(bridge.uuid(), fields.get(0).type());
        assertEquals(bridge.bigint(), fields.get(1).type());
        assertEquals(CqlField.CqlType.InternalType.Frozen, fields.get(2).type().internalType());
        assertEquals(bridge.text(), fields.get(3).type());

        final CqlField.CqlMap mapField = (CqlField.CqlMap) ((CqlField.CqlFrozen) fields.get(2).type()).inner();
        assertEquals(bridge.text(), mapField.keyType());
        final CqlField.CqlFrozen valueType = (CqlField.CqlFrozen) mapField.valueType();
        final CqlField.CqlUdt udtField = (CqlField.CqlUdt) valueType.inner();
        assertEquals(bridge.timestamp(), udtField.field(0).type());
        assertEquals(bridge.text(), udtField.field(1).type());
        assertEquals(bridge.aFloat(), udtField.field(2).type());
        assertEquals(bridge.aInt(), udtField.field(3).type());
    }

    @Test
    public void testParseUdt()
    {
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final CqlTable schema = bridge.buildSchema("test_keyspace", SCHEMA_TXT, rf, Partitioner.Murmur3Partitioner, toSet("CREATE TYPE test_keyspace.tuple_test (a int, b bigint, c blob, d text)"), null);
        assertEquals(1, schema.udts().size());
        final CqlField.CqlUdt udt = schema.udts().stream().findFirst().get();
        assertEquals("tuple_test", udt.name());
        final List<CqlField> fields = udt.fields();
        assertEquals(4, fields.size());
        assertEquals(bridge.aInt(), fields.get(0).type());
        assertEquals(bridge.bigint(), fields.get(1).type());
        assertEquals(bridge.blob(), fields.get(2).type());
        assertEquals(bridge.text(), fields.get(3).type());
    }

    @Test
    public void testParseTuple()
    {
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final CqlTable schema = bridge.buildSchema("tuple_keyspace", "CREATE TABLE tuple_keyspace.tuple_test (\n" +
                                                                     "    account_id uuid PRIMARY KEY,\n" +
                                                                     "    balance bigint,\n" +
                                                                     "    info tuple<bigint, text, float, boolean>," +
                                                                     "    name text\n" +
                                                                     ")", rf, Partitioner.Murmur3Partitioner);
        final List<CqlField> fields = schema.fields();
        assertEquals(4, fields.size());
        assertEquals(bridge.uuid(), fields.get(0).type());
        assertEquals(bridge.bigint(), fields.get(1).type());
        assertEquals(bridge.text(), fields.get(3).type());

        assertEquals(CqlField.CqlType.InternalType.Frozen, fields.get(2).type().internalType());
        final CqlField.CqlTuple tuple = (CqlField.CqlTuple) ((CqlField.CqlFrozen) fields.get(2).type()).inner();
        assertEquals(bridge.bigint(), tuple.type(0));
        assertEquals(bridge.text(), tuple.type(1));
        assertEquals(bridge.aFloat(), tuple.type(2));
        assertEquals(bridge.bool(), tuple.type(3));
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
        final CqlTable schema = bridge.buildSchema(keyspace, table, rf, Partitioner.Murmur3Partitioner, toSet(type1, type2, type3, type4));
        assertEquals("books_ltd_v3", schema.table());
        assertEquals(keyspace, schema.keyspace());
        assertEquals(7, schema.fields().size());
        assertEquals(1, schema.partitionKeys().size());
        assertEquals(1, schema.clusteringKeys().size());

        final List<CqlField> fields = schema.fields();
        assertEquals(7, fields.size());
        assertEquals("consumerId", fields.get(0).name());
        assertEquals(bridge.text(), fields.get(0).type());
        final CqlField clusteringKey = fields.get(1);
        assertEquals("dimensions", clusteringKey.name());
        assertEquals(CqlField.CqlType.InternalType.Frozen, clusteringKey.type().internalType());

        final CqlField.CqlUdt clusteringUDT = (CqlField.CqlUdt) ((CqlField.CqlFrozen) clusteringKey.type()).inner();
        assertEquals("first_last_seen_dimensions_v1", clusteringUDT.name());
        assertEquals(keyspace, clusteringUDT.keyspace());
        assertEquals("osMajorVersion", clusteringUDT.field(0).name());
        assertEquals(bridge.text(), clusteringUDT.field(0).type());
        assertEquals("storeFrontId", clusteringUDT.field(1).name());
        assertEquals(bridge.text(), clusteringUDT.field(1).type());
        assertEquals("platform", clusteringUDT.field(2).name());
        assertEquals(bridge.text(), clusteringUDT.field(2).type());
        assertEquals("time_range", clusteringUDT.field(3).name());
        assertEquals(bridge.text(), clusteringUDT.field(3).type());

        final CqlField.CqlUdt fieldsUDT = (CqlField.CqlUdt) ((CqlField.CqlFrozen) fields.get(2).type()).inner();
        assertEquals("first_last_seen_fields_v1", fieldsUDT.name());
        assertEquals("firstSeen", fieldsUDT.field(0).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(0).type()).inner().name());
        assertEquals("lastSeen", fieldsUDT.field(1).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(1).type()).inner().name());
        assertEquals("firstTransaction", fieldsUDT.field(2).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(2).type()).inner().name());
        assertEquals("lastTransaction", fieldsUDT.field(3).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(3).type()).inner().name());
        assertEquals("firstListening", fieldsUDT.field(4).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(4).type()).inner().name());
        assertEquals("lastListening", fieldsUDT.field(5).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(5).type()).inner().name());
        assertEquals("firstReading", fieldsUDT.field(6).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(6).type()).inner().name());
        assertEquals("lastReading", fieldsUDT.field(7).name());
        assertEquals("field_with_timestamp", ((CqlField.CqlFrozen) fieldsUDT.field(7).type()).inner().name());
        assertEquals("outputEvent", fieldsUDT.field(8).name());
        assertEquals(bridge.text(), fieldsUDT.field(8).type());
        assertEquals("eventHistory", fieldsUDT.field(9).name());
        assertEquals(bridge.bigint(), ((CqlField.CqlMap) ((CqlField.CqlFrozen) fieldsUDT.field(9).type()).inner()).keyType());
        assertEquals(CqlField.CqlType.InternalType.Frozen, ((CqlField.CqlMap) ((CqlField.CqlFrozen) fieldsUDT.field(9).type()).inner()).valueType().internalType());
    }

    @Test
    public void testNestedUDTs()
    {
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final String keyspace = "nested_udt_schema";
        final CqlTable schema = bridge.buildSchema("test_keyspace", "CREATE TABLE " + keyspace + ".udt_test (\n" +
                                                                    "    a uuid PRIMARY KEY,\n" +
                                                                    "    b bigint,\n" +
                                                                    "    c a_udt,\n" +
                                                                    "    d text\n" +
                                                                    ");", rf, Partitioner.Murmur3Partitioner,
                                                   toSet("CREATE TYPE " + keyspace + ".a_udt (col1 bigint, col2 text, col3 frozen<map<uuid, b_udt>>);",
                                                         "CREATE TYPE " + keyspace + ".b_udt (col1 timeuuid, col2 text, col3 frozen<set<c_udt>>);",
                                                         "CREATE TYPE " + keyspace + ".c_udt (col1 float, col2 uuid, col3 int);"));
        assertEquals(3, schema.udts().size());
    }

    private static Set<String> toSet(final String... strs)
    {
        return new HashSet<>(Arrays.asList(strs));
    }
}
