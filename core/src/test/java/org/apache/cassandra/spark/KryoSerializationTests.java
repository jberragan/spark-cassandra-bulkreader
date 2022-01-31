package org.apache.cassandra.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.spark.data.VersionRunner;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.booleans;
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

public class KryoSerializationTests extends VersionRunner
{
    private static final Kryo KRYO = new Kryo();

    static
    {
        new KryoRegister().registerClasses(KRYO);
    }

    public KryoSerializationTests(CassandraBridge.CassandraVersion version)
    {
        super(version);
    }

    @Test
    public void testCqlField()
    {
        qt().withExamples(25)
            .forAll(booleans().all(), booleans().all(), TestUtils.cql3Type(bridge), integers().all())
            .checkAssert((isPartitionKey, isClusteringKey, cqlType, pos) -> {
                final CqlField field = new CqlField(isPartitionKey, (isClusteringKey && !isPartitionKey), false, RandomStringUtils.randomAlphanumeric(5, 20), cqlType, pos);
                final Output out = KryoSerializationTests.serialize(field);
                final CqlField deserialized = KryoSerializationTests.deserialize(out, CqlField.class);
                assertEquals(field, deserialized);
                assertEquals(field.name(), deserialized.name());
                assertEquals(field.type(), deserialized.type());
                assertEquals(field.pos(), deserialized.pos());
                assertEquals(field.isPartitionKey(), deserialized.isPartitionKey());
                assertEquals(field.isClusteringColumn(), deserialized.isClusteringColumn());
            });
    }

    @Test
    public void testCqlFieldSet()
    {
        qt().withExamples(25)
            .forAll(booleans().all(), booleans().all(), TestUtils.cql3Type(bridge), integers().all())
            .checkAssert((isPartitionKey, isClusteringKey, cqlType, pos) -> {
                final CqlField.CqlSet setType = bridge.set(cqlType);
                final CqlField field = new CqlField(isPartitionKey, (isClusteringKey && !isPartitionKey), false, RandomStringUtils.randomAlphanumeric(5, 20), setType, pos);
                final Output out = KryoSerializationTests.serialize(field);
                final CqlField deserialized = KryoSerializationTests.deserialize(out, CqlField.class);
                assertEquals(field, deserialized);
                assertEquals(field.name(), deserialized.name());
                assertEquals(field.type(), deserialized.type());
                assertEquals(field.pos(), deserialized.pos());
                assertEquals(field.isPartitionKey(), deserialized.isPartitionKey());
                assertEquals(field.isClusteringColumn(), deserialized.isClusteringColumn());
            });
    }

    @Test
    public void testCqlFieldList()
    {
        qt().withExamples(25)
            .forAll(booleans().all(), booleans().all(), TestUtils.cql3Type(bridge), integers().all())
            .checkAssert((isPartitionKey, isClusteringKey, cqlType, pos) -> {
                final CqlField.CqlList listType = bridge.list(cqlType);
                final CqlField field = new CqlField(isPartitionKey, (isClusteringKey && !isPartitionKey), false, RandomStringUtils.randomAlphanumeric(5, 20), listType, pos);
                final Output out = KryoSerializationTests.serialize(field);
                final CqlField deserialized = KryoSerializationTests.deserialize(out, CqlField.class);
                assertEquals(field, deserialized);
                assertEquals(field.name(), deserialized.name());
                assertEquals(field.type(), deserialized.type());
                assertEquals(field.pos(), deserialized.pos());
                assertEquals(field.isPartitionKey(), deserialized.isPartitionKey());
                assertEquals(field.isClusteringColumn(), deserialized.isClusteringColumn());
            });
    }

    @Test
    public void testCqlFieldMap()
    {
        qt().withExamples(25)
            .forAll(booleans().all(), booleans().all(), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((isPartitionKey, isClusteringKey, cqlType1, cqlType2) -> {
                final CqlField.CqlMap mapType = bridge.map(cqlType1, cqlType2);
                final CqlField field = new CqlField(isPartitionKey, (isClusteringKey && !isPartitionKey), false, RandomStringUtils.randomAlphanumeric(5, 20), mapType, 2);
                final Output out = KryoSerializationTests.serialize(field);
                final CqlField deserialized = KryoSerializationTests.deserialize(out, CqlField.class);
                assertEquals(field, deserialized);
                assertEquals(field.name(), deserialized.name());
                assertEquals(field.type(), deserialized.type());
                assertEquals(field.pos(), deserialized.pos());
                assertEquals(field.isPartitionKey(), deserialized.isPartitionKey());
                assertEquals(field.isClusteringColumn(), deserialized.isClusteringColumn());
            });
    }

    @Test
    public void testCqlUdt()
    {
        qt().withExamples(25)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2) -> {
                final CqlField.CqlUdt udt = bridge.udt("keyspace", "testudt").withField("a", type1).withField("b", type2).build();
                final CqlField field = new CqlField(false, false, false, RandomStringUtils.randomAlphanumeric(5, 20), udt, 2);
                final Output out = KryoSerializationTests.serialize(field);
                final CqlField deserialized = KryoSerializationTests.deserialize(out, CqlField.class);
                assertEquals(field, deserialized);
                assertEquals(field.name(), deserialized.name());
                assertEquals(udt, deserialized.type());
                assertEquals(field.pos(), deserialized.pos());
                assertEquals(field.isPartitionKey(), deserialized.isPartitionKey());
                assertEquals(field.isClusteringColumn(), deserialized.isClusteringColumn());
            });
    }

    @Test
    public void testCqlTuple()
    {
        qt().withExamples(25)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2) -> {
                final CqlField.CqlTuple tuple = bridge.tuple(type1, bridge.blob(), type2, bridge.set(bridge.text()), bridge.bigint(),
                                                             bridge.map(type2, bridge.timeuuid()));
                final CqlField field = new CqlField(false, false, false, RandomStringUtils.randomAlphanumeric(5, 20), tuple, 2);
                final Output out = KryoSerializationTests.serialize(field);
                final CqlField deserialized = KryoSerializationTests.deserialize(out, CqlField.class);
                assertEquals(field, deserialized);
                assertEquals(field.name(), deserialized.name());
                assertEquals(tuple, deserialized.type());
                assertEquals(field.pos(), deserialized.pos());
                assertEquals(field.isPartitionKey(), deserialized.isPartitionKey());
                assertEquals(field.isClusteringColumn(), deserialized.isClusteringColumn());
            });
    }

    @Test
    public void testCqlSchema()
    {
        final List<CqlField> fields = new ArrayList<>(5);
        fields.add(new CqlField(true, false, false, "a", bridge.bigint(), 0));
        fields.add(new CqlField(true, false, false, "b", bridge.bigint(), 1));
        fields.add(new CqlField(false, true, false, "c", bridge.bigint(), 2));
        fields.add(new CqlField(false, false, false, "d", bridge.timestamp(), 3));
        fields.add(new CqlField(false, false, false, "e", bridge.text(), 4));
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        final CqlSchema schema = new CqlSchema("test_keyspace", "test_table", "create table test_keyspace.test_table (a bigint, b bigint, c bigint, d bigint, e bigint, primary key((a, b), c));", rf, fields);

        final Output out = KryoSerializationTests.serialize(schema);
        final CqlSchema deserialized = KryoSerializationTests.deserialize(out, CqlSchema.class);
        assertNotNull(deserialized);
        assertEquals(schema, deserialized);
    }

    @Test
    public void testCassandraInstance()
    {
        final CassandraInstance instance = new CassandraInstance("-9223372036854775807", "local1-i1", "DC1");
        final Output out = KryoSerializationTests.serialize(instance);
        final CassandraInstance deserialized = KryoSerializationTests.deserialize(out, CassandraInstance.class);
        assertNotNull(deserialized);
        assertEquals(instance, deserialized);
    }

    @Test
    public void testCassandraRing()
    {
        qt().forAll(TestUtils.partitioners())
            .checkAssert(partitioner -> {
                final CassandraRing ring = TestUtils.createRing(partitioner, ImmutableMap.of("DC1", 3, "DC2", 3));
                final Output out = KryoSerializationTests.serialize(ring);
                final CassandraRing deserialized = KryoSerializationTests.deserialize(out, CassandraRing.class);
                assertNotNull(deserialized);
                assertEquals(ring, deserialized);
                assertEquals(partitioner, deserialized.partitioner());
            });
    }

    @Test
    public void testLocalDataLayerThreeZero()
    {
        final String path1 = UUID.randomUUID().toString(), path2 = UUID.randomUUID().toString(), path3 = UUID.randomUUID().toString();
        final LocalDataLayer localDataLayer = new LocalDataLayer(CassandraBridge.CassandraVersion.THREEZERO, "test_keyspace", "create table test_keyspace.test_table (a int, b int, c int, primary key(a, b));", path1, path2, path3);
        final Output out = KryoSerializationTests.serialize(localDataLayer);
        final LocalDataLayer deserialized = KryoSerializationTests.deserialize(out, LocalDataLayer.class);
        assertNotNull(deserialized);
        assertEquals(localDataLayer.version(), deserialized.version());
        assertEquals(localDataLayer, deserialized);
    }

    @Test
    public void testLocalDataLayerFourZero()
    {
        final String path1 = UUID.randomUUID().toString(), path2 = UUID.randomUUID().toString(), path3 = UUID.randomUUID().toString();
        final LocalDataLayer localDataLayer = new LocalDataLayer(CassandraBridge.CassandraVersion.FOURZERO, "test_keyspace", "create table test_keyspace.test_table (a int, b int, c int, primary key(a, b));", path1, path2, path3);
        final Output out = KryoSerializationTests.serialize(localDataLayer);
        final LocalDataLayer deserialized = KryoSerializationTests.deserialize(out, LocalDataLayer.class);
        assertNotNull(deserialized);
        assertEquals(localDataLayer.version(), deserialized.version());
        assertEquals(localDataLayer, deserialized);
    }

    @Test
    public void testTokenPartitioner()
    {
        qt().forAll(TestUtils.partitioners(), arbitrary().pick(Arrays.asList(3, 16, 128)), arbitrary().pick(Arrays.asList(1, 4, 16)), arbitrary().pick(Arrays.asList(4, 16, 64)))
            .checkAssert((partitioner, numInstances, defaultParallelism, numCores) -> {
                final CassandraRing ring = TestUtils.createRing(partitioner, numInstances);
                final TokenPartitioner tokenPartitioner = new TokenPartitioner(ring, defaultParallelism, numCores);
                final Output out = KryoSerializationTests.serialize(tokenPartitioner);
                final TokenPartitioner deserialized = KryoSerializationTests.deserialize(out, TokenPartitioner.class);
                assertNotNull(deserialized);
                assertEquals(tokenPartitioner.numPartitions(), deserialized.numPartitions());
                assertEquals(tokenPartitioner.subRanges().size(), deserialized.subRanges().size());
                for (int i = 0; i < tokenPartitioner.subRanges().size(); i++)
                {
                    assertEquals(tokenPartitioner.subRanges().get(i), deserialized.subRanges().get(i));
                }
                assertEquals(tokenPartitioner.ring(), deserialized.ring());
            });
    }

    @Test
    public void testCqlUdtField()
    {
        final CqlField.CqlUdt udt = bridge
                                    .udt("udt_keyspace", "udt_table")
                                    .withField("c", bridge.text())
                                    .withField("b", bridge.timestamp())
                                    .withField("a", bridge.bigint())
                                    .build();
        final Output out = new Output(1024, -1);
        udt.write(out);
        out.close();
        final Input in = new Input(out.getBuffer(), 0, out.position());
        final CqlField.CqlUdt deserialized = (CqlField.CqlUdt) CqlField.CqlType.read(in);
        assertEquals(udt, deserialized);
        for (int i = 0; i < deserialized.fields().size(); i++)
        {
            assertEquals(udt.field(i), deserialized.field(i));
        }
    }

    private static Output serialize(final Object obj)
    {
        final Output out = new Output(1024, -1);
        KRYO.writeObject(out, obj);
        out.close();
        return out;
    }

    private static <T> T deserialize(final Output out, final Class<T> type)
    {
        final Input in = new Input(out.getBuffer(), 0, out.position());
        final T deserialized = KRYO.readObject(in, type);
        in.close();
        return deserialized;
    }
}
