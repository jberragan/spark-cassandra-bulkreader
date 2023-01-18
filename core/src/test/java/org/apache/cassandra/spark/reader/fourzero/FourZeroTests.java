package org.apache.cassandra.spark.reader.fourzero;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.fourzero.FourZeroCqlType;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.LongType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UTF8Type;

import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
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

public class FourZeroTests
{
    static final CassandraBridge BRIDGE = CassandraBridge.get(CassandraBridge.CassandraVersion.FOURZERO);

    @Test
    public void testSinglePartitionKey()
    {
        final List<CqlField> singlePartitionKey = Collections.singletonList(new CqlField(true, false, false, "a", BRIDGE.aInt(), 0));

        final CqlTable schema = mock(CqlTable.class);
        when(schema.partitionKeys()).thenReturn(singlePartitionKey);

        runTest((partitioner, dir, bridge) -> {
                    final ByteBuffer key = Int32Type.instance.fromString("1");
                    final Pair<ByteBuffer, BigInteger> actualKey = bridge.getPartitionKey(schema, partitioner, Collections.singletonList("1"));
                    assertEquals(0, key.compareTo(actualKey.getLeft()));
                    assertEquals(0, bridge.hash(partitioner, key).compareTo(actualKey.getRight()));
                    assertTrue(Int32Type.instance.fromString("2").compareTo(actualKey.getLeft()) != 0);
                }
        );
    }

    @Test
    public void testMultiplePartitionKey()
    {

        final List<CqlField> multiplePartitionKey = Arrays.asList(new CqlField(true, false, false, "a", BRIDGE.aInt(), 0),
                                                                  new CqlField(true, false, false, "b", BRIDGE.bigint(), 1),
                                                                  new CqlField(true, false, false, "c", BRIDGE.text(), 2));

        final CqlTable schema = mock(CqlTable.class);
        when(schema.partitionKeys()).thenReturn(multiplePartitionKey);

        runTest((partitioner, dir, bridge) -> {
                    final ByteBuffer key = CompositeType.getInstance(Int32Type.instance, LongType.instance, UTF8Type.instance).fromString("3:" + BigInteger.ONE + ":xyz");
                    final Pair<ByteBuffer, BigInteger> actualKey = bridge.getPartitionKey(schema, partitioner, Arrays.asList("3", BigInteger.ONE.toString(), "xyz"));
                    assertEquals(0, key.compareTo(actualKey.getLeft()));
                    assertEquals(0, bridge.hash(partitioner, key).compareTo(actualKey.getRight()));
                }
        );
    }

    @Test
    public void testBuildPartitionKey()
    {
        qt().forAll(TestUtils.cql3Type(BRIDGE))
            .checkAssert((partitionKeyType) -> {
                final CqlTable schema = TestSchema.builder().withPartitionKey("a", partitionKeyType)
                                                  .withClusteringKey("b", BRIDGE.aInt())
                                                  .withColumn("c", BRIDGE.aInt()).build().buildSchema();
                final Object value = partitionKeyType.randomValue(100);
                final String str = ((FourZeroCqlType) partitionKeyType).serializer().toString(value);
                final ByteBuffer buf = FourZero.buildPartitionKey(schema, Collections.singletonList(str));
                assertTrue(TestUtils.equals(value, partitionKeyType.toTestRowType(partitionKeyType.deserialize(buf))));
            });
    }

    @Test
    public void testBuildCompositePartitionKey()
    {
        qt().forAll(TestUtils.cql3Type(BRIDGE))
            .checkAssert(
            (partitionKeyType) -> {
                final CqlTable schema = TestSchema.builder()
                                                  .withPartitionKey("a", BRIDGE.aInt())
                                                  .withPartitionKey("b", partitionKeyType)
                                                  .withPartitionKey("c", BRIDGE.text())
                                                  .withClusteringKey("d", BRIDGE.aInt())
                                                  .withColumn("e", BRIDGE.aInt()).build().buildSchema();
                final List<AbstractType<?>> partitionKeyColumnTypes = FourZero.partitionKeyColumnTypes(schema);
                final CompositeType compositeType = CompositeType.getInstance(partitionKeyColumnTypes);

                final int colA = (int) BRIDGE.aInt().randomValue(1024);
                final Object colB = partitionKeyType.randomValue(1024);
                final String colBStr = ((FourZeroCqlType) partitionKeyType).serializer().toString(colB);
                final String colC = (String) BRIDGE.text().randomValue(1024);

                final ByteBuffer buf = FourZero.buildPartitionKey(schema, Arrays.asList(Integer.toString(colA), colBStr, colC));
                final ByteBuffer[] bufs = compositeType.split(buf);
                assertEquals(3, bufs.length);

                assertEquals(colA, bufs[0].getInt());
                assertEquals(colB, partitionKeyType.toTestRowType(partitionKeyType.deserialize(bufs[1])));
                assertEquals(colC, BRIDGE.text().toTestRowType(BRIDGE.text().deserialize(bufs[2])));
            }
            );
    }

    @Test
    public void testUpdateCdcSchema()
    {
        FourZero.updateCdcSchema(Collections.emptySet(), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);

        final TestSchema testSchema1 = TestSchema.builder()
                                      .withPartitionKey("a", BRIDGE.bigint())
                                      .withClusteringKey("b", BRIDGE.text())
                                      .withColumn("c", BRIDGE.timeuuid())
                                      .build();
        final CqlTable cqlTable1 = testSchema1.buildSchema();

        final TestSchema testSchema2 = TestSchema.builder()
                                                 .withPartitionKey("pk", BRIDGE.uuid())
                                                 .withClusteringKey("ck", BRIDGE.aInt())
                                                 .withColumn("val", BRIDGE.blob())
                                                 .build();
        final CqlTable cqlTable2 = testSchema2.buildSchema();

        assertFalse(SchemaUtils.isCdcEnabled(cqlTable1));
        assertFalse(SchemaUtils.isCdcEnabled(cqlTable2));

        FourZero.updateCdcSchema(new HashSet<>(Arrays.asList(cqlTable1, cqlTable2)), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);
        assertTrue(SchemaUtils.isCdcEnabled(cqlTable1));
        assertTrue(SchemaUtils.isCdcEnabled(cqlTable2));

        SchemaUtils.disableCdc(cqlTable2);
        assertTrue(SchemaUtils.isCdcEnabled(cqlTable1));
        assertFalse(SchemaUtils.isCdcEnabled(cqlTable2));

        SchemaUtils.disableCdc(cqlTable1);
        assertFalse(SchemaUtils.isCdcEnabled(cqlTable1));
        assertFalse(SchemaUtils.isCdcEnabled(cqlTable2));

        SchemaUtils.enableCdc(cqlTable1);
        assertTrue(SchemaUtils.isCdcEnabled(cqlTable1));
        assertFalse(SchemaUtils.isCdcEnabled(cqlTable2));

        SchemaUtils.enableCdc(cqlTable2);
        assertTrue(SchemaUtils.isCdcEnabled(cqlTable1));
        assertTrue(SchemaUtils.isCdcEnabled(cqlTable2));

        FourZero.updateCdcSchema(new HashSet<>(Collections.singletonList(cqlTable1)), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);
        assertTrue(SchemaUtils.isCdcEnabled(cqlTable1));
        assertFalse(SchemaUtils.isCdcEnabled(cqlTable2));

        FourZero.updateCdcSchema(new HashSet<>(), Partitioner.Murmur3Partitioner, (keyspace, table) -> null);
        assertFalse(SchemaUtils.isCdcEnabled(cqlTable1));
        assertFalse(SchemaUtils.isCdcEnabled(cqlTable2));
    }
}
