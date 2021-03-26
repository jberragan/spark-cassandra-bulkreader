package org.apache.cassandra.spark.reader.fourzero;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.LongType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UTF8Type;

import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

        final CqlSchema schema = mock(CqlSchema.class);
        when(schema.partitionKeys()).thenReturn(singlePartitionKey);

        runTest((partitioner, dir, bridge) -> {
                    final ByteBuffer key = Int32Type.instance.fromString("1");
                    final Pair<ByteBuffer, BigInteger> actualKey = bridge.getPartitionKey(schema, partitioner, "1");
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

        final CqlSchema schema = mock(CqlSchema.class);
        when(schema.partitionKeys()).thenReturn(multiplePartitionKey);

        runTest((partitioner, dir, bridge) -> {
                    final ByteBuffer key = CompositeType.getInstance(Int32Type.instance, LongType.instance, UTF8Type.instance).fromString("3:" + BigInteger.ONE.toString() + ":xyz");
                    final Pair<ByteBuffer, BigInteger> actualKey = bridge.getPartitionKey(schema, partitioner, "3:" + BigInteger.ONE.toString() + ":xyz");
                    assertEquals(0, key.compareTo(actualKey.getLeft()));
                    assertEquals(0, bridge.hash(partitioner, key).compareTo(actualKey.getRight()));
                }
        );
    }
}
