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

package org.apache.cassandra.spark.cdc;

import java.nio.ByteBuffer;
import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.collection.mutable.WrappedArray;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SparkRangeTombstoneTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testRangeTombstoneToRowThrows()
    {
        expectedException.expect(IllegalStateException.class);
        SparkRangeTombstone.EMPTY.toRow();
    }

    @Test
    public void testRangeTombstoneToRow()
    {
        SparkRangeTombstone rt = SparkRangeTombstone.of(Collections.singletonList(SparkValueWithMetadata.of("c1", "text", ByteBuffer.wrap("a".getBytes()))), true,
                                                        Collections.singletonList(SparkValueWithMetadata.of("c1", "text", ByteBuffer.wrap("b".getBytes()))), false);
        InternalRow row = rt.toRow();
        assertTrue(row.getBoolean(SparkRangeTombstone.SCHEMA.fieldIndex(RangeTombstone.RANGE_START_INCL)));
        assertFalse(row.isNullAt(SparkRangeTombstone.SCHEMA.fieldIndex(RangeTombstone.RANGE_START)));
        assertFalse(row.getBoolean(SparkRangeTombstone.SCHEMA.fieldIndex(RangeTombstone.RANGE_END_INCL)));
        assertFalse(row.isNullAt(SparkRangeTombstone.SCHEMA.fieldIndex(RangeTombstone.RANGE_END)));
    }

    @Test
    public void testRangeTombstoneFromRow()
    {
        Row row = new GenericRowWithSchema(new Object[]{
        WrappedArray.make(new Object[]{ new GenericRow(new Object[]{ "c1", "text", "a".getBytes() }) }),
        true,
        WrappedArray.make(new Object[]{ new GenericRow(new Object[]{ "c1", "text", "b".getBytes() }) }),
        false
        }, SparkRangeTombstone.SCHEMA);
        SparkRangeTombstone rt = SparkRangeTombstone.EMPTY.fromRow(row);
        assertTrue(rt.startInclusive);
        assertFalse(rt.endInclusive);
        assertEquals(1, rt.getStartBound().size());
        assertEquals(1, rt.getEndBound().size());
        assertEquals("c1", rt.getStartBound().get(0).columnName);
        assertEquals("text", rt.getStartBound().get(0).columnType);
        assertArrayEquals("a".getBytes(), rt.getStartBound().get(0).getBytes());
        assertEquals("c1", rt.getEndBound().get(0).columnName);
        assertEquals("text", rt.getEndBound().get(0).columnType);
        assertArrayEquals("b".getBytes(), rt.getEndBound().get(0).getBytes());
    }
}
