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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test the data classes defined in AbstractCdcEvent
 */
public class ValueWithMetadataTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testEmptyValueWithMetadataToRowThrows()
    {
        expectedException.expect(IllegalStateException.class);
        ValueWithMetadata.EMPTY.toRow();
    }

    @Test
    public void testValueWithMetadataToRow()
    {
        InternalRow row = ValueWithMetadata.of("name", "type", null)
                                           .toRow();
        assertNotNull(row);
        assertEquals("name", row.getString(0));
        assertEquals("type", row.getString(1));
        assertTrue(row.isNullAt(2));
    }

    @Test
    public void testValueWithMetadataFromRow()
    {
        ValueWithMetadata val = ValueWithMetadata.EMPTY.fromRow(
            new GenericRow(new Object[] { "name", "type", null }));
        assertEquals("name", val.columnName);
        assertEquals("type", val.columnType);
        assertNull(val.getValue());

        val = ValueWithMetadata.EMPTY.fromRow(
            new GenericRow(new Object[] { "name", "type", "bytes".getBytes() }));
        assertArrayEquals("bytes".getBytes(), val.getBytes());

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Input row has invalid data type for 'value' column. ");
        ValueWithMetadata.EMPTY.fromRow(
            new GenericRow(new Object[] { "name", "type", "invalid_value_type" }));
    }

}
