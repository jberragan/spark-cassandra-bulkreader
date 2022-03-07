package org.apache.cassandra.spark.reader;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
public class RidTests
{

    @Test
    public void testInit()
    {
        final Rid rid = new Rid();
        assertNull(rid.getPartitionKey());
        assertNull(rid.getColumnName());
        assertNull(rid.getValue());
        assertFalse(rid.isNewPartition);
    }

    @Test
    public void testSetPartitionKey()
    {
        final Rid rid = new Rid();
        rid.setPartitionKeyCopy(ByteBuffer.wrap("101".getBytes()), BigInteger.ZERO);
        assertNotNull(rid.getPartitionKey());
        assertNull(rid.getColumnName());
        assertNull(rid.getValue());

        assertTrue(rid.isNewPartition);
        assertTrue(rid.isNewPartition());
        assertFalse(rid.isNewPartition);
        assertEquals("101", toString(rid.getPartitionKey()));

        rid.setPartitionKeyCopy(ByteBuffer.wrap("102".getBytes()), BigInteger.ZERO);
        assertTrue(rid.isNewPartition);
        assertTrue(rid.isNewPartition());
        assertFalse(rid.isNewPartition);
        assertEquals("102", toString(rid.getPartitionKey()));
    }

    @Test
    public void testSetColumnKey()
    {
        final Rid rid = new Rid();
        assertNull(rid.getPartitionKey());
        assertNull(rid.getColumnName());
        assertNull(rid.getValue());
        rid.setColumnNameCopy(ByteBuffer.wrap("101".getBytes()));

        assertNull(rid.getPartitionKey());
        assertNotNull(rid.getColumnName());
        assertNull(rid.getValue());
        assertEquals("101", toString(rid.getColumnName()));
    }

    @Test
    public void testSetValue()
    {
        final Rid rid = new Rid();
        assertNull(rid.getPartitionKey());
        assertNull(rid.getColumnName());
        assertNull(rid.getValue());
        rid.setValueCopy(ByteBuffer.wrap("101".getBytes()));

        assertNull(rid.getPartitionKey());
        assertNull(rid.getColumnName());
        assertNotNull(rid.getValue());
        assertEquals("101", toString(rid.getValue()));
    }

    @Test
    public void testSetAll()
    {
        final Rid rid = new Rid();
        assertNull(rid.getPartitionKey());
        assertNull(rid.getColumnName());
        assertNull(rid.getValue());
        rid.setPartitionKeyCopy(ByteBuffer.wrap("101".getBytes()), BigInteger.ZERO);
        rid.setColumnNameCopy(ByteBuffer.wrap("102".getBytes()));
        rid.setValueCopy(ByteBuffer.wrap("103".getBytes()));

        assertTrue(rid.isNewPartition);
        assertTrue(rid.isNewPartition());
        assertFalse(rid.isNewPartition);

        assertNotNull(rid.getPartitionKey());
        assertNotNull(rid.getColumnName());
        assertNotNull(rid.getValue());
        assertEquals("101", toString(rid.getPartitionKey()));
        assertEquals("102", toString(rid.getColumnName()));
        assertEquals("103", toString(rid.getValue()));

        rid.setPartitionKeyCopy(ByteBuffer.wrap("104".getBytes()), BigInteger.ZERO);
        assertTrue(rid.isNewPartition);
        assertTrue(rid.isNewPartition());
        assertFalse(rid.isNewPartition);
        rid.setColumnNameCopy(ByteBuffer.wrap("105".getBytes()));
        rid.setValueCopy(ByteBuffer.wrap("106".getBytes()));
        assertEquals("104", toString(rid.getPartitionKey()));
        assertEquals("105", toString(rid.getColumnName()));
        assertEquals("106", toString(rid.getValue()));
    }

    private static String toString(final ByteBuffer buf)
    {
        final byte[] ar = new byte[buf.remaining()];
        buf.get(ar);
        return new String(ar, StandardCharsets.UTF_8);
    }
}
