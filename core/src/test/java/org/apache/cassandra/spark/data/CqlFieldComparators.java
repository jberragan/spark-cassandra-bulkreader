package org.apache.cassandra.spark.data;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.spark.sql.types.Decimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;
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
public class CqlFieldComparators extends VersionRunner
{

    public CqlFieldComparators(CassandraBridge.CassandraVersion version)
    {
        super(version);
    }

    private static CqlField createField(final CqlField.CqlType type)
    {
        return new CqlField(false, false, false, "a", type, 0);
    }

    @Test
    public void testStringComparator()
    {
        // ASCII
        assertTrue(createField(bridge.ascii()).compare("a", "b") < 0);
        assertEquals(0, createField(bridge.ascii()).compare("b", "b"));
        assertTrue(createField(bridge.ascii()).compare("c", "b") > 0);
        assertTrue(createField(bridge.ascii()).compare("b", "a") > 0);

        assertTrue(createField(bridge.ascii()).compare("1", "2") < 0);
        assertEquals(0, createField(bridge.ascii()).compare("2", "2"));
        assertTrue(createField(bridge.ascii()).compare("3", "2") > 0);
        assertTrue(createField(bridge.ascii()).compare("2", "1") > 0);

        // TIMEUUID
        assertTrue(createField(bridge.timeuuid()).compare("856f3600-8d57-11e9-9298-798dbb8bb043", "7a146960-8d57-11e9-94f8-1763d9f66f5e") < 0);
        assertTrue(createField(bridge.timeuuid()).compare("964116b0-8d57-11e9-8097-5f40ae53943c", "8ebe0600-8d57-11e9-b507-7769fecef72d") > 0);
        assertEquals(0, createField(bridge.timeuuid()).compare("9dda9590-8d57-11e9-9906-8b25b9c1ff19", "9dda9590-8d57-11e9-9906-8b25b9c1ff19"));

        // UUID
        final UUID u1 = UUID.randomUUID(), u2 = UUID.randomUUID();
        final UUID larger = u1.compareTo(u2) < 0 ? u2 : u1;
        final UUID smaller = larger == u1 ? u2 : u1;
        assertTrue(createField(bridge.uuid()).compare(smaller, larger) < 0);
        assertTrue(createField(bridge.uuid()).compare(larger, smaller) > 0);
        assertEquals(0, createField(bridge.uuid()).compare(smaller, smaller));
        assertEquals(0, createField(bridge.uuid()).compare(larger, larger));

        // TEXT
        assertTrue(createField(bridge.text()).compare("abc", "abd") < 0);
        assertTrue(createField(bridge.text()).compare("abd", "abc") > 0);
        assertEquals(0, createField(bridge.text()).compare("abc", "abc"));
        assertEquals(0, createField(bridge.text()).compare("abd", "abd"));

        // VARCHAR
        assertTrue(createField(bridge.varchar()).compare("abc", "abd") < 0);
        assertTrue(createField(bridge.varchar()).compare("abd", "abc") > 0);
        assertEquals(0, createField(bridge.varchar()).compare("abc", "abc"));
        assertEquals(0, createField(bridge.varchar()).compare("abd", "abd"));
    }

    @Test
    public void testBigDecimalComparator()
    {
        final BigDecimal value = BigDecimal.valueOf(Long.MAX_VALUE).multiply(BigDecimal.valueOf(2));
        final Decimal b1 = Decimal.apply(value);
        final Decimal b2 = Decimal.apply(value.add(BigDecimal.valueOf(1L)));
        assertTrue(createField(bridge.decimal()).compare(b1, b2) < 0);
        assertEquals(0, createField(bridge.decimal()).compare(b1, b1));
        assertEquals(0, createField(bridge.decimal()).compare(b2, b2));
        assertTrue(createField(bridge.decimal()).compare(b2, b1) > 0);
    }

    @Test
    public void testVarIntComparator()
    {
        final BigDecimal value = BigDecimal.valueOf(Long.MAX_VALUE).multiply(BigDecimal.valueOf(2));
        final Decimal b1 = Decimal.apply(value);
        final Decimal b2 = Decimal.apply(value.add(BigDecimal.valueOf(1L)));
        assertTrue(createField(bridge.varint()).compare(b1, b2) < 0);
        assertEquals(0, createField(bridge.varint()).compare(b1, b1));
        assertEquals(0, createField(bridge.varint()).compare(b2, b2));
        assertTrue(createField(bridge.varint()).compare(b2, b1) > 0);
    }

    @Test
    public void testIntegerComparator()
    {
        qt().forAll(integers().between(Integer.MIN_VALUE, Integer.MAX_VALUE - 1))
            .checkAssert(i -> {
                assertTrue(createField(bridge.aInt()).compare(i, i + 1) < 0);
                assertEquals(0, createField(bridge.aInt()).compare(i, i));
                assertTrue(createField(bridge.aInt()).compare(i + 1, i) > 0);
            });
        assertEquals(0, createField(bridge.aInt()).compare(Integer.MAX_VALUE, Integer.MAX_VALUE));
        assertEquals(0, createField(bridge.aInt()).compare(Integer.MIN_VALUE, Integer.MIN_VALUE));
        assertTrue(createField(bridge.aInt()).compare(Integer.MIN_VALUE, Integer.MAX_VALUE) < 0);
        assertTrue(createField(bridge.aInt()).compare(Integer.MAX_VALUE, Integer.MIN_VALUE) > 0);
    }

    @Test
    public void testLongComparator()
    {
        assertTrue(createField(bridge.bigint()).compare(0L, 1L) < 0);
        assertEquals(0, createField(bridge.bigint()).compare(1L, 1L));
        assertTrue(createField(bridge.bigint()).compare(2L, 1L) > 0);
        assertEquals(0, createField(bridge.bigint()).compare(Long.MAX_VALUE, Long.MAX_VALUE));
        assertEquals(0, createField(bridge.bigint()).compare(Long.MIN_VALUE, Long.MIN_VALUE));
        assertTrue(createField(bridge.bigint()).compare(Long.MIN_VALUE, Long.MAX_VALUE) < 0);
        assertTrue(createField(bridge.bigint()).compare(Long.MAX_VALUE, Long.MIN_VALUE) > 0);
    }

    @Test
    public void testTimeComparator()
    {
        assertTrue(createField(bridge.time()).compare(0L, 1L) < 0);
        assertEquals(0, createField(bridge.time()).compare(1L, 1L));
        assertTrue(createField(bridge.time()).compare(2L, 1L) > 0);
        assertEquals(0, createField(bridge.time()).compare(Long.MAX_VALUE, Long.MAX_VALUE));
        assertEquals(0, createField(bridge.time()).compare(Long.MIN_VALUE, Long.MIN_VALUE));
        assertTrue(createField(bridge.time()).compare(Long.MIN_VALUE, Long.MAX_VALUE) < 0);
        assertTrue(createField(bridge.time()).compare(Long.MAX_VALUE, Long.MIN_VALUE) > 0);
    }

    @Test
    public void testBooleanComparator()
    {
        assertTrue(createField(bridge.bool()).compare(false, true) < 0);
        assertEquals(0, createField(bridge.bool()).compare(false, false));
        assertEquals(0, createField(bridge.bool()).compare(true, true));
        assertTrue(createField(bridge.bool()).compare(true, false) > 0);
    }

    @Test
    public void testFloatComparator()
    {
        assertTrue(createField(bridge.aFloat()).compare(1f, 2f) < 0);
        assertEquals(0, createField(bridge.aFloat()).compare(2f, 2f));
        assertTrue(createField(bridge.aFloat()).compare(2f, 1f) > 0);
    }

    @Test
    public void testDoubleComparator()
    {
        assertTrue(createField(bridge.aDouble()).compare(1.0, 2.0) < 0);
        assertEquals(0, createField(bridge.aDouble()).compare(2.0, 2.0));
        assertTrue(createField(bridge.aDouble()).compare(2.0, 1.0) > 0);
    }

    @Test
    public void testTimestampComparator()
    {
        final long t1 = 1L;
        final long t2 = 2L;
        assertTrue(createField(bridge.timestamp()).compare(t1, t2) < 0);
        assertEquals(0, createField(bridge.timestamp()).compare(t1, t1));
        assertEquals(0, createField(bridge.timestamp()).compare(t2, t2));
        assertTrue(createField(bridge.timestamp()).compare(t2, t1) > 0);
    }

    @Test
    public void testDateComparator()
    {
        final int t1 = 1;
        final int t2 = 2;
        assertTrue(createField(bridge.date()).compare(t1, t2) < 0);
        assertEquals(0, createField(bridge.date()).compare(t1, t1));
        assertEquals(0, createField(bridge.date()).compare(t2, t2));
        assertTrue(createField(bridge.date()).compare(t2, t1) > 0);
    }

    @Test
    public void testVoidComparator()
    {
        assertEquals(0, createField(bridge.empty()).compare(null, null));
    }

    @Test
    public void testShortComparator()
    {
        assertTrue(createField(bridge.smallint()).compare((short) 1, (short) 2) < 0);
        assertEquals(0, createField(bridge.smallint()).compare((short) 2, (short) 2));
        assertTrue(createField(bridge.smallint()).compare((short) 2, (short) 1) > 0);
    }

    @Test
    public void testByteArrayComparator()
    {
        final byte[] b1 = new byte[]{ 0, 0, 0, 101 };
        final byte[] b2 = new byte[]{ 0, 0, 0, 102 };
        final byte[] b3 = new byte[]{ 0, 0, 1, 0 };
        final byte[] b4 = new byte[]{ 1, 0, 0, 0 };
        assertTrue(createField(bridge.blob()).compare(b1, b2) < 0);
        assertEquals(0, createField(bridge.blob()).compare(b1, b1));
        assertEquals(0, createField(bridge.blob()).compare(b2, b2));
        assertTrue(createField(bridge.blob()).compare(b2, b1) > 0);
        assertTrue(createField(bridge.blob()).compare(b3, b1) > 0);
        assertTrue(createField(bridge.blob()).compare(b3, b2) > 0);
        assertTrue(createField(bridge.blob()).compare(b4, b3) > 0);
    }

    @Test
    public void testInetComparator() throws UnknownHostException
    {
        final byte[] i1 = InetAddress.getByAddress(CqlFieldComparators.toByteArray(2130706433)).getAddress(); // 127.0.0.1
        final byte[] i2 = InetAddress.getByAddress(CqlFieldComparators.toByteArray(2130706434)).getAddress(); // 127.0.0.2
        assertTrue(createField(bridge.inet()).compare(i1, i2) < 0);
        assertEquals(0, createField(bridge.inet()).compare(i1, i1));
        assertEquals(0, createField(bridge.inet()).compare(i2, i2));
        assertTrue(createField(bridge.inet()).compare(i2, i1) > 0);
    }

    private static byte[] toByteArray(final int value)
    {
        return new byte[]{ (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value };
    }

    @Test
    public void testByteComparator()
    {
        final byte b1 = 101;
        final byte b2 = 102;
        assertTrue(createField(bridge.tinyint()).compare(b1, b2) < 0);
        assertEquals(0, createField(bridge.tinyint()).compare(b1, b1));
        assertEquals(0, createField(bridge.tinyint()).compare(b2, b2));
        assertTrue(createField(bridge.tinyint()).compare(b2, b1) > 0);
    }
}
