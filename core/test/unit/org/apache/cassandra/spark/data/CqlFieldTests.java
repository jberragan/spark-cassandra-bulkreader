package org.apache.cassandra.spark.data;

import java.util.ArrayList;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
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
public class CqlFieldTests
{
    @Test
    public void testEquality()
    {
        final CqlField f1 = new CqlField(true, false, false, "a", CqlField.NativeCql3Type.BIGINT, 0);
        final CqlField f2 = new CqlField(true, false, false, "a", CqlField.NativeCql3Type.BIGINT, 0);
        assertNotSame(f1, f2);
        assertEquals(f1, f2);
        assertEquals(f1.hashCode(), f2.hashCode());
        assertNotEquals(null, f1);
        assertNotEquals(null, f2);
        assertNotEquals(new ArrayList<>(), f1);
        assertEquals(f1, f1);
    }

    @Test
    public void testNotEqualsName()
    {
        final CqlField f1 = new CqlField(true, false, false, "a", CqlField.NativeCql3Type.BIGINT, 0);
        final CqlField f2 = new CqlField(true, false, false, "b", CqlField.NativeCql3Type.BIGINT, 0);
        assertNotSame(f1, f2);
        assertNotEquals(f1, f2);
        assertNotEquals(f1.hashCode(), f2.hashCode());
    }

    @Test
    public void testNotEqualsType()
    {
        final CqlField f1 = new CqlField(true, false, false, "a", CqlField.NativeCql3Type.BIGINT, 0);
        final CqlField f2 = new CqlField(true, false, false, "a", CqlField.NativeCql3Type.TIMESTAMP, 0);
        assertNotSame(f1, f2);
        assertNotEquals(f1, f2);
        assertNotEquals(f1.hashCode(), f2.hashCode());
    }

    @Test
    public void testNotEqualsKey()
    {
        final CqlField f1 = new CqlField(true, false, false, "a", CqlField.NativeCql3Type.BIGINT, 0);
        final CqlField f2 = new CqlField(false, true, false, "a", CqlField.NativeCql3Type.BIGINT, 0);
        assertNotSame(f1, f2);
        assertNotEquals(f1, f2);
        assertNotEquals(f1.hashCode(), f2.hashCode());
    }

    @Test
    public void testNotEqualsPos()
    {
        final CqlField f1 = new CqlField(true, false, false, "a", CqlField.NativeCql3Type.BIGINT, 0);
        final CqlField f2 = new CqlField(true, false, false, "a", CqlField.NativeCql3Type.BIGINT, 1);
        assertNotSame(f1, f2);
        assertNotEquals(f1, f2);
        assertNotEquals(f1.hashCode(), f2.hashCode());
    }

    @Test
    public void testCqlTypeParser()
    {
        testCqlTypeParser("set<text>", CqlField.NativeCql3Type.TEXT);
        testCqlTypeParser("set<float>", CqlField.NativeCql3Type.FLOAT);
        testCqlTypeParser("set<time>", CqlField.NativeCql3Type.TIME);
        testCqlTypeParser("SET<BLOB>", CqlField.NativeCql3Type.BLOB);
        testCqlTypeParser("list<ascii>", CqlField.NativeCql3Type.ASCII);
        testCqlTypeParser("list<int>", CqlField.NativeCql3Type.INT);
        testCqlTypeParser("LIST<BIGINT>", CqlField.NativeCql3Type.BIGINT);
        testCqlTypeParser("map<int,text>", CqlField.NativeCql3Type.INT, CqlField.NativeCql3Type.TEXT);
        testCqlTypeParser("map<boolean , decimal>", CqlField.NativeCql3Type.BOOLEAN, CqlField.NativeCql3Type.DECIMAL);
        testCqlTypeParser("MAP<TIMEUUID,TIMESTAMP>", CqlField.NativeCql3Type.TIMEUUID, CqlField.NativeCql3Type.TIMESTAMP);
        testCqlTypeParser("MAP<VARCHAR , double>", CqlField.NativeCql3Type.VARCHAR, CqlField.NativeCql3Type.DOUBLE);
        testCqlTypeParser("tuple<int, text>", CqlField.NativeCql3Type.INT, CqlField.NativeCql3Type.TEXT);
    }

    @Test
    public void testSplitMapTypes()
    {
        splitMap("", "", null);
        splitMap("text", "text", null);
        splitMap("bigint", "bigint", null);
        splitMap("set<text>", "set<text>", null);
        splitMap("text,bigint", "text", "bigint");
        splitMap("varchar , float", "varchar", "float");
        splitMap("varchar , float", "varchar", "float");
        splitMap("date, frozen<set<text>>", "date", "frozen<set<text>>");
        splitMap("timestamp, frozen<map<int, blob>>", "timestamp", "frozen<map<int, blob>>");
        splitMap("frozen<list<timeuuid>>, frozen<map<uuid, double>>", "frozen<list<timeuuid>>", "frozen<map<uuid, double>>");
        splitMap("frozen<map<int, float>>, frozen<map<blob, decimal>>", "frozen<map<int, float>>", "frozen<map<blob, decimal>>");
        splitMap("frozen<map<int,float>>,frozen<map<blob,decimal>>", "frozen<map<int,float>>", "frozen<map<blob,decimal>>");
        splitMap("text, frozen<map<text, set<text>>>", "text", "frozen<map<text, set<text>>>");
        splitMap("frozen<map<set<int>,blob>>,   frozen<map<text, frozen<map<bigint, double>>>>", "frozen<map<set<int>,blob>>", "frozen<map<text, frozen<map<bigint, double>>>>");
    }

    @Test
    public void testCqlNames() {
        assertEquals("set<bigint>", CqlField.CqlCollection.build("set", CqlField.NativeCql3Type.BIGINT).cqlName());
        assertEquals("list<timestamp>", CqlField.CqlCollection.build("LIST", CqlField.NativeCql3Type.TIMESTAMP).cqlName());
        assertEquals("map<text, int>", CqlField.CqlCollection.build("Map", CqlField.NativeCql3Type.TEXT, CqlField.NativeCql3Type.INT).cqlName());
        assertEquals("tuple<int, blob, varchar>", CqlField.CqlCollection.build("tuple", CqlField.NativeCql3Type.INT, CqlField.NativeCql3Type.BLOB, CqlField.NativeCql3Type.VARCHAR).cqlName());
        assertEquals("tuple<int, blob, map<int, float>>", CqlField.CqlCollection.build("tuPLe", CqlField.NativeCql3Type.INT, CqlField.NativeCql3Type.BLOB, CqlField.map(CqlField.NativeCql3Type.INT, CqlField.NativeCql3Type.FLOAT)).cqlName());
    }

    @Test
    public void testTuple()
    {
        final String[] result = CqlField.splitInnerTypes("a, b, c, d,e, f, g");
        assertEquals("a", result[0]);
        assertEquals("b", result[1]);
        assertEquals("c", result[2]);
        assertEquals("d", result[3]);
        assertEquals("e", result[4]);
        assertEquals("f", result[5]);
        assertEquals("g", result[6]);
    }

    private static void splitMap(final String str, final String left, final String right)
    {
        final String[] result = CqlField.splitInnerTypes(str);
        if (left != null)
        {
            assertEquals(left, result[0]);
        }
        if (right != null)
        {
            assertEquals(right, result[1]);
        }
    }

    @Test
    public void testNestedSet()
    {
        final CqlField.CqlType type = CqlField.parseType("set<frozen<map<text, list<double>>>>");
        assertNotNull(type);
        assertEquals(type.internalType(), CqlField.CqlType.InternalType.Set);
        final CqlField.CqlType frozen = ((CqlField.CqlSet) type).type();
        assertEquals(frozen.internalType(), CqlField.CqlType.InternalType.Frozen);
        final CqlField.CqlMap map = (CqlField.CqlMap) ((CqlField.CqlFrozen) frozen).inner();
        assertEquals(map.keyType(), CqlField.NativeCql3Type.TEXT);
        assertEquals(map.valueType().internalType(), CqlField.CqlType.InternalType.List);
        final CqlField.CqlList list = (CqlField.CqlList) map.valueType();
        assertEquals(list.type(), CqlField.NativeCql3Type.DOUBLE);
    }

    @Test
    public void testFrozenCqlTypeParser()
    {
        final CqlField.CqlType type = CqlField.parseType("frozen<map<text, float>>");
        assertNotNull(type);
        assertEquals(type.internalType(), CqlField.CqlType.InternalType.Frozen);
        final CqlField.CqlType inner = ((CqlField.CqlFrozen) type).inner();
        assertEquals(inner.internalType(), CqlField.CqlType.InternalType.Map);
        final CqlField.CqlMap map = (CqlField.CqlMap) inner;
        assertEquals(map.keyType(), CqlField.NativeCql3Type.TEXT);
        assertEquals(map.valueType(), CqlField.NativeCql3Type.FLOAT);
    }

    @Test
    public void testFrozenCqlTypeNested()
    {
        final CqlField.CqlType type = CqlField.parseType("map<frozen<set<text>>, frozen<map<int, list<blob>>>>");
        assertNotNull(type);
        assertEquals(type.internalType(), CqlField.CqlType.InternalType.Map);

        final CqlField.CqlType key = ((CqlField.CqlMap) type).keyType();
        assertEquals(key.internalType(), CqlField.CqlType.InternalType.Frozen);
        final CqlField.CqlCollection keyInner = (CqlField.CqlCollection) ((CqlField.CqlFrozen) key).inner();
        assertEquals(keyInner.internalType(), CqlField.CqlType.InternalType.Set);
        assertEquals(keyInner.type(), CqlField.NativeCql3Type.TEXT);

        final CqlField.CqlType value = ((CqlField.CqlMap) type).valueType();
        assertEquals(value.internalType(), CqlField.CqlType.InternalType.Frozen);
        final CqlField.CqlCollection valueInner = (CqlField.CqlCollection) ((CqlField.CqlFrozen) value).inner();
        assertEquals(valueInner.internalType(), CqlField.CqlType.InternalType.Map);
        final CqlField.CqlMap valueMap = (CqlField.CqlMap) valueInner;
        assertEquals(valueMap.keyType(), CqlField.NativeCql3Type.INT);
        assertEquals(valueMap.valueType().internalType(), CqlField.CqlType.InternalType.List);
        assertEquals(((CqlField.CqlList) valueMap.valueType()).type(), CqlField.NativeCql3Type.BLOB);
    }

    private static void testCqlTypeParser(final String str, final CqlField.NativeCql3Type expectedType)
    {
        testCqlTypeParser(str, expectedType, null);
    }

    private static void testCqlTypeParser(final String str, final CqlField.NativeCql3Type expectedType, final CqlField.NativeCql3Type otherType)
    {
        final CqlField.CqlType type = CqlField.parseType(str);
        if (type instanceof CqlField.CqlTuple)
        {
            assertEquals(((CqlField.CqlTuple) type).type(0), expectedType);
            if (otherType != null)
            {
                assertEquals(((CqlField.CqlTuple) type).type(1), otherType);
            }
        }
        else if (type instanceof CqlField.CqlCollection)
        {
            assertEquals(((CqlField.CqlCollection) type).type(), expectedType);
            if (otherType != null)
            {
                assertTrue(type instanceof CqlField.CqlMap);
                assertEquals(((CqlField.CqlMap) type).valueType(), otherType);
            }
        }
        else
        {
            assertTrue(type instanceof CqlField.NativeCql3Type);
            assertEquals(type, expectedType);
        }
    }
}
