package org.apache.cassandra.spark.reader.fourzero;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.net.InetAddresses;
import org.junit.Test;

import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.AsciiSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.BooleanSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.ByteSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.BytesSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.DecimalSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.DoubleSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.EmptySerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.FloatSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.IntegerSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.LongSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.ShortSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TimeSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TimeUUIDSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TimestampSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.UUIDGen;
import org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.utils.UUIDs;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.bigDecimals;
import static org.quicktheories.generators.SourceDSL.bigIntegers;
import static org.quicktheories.generators.SourceDSL.dates;
import static org.quicktheories.generators.SourceDSL.doubles;
import static org.quicktheories.generators.SourceDSL.floats;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.longs;
import static org.quicktheories.generators.SourceDSL.strings;

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

public class DataTypeSerializationTests
{
    private static final int MAX_TESTS = 1000;
    private static final Random RANDOM = new Random();

    @Test
    public void testVarInt()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.varint().deserialize(IntegerSerializer.instance.serialize(BigInteger.valueOf(500L))) instanceof Decimal);
            assertEquals(Decimal.apply(500), bridge.varint().deserialize(IntegerSerializer.instance.serialize(BigInteger.valueOf(500L))));
            assertNotSame(Decimal.apply(501), bridge.varint().deserialize(IntegerSerializer.instance.serialize(BigInteger.valueOf(500L))));
            assertEquals(Decimal.apply(-1), bridge.varint().deserialize(IntegerSerializer.instance.serialize(BigInteger.valueOf(-1L))));
            assertEquals(Decimal.apply(Long.MAX_VALUE), bridge.varint().deserialize(IntegerSerializer.instance.serialize(BigInteger.valueOf(Long.MAX_VALUE))));
            assertEquals(Decimal.apply(Long.MIN_VALUE), bridge.varint().deserialize(IntegerSerializer.instance.serialize(BigInteger.valueOf(Long.MIN_VALUE))));
            assertEquals(Decimal.apply(Integer.MAX_VALUE), bridge.varint().deserialize(IntegerSerializer.instance.serialize(BigInteger.valueOf(Integer.MAX_VALUE))));
            assertEquals(Decimal.apply(Integer.MIN_VALUE), bridge.varint().deserialize(IntegerSerializer.instance.serialize(BigInteger.valueOf(Integer.MIN_VALUE))));
            final BigInteger veryLargeValue = BigInteger.valueOf(Integer.MAX_VALUE).multiply(BigInteger.valueOf(5));
            assertEquals(Decimal.apply(veryLargeValue), bridge.varint().deserialize(IntegerSerializer.instance.serialize(veryLargeValue)));

            qt().withExamples(MAX_TESTS).forAll(bigIntegers().ofBytes(128))
                .checkAssert(i -> assertEquals(Decimal.apply(i), bridge.varint().deserialize(IntegerSerializer.instance.serialize(i))));
        });
    }

    @Test
    public void testInt()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.aInt().deserialize(Int32Serializer.instance.serialize(5)) instanceof Integer);
            assertEquals(999, bridge.aInt().deserialize(ByteBuffer.allocate(4).putInt(0, 999)));
            qt().forAll(integers().all())
                .checkAssert(i -> assertEquals(i, bridge.aInt().deserialize(Int32Serializer.instance.serialize(i))));
        });
    }

    @Test
    public void testBoolean()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.bool().deserialize(BooleanSerializer.instance.serialize(true)) instanceof Boolean);
            assertTrue((Boolean) bridge.bool().deserialize(BooleanSerializer.instance.serialize(true)));
            assertFalse((Boolean) bridge.bool().deserialize(BooleanSerializer.instance.serialize(false)));
        });
    }

    @Test
    public void testTimeUUID()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.timeuuid().deserialize(TimeUUIDSerializer.instance.serialize(UUIDGen.getTimeUUID())) instanceof UTF8String);
            for (int i = 0; i < MAX_TESTS; i++)
            {
                final UUID expected = UUIDGen.getTimeUUID();
                assertEquals(expected.toString(), bridge.timeuuid().deserialize(TimeUUIDSerializer.instance.serialize(expected)).toString());
            }
        });
    }

    @Test
    public void testUUID()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.uuid().deserialize(UUIDSerializer.instance.serialize(UUID.randomUUID())) instanceof UTF8String);
            for (int i = 0; i < MAX_TESTS; i++)
            {
                final UUID expected = UUID.randomUUID();
                assertEquals(expected.toString(), bridge.uuid().deserialize(UUIDSerializer.instance.serialize(expected)).toString());
            }
        });
    }

    @Test
    public void testLong()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.bigint().deserialize(LongSerializer.instance.serialize(Long.MAX_VALUE)) instanceof Long);
            assertEquals(Long.MAX_VALUE, bridge.bigint().deserialize(ByteBuffer.allocate(8).putLong(0, Long.MAX_VALUE)));
            qt().forAll(integers().all())
                .checkAssert(i -> assertEquals((long) i, bridge.bigint().deserialize(LongSerializer.instance.serialize((long) i))));
            assertEquals(Long.MAX_VALUE, bridge.bigint().deserialize(LongSerializer.instance.serialize(Long.MAX_VALUE)));
            assertEquals(Long.MIN_VALUE, bridge.bigint().deserialize(LongSerializer.instance.serialize(Long.MIN_VALUE)));
            qt().withExamples(MAX_TESTS).forAll(longs().all())
                .checkAssert(i -> assertEquals(i, bridge.bigint().deserialize(LongSerializer.instance.serialize(i))));
        });
    }

    @Test
    public void testDecimal()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.decimal().deserialize(DecimalSerializer.instance.serialize(BigDecimal.valueOf(500L))) instanceof Decimal);
            assertEquals(Decimal.apply(500), bridge.decimal().deserialize(DecimalSerializer.instance.serialize(BigDecimal.valueOf(500L))));
            assertNotSame(Decimal.apply(501), bridge.decimal().deserialize(DecimalSerializer.instance.serialize(BigDecimal.valueOf(500L))));
            assertEquals(Decimal.apply(-1), bridge.decimal().deserialize(DecimalSerializer.instance.serialize(BigDecimal.valueOf(-1L))));
            assertEquals(Decimal.apply(Long.MAX_VALUE), bridge.decimal().deserialize(DecimalSerializer.instance.serialize(BigDecimal.valueOf(Long.MAX_VALUE))));
            assertEquals(Decimal.apply(Long.MIN_VALUE), bridge.decimal().deserialize(DecimalSerializer.instance.serialize(BigDecimal.valueOf(Long.MIN_VALUE))));
            assertEquals(Decimal.apply(Integer.MAX_VALUE), bridge.decimal().deserialize(DecimalSerializer.instance.serialize(BigDecimal.valueOf(Integer.MAX_VALUE))));
            assertEquals(Decimal.apply(Integer.MIN_VALUE), bridge.decimal().deserialize(DecimalSerializer.instance.serialize(BigDecimal.valueOf(Integer.MIN_VALUE))));
            final BigDecimal veryLargeValue = BigDecimal.valueOf(Integer.MAX_VALUE).multiply(BigDecimal.valueOf(5));
            assertEquals(Decimal.apply(veryLargeValue), bridge.decimal().deserialize(DecimalSerializer.instance.serialize(veryLargeValue)));
            qt().withExamples(MAX_TESTS).forAll(bigDecimals().ofBytes(128).withScale(10))
                .checkAssert(i -> assertEquals(Decimal.apply(i), bridge.decimal().deserialize(DecimalSerializer.instance.serialize(i))));
        });
    }

    @Test
    public void testFloat()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.aFloat().deserialize(FloatSerializer.instance.serialize(Float.MAX_VALUE)) instanceof Float);
            assertEquals(Float.MAX_VALUE, bridge.aFloat().deserialize(ByteBuffer.allocate(4).putFloat(0, Float.MAX_VALUE)));
            qt().forAll(integers().all())
                .checkAssert(i -> assertEquals((float) i, bridge.aFloat().deserialize(FloatSerializer.instance.serialize((float) i))));
            assertEquals(Float.MAX_VALUE, bridge.aFloat().deserialize(FloatSerializer.instance.serialize(Float.MAX_VALUE)));
            assertEquals(Float.MIN_VALUE, bridge.aFloat().deserialize(FloatSerializer.instance.serialize(Float.MIN_VALUE)));
            qt().withExamples(MAX_TESTS).forAll(floats().any())
                .checkAssert(i -> assertEquals(i, bridge.aFloat().deserialize(FloatSerializer.instance.serialize(i))));
        });
    }

    @Test
    public void testDouble()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.aDouble().deserialize(DoubleSerializer.instance.serialize(Double.MAX_VALUE)) instanceof Double);
            assertEquals(Double.MAX_VALUE, bridge.aDouble().deserialize(ByteBuffer.allocate(8).putDouble(0, Double.MAX_VALUE)));
            qt().forAll(integers().all())
                .checkAssert(i -> assertEquals((double) i, bridge.aDouble().deserialize(DoubleSerializer.instance.serialize((double) i))));
            assertEquals(Double.MAX_VALUE, bridge.aDouble().deserialize(DoubleSerializer.instance.serialize(Double.MAX_VALUE)));
            assertEquals(Double.MIN_VALUE, bridge.aDouble().deserialize(DoubleSerializer.instance.serialize(Double.MIN_VALUE)));
            qt().withExamples(MAX_TESTS).forAll(doubles().any())
                .checkAssert(i -> assertEquals(i, bridge.aDouble().deserialize(DoubleSerializer.instance.serialize(i))));
        });
    }

    @Test
    public void testAscii()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.ascii().deserialize(AsciiSerializer.instance.serialize("abc")) instanceof UTF8String);
            qt().withExamples(MAX_TESTS).forAll(strings().ascii().ofLengthBetween(0, 100))
                .checkAssert(i -> assertEquals(i, bridge.ascii().deserialize(AsciiSerializer.instance.serialize(i)).toString()));
        });
    }

    @Test
    public void testText()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.text().deserialize(UTF8Serializer.instance.serialize("abc")) instanceof UTF8String);
            qt().withExamples(MAX_TESTS).forAll(strings().ascii().ofLengthBetween(0, 100))
                .checkAssert(i -> assertEquals(i, bridge.text().deserialize(UTF8Serializer.instance.serialize(i)).toString()));
            qt().withExamples(MAX_TESTS).forAll(strings().basicLatinAlphabet().ofLengthBetween(0, 100))
                .checkAssert(i -> assertEquals(i, bridge.text().deserialize(UTF8Serializer.instance.serialize(i)).toString()));
            qt().withExamples(MAX_TESTS).forAll(strings().numeric())
                .checkAssert(i -> assertEquals(i, bridge.text().deserialize(UTF8Serializer.instance.serialize(i)).toString()));
        });
    }

    @Test
    public void testVarchar()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.varchar().deserialize(UTF8Serializer.instance.serialize("abc")) instanceof UTF8String);
            qt().withExamples(MAX_TESTS).forAll(strings().ascii().ofLengthBetween(0, 100))
                .checkAssert(i -> assertEquals(i, bridge.varchar().deserialize(UTF8Serializer.instance.serialize(i)).toString()));
            qt().withExamples(MAX_TESTS).forAll(strings().basicLatinAlphabet().ofLengthBetween(0, 100))
                .checkAssert(i -> assertEquals(i, bridge.varchar().deserialize(UTF8Serializer.instance.serialize(i)).toString()));
            qt().withExamples(MAX_TESTS).forAll(strings().numeric())
                .checkAssert(i -> assertEquals(i, bridge.varchar().deserialize(UTF8Serializer.instance.serialize(i)).toString()));
        });
    }

    @Test
    public void testInet()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.inet().deserialize(InetAddressSerializer.instance.serialize(randomInet())) instanceof byte[]);
            for (int i = 0; i < MAX_TESTS; i++)
            {
                final InetAddress expected = randomInet();
                assertArrayEquals(expected.getAddress(), (byte[]) bridge.inet().deserialize(InetAddressSerializer.instance.serialize(expected)));
            }
        });
    }

    @SuppressWarnings("UnstableApiUsage")
    private static InetAddress randomInet()
    {
        return InetAddresses.fromInteger(RANDOM.nextInt());
    }

    @Test
    public void testDate()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.date().deserialize(SimpleDateSerializer.instance.serialize(5)) instanceof Integer);
            qt().forAll(integers().all())
                .checkAssert(i -> assertEquals(i, bridge.date().deserialize(SimpleDateSerializer.instance.serialize(i))));
        });
    }

    @Test
    public void testTime()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.time().deserialize(TimeSerializer.instance.serialize(Long.MAX_VALUE)) instanceof Long);
            qt().forAll(integers().all())
                .checkAssert(i -> assertEquals((long) i, bridge.time().deserialize(TimeSerializer.instance.serialize((long) i))));
            assertEquals(Long.MAX_VALUE, bridge.time().deserialize(TimeSerializer.instance.serialize(Long.MAX_VALUE)));
            assertEquals(Long.MIN_VALUE, bridge.time().deserialize(TimeSerializer.instance.serialize(Long.MIN_VALUE)));
            qt().withExamples(MAX_TESTS).forAll(longs().all())
                .checkAssert(i -> assertEquals(i, bridge.time().deserialize(TimeSerializer.instance.serialize(i))));
        });
    }

    @Test
    public void testTimestamp()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            final Date now = new Date();
            assertTrue(bridge.timestamp().deserialize(TimestampSerializer.instance.serialize(now)) instanceof Long);
            assertEquals(java.sql.Timestamp.from(now.toInstant()).getTime() * 1000L, bridge.timestamp().deserialize(TimestampSerializer.instance.serialize(now)));
            qt().withExamples(MAX_TESTS).forAll(dates().withMillisecondsBetween(0, Long.MAX_VALUE))
                .checkAssert(i -> assertEquals(java.sql.Timestamp.from(i.toInstant()).getTime() * 1000L, bridge.timestamp().deserialize(TimestampSerializer.instance.serialize(i))));
        });
    }

    @Test
    public void testBlob()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.blob().deserialize(BytesSerializer.instance.serialize(ByteBuffer.wrap(randomBytes(5)))) instanceof byte[]);
            for (int i = 0; i < MAX_TESTS; i++)
            {
                final int size = RANDOM.nextInt(1024);
                final byte[] expected = randomBytes(size);
                assertArrayEquals(expected, (byte[]) bridge.blob().deserialize(BytesSerializer.instance.deserialize(ByteBuffer.wrap(expected))));
            }
        });
    }

    @Test
    public void testEmpty()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> assertNull(bridge.empty().deserialize(EmptySerializer.instance.serialize(null))));
    }

    @Test
    public void testSmallInt()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.smallint().deserialize(ShortSerializer.instance.serialize((short) 5)) instanceof Short);
            qt().forAll(integers().between(Short.MIN_VALUE, Short.MAX_VALUE))
                .checkAssert(i -> {
                    final short val = i.shortValue();
                    assertEquals(val, bridge.smallint().deserialize(ShortSerializer.instance.serialize(val)));
                });
        });
    }

    @Test
    public void testTinyInt()
    {
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            assertTrue(bridge.tinyint().deserialize(ByteSerializer.instance.serialize(randomByte())) instanceof Byte);
            for (int i = 0; i < MAX_TESTS; i++)
            {
                final byte expected = randomByte();
                assertEquals(expected, bridge.tinyint().deserialize(ByteSerializer.instance.serialize(expected)));
            }
        });
    }

    @Test
    public void testSerialization()
    {
        // CassandraBridge.serialize is mostly used for unit tests
        qt().forAll(TestUtils.bridges()).checkAssert(bridge -> {
            //BLOB,  VARINT
            assertEquals("ABC", bridge.ascii().deserialize(bridge.ascii().serialize("ABC")).toString());
            assertEquals(500L, bridge.bigint().deserialize(bridge.bigint().serialize(500L)));
            assertEquals(true, bridge.bool().deserialize(bridge.bool().serialize(true)));
            assertEquals(false, bridge.bool().deserialize(bridge.bool().serialize(false)));

            final byte[] ar = new byte[]{ 'a', 'b', 'c', 'd' };
            final ByteBuffer buf = bridge.blob().serialize(ByteBuffer.wrap(ar));
            final byte[] result = new byte[4];
            buf.get(result);
            assertArrayEquals(ar, result);

            assertEquals(500, bridge.date().deserialize(bridge.date().serialize(500)));
            assertEquals(Decimal.apply(500000.2038484), bridge.decimal().deserialize(bridge.decimal().serialize(BigDecimal.valueOf(500000.2038484))));
            assertEquals(123211.023874839, bridge.aDouble().deserialize(bridge.aDouble().serialize(123211.023874839)));
            assertEquals(58383.23737832839f, bridge.aFloat().deserialize(bridge.aFloat().serialize(58383.23737832839f)));
            try
            {
                assertEquals(InetAddress.getByName("www.google.com"), InetAddress.getByAddress((byte[]) bridge.inet().deserialize(bridge.inet().serialize(InetAddress.getByName("www.google.com")))));
            }
            catch (final UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
            assertEquals(283848498, bridge.aInt().deserialize(bridge.aInt().serialize(283848498)));
            assertEquals((short) 29, bridge.smallint().deserialize(bridge.smallint().serialize((short) 29)));
            assertEquals("hello world", bridge.ascii().deserialize(bridge.text().serialize("hello world")).toString());
            assertEquals(5002839L, bridge.time().deserialize(bridge.time().serialize(5002839L)));
            final Date now = new Date();
            assertEquals(now.getTime() * 1000L, bridge.timestamp().deserialize(bridge.timestamp().serialize(now)));
            final UUID timeUuid = UUIDs.timeBased();
            assertEquals(timeUuid, UUID.fromString(bridge.timeuuid().deserialize(bridge.timeuuid().serialize(timeUuid)).toString()));
            assertEquals((byte) 100, bridge.tinyint().deserialize(bridge.tinyint().serialize((byte) 100)));
            final UUID uuid = UUID.randomUUID();
            assertEquals(uuid, UUID.fromString(bridge.uuid().deserialize(bridge.uuid().serialize(uuid)).toString()));
            assertEquals("ABCDEFG", bridge.varchar().deserialize(bridge.varchar().serialize("ABCDEFG")).toString());
            assertEquals(Decimal.apply(12841924), bridge.varint().deserialize(bridge.varint().serialize(BigInteger.valueOf(12841924))));
        });
    }

    @Test
    public void testList()
    {
        runTest((partitioner, dir, bridge) ->
                qt().forAll(TestUtils.cql3Type(bridge))
                    .checkAssert((type) -> {
                        final CqlField.CqlList list = bridge.list(type);
                        final List<Object> expected = IntStream.range(0, 128).mapToObj(i -> TestUtils.randomValue(type)).collect(Collectors.toList());
                        final ByteBuffer buf = list.serialize(expected);
                        final List<Object> actual = Arrays.asList(((ArrayData) list.deserialize(buf)).array());
                        assertEquals(expected.size(), actual.size());
                        for (int i = 0; i < expected.size(); i++)
                        {
                            assertEquals(expected.get(i), type.toTestRowType(actual.get(i)));
                        }
                    }));
    }

    @Test
    public void testSet()
    {
        runTest((partitioner, dir, bridge) ->
                qt().forAll(TestUtils.cql3Type(bridge))
                    .checkAssert((type) -> {
                        final CqlField.CqlSet set = bridge.set(type);
                        final Set<Object> expected = IntStream.range(0, 128).mapToObj(i -> TestUtils.randomValue(type)).collect(Collectors.toSet());
                        final ByteBuffer buf = set.serialize(expected);
                        final Set<Object> actual = new HashSet<>(Arrays.asList(((ArrayData) set.deserialize(buf)).array()));
                        assertEquals(expected.size(), actual.size());
                        for (final Object value : actual)
                        {
                            assertTrue(expected.contains(type.toTestRowType(value)));
                        }
                    }));
    }

    @Test
    public void testMap()
    {
        runTest((partitioner, dir, bridge) ->
                qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
                    .checkAssert((keyType, valueType) -> {
                        final CqlField.CqlMap map = bridge.map(keyType, valueType);
                        final int count = keyType.cardinality(128);
                        final Map<Object, Object> expected = new HashMap<>(count);
                        for (int i = 0; i < count; i++)
                        {
                            Object key = TestUtils.randomValue(keyType);
                            while (expected.containsKey(key))
                            {
                                key = TestUtils.randomValue(keyType);
                            }
                            expected.put(key, TestUtils.randomValue(valueType));
                        }
                        final ByteBuffer buf = map.serialize(expected);
                        final ArrayBasedMapData mapData = ((ArrayBasedMapData) map.deserialize(buf));
                        final ArrayData keys = mapData.keyArray();
                        final ArrayData values = mapData.valueArray();
                        final Map<Object, Object> actual = new HashMap<>(keys.numElements());
                        for (int i = 0; i < keys.numElements(); i++)
                        {
                            final Object key = keyType.toTestRowType(keys.get(i, keyType.sparkSqlType(CassandraBridge.BigNumberConfig.DEFAULT)));
                            final Object value = valueType.toTestRowType(values.get(i, valueType.sparkSqlType(CassandraBridge.BigNumberConfig.DEFAULT)));
                            actual.put(key, value);
                        }
                        assertEquals(expected.size(), actual.size());
                        for (final Map.Entry<Object, Object> entry : expected.entrySet())
                        {
                            assertEquals(entry.getValue(), actual.get(entry.getKey()));
                        }
                    }));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUdts()
    {
        runTest((partitioner, dir, bridge) ->
                qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
                    .checkAssert((type1, type2) -> {
                        final CqlField.CqlUdt udt = bridge.udt("keyspace", "testudt")
                                                 .withField("a", type1)
                                                 .withField("b", bridge.ascii())
                                                 .withField("c", type2)
                                                 .build();
                        final Map<String, Object> expected = (Map<String, Object>) TestUtils.randomValue(udt);
                        assert expected != null;
                        final ByteBuffer buf = udt.serializeUdt(expected);
                        final Map<String, Object> actual = udt.deserializeUdt(buf, false);
                        assertEquals(expected.size(), actual.size());
                        for (final Map.Entry<String, Object> entry : expected.entrySet())
                        {
                            assertEquals(entry.getValue(), udt.field(entry.getKey()).type().toTestRowType(actual.get(entry.getKey())));
                        }
                    }));
    }

    @Test
    public void testTuples()
    {
        runTest((partitioner, dir, bridge) ->
                qt().forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
                    .checkAssert((type1, type2) -> {
                        final CqlField.CqlTuple tuple = bridge.tuple(type1, bridge.ascii(), type2, bridge.timestamp(), bridge.uuid(), bridge.varchar());
                        final Object[] expected = (Object[]) TestUtils.randomValue(tuple);
                        assert expected != null;
                        final ByteBuffer buf = tuple.serializeTuple(expected);
                        final Object[] actual = tuple.deserializeTuple(buf, false);
                        assertEquals(expected.length, actual.length);
                        for (int i = 0; i < expected.length; i++)
                        {
                            assertEquals(expected[i], tuple.type(i).toTestRowType(actual[i]));
                        }
                    }));
    }

    private byte[] randomBytes(final int size)
    {
        final byte[] ar = new byte[size];
        RANDOM.nextBytes(ar);
        return ar;
    }

    private byte randomByte()
    {
        return randomBytes(1)[0];
    }
}
