package org.apache.cassandra.spark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.UnsignedBytes;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlUdt;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.fourzero.FourZero;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.SSTableTombstoneWriter;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.tools.JsonTransformer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.tools.Util;
import org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.spark.sparksql.LocalDataSource;
import org.apache.cassandra.spark.utils.FilterUtils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.quicktheories.core.Gen;
import org.spark_project.guava.collect.ImmutableMap;
import org.spark_project.guava.collect.Sets;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

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
public class TestUtils
{
    public static final Random RANDOM = new Random();
    private static final SparkSession SPARK = SparkSession
                                              .builder()
                                              .appName("Java Test")
                                              .config("spark.master", "local")
                                              .getOrCreate();
    public static final int NUM_ROWS = 50;
    public static final int NUM_COLS = 25;
    public static final int MIN_COLLECTION_SIZE = 16;
    private static final Comparator<Object> INET_COMPARATOR = (o1, o2) -> UnsignedBytes.lexicographicalComparator().compare(((Inet4Address) o1).getAddress(), ((Inet4Address) o2).getAddress());

    public static final Comparator<Object> NESTED_COMPARATOR = new Comparator<Object>()
    {
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public int compare(Object o1, Object o2)
        {
            if (o1 instanceof Comparable && o2 instanceof Comparable)
            {
                return ((Comparable) o1).compareTo(o2);
            }
            else if (o1 instanceof Object[] && o2 instanceof Object[])
            {
                final Object[] a1 = (Object[]) o1;
                final Object[] a2 = (Object[]) o2;
                int pos = 0;
                while (pos < a1.length && pos < a2.length)
                {
                    int c = NESTED_COMPARATOR.compare(a1[pos], a2[pos]);
                    if (c != 0)
                    {
                        return c;
                    }
                    pos++;
                }
                return Integer.compare(a1.length, a2.length);
            }
            else if (o1 instanceof Map && o2 instanceof Map)
            {
                final Map<?, ?> m1 = (Map<?, ?>) o1;
                final Map<?, ?> m2 = (Map<?, ?>) o2;
                for (final Object key : m1.keySet()) {
                    int c = NESTED_COMPARATOR.compare(m1.get(key), m2.get(key));
                    if (c != 0)
                    {
                        return c;
                    }
                }
                return Integer.compare(m1.size(), m2.size());
            }
            else if (o1 instanceof Collection && o2 instanceof  Collection) {
                return NESTED_COMPARATOR.compare(((Collection) o1).toArray(new Object[0]), ((Collection) o2).toArray(new Object[0]));
            }
            else if (o1 instanceof Inet4Address && o2 instanceof Inet4Address)
            {
                return INET_COMPARATOR.compare(o1, o2);
            }
            throw new IllegalStateException("Unexpected comparable type: " + o1.getClass().getName());
        }
    };

    private static int randomPositiveInt(final int bound)
    {
        return TestUtils.RANDOM.nextInt(bound - 1) + 1;
    }

    public static Object randomValue(final CqlField.CqlType type)
    {
        return randomValue(type, MIN_COLLECTION_SIZE);
    }

    @SuppressWarnings("UnstableApiUsage")
    public static Object randomValue(final CqlField.CqlType type, final int minCollectionSize)
    {
        switch (type.internalType())
        {
            case NativeCql:
                switch ((CqlField.NativeCql3Type) type)
                {
                    case TEXT:
                    case VARCHAR:
                    case ASCII:
                        return RandomStringUtils.randomAlphanumeric(TestUtils.randomPositiveInt(32));
                    case BIGINT:
                    case TIME:
                        return (long) RANDOM.nextInt(5000000); // keep within bound to avoid overflows
                    case BLOB:
                        final byte[] b = new byte[TestUtils.randomPositiveInt(256)];
                        RANDOM.nextBytes(b);
                        return ByteBuffer.wrap(b);
                    case BOOLEAN:
                        return RANDOM.nextBoolean();
                    case DATE:
                        return TestUtils.randomPositiveInt(30000);
                    case INT:
                        return RANDOM.nextInt();
                    case VARINT:
                        return new BigInteger(CassandraBridge.BigNumberConfig.DEFAULT.bigIntegerPrecision(), RANDOM);
                    case TINYINT:
                        final byte[] bytes = new byte[1];
                        RANDOM.nextBytes(bytes);
                        return bytes[0];
                    case DECIMAL:
                        final BigInteger unscaledVal = new BigInteger(CassandraBridge.BigNumberConfig.DEFAULT.bigDecimalPrecision(), RANDOM);
                        final int scale = RANDOM.nextInt(CassandraBridge.BigNumberConfig.DEFAULT.bigDecimalScale());
                        return new BigDecimal(unscaledVal, scale);
                    case DOUBLE:
                        return RANDOM.nextDouble();
                    case EMPTY:
                        return null;
                    case FLOAT:
                        return RANDOM.nextFloat();
                    case INET:
                        return InetAddresses.fromInteger(RANDOM.nextInt());
                    case TIMESTAMP:
                        return new java.util.Date();
                    case TIMEUUID:
                        return UUIDs.timeBased();
                    case UUID:
                        return UUID.randomUUID();
                    case SMALLINT:
                        return (short) RANDOM.nextInt(Short.MAX_VALUE + 1);
                }
            case Set:
                assert type instanceof CqlField.CqlSet;
                final CqlField.CqlSet setType = (CqlField.CqlSet) type;
                return IntStream.range(0, RANDOM.nextInt(16) + minCollectionSize)
                                .mapToObj(i -> TestUtils.randomValue(setType.type(), minCollectionSize))
                                .collect(Collectors.toSet());
            case List:
                assert type instanceof CqlField.CqlList;
                final CqlField.CqlList listType = (CqlField.CqlList) type;
                return IntStream.range(0, RANDOM.nextInt(16) + minCollectionSize)
                                .mapToObj(i -> TestUtils.randomValue(listType.type(), minCollectionSize))
                                .collect(Collectors.toList());
            case Map:
                assert type instanceof CqlField.CqlMap;
                final CqlField.CqlMap mapType = (CqlField.CqlMap) type;
                return IntStream.range(0, RANDOM.nextInt(16) + minCollectionSize)
                                .mapToObj(i -> Pair.of(TestUtils.randomValue(mapType.keyType(), minCollectionSize), TestUtils.randomValue(mapType.valueType(), minCollectionSize)))
                                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (a, b) -> a));
            case Frozen:
                return randomValue(((CqlField.CqlFrozen) type).inner(), minCollectionSize);
            case Udt:
                return ((CqlUdt) type).fields().stream().collect(Collectors.toMap(CqlField::name, f -> Objects.requireNonNull(randomValue(f.type(), minCollectionSize))));
            case Tuple:
                return ((CqlField.CqlTuple) type).types().stream().map(t -> TestUtils.randomValue(t, minCollectionSize)).toArray();
            default:
                throw new IllegalStateException("Should not reach here");
        }
    }

    static Object sparkSqlRowValue(final CqlField.CqlType type, final Row row, final int pos)
    {
        switch (type.internalType())
        {
            case NativeCql:
                final CqlField.NativeCql3Type nativeCql3Type = (CqlField.NativeCql3Type) type;
                return toTestRowType(nativeCql3Type, nativeTypeSparkRowValue(nativeCql3Type, row, pos));
            case Set:
                final CqlField.CqlSet setType = (CqlField.CqlSet) type;
                return row.getList(pos).stream().map(o -> toTestRowType(setType.type(), o)).collect(Collectors.toSet());
            case List:
                final CqlField.CqlList listType = (CqlField.CqlList) type;
                return row.getList(pos).stream().map(o -> toTestRowType(listType.type(), o)).collect(Collectors.toList());
            case Map:
                final CqlField.CqlMap mapType = (CqlField.CqlMap) type;
                return row.getJavaMap(pos).entrySet().stream()
                          .collect(Collectors.toMap(e -> toTestRowType(mapType.keyType(), e.getKey()), e -> toTestRowType(mapType.valueType(), e.getValue())));
            case Frozen:
                return sparkSqlRowValue(((CqlField.CqlFrozen) type).inner(), row, pos);
            case Udt:
                final CqlUdt udt = (CqlUdt) type;
                final Row struct = row.getStruct(pos);
                return IntStream.range(0, struct.size()).boxed()
                                .filter(i -> !struct.isNullAt(i))
                                .collect(Collectors.toMap(
                                i -> struct.schema().fields()[i].name(),
                                i -> toTestRowType(udt.field(i).type(), struct.get(i))
                                ));
            case Tuple:
                final CqlField.CqlTuple tuple = (CqlField.CqlTuple) type;
                final Row tupleStruct = row.getStruct(pos);
                return IntStream.range(0, tupleStruct.size()).boxed()
                                .filter(i -> !tupleStruct.isNullAt(i))
                                .map(i -> toTestRowType(tuple.type(i), tupleStruct.get(i)))
                                .toArray();
            default:
                throw new IllegalStateException("Unsupported data type: " + type);
        }
    }

    static Object sparkSqlRowValue(final CqlField.CqlType type, final GenericInternalRow row, final int pos)
    {
        switch (type.internalType())
        {
            case NativeCql:
                final CqlField.NativeCql3Type nativeCql3Type = (CqlField.NativeCql3Type) type;
                return toTestRowType(nativeCql3Type, nativeTypeSparkSqlRowValue(nativeCql3Type, row, pos));
            case Set:
                final CqlField.CqlSet setType = (CqlField.CqlSet) type;
                return Arrays.stream(row.getArray(pos).array()).map(o -> toTestRowType(setType.type(), o)).collect(Collectors.toSet());
            case List:
                final CqlField.CqlList listType = (CqlField.CqlList) type;
                return Arrays.stream(row.getArray(pos).array()).map(o -> toTestRowType(listType.type(), o)).collect(Collectors.toList());
            case Map:
                final CqlField.CqlMap mapType = (CqlField.CqlMap) type;
                final MapData map = row.getMap(pos);
                final ArrayData keys = map.keyArray();
                final ArrayData values = map.valueArray();
                final Map<Object, Object> result = new HashMap<>(keys.numElements());
                for (int i = 0; i < keys.numElements(); i++)
                {
                    final Object key = toTestRowType(mapType.keyType(), keys.get(i, CassandraBridge.defaultSparkSQLType(mapType.keyType())));
                    final Object value = toTestRowType(mapType.valueType(), values.get(i, CassandraBridge.defaultSparkSQLType(mapType.valueType())));
                    result.put(key, value);
                }
                return result;
            case Frozen:
                return sparkSqlRowValue(((CqlField.CqlFrozen) type).inner(), row, pos);
            case Udt:
                final CqlUdt udt = (CqlUdt) type;
                final InternalRow struct = row.getStruct(pos, udt.size());
                return IntStream.range(0, udt.size()).boxed()
                                .collect(Collectors.toMap(
                                i -> udt.field(i).name(),
                                i -> toTestRowType(udt.field(i).type(), struct.get(i, CassandraBridge.defaultSparkSQLType(udt.field(i).type()))
                                )));
            case Tuple:
                final CqlField.CqlTuple tuple = (CqlField.CqlTuple) type;
                final InternalRow tupleStruct = row.getStruct(pos, tuple.size());
                return IntStream.range(0, tuple.size()).boxed()
                                .map(i -> toTestRowType(tuple.type(i), tupleStruct.get(i, CassandraBridge.defaultSparkSQLType(tuple.type(i)))))
                                .toArray();
            default:
                throw new IllegalStateException("Unsupported data type: " + type);
        }
    }

    static Object nativeTypeSparkRowValue(final CqlField.NativeCql3Type type, final Row row, final int pos)
    {
        if (row.isNullAt(pos))
        {
            return null;
        }

        switch (type)
        {
            case TIMEUUID:
            case UUID:
            case ASCII:
            case TEXT:
            case VARCHAR:
                return row.getString(pos);
            case INET:
            case BLOB:
                return row.getAs(pos);
            case BIGINT:
            case TIME:
                return row.getLong(pos);
            case BOOLEAN:
                return row.getBoolean(pos);
            case DATE:
                return row.getDate(pos);
            case INT:
                return row.getInt(pos);
            case VARINT:
                return row.getDecimal(pos).toBigInteger();
            case TINYINT:
                return row.getByte(pos);
            case DECIMAL:
                return row.getDecimal(pos);
            case DOUBLE:
                return row.getDouble(pos);
            case EMPTY:
                return null;
            case FLOAT:
                return row.getFloat(pos);
            case SMALLINT:
                return row.getShort(pos);
            case TIMESTAMP:
                return new java.util.Date(row.getTimestamp(pos).getTime());
            default:
                throw new IllegalStateException("Unsupported data type: " + type);
        }
    }

    static Object nativeTypeSparkSqlRowValue(final CqlField.NativeCql3Type type, final GenericInternalRow row, final int pos)
    {
        switch (type)
        {
            case TIMEUUID:
            case UUID:
            case ASCII:
            case TEXT:
            case VARCHAR:
                return row.getString(pos);
            case INET:
            case BLOB:
                return row.getBinary(pos);
            case BIGINT:
            case TIME:
            case TIMESTAMP:
                return row.getLong(pos);
            case BOOLEAN:
                return row.getBoolean(pos);
            case DATE:
            case INT:
                return row.getInt(pos);
            case VARINT:
                return row.getDecimal(pos, CassandraBridge.BigNumberConfig.DEFAULT.bigIntegerPrecision(), CassandraBridge.BigNumberConfig.DEFAULT.bigIntegerScale());
            case TINYINT:
                return row.getByte(pos);
            case DECIMAL:
                return row.getDecimal(pos, CassandraBridge.BigNumberConfig.DEFAULT.bigDecimalPrecision(), CassandraBridge.BigNumberConfig.DEFAULT.bigDecimalScale());
            case DOUBLE:
                return row.getDouble(pos);
            case EMPTY:
                return null;
            case FLOAT:
                return row.getFloat(pos);
            case SMALLINT:
                return row.getShort(pos);
            default:
                throw new IllegalStateException("Unsupported data type: " + type);
        }
    }

    @SuppressWarnings("unchecked")
    public static Object toTestRowType(final CqlField.CqlType type, final Object value)
    {
        switch (type.internalType())
        {
            case NativeCql:
                return toTestRowType((CqlField.NativeCql3Type) type, value);
            case Set:
                final CqlField.CqlSet set = (CqlField.CqlSet) type;
                return Stream.of((Object[]) ((WrappedArray<Object>) value).array()).map(v -> toTestRowType(set.type(), v)).collect(Collectors.toSet());
            case List:
                final CqlField.CqlList list = (CqlField.CqlList) type;
                return Stream.of((Object[]) ((WrappedArray<Object>) value).array()).map(v -> toTestRowType(list.type(), v)).collect(Collectors.toList());
            case Map:
                final CqlField.CqlMap map = (CqlField.CqlMap) type;
                return ((Map<Object, Object>) JavaConverters.mapAsJavaMapConverter(((scala.collection.immutable.Map<?, ?>) value)).asJava())
                       .entrySet().stream().collect(Collectors.toMap(e -> toTestRowType(map.keyType(), e.getKey()), e -> toTestRowType(map.valueType(), e.getValue())));
            case Frozen:
                return toTestRowType(((CqlField.CqlFrozen) type).inner(), value);
            case Udt:
                final CqlUdt udt = (CqlUdt) type;
                final GenericRowWithSchema row = (GenericRowWithSchema) value;
                final String[] fieldNames = row.schema().fieldNames();
                final Map<String, Object> result = new HashMap<>(fieldNames.length);
                for (int i = 0; i < fieldNames.length; i++)
                {
                    result.put(fieldNames[i], toTestRowType(udt.field(i).type(), row.get(i)));
                }
                return result;
            case Tuple:
                final CqlField.CqlTuple tuple = (CqlField.CqlTuple) type;
                final GenericRowWithSchema tupleRow = (GenericRowWithSchema) value;
                final Object[] tupleResult = new Object[tupleRow.size()];
                for (int i = 0; i < tupleRow.size(); i++)
                {
                    tupleResult[i] = toTestRowType(tuple.type(i), tupleRow.get(i));
                }
                return tupleResult;
            default:
                throw new IllegalStateException("Unsupported data type: " + type);
        }
    }

    private static Object toTestRowType(final CqlField.NativeCql3Type type, final Object value)
    {
        switch (type)
        {
            case ASCII:
            case VARCHAR:
            case TEXT:
                if (value instanceof UTF8String)
                {
                    return ((UTF8String) value).toString();
                }
                return value;
            case TIMEUUID:
            case UUID:
                return UUID.fromString(value.toString());
            case INET:
                try
                {
                    return InetAddress.getByAddress((byte[]) value);
                }
                catch (final UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
            case BLOB:
                return ByteBuffer.wrap((byte[]) value);
            case VARINT:
                if (value instanceof BigInteger)
                {
                    return value;
                }
                else if (value instanceof BigDecimal)
                {
                    return ((BigDecimal) value).toBigInteger();
                }
                return ((Decimal) value).toJavaBigInteger();
            case DECIMAL:
                if (value instanceof BigDecimal)
                {
                    return value;
                }
                return ((Decimal) value).toJavaBigDecimal();
            case TIMESTAMP:
                if (value instanceof java.util.Date)
                {
                    return value;
                }
                return new java.util.Date((long) value / 1000L);
            case DATE:
                if (value instanceof java.sql.Date)
                {
                    // round up to convert date back to days since epoch
                    return (int) ((java.sql.Date) value).toLocalDate().toEpochDay();
                }
                return value;
            default:
                return value;
        }
    }

    public static boolean equals(final Object[] ar1, final Object[] ar2)
    {
        if (ar1 == ar2)
        {
            return true;
        }
        if (ar1 == null || ar2 == null)
        {
            return false;
        }

        final int length = ar1.length;
        if (ar2.length != length)
        {
            return false;
        }

        for (int i = 0; i < length; i++)
        {
            if (!TestUtils.equals(ar1[i], ar2[i]))
            {
                return false;
            }
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    private static boolean equals(final Object o1, final Object o2)
    {
        if ((Objects.equals(o1, o2)))
        {
            return true;
        }

        if (o1 instanceof BigDecimal && o2 instanceof BigDecimal)
        {
            return ((BigDecimal) o1).compareTo((BigDecimal) o2) == 0;
        }
        else if (o1 instanceof Collection && o2 instanceof Collection)
        {
            final Object[] a3 = ((Collection<Object>) o1).toArray(new Object[0]);
            Arrays.sort(a3, NESTED_COMPARATOR);
            final Object[] a4 = ((Collection<Object>) o2).toArray(new Object[0]);
            Arrays.sort(a4, NESTED_COMPARATOR);
            return TestUtils.equals(a3, a4);
        }
        else if (o1 instanceof Map && o2 instanceof Map)
        {
            final Object[] k1 = ((Map<Object, Object>) o1).keySet().toArray(new Object[0]);
            final Object[] k2 = ((Map<Object, Object>) o2).keySet().toArray(new Object[0]);
            if (k1[0] instanceof Comparable)
            {
                Arrays.sort(k1);
                Arrays.sort(k2);
            }
            else if (k1[0] instanceof Inet4Address) // Inet4Address is not Comparable so do byte ordering
            {
                Arrays.sort(k1, TestUtils.INET_COMPARATOR);
                Arrays.sort(k2, TestUtils.INET_COMPARATOR);
            }
            if (TestUtils.equals(k1, k2))
            {
                final Object[] v1 = new Object[k1.length];
                final Object[] v2 = new Object[k2.length];
                IntStream.range(0, k1.length).forEach(pos -> {
                    v1[pos] = ((Map<Object, Object>) o1).get(k1[pos]);
                    v2[pos] = ((Map<Object, Object>) o2).get(k2[pos]);
                });
                return TestUtils.equals(v1, v2);
            }
            return false;
        }
        else if (o1 instanceof Object[] && o2 instanceof Object[])
        {
            return TestUtils.equals((Object[]) o1, (Object[]) o2);
        }
        return false;
    }

    @SuppressWarnings("SameParameterValue")
    public static int getCardinality(final CqlField.NativeCql3Type type, final int orElse)
    {
        switch (type)
        {
            case BOOLEAN:
                return 2;
            case EMPTY:
                return 1;
            default:
                return orElse;
        }
    }

    public static long countSSTables(final Path dir) throws IOException
    {
        return getFileType(dir, DataLayer.FileType.DATA).count();
    }

    public static Path getFirstFileType(final Path dir, final DataLayer.FileType fileType) throws IOException
    {
        return getFileType(dir, fileType).findFirst().orElseThrow(() -> new IllegalStateException(String.format("Could not find %s file", fileType.getFileSuffix())));
    }

    public static Stream<Path> getFileType(final Path dir, final DataLayer.FileType fileType) throws IOException
    {
        return Files
               .list(dir)
               .filter(path -> path.getFileName().toString().endsWith("-" + fileType.getFileSuffix()));
    }

    /**
     * Run test for all supported Cassandra versions
     *
     * @param test unit test
     */
    public static void runTest(final TestRunnable test)
    {
        qt().forAll(TestUtils.partitioners(), TestUtils.bridges())
            .checkAssert((partitioner, bridge) -> TestUtils.runTest(partitioner, bridge, test));
    }

    public static void runTest(final CassandraBridge.CassandraVersion version, final TestRunnable test)
    {
        qt().forAll(TestUtils.partitioners())
            .checkAssert((partitioner) -> TestUtils.runTest(partitioner, version, test));
    }

    public static void runTest(final Partitioner partitioner, final CassandraBridge.CassandraVersion version, final TestRunnable test)
    {
        runTest(partitioner, CassandraBridge.get(version), test);
    }

    /**
     * Create tmp directory and clean up after test
     *
     * @param bridge cassandra bridge
     * @param test   unit test
     */
    public static void runTest(final Partitioner partitioner, final CassandraBridge bridge, final TestRunnable test)
    {
        Path dir = null;
        try
        {
            dir = Files.createTempDirectory(UUID.randomUUID().toString());
            test.run(partitioner, dir, bridge);
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            if (dir != null)
            {
                try
                {
                    FileUtils.deleteDirectory(dir.toFile());
                }
                catch (final IOException ignore)
                {

                }
            }
        }
    }

    static Dataset<Row> openLocalDataset(final Partitioner partitioner,
                                         final Path dir,
                                         final String keyspace,
                                         final String createStmt,
                                         final CassandraBridge.CassandraVersion version,
                                         final Set<CqlUdt> udts,
                                         final String filterExpression)
    {
        final DataFrameReader frameReader = SPARK.read().format(LocalDataSource.class.getName())
                                                 .option("keyspace", keyspace)
                                                 .option("createStmt", createStmt)
                                                 .option("dirs", dir.toAbsolutePath().toString())
                                                 .option("version", version.toString())
                                                 .option("partitioner", partitioner.name())
                                                 .option("udts", udts.stream().map(f -> f.createStmt(keyspace)).collect(Collectors.joining("\n")));
        return filterExpression == null ? frameReader.load() : frameReader.load().filter(filterExpression);
    }

    public static void writeSSTable(final CassandraBridge bridge, final Path dir, final Partitioner partitioner, final TestSchema schema, Consumer<CassandraBridge.IWriter> consumer)
    {
        bridge.writeSSTable(partitioner, schema.keyspace, dir, schema.createStmt, schema.insertStmt, schema.udts, consumer);
    }

    public static void writeSSTable(final CassandraBridge bridge, final Path dir, final Partitioner partitioner, final TestSchema schema, Tester.Writer writer)
    {
        TestUtils.writeSSTable(bridge, dir, partitioner, schema, writer.consumer);
    }

    // write tombstones

    public static void writeTombstoneSSTable(final Partitioner partitioner, final CassandraBridge.CassandraVersion version, final Path dir, final String createStmt, final String deleteStmt, final Consumer<CassandraBridge.IWriter> writerConsumer)
    {
        if (version == CassandraBridge.CassandraVersion.FOURZERO)
        {
            writeFourZeroTombstoneSSTable(partitioner, dir, createStmt, deleteStmt, writerConsumer);
            return;
        }
        throw new NotImplementedException("Tombstone writer not implemented for version: " + version);
    }

    private static void writeFourZeroTombstoneSSTable(final Partitioner partitioner, final Path dir, final String createStmt, final String deleteStmt, final Consumer<CassandraBridge.IWriter> writerConsumer)
    {
        try (final SSTableTombstoneWriter writer = SSTableTombstoneWriter.builder()
                                                                         .inDirectory(dir.toFile())
                                                                         .forTable(createStmt)
                                                                         .withPartitioner(FourZero.getPartitioner(partitioner))
                                                                         .using(deleteStmt)
                                                                         .withBufferSizeInMB(128).build())
        {
            writerConsumer.accept(values -> {
                try
                {
                    writer.addRow(values);
                }
                catch (final IOException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /* sstable to json */

    public static void sstableToJson(final CassandraBridge.CassandraVersion version, final Path dataDbFile, final OutputStream out) throws FileNotFoundException
    {
        if (version == CassandraBridge.CassandraVersion.FOURZERO)
        {
            sstableToJsonFourZero(dataDbFile, out);
            return;
        }
        throw new NotImplementedException("SSTableToJson not implemented for version: " + version);
    }

    private static void sstableToJsonFourZero(final Path dataDbFile, final OutputStream out) throws FileNotFoundException
    {
        if (!Files.exists(dataDbFile))
        {
            throw new FileNotFoundException("Cannot find file " + dataDbFile.toAbsolutePath().toString());
        }
        if (!Descriptor.isValidFile(dataDbFile.toFile()))
        {
            throw new RuntimeException("Invalid sstable file");
        }

        final Descriptor desc = Descriptor.fromFilename(dataDbFile.toAbsolutePath().toString());
        try
        {
            final TableMetadataRef metadata = TableMetadataRef.forOfflineTools(Util.metadataFromSSTable(desc));
            final SSTableReader sstable = SSTableReader.openNoValidation(desc, metadata);
            final ISSTableScanner currentScanner = sstable.getScanner();
            final Stream<UnfilteredRowIterator> partitions = iterToStream(currentScanner);
            JsonTransformer.toJson(currentScanner, partitions, false, metadata.get(), out);
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static <T> Stream<T> iterToStream(final Iterator<T> iter)
    {
        final Spliterator<T> splititer = Spliterators.spliteratorUnknownSize(iter, Spliterator.IMMUTABLE);
        return StreamSupport.stream(splititer, false);
    }

    public static ReplicationFactor simpleStrategy()
    {
        return new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, ImmutableMap.of("DC1", 3));
    }

    public static ReplicationFactor networkTopologyStrategy()
    {
        return networkTopologyStrategy(ImmutableMap.of("DC1", 3));
    }

    public static ReplicationFactor networkTopologyStrategy(final Map<String, Integer> options)
    {
        return new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, options);
    }

    /* quick theories helpers */

    public static Gen<CassandraBridge.CassandraVersion> versions()
    {
        return arbitrary().pick(new ArrayList<>(CassandraBridge.SUPPORTED_VERSIONS));
    }

    public static Gen<CassandraBridge> bridges()
    {
        return arbitrary().pick(testableVersions().stream().map(CassandraBridge::get).collect(Collectors.toList()));
    }

    static List<CassandraBridge.CassandraVersion> testableVersions()
    {
        return new ArrayList<>(Collections.singletonList(CassandraBridge.CassandraVersion.FOURZERO));
    }

    public static Gen<CqlField.NativeCql3Type> cql3Type()
    {
        return arbitrary().pick(new ArrayList<>(Sets.complementOf(CqlField.UNSUPPORTED_TYPES)));
    }

    public static Gen<CqlField.SortOrder> sortOrder()
    {
        return arbitrary().enumValues(CqlField.SortOrder.class);
    }

    public static Gen<CassandraBridge.CassandraVersion> tombstoneVersions()
    {
        return arbitrary().pick(tombstoneTestableVersions());
    }

    static List<CassandraBridge.CassandraVersion> tombstoneTestableVersions()
    {
        return Collections.singletonList(CassandraBridge.CassandraVersion.FOURZERO);
    }

    public static Gen<Partitioner> partitioners()
    {
        return arbitrary().enumValues(Partitioner.class);
    }

    public static BigInteger randomBigInteger(final Partitioner partitioner)
    {
        final BigInteger range = partitioner.maxToken().subtract(partitioner.minToken());
        final int len = partitioner.maxToken().bitLength();
        BigInteger result = new BigInteger(len, RANDOM);
        if (result.compareTo(partitioner.minToken()) < 0)
        {
            result = result.add(partitioner.minToken());
        }
        if (result.compareTo(range) >= 0)
        {
            result = result.mod(range).add(partitioner.minToken());
        }
        return result;
    }

    public static CassandraRing createRing(final Partitioner partitioner, int numInstances)
    {
        return createRing(partitioner, ImmutableMap.of("DC1", numInstances));
    }

    public static CassandraRing createRing(final Partitioner partitioner, final Map<String, Integer> numInstances)
    {
        final Collection<CassandraInstance> instances = numInstances.entrySet().stream().map(e -> TestUtils.createInstances(partitioner, e.getValue(), e.getKey())).flatMap(Collection::stream).collect(Collectors.toList());
        final Map<String, Integer> dcs = numInstances.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, a -> Math.min(a.getValue(), 3)));
        return new CassandraRing(partitioner, "test", new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, dcs), instances);
    }

    public static Collection<CassandraInstance> createInstances(final Partitioner partitioner, final int numInstances, final String dc)
    {
        Preconditions.checkArgument(numInstances > 0, "NumInstances must be greater than zero");
        final BigInteger split = partitioner.maxToken().subtract(partitioner.minToken()).divide(BigInteger.valueOf(numInstances));
        final Collection<CassandraInstance> instances = new ArrayList<>(numInstances);
        BigInteger token = partitioner.minToken();
        for (int i = 0; i < numInstances; i++)
        {
            instances.add(new CassandraInstance(token.toString(), "local-i" + i, dc));
            token = token.add(split);
            assertTrue(token.compareTo(partitioner.maxToken()) <= 0);
        }
        return instances;
    }

    public static Set<String> getKeys(final List<List<String>> values)
    {
        final Set<String> filterKeys = new HashSet<>();
        FilterUtils.cartesianProduct(values).forEach(keys ->
                                                     {
                                                         final String compositeKey = String.join(":", keys);
                                                         filterKeys.add(compositeKey);
                                                     });
        return filterKeys;
    }
}
