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

package org.apache.cassandra.spark.data;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.primitives.UnsignedBytes;

import org.apache.cassandra.spark.reader.BigNumberConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import scala.collection.mutable.WrappedArray;

// decorates CqlTypes with SparkSQL data types and helper methods
@SuppressWarnings("UnnecessaryModifier")
public class SparkCqlField extends CqlField
{
    public static final Comparator<String> UUID_COMPARATOR = Comparator.comparing(java.util.UUID::fromString);
    public static final Comparator<String> STRING_COMPARATOR = String::compareTo;
    public static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR = UnsignedBytes.lexicographicalComparator();
    public static final Comparator<Long> LONG_COMPARATOR = Long::compareTo;
    public static final Comparator<org.apache.spark.sql.types.Decimal> DECIMAL_COMPARATOR = Comparator.naturalOrder();
    public static final Comparator<Integer> INTEGER_COMPARATOR = Integer::compareTo;

    public SparkCqlField(boolean isPartitionKey,
                         boolean isClusteringColumn,
                         boolean isStaticColumn,
                         String name,
                         SparkCqlType type,
                         int pos)
    {
        super(isPartitionKey, isClusteringColumn, isStaticColumn, name, type, pos);
    }

    @Nullable
    public Object deserialize(final ByteBuffer buf)
    {
        return deserialize(buf, false);
    }

    @Nullable
    public Object deserialize(final ByteBuffer buf, boolean isFrozen)
    {
        return type().deserialize(buf, isFrozen);
    }

    @Override
    public SparkCqlType type()
    {
        return (SparkCqlType) super.type();
    }

    public static List<SparkCqlField> castToSpark(List<CqlField> fields)
    {
        return fields.stream().map(m -> (SparkCqlField) m).collect(Collectors.toList());
    }

    public static List<CqlField> cast(List<SparkCqlField> fields)
    {
        return fields.stream().map(f -> (CqlField) f).collect(Collectors.toList());
    }

    public SparkCqlField cloneWithPos(final int pos)
    {
        return new SparkCqlField(isPartitionKey(), isClusteringColumn(), isStaticColumn(), name(), type(), pos);
    }

    public boolean equals(final Object o1, final Object o2)
    {
        return type().equals(o1, o2);
    }

    public static boolean equalsArrays(final Object[] lhs, final Object[] rhs, final Function<Integer, SparkCqlType> types)
    {
        for (int pos = 0; pos < Math.min(lhs.length, rhs.length); pos++)
        {
            if (!types.apply(pos).equals(lhs[pos], rhs[pos]))
            {
                return false;
            }
        }
        return lhs.length == rhs.length;
    }

    public int compare(final Object o1, final Object o2)
    {
        return type().compare(o1, o2);
    }

    public static int compareArrays(final Object[] lhs, final Object[] rhs, final Function<Integer, SparkCqlType> types)
    {
        for (int pos = 0; pos < Math.min(lhs.length, rhs.length); pos++)
        {
            final int c = types.apply(pos).compare(lhs[pos], rhs[pos]);
            if (c != 0)
            {
                return c;
            }
        }
        return Integer.compare(lhs.length, rhs.length);
    }

    public interface SparkCqlType extends CqlField.CqlType, Comparator<Object>
    {
        public default Object toSparkSqlType(@NotNull Object o)
        {
            return toSparkSqlType(o, false);
        }

        public default boolean equals(Object o1, Object o2)
        {
            if (o1 == o2)
            {
                return true;
            }
            else if (o1 == null || o2 == null)
            {
                return false;
            }
            return equalsTo(o1, o2);
        }

        public default boolean equalsTo(Object o1, Object o2)
        {
            return compare(o1, o2) == 0;
        }

        public default int compare(Object o1, Object o2)
        {
            if (o1 == null || o2 == null)
            {
                return o1 == o2 ? 0 : (o1 == null ? -1 : 1);
            }
            return compareTo(o1, o2);
        }

        public default int compareTo(Object o1, Object o2)
        {
            throw CqlField.notImplemented(this);
        }

        public default Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
        {
            // all other non-overridden data types work as ordinary Java data types.
            return o;
        }

        @Nullable
        public default Object deserialize(final ByteBuffer buf)
        {
            return deserialize(buf, false);
        }

        @Nullable
        public default Object deserialize(final ByteBuffer buf, final boolean isFrozen)
        {
            final Object val = deserializeToJava(buf);
            return val == null ? null : toSparkSqlType(val);
        }

        /*
            SparkSQL      |    Java
            ByteType      |    byte or Byte
            ShortType     |    short or Short
            IntegerType   |    int or Integer
            LongType      |    long or Long
            FloatType     |    float or Float
            DoubleType    |    double or Double
            DecimalType   |    java.math.BigDecimal
            StringType    |    String
            BinaryType    |    byte[]
            BooleanType   |    boolean or Boolean
            TimestampType |    java.sql.Timestamp
            DateType      |    java.sql.Date
            ArrayType     |    java.util.List
            MapType       |    java.util.Map

            see: https://spark.apache.org/docs/latest/sql-reference.html
        */
        public default DataType sparkSqlType()
        {
            return sparkSqlType(BigNumberConfig.DEFAULT);
        }

        public default Object toTestRowType(Object value)
        {
            return value;
        }

        DataType sparkSqlType(BigNumberConfig bigNumberConfig);

        public default Object sparkSqlRowValue(GenericInternalRow row, int pos)
        {
            // we need to convert native types to TestRow types
            return row.isNullAt(pos) ? null : toTestRowType(nativeSparkSqlRowValue(row, pos));
        }

        public Object nativeSparkSqlRowValue(GenericInternalRow row, int pos);

        public default Object sparkSqlRowValue(Row row, int pos)
        {
            // we need to convert native types to TestRow types
            return row.isNullAt(pos) ? null : toTestRowType(nativeSparkSqlRowValue(row, pos));
        }

        public Object nativeSparkSqlRowValue(Row row, int pos);
    }

    public interface SparkFrozen extends SparkCqlType
    {
        public SparkCqlType inner();
    }

    public interface Freezable
    {
        SparkCqlField.SparkFrozen freeze();
    }

    public interface SparkCollection extends SparkCqlType, Freezable
    {
        List<SparkCqlType> sparkTypes();

        SparkCqlType sparkType();

        SparkCqlType sparkType(int i);
    }

    public interface SparkSet extends SparkCollection
    {

    }

    public interface SparkList extends SparkCollection
    {

    }

    public interface SparkMap extends SparkCollection
    {
        SparkCqlType keyType();

        SparkCqlType valueType();
    }

    public interface SparkUdt extends SparkCqlType, Freezable
    {
        public SparkCqlField field(int pos);

        SparkCqlField field(String name);

        List<SparkCqlField> sparkFields();

        ByteBuffer serializeUdt(final Map<String, Object> values);

        Map<String, Object> deserializeUdt(final ByteBuffer buf, final boolean isFrozen);
    }

    public interface SparkUdtBuilder
    {
        SparkUdtBuilder withField(String name, SparkCqlType type);

        SparkUdt build();
    }

    public interface SparkTuple extends SparkCollection
    {
        ByteBuffer serializeTuple(final Object[] values);

        Object[] deserializeTuple(final ByteBuffer buf, final boolean isFrozen);
    }

    // helper mix-ins

    public interface StringTraits extends SparkCqlType
    {
        @Override
        public default int compareTo(Object o1, Object o2)
        {
            return STRING_COMPARATOR.compare(o1.toString(), o2.toString());
        }

        @Override
        public default boolean equalsTo(Object o1, Object o2)
        {
            // UUID comparator is particularly slow because of UUID.fromString
            // so compare for equality as strings
            return o1.equals(o2);
        }

        @Override
        public default Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
        {
            return UTF8String.fromString(o.toString()); // UTF8String
        }

        @Override
        public default Object toTestRowType(Object value)
        {
            if (value instanceof UTF8String)
            {
                return ((UTF8String) value).toString();
            }
            return value;
        }

        @Override
        public default Object nativeSparkSqlRowValue(Row row, int pos)
        {
            return row.getString(pos);
        }

        @Override
        public default Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
        {
            return row.getString(pos);
        }

        @Override
        public default DataType sparkSqlType(BigNumberConfig bigNumberConfig)
        {
            return DataTypes.StringType;
        }
    }

    public interface UUIDTraits extends StringTraits
    {
        @Override
        public default int compareTo(Object o1, Object o2)
        {
            return UUID_COMPARATOR.compare(o1.toString(), o2.toString());
        }

        @Override
        public default Object toTestRowType(Object value)
        {
            return java.util.UUID.fromString(value.toString());
        }
    }

    public interface BinaryTraits extends SparkCqlType
    {
        @Override
        public default int compareTo(Object o1, Object o2)
        {
            return BYTE_ARRAY_COMPARATOR.compare((byte[]) o1, (byte[]) o2);
        }

        @Override
        public default DataType sparkSqlType(BigNumberConfig bigNumberConfig)
        {
            return DataTypes.BinaryType;
        }

        @Override
        public default Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
        {
            return row.getBinary(pos);
        }

        @Override
        public default Object nativeSparkSqlRowValue(Row row, int pos)
        {
            return row.getAs(pos);
        }
    }

    public interface LongTraits extends SparkCqlType
    {
        @Override
        public default int compareTo(Object o1, Object o2)
        {
            return LONG_COMPARATOR.compare((Long) o1, (Long) o2);
        }

        @Override
        public default DataType sparkSqlType(BigNumberConfig bigNumberConfig)
        {
            return DataTypes.LongType;
        }

        @Override
        public default Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
        {
            return row.getLong(pos);
        }

        @Override
        public default Object nativeSparkSqlRowValue(Row row, int pos)
        {
            return row.getLong(pos);
        }
    }

    public interface DecimalTraits extends SparkCqlType
    {
        @Override
        public default int compareTo(Object o1, Object o2)
        {
            return DECIMAL_COMPARATOR.compare((org.apache.spark.sql.types.Decimal) o1, (org.apache.spark.sql.types.Decimal) o2);
        }
    }

    public interface IntTrait extends SparkCqlType
    {
        @Override
        public default DataType sparkSqlType(BigNumberConfig bigNumberConfig)
        {
            return DataTypes.IntegerType;
        }

        @Override
        public default Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
        {
            return row.getInt(pos);
        }

        @Override
        public default Object nativeSparkSqlRowValue(Row row, int pos)
        {
            return row.getInt(pos);
        }

        @Override
        public default int compareTo(Object o1, Object o2)
        {
            return INTEGER_COMPARATOR.compare((Integer) o1, (Integer) o2);
        }
    }

    public interface ComplexTrait extends SparkCqlType
    {
        @Override
        public default Object nativeSparkSqlRowValue(GenericInternalRow row, int pos)
        {
            throw new IllegalStateException("nativeSparkSqlRowValue should not be called for collection");
        }

        @Override
        public default Object nativeSparkSqlRowValue(Row row, int pos)
        {
            throw new IllegalStateException("nativeSparkSqlRowValue should not be called for collection");
        }
    }

    @SuppressWarnings("unchecked")
    public interface ListTrait extends SparkCollection
    {
        @Override
        public default DataType sparkSqlType(BigNumberConfig bigNumberConfig)
        {
            return DataTypes.createArrayType(sparkType().sparkSqlType(bigNumberConfig));
        }

        @SuppressWarnings("unchecked")
        @Override
        public default Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
        {
            return ArrayData.toArrayData(((Collection<Object>) o)
                                         .stream()
                                         .map(a -> sparkType().toSparkSqlType(a)).toArray());
        }

        @Override
        public default Object sparkSqlRowValue(GenericInternalRow row, int pos)
        {
            return Arrays.stream(row.getArray(pos).array())
                         .map(o -> sparkType().toTestRowType(o))
                         .collect(collector());
        }

        @Override
        public default Object sparkSqlRowValue(Row row, int pos)
        {
            return row.getList(pos).stream()
                      .map(o -> sparkType().toTestRowType(o))
                      .collect(collector());
        }

        @SuppressWarnings({ "unchecked", "RedundantCast" }) // redundant cast to (Object[]) is required
        @Override
        public default Object toTestRowType(Object value)
        {
            return Stream.of((Object[]) ((WrappedArray<Object>) value).array())
                         .map(v -> sparkType().toTestRowType(v))
                         .collect(Collectors.toList());
        }

        @SuppressWarnings("rawtypes")
        public default Collector collector()
        {
            return Collectors.toList();
        }

        @Override
        public default boolean equals(Object o1, Object o2)
        {
            return SparkCqlField.equalsArrays(((GenericArrayData) o1).array(), ((GenericArrayData) o2).array(), (pos) -> sparkType());
        }

        @Override
        public default int compare(Object o1, Object o2)
        {
            return SparkCqlField.compareArrays(((GenericArrayData) o1).array(), ((GenericArrayData) o2).array(), (pos) -> sparkType());
        }
    }

    public interface NotImplementedTrait extends SparkCqlType
    {
        public default DataType sparkSqlType(BigNumberConfig bigNumberConfig)
        {
            throw CqlField.notImplemented(this);
        }

        public default Object nativeSparkSqlRowValue(GenericInternalRow row, int pos)
        {
            throw CqlField.notImplemented(this);
        }

        public default Object nativeSparkSqlRowValue(Row row, int pos)
        {
            return row.isNullAt(pos) ? null : toTestRowType(nativeSparkSqlRowValue(row, pos));
        }
    }
}
