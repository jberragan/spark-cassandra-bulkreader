package org.apache.cassandra.spark.data;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.NotImplementedException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
@SuppressWarnings({ "WeakerAccess", "unused" })
public class CqlField implements Serializable, Comparable<CqlField>
{

    public static final long serialVersionUID = 42L;

    public interface CqlType extends Serializable, Comparator<Object>
    {
        enum InternalType
        {
            NativeCql, Set, List, Map, Frozen, Udt, Tuple;

            public static InternalType fromString(final String name)
            {
                switch (name.toLowerCase())
                {
                    case "set":
                        return Set;
                    case "list":
                        return List;
                    case "map":
                        return Map;
                    case "tuple":
                        return Tuple;
                    case "udt":
                        return Udt;
                    case "frozen":
                        return Frozen;
                    default:
                        return NativeCql;
                }
            }
        }

        boolean isSupported();

        Object toSparkSqlType(@NotNull Object o);

        Object toSparkSqlType(@NotNull Object o, boolean isFrozen);

        @Nullable
        Object deserialize(final ByteBuffer buf);

        @Nullable
        Object deserialize(final ByteBuffer buf, final boolean isFrozen);

        @Nullable
        Object deserializeToJava(final ByteBuffer buf);

        @Nullable
        Object deserializeToJava(final ByteBuffer buf, final boolean isFrozen);

        ByteBuffer serialize(final Object value);

        boolean equals(Object o1, Object o2);

        CassandraBridge.CassandraVersion version();

        InternalType internalType();

        String name();

        String cqlName();

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
        DataType sparkSqlType();

        DataType sparkSqlType(CassandraBridge.BigNumberConfig bigNumberConfig);

        void write(final Output output);

        Set<CqlField.CqlUdt> udts();

        @VisibleForTesting
        int cardinality(int orElse);

        @VisibleForTesting
        Object sparkSqlRowValue(final GenericInternalRow row, final int pos);

        @VisibleForTesting
        Object sparkSqlRowValue(final Row row, final int pos);

        @VisibleForTesting
        Object randomValue(int minCollectionSize);

        @VisibleForTesting
        Object toTestRowType(Object value);

        @VisibleForTesting
        Object convertForCqlWriter(final Object value, final CassandraBridge.CassandraVersion version);

        static void write(CqlType type, final Output out)
        {
            out.writeInt(type.version().ordinal());
            out.writeInt(type.internalType().ordinal());
        }

        static CqlType read(final Input input)
        {
            final CassandraBridge.CassandraVersion version = CassandraBridge.CassandraVersion.values()[input.readInt()];
            final InternalType internalType = InternalType.values()[input.readInt()];
            return CassandraBridge.get(version).readType(internalType, input);
        }
    }

    public interface NativeType extends CqlType
    {
    }

    public interface CqlCollection extends CqlType
    {
        CqlFrozen frozen();

        List<CqlType> types();

        CqlField.CqlType type();

        CqlField.CqlType type(int i);
    }

    public interface CqlMap extends CqlCollection
    {
        CqlField.CqlType keyType();

        CqlField.CqlType valueType();
    }

    public interface CqlSet extends CqlCollection
    {

    }

    public interface CqlList extends CqlCollection
    {

    }

    public interface CqlTuple extends CqlCollection
    {
        ByteBuffer serializeTuple(final Object[] values);

        Object[] deserializeTuple(final ByteBuffer buf, final boolean isFrozen);
    }

    public interface CqlFrozen extends CqlType
    {
        CqlField.CqlType inner();
    }

    public interface CqlUdt extends CqlType
    {
        CqlFrozen frozen();

        String createStmt(final String keyspace);

        String keyspace();

        List<CqlField> fields();

        CqlField field(String name);

        CqlField field(int i);

        ByteBuffer serializeUdt(final Map<String, Object> values);

        Map<String, Object> deserializeUdt(final ByteBuffer buf, final boolean isFrozen);
    }

    public interface CqlUdtBuilder
    {
        CqlUdtBuilder withField(String name, CqlField.CqlType type);

        CqlField.CqlUdt build();
    }

    public enum SortOrder
    {
        ASC, DESC
    }

    private final String name;
    private final boolean isPartitionKey, isClusteringColumn, isStaticColumn;
    private final CqlType type;
    private final int pos;

    public CqlField(final boolean isPartitionKey,
                    final boolean isClusteringColumn,
                    final boolean isStaticColumn,
                    final String name,
                    final CqlType type,
                    final int pos)
    {
        Preconditions.checkArgument(!(isPartitionKey && isClusteringColumn), "Field cannot be both partition key and clustering key");
        Preconditions.checkArgument(!(isPartitionKey && isStaticColumn), "Field cannot be both partition key and static column");
        Preconditions.checkArgument(!(isClusteringColumn && isStaticColumn), "Field cannot be both clustering key and static column");
        this.isPartitionKey = isPartitionKey;
        this.isClusteringColumn = isClusteringColumn;
        this.isStaticColumn = isStaticColumn;
        this.name = name.replaceAll("\"", "");
        this.type = type;
        this.pos = pos;
    }

    public boolean isPartitionKey()
    {
        return isPartitionKey;
    }

    public boolean isPrimaryKey()
    {
        return isPartitionKey || isClusteringColumn;
    }

    public boolean isClusteringColumn()
    {
        return isClusteringColumn;
    }


    public boolean isStaticColumn()
    {
        return isStaticColumn;
    }

    public boolean isValueColumn()
    {
        return !isPartitionKey && !isClusteringColumn && !isStaticColumn;
    }

    public boolean isNonValueColumn()
    {
        return !isValueColumn();
    }

    public String name()
    {
        return name;
    }

    public CqlType type()
    {
        return type;
    }

    @Nullable
    public Object deserialize(final ByteBuffer buf) {
        return deserialize(buf, false);
    }

    @Nullable
    public Object deserialize(final ByteBuffer buf, boolean isFrozen) {
        return type().deserialize(buf, isFrozen);
    }

    public ByteBuffer serialize(final Object value) {
        return type.serialize(value);
    }

    public String cqlTypeName()
    {
        return type.cqlName();
    }

    public int pos()
    {
        return pos;
    }

    @VisibleForTesting
    public CqlField cloneWithPos(final int pos)
    {
        return new CqlField(isPartitionKey, isClusteringColumn, isStaticColumn, name, type, pos);
    }

    @Override
    public String toString()
    {
        return name + " (" + type + ")";
    }

    @Override
    public int compareTo(@NotNull final CqlField o)
    {
        return Integer.compare(this.pos, o.pos);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(67, 71)
               .append(name)
               .append(isPartitionKey)
               .append(isClusteringColumn)
               .append(isStaticColumn)
               .append(type)
               .append(pos)
               .toHashCode();
    }

    @Override
    public boolean equals(final Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }
        if (obj.getClass() != getClass())
        {
            return false;
        }

        final CqlField rhs = (CqlField) obj;
        return new EqualsBuilder()
               .append(name, rhs.name)
               .append(isPartitionKey, rhs.isPartitionKey)
               .append(isClusteringColumn, rhs.isClusteringColumn)
               .append(isStaticColumn, rhs.isStaticColumn)
               .append(type, rhs.type)
               .append(pos, rhs.pos)
               .isEquals();
    }

    public boolean equals(final Object o1, final Object o2)
    {
        return type().equals(o1, o2);
    }

    public static boolean equalsArrays(final Object[] lhs, final Object[] rhs, final Function<Integer, CqlType> types)
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

    public static int compareArrays(final Object[] lhs, final Object[] rhs, final Function<Integer, CqlType> types)
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

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CqlField>
    {
        @Override
        public CqlField read(final Kryo kryo, final Input input, final Class type)
        {
            return new CqlField(input.readBoolean(),
                                input.readBoolean(),
                                input.readBoolean(),
                                input.readString(),
                                CqlType.read(input),
                                input.readInt());
        }

        @Override
        public void write(final Kryo kryo, final Output output, final CqlField field)
        {
            output.writeBoolean(field.isPartitionKey());
            output.writeBoolean(field.isClusteringColumn());
            output.writeBoolean(field.isStaticColumn());
            output.writeString(field.name());
            field.type().write(output);
            output.writeInt(field.pos());
        }
    }

    public static NotImplementedException notImplemented(final CqlType type)
    {
        return new NotImplementedException(type.toString() + " type not implemented");
    }
}
