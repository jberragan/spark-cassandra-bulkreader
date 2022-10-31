package org.apache.cassandra.spark.data.fourzero.complex;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.fourzero.FourZeroCqlType;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.TupleHelper;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TupleType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TupleSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.TupleValue;
import org.apache.cassandra.spark.utils.ByteBufUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

public class CqlTuple extends CqlCollection implements CqlField.CqlTuple
{
    CqlTuple(final CqlField.CqlType... types)
    {
        super(types);
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        return new TupleType(types()
                             .stream()
                             .map(type -> (FourZeroCqlType) type)
                             .map(FourZeroCqlType::dataType)
                             .collect(Collectors.toList()));
    }

    @Override
    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        if (o instanceof ByteBuffer)
        {
            // need to deserialize first, e.g. if tuple is frozen inside collections
            return (GenericInternalRow) this.deserialize((ByteBuffer) o);
        }
        return new GenericInternalRow((Object[]) o);
    }

    @Override
    public ByteBuffer serialize(Object value)
    {
        return serializeTuple((Object[]) value);
    }

    @Override
    public boolean equals(Object o1, Object o2)
    {
        return CqlField.equalsArrays(((GenericInternalRow) o1).values(), ((GenericInternalRow) o2).values(), this::type);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> TypeSerializer<T> serializer()
    {
        return (TypeSerializer<T>) new TupleSerializer(
        types().stream()
               .map(type -> (FourZeroCqlType) type)
               .map(FourZeroCqlType::serializer)
               .collect(Collectors.toList())
        );
    }

    @Nullable
    @Override @SuppressWarnings("uncheck")
    public Object deserialize(ByteBuffer buf, boolean isFrozen)
    {
        final Object[] tuple = (Object[]) deserializeToJava(buf, isFrozen);
        return tuple == null ? null : toSparkSqlType(tuple);
    }

    @Override
    public Object deserializeToJava(ByteBuffer buf, boolean isFrozen)
    {
        return deserializeTuple(buf, isFrozen);
    }

    @Override
    public InternalType internalType()
    {
        return InternalType.Tuple;
    }

    @Override
    public String name()
    {
        return "tuple";
    }

    @Override
    public DataType sparkSqlType(CassandraBridge.BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createStructType(
        IntStream.range(0, size())
                 .mapToObj(i -> DataTypes.createStructField(Integer.toString(i), type(i).sparkSqlType(bigNumberConfig), true))
                 .toArray(StructField[]::new)
        );
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int pos)
    {
        final InternalRow tupleStruct = row.getStruct(pos, size());
        return IntStream.range(0, size()).boxed()
                        .map(i -> type(i).toTestRowType(tupleStruct.get(i, type(i).sparkSqlType())))
                        .toArray();
    }

    @Override
    public Object sparkSqlRowValue(Row row, int pos)
    {
        final Row tupleStruct = row.getStruct(pos);
        return IntStream.range(0, tupleStruct.size()).boxed()
                        .filter(i -> !tupleStruct.isNullAt(i))
                        .map(i -> type(i).toTestRowType(tupleStruct.get(i)))
                        .toArray();
    }

    public ByteBuffer serializeTuple(final Object[] values)
    {
        final List<ByteBuffer> bufs = IntStream.range(0, size())
                                               .mapToObj(i -> type(i).serialize(values[i]))
                                               .collect(Collectors.toList());
        final ByteBuffer result = ByteBuffer.allocate(bufs.stream().map(Buffer::remaining).map(a -> a + 4).reduce(Integer::sum).orElse(0));
        for (final ByteBuffer buf : bufs)
        {
            result.putInt(buf.remaining()); // len
            result.put(buf.duplicate()); // value
        }
        return (ByteBuffer) result.flip();
    }

    public Object[] deserializeTuple(final ByteBuffer buf, final boolean isFrozen)
    {
        final Object[] result = new Object[size()];
        int pos = 0;
        for (final CqlField.CqlType type : types())
        {
            if (buf.remaining() < 4)
            {
                break;
            }
            final int len = buf.getInt();
            result[pos++] = len <= 0 ? null : type.deserialize(ByteBufUtils.readBytes(buf, len), isFrozen);
        }
        return result;
    }

    @Override
    public int compare(Object o1, Object o2)
    {
        return CqlField.compareArrays(((GenericInternalRow) o1).values(), ((GenericInternalRow) o2).values(), this::type);
    }

    @Override
    public Object toTestRowType(Object value)
    {
        final GenericRowWithSchema tupleRow = (GenericRowWithSchema) value;
        final Object[] tupleResult = new Object[tupleRow.size()];
        for (int i = 0; i < tupleRow.size(); i++)
        {
            tupleResult[i] = type(i).toTestRowType(tupleRow.get(i));
        }
        return tupleResult;
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int pos, Object value)
    {
        udtValue.setTupleValue(pos, toTupleValue(CassandraBridge.CassandraVersion.FOURZERO, this, value));
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return types().stream().map(t -> t.randomValue(minCollectionSize)).toArray();
    }

    @Override
    public org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return TupleHelper.buildTupleType(this, isFrozen);
    }

    @Override
    public Object convertForCqlWriter(Object value, CassandraBridge.CassandraVersion version)
    {
        return toTupleValue(version, this, value);
    }

    public static TupleValue toTupleValue(final CassandraBridge.CassandraVersion version, final CqlTuple tuple, final Object value)
    {
        if (value instanceof TupleValue)
        {
            return (TupleValue) value;
        }

        final TupleValue tupleValue = TupleHelper.buildTupleValue(tuple);
        final Object[] ar = (Object[]) value;
        for (int pos = 0; pos < ar.length; pos++)
        {
            CqlUdt.setInnerValue(version, tupleValue, (FourZeroCqlType) tuple.type(pos), pos, ar[pos]);
        }
        return tupleValue;
    }
}
