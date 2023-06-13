package org.apache.cassandra.spark.data.fourzero.complex;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.fourzero.FourZeroCqlType;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.TupleHelper;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.TupleValue;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TupleType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TupleSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.utils.ByteBufUtils;

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
    public CqlTuple(final CqlField.CqlType... types)
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
    public ByteBuffer serialize(Object value)
    {
        return serializeTuple((Object[]) value);
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
            result[pos++] = len <= 0 ? null : type.deserializeToJava(ByteBufUtils.readBytes(buf, len), isFrozen);
        }
        return result;
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int pos, Object value)
    {
        udtValue.setTupleValue(pos, toTupleValue(CassandraVersion.FOURZERO, this, value));
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
    public Object convertForCqlWriter(Object value, CassandraVersion version)
    {
        return toTupleValue(version, this, value);
    }

    public static TupleValue toTupleValue(final CassandraVersion version, final CqlTuple tuple, final Object value)
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
