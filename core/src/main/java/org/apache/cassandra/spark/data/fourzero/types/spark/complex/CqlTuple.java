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

package org.apache.cassandra.spark.data.fourzero.types.spark.complex;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.SparkCqlField;
import org.apache.cassandra.spark.data.fourzero.types.spark.FourZeroSparkCqlField;
import org.apache.cassandra.spark.reader.BigNumberConfig;
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

public class CqlTuple extends org.apache.cassandra.spark.data.fourzero.complex.CqlTuple implements SparkCqlField.ComplexTrait, SparkCqlField.SparkTuple
{
    public CqlTuple(CqlField.CqlTuple tuple)
    {
        super(
        tuple.types().stream()
             .map(FourZeroSparkCqlField::decorate)
             .toArray(CqlField.CqlType[]::new)
        );
    }

    @Override
    public SparkCqlField.SparkCqlType type()
    {
        return (SparkCqlField.SparkCqlType) super.type();
    }

    @Override
    public SparkCqlField.SparkCqlType type(int i)
    {
        return (SparkCqlField.SparkCqlType) super.type(i);
    }

    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        if (o instanceof ByteBuffer)
        {
            // need to deserialize first, e.g. if tuple is frozen inside collections
            return this.deserialize((ByteBuffer) o);
        }
        return new GenericInternalRow((Object[]) o);
    }

    @Override
    public DataType sparkSqlType(BigNumberConfig bigNumberConfig)
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

    @Nullable
    @Override
    public Object deserialize(ByteBuffer buf, boolean isFrozen)
    {
        final Object[] tuple = (Object[]) deserializeToJava(buf, isFrozen);
        return tuple == null ? null : toSparkSqlType(tuple);
    }

    public Object[] deserializeTuple(final ByteBuffer buf, final boolean isFrozen)
    {
        final Object[] result = new Object[size()];
        int pos = 0;
        for (final SparkCqlField.SparkCqlType type : sparkTypes())
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

    public SparkCqlField.SparkFrozen freeze()
    {
        return new CqlFrozen(this);
    }

    public List<SparkCqlField.SparkCqlType> sparkTypes()
    {
        return super.types().stream()
                    .map(f -> (SparkCqlField.SparkCqlType) f)
                    .collect(Collectors.toList());
    }

    public SparkCqlField.SparkCqlType sparkType()
    {
        return (SparkCqlField.SparkCqlType) super.type();
    }

    public SparkCqlField.SparkCqlType sparkType(int i)
    {
        return (SparkCqlField.SparkCqlType) super.type(i);
    }

    @Override
    public boolean equals(Object o1, Object o2)
    {
        return SparkCqlField.equalsArrays(((GenericInternalRow) o1).values(), ((GenericInternalRow) o2).values(), this::type);
    }

    @Override
    public int compare(Object o1, Object o2)
    {
        return SparkCqlField.compareArrays(((GenericInternalRow) o1).values(), ((GenericInternalRow) o2).values(), this::type);
    }
}
