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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

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

public class CqlUdt extends org.apache.cassandra.spark.data.fourzero.complex.CqlUdt implements SparkCqlField.ComplexTrait, SparkCqlField.SparkUdt
{
    public CqlUdt(CqlField.CqlUdt udt)
    {
        super(udt.keyspace(), udt.name(), udt.fields().stream().map(FourZeroSparkCqlField::decorate).collect(Collectors.toList()));
    }

    public CqlUdt(String keyspace, String name, List<SparkCqlField> fields)
    {
        super(keyspace, name, SparkCqlField.cast(fields));
    }

    public static class Builder implements SparkCqlField.SparkUdtBuilder
    {
        private final String keyspace, name;
        private final List<SparkCqlField> fields = new ArrayList<>();

        public Builder(final String keyspace, final String name)
        {
            this.keyspace = keyspace;
            this.name = name;
        }

        @Override
        public CqlUdt.Builder withField(final String name, final SparkCqlField.SparkCqlType type)
        {
            this.fields.add(new SparkCqlField(false, false, false, name, type, this.fields.size()));
            return this;
        }

        @Override
        public SparkCqlField.SparkUdt build()
        {
            return new CqlUdt(keyspace, name, fields);
        }
    }

    public SparkCqlField field(int i)
    {
        return (SparkCqlField) super.field(i);
    }

    public SparkCqlField field(String name)
    {
        return (SparkCqlField) super.field(name);
    }

    public List<SparkCqlField> sparkFields()
    {
        return SparkCqlField.castToSpark(super.fields());
    }

    @Override
    public SparkCqlField.SparkCqlType type(int i)
    {
        return (SparkCqlField.SparkCqlType) super.type(i);
    }

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public Object deserialize(ByteBuffer buf, boolean isFrozen)
    {
        return udtToSparkSqlType((Map<String, Object>) deserializeToJava(buf, isFrozen));
    }

    @Override
    public Map<String, Object> deserializeUdt(final ByteBuffer buf, final boolean isFrozen)
    {
        if (!isFrozen)
        {
            final int fieldCount = buf.getInt();
            Preconditions.checkArgument(fieldCount == size(),
                                        String.format("Unexpected number of fields deserializing UDT '%s', expected %d fields but %d found", cqlName(), size(), fieldCount));
        }

        final Map<String, Object> result = new HashMap<>(size());
        for (final SparkCqlField field : sparkFields())
        {
            if (buf.remaining() < 4)
            {
                break;
            }
            final int len = buf.getInt();
            result.put(field.name(), len <= 0 ? null : field.deserialize(ByteBufUtils.readBytes(buf, len), isFrozen));
        }

        return result;
    }

    @Override
    public DataType sparkSqlType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createStructType(
        sparkFields().stream()
                     .map(f -> DataTypes.createStructField(f.name(), f.type().sparkSqlType(bigNumberConfig), true))
                     .toArray(StructField[]::new)
        );
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int pos)
    {
        final InternalRow struct = row.getStruct(pos, size());
        return IntStream.range(0, size()).boxed()
                        .collect(Collectors.toMap(
                        i -> field(i).name(),
                        i -> type(i).toTestRowType(struct.get(i, type(i).sparkSqlType())
                        )));
    }

    @Override
    public Object sparkSqlRowValue(Row row, int pos)
    {
        final Row struct = row.getStruct(pos);
        return IntStream.range(0, struct.size()).boxed()
                        .filter(i -> !struct.isNullAt(i))
                        .collect(Collectors.toMap(
                        i -> struct.schema().fields()[i].name(),
                        i -> field(i).type().toTestRowType(struct.get(i))
                        ));
    }

    @Override
    public Object toTestRowType(Object value)
    {
        final GenericRowWithSchema row = (GenericRowWithSchema) value;
        final String[] fieldNames = row.schema().fieldNames();
        final Map<String, Object> result = new HashMap<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++)
        {
            result.put(fieldNames[i], field(i).type().toTestRowType(row.get(i)));
        }
        return result;
    }

    @Override
    public Object toSparkSqlType(@NotNull Object o, final boolean isFrozen)
    {
        return udtToSparkSqlType(o, isFrozen);
    }

    @SuppressWarnings("unchecked")
    private GenericInternalRow udtToSparkSqlType(final Object o, final boolean isFrozen)
    {
        if (o instanceof ByteBuffer)
        {
            // need to deserialize first, e.g. if udt is frozen inside collections
            return udtToSparkSqlType(deserializeUdt((ByteBuffer) o, isFrozen));
        }
        return udtToSparkSqlType((Map<String, Object>) o);
    }

    private GenericInternalRow udtToSparkSqlType(final Map<String, Object> o)
    {
        final Object[] ar = new Object[size()];
        for (int i = 0; i < size(); i++)
        {
            ar[i] = o.getOrDefault(field(i).name(), null);
        }
        return new GenericInternalRow(ar);
    }

    public SparkCqlField.SparkFrozen freeze()
    {
        return new CqlFrozen(this);
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
