package org.apache.cassandra.spark.data.fourzero.complex;

import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.fourzero.FourZeroCqlType;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.SettableByIndexData;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;

import java.nio.ByteBuffer;
import java.util.Set;

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

public class CqlFrozen extends FourZeroCqlType implements CqlField.CqlFrozen
{
    private final CqlField.CqlType inner;
    private final int hashCode;

    public CqlFrozen(final CqlField.CqlType inner)
    {
        this.inner = inner;
        this.hashCode = new HashCodeBuilder(83, 89)
                        .append(internalType().ordinal())
                        .append(inner)
                        .toHashCode();
    }

    public static CqlFrozen build(final CqlField.CqlType inner)
    {
        return new CqlFrozen(inner);
    }

    @Override
    public boolean isSupported()
    {
        return true;
    }

    @Override
    public AbstractType<?> dataType()
    {
        return ((FourZeroCqlType) inner()).dataType(false); // if frozen collection then isMultiCell = false
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        return dataType();
    }

    @Override
    public Object toSparkSqlType(Object o)
    {
        return inner().toSparkSqlType(o, true);
    }

    @Override
    public Object toSparkSqlType(Object o, boolean isFrozen)
    {
        return toSparkSqlType(o);
    }

    @Override
    public <T> TypeSerializer<T> serializer()
    {
        return ((FourZeroCqlType) inner()).serializer();
    }

    @Override
    public Object deserialize(ByteBuffer buf)
    {
        return inner().deserialize(buf, true);
    }

    @Override
    public Object deserialize(ByteBuffer buf, boolean isFrozen)
    {
        return deserialize(buf);
    }

    @Override
    public ByteBuffer serialize(Object value)
    {
        return inner().serialize(value);
    }

    @Override
    public boolean equals(Object o1, Object o2)
    {
        return inner().equals(o1, o2);
    }

    public InternalType internalType()
    {
        return InternalType.Frozen;
    }

    @Override
    public String name()
    {
        return "frozen";
    }

    public CqlField.CqlType inner()
    {
        return inner;
    }

    public String cqlName()
    {
        return String.format("frozen<%s>", this.inner.cqlName());
    }

    @Override
    public DataType sparkSqlType(CassandraBridge.BigNumberConfig bigNumberConfig)
    {
        return inner.sparkSqlType(bigNumberConfig);
    }

    @Override
    public Set<CqlField.CqlUdt> udts()
    {
        return inner.udts();
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int pos)
    {
        return inner.sparkSqlRowValue(row, pos);
    }

    @Override
    public Object sparkSqlRowValue(Row row, int pos)
    {
        return inner.sparkSqlRowValue(row, pos);
    }

    @Override
    public Object toTestRowType(Object value)
    {
        return inner.toTestRowType(value);
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int pos, Object value)
    {
        ((FourZeroCqlType) inner()).setInnerValue(udtValue, pos, value);
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return inner.randomValue(minCollectionSize);
    }

    @Override
    public org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return ((FourZeroCqlType) inner()).driverDataType(true);
    }

    @Override
    public Object convertForCqlWriter(Object value, CassandraBridge.CassandraVersion version)
    {
        return inner.convertForCqlWriter(value, version);
    }

    @Override
    public void write(Output output)
    {
        CqlField.CqlType.write(this, output);
        inner.write(output);
    }

    @Override
    public String toString()
    {
        return this.cqlName();
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }

    @Override
    public int compare(Object o1, Object o2)
    {
        return inner.compare(o1, o2);
    }

    @Override
    public boolean equals(Object obj)
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

        final CqlFrozen rhs = (CqlFrozen) obj;
        return new EqualsBuilder()
               .append(internalType(), rhs.internalType())
               .append(this.inner, rhs.inner)
               .isEquals();
    }
}
