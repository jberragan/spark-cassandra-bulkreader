package org.apache.cassandra.spark.data.fourzero;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.CodecRegistry;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.DataType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TypeSerializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

import java.nio.ByteBuffer;

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

public abstract class FourZeroCqlType implements CqlField.CqlType
{

    public static final CodecRegistry CODEC_REGISTRY = new CodecRegistry();

    @Override
    public CassandraBridge.CassandraVersion version()
    {
        return CassandraBridge.CassandraVersion.FOURZERO;
    }

    abstract public AbstractType<?> dataType();

    abstract public AbstractType<?> dataType(boolean isMultiCell);

    @Override
    public Object deserialize(final ByteBuffer buf)
    {
        return deserialize(buf, false);
    }

    @Override
    public Object deserialize(final ByteBuffer buf, final boolean isFrozen)
    {
        return toSparkSqlType(serializer().deserialize(buf));
    }

    abstract public <T> TypeSerializer<T> serializer();

    @Override
    public ByteBuffer serialize(final Object value)
    {
        return serializer().serialize(value);
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        throw CqlField.notImplemented(this);
    }

    public DataType driverDataType()
    {
        return driverDataType(false);
    }

    public org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        throw CqlField.notImplemented(this);
    }

    @Override
    public org.apache.spark.sql.types.DataType sparkSqlType()
    {
        return sparkSqlType(CassandraBridge.BigNumberConfig.DEFAULT);
    }

    @Override
    public org.apache.spark.sql.types.DataType sparkSqlType(CassandraBridge.BigNumberConfig bigNumberConfig)
    {
        throw CqlField.notImplemented(this);
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int pos)
    {
        throw CqlField.notImplemented(this);
    }

    @Override
    public Object sparkSqlRowValue(Row row, int pos)
    {
        throw CqlField.notImplemented(this);
    }

    // set inner value for UDTs or Tuples
    public void setInnerValue(SettableByIndexData<?> udtValue, int pos, Object value)
    {
        throw CqlField.notImplemented(this);
    }

    @Override
    public String toString()
    {
        return this.cqlName();
    }

    @Override
    public int cardinality(int orElse)
    {
        return orElse;
    }

    @Override
    public Object toTestRowType(Object value)
    {
        return value;
    }
}
