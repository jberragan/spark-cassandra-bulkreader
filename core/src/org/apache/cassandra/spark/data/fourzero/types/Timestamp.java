package org.apache.cassandra.spark.data.fourzero.types;

import org.apache.cassandra.spark.data.fourzero.NativeType;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.SettableByIndexData;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

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

public class Timestamp extends NativeType
{
    public static final Timestamp INSTANCE = new Timestamp();

    @Override
    public String name()
    {
        return "timestamp";
    }

    @Override
    public DataType sparkSqlType(CassandraBridge.BigNumberConfig bigNumberConfig)
    {
        return DataTypes.TimestampType;
    }

    @Override
    public AbstractType<?> dataType()
    {
        return TimestampType.instance;
    }

    @Override
    public Object toSparkSqlType(Object o, boolean isFrozen)
    {
        return ((java.util.Date) o).getTime() * 1000L; // long
    }

    @Override
    protected int compareTo(Object o1, Object o2)
    {
        return BigInt.LONG_COMPARATOR.compare((Long) o1, (Long) o2);
    }

    @Override
    protected Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getLong(pos);
    }

    @Override
    protected Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return new java.util.Date(row.getTimestamp(pos).getTime());
    }

    @Override
    public Object toTestRowType(Object value)
    {
        if (value instanceof java.util.Date)
        {
            return value;
        }
        return new java.util.Date((long) value / 1000L);
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int pos, Object value)
    {
        udtValue.setTimestamp(pos, (java.util.Date) value);
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return new java.util.Date();
    }

    @Override
    public org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.DataType.timestamp();
    }
}
