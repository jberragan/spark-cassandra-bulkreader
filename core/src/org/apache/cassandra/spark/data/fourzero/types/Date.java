package org.apache.cassandra.spark.data.fourzero.types;

import org.apache.cassandra.spark.data.fourzero.NativeType;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.CodecUtils;
import org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.LocalDate;
import org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.SettableByIndexData;
import org.apache.cassandra.spark.utils.RandomUtils;
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

public class Date extends NativeType
{
    public static final Date INSTANCE = new Date();

    @Override
    public String name()
    {
        return "date";
    }

    @Override
    public DataType sparkSqlType(CassandraBridge.BigNumberConfig bigNumberConfig)
    {
        return DataTypes.DateType;
    }

    @Override
    public AbstractType<?> dataType()
    {
        return SimpleDateType.instance;
    }

    @Override
    protected int compareTo(Object o1, Object o2)
    {
        return Int.INTEGER_COMPARATOR.compare((Integer) o1, (Integer) o2);
    }

    @Override
    protected Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getInt(pos);
    }

    @Override
    protected Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getDate(pos);
    }

    @Override
    public Object toTestRowType(Object value)
    {
        if (value instanceof java.sql.Date)
        {
            // round up to convert date back to days since epoch
            return (int) ((java.sql.Date) value).toLocalDate().toEpochDay();
        }
        return value;
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return RandomUtils.randomPositiveInt(30000);
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int pos, Object value)
    {
        udtValue.setDate(pos, (LocalDate) value);
    }

    @Override
    public org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.DataType.date();
    }

    @Override
    public Object convertForCqlWriter(Object value, CassandraBridge.CassandraVersion version)
    {
        // 4.0 no longer allows writing date types as Integers in CqlWriter
        // so we need to convert to LocalDate before writing in tests
        if (version == CassandraBridge.CassandraVersion.FOURZERO)
        {
            return LocalDate.fromDaysSinceEpoch(CodecUtils.fromUnsignedToSignedInt((int) value));
        }
        return value;
    }
}
