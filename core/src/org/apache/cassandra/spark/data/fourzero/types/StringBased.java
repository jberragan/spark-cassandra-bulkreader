package org.apache.cassandra.spark.data.fourzero.types;

import org.apache.cassandra.spark.data.fourzero.NativeType;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.SettableByIndexData;
import org.apache.cassandra.spark.utils.RandomUtils;

import org.apache.commons.lang3.RandomStringUtils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Comparator;

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

public abstract class StringBased extends NativeType
{
    private static final Comparator<String> STRING_COMPARATOR = String::compareTo;

    @Override
    public Object toSparkSqlType(Object o, boolean isFrozen)
    {
        return UTF8String.fromString(o.toString()); // UTF8String
    }

    @Override
    protected int compareTo(Object o1, Object o2)
    {
        return STRING_COMPARATOR.compare(o1.toString(), o2.toString());
    }

    @Override
    protected boolean equalsTo(Object o1, Object o2)
    {
        // UUID comparator is particularly slow because of UUID.fromString
        // so compare for equality as strings
        return o1.equals(o2);
    }

    @Override
    public DataType sparkSqlType(CassandraBridge.BigNumberConfig bigNumberConfig)
    {
        return DataTypes.StringType;
    }

    @Override
    public Object toTestRowType(Object value)
    {
        if (value instanceof UTF8String)
        {
            return ((UTF8String) value).toString();
        }
        return value;
    }

    @Override
    protected Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getString(pos);
    }

    @Override
    protected Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getString(pos);
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return RandomStringUtils.randomAlphanumeric(RandomUtils.randomPositiveInt(32));
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int pos, Object value)
    {
        udtValue.setString(pos, (String) value);
    }
}
