package org.apache.cassandra.spark.data.fourzero.types;

import java.util.Comparator;

import org.apache.cassandra.spark.data.fourzero.NativeType;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ShortType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.SettableByIndexData;
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

public class SmallInt extends NativeType
{
    public static final SmallInt INSTANCE = new SmallInt();
    private static final Comparator<Short> SHORT_COMPARATOR = Short::compare;

    @Override
    public String name()
    {
        return "smallint";
    }

    @Override
    public DataType sparkSqlType(CassandraBridge.BigNumberConfig bigNumberConfig)
    {
        return DataTypes.ShortType;
    }

    @Override
    public AbstractType<?> dataType()
    {
        return ShortType.instance;
    }

    @Override
    protected int compareTo(Object o1, Object o2)
    {
        return SHORT_COMPARATOR.compare((Short) o1, (Short) o2);
    }

    @Override
    protected Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getShort(pos);
    }

    @Override
    protected Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getShort(pos);
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return (short) RandomUtils.RANDOM.nextInt(Short.MAX_VALUE + 1);
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int pos, Object value)
    {
        udtValue.setShort(pos, (short) value);
    }

    @Override
    public org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.DataType.smallint();
    }
}
