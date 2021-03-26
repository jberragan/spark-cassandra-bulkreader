package org.apache.cassandra.spark.data.fourzero.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;

import org.apache.cassandra.spark.data.fourzero.NativeType;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.DecimalType;
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

public class Decimal extends NativeType
{
    public static final Decimal INSTANCE = new Decimal();
    private static final Comparator<org.apache.spark.sql.types.Decimal> DECIMAL_COMPARATOR = Comparator.naturalOrder();

    @Override
    public String name()
    {
        return "decimal";
    }

    @Override
    public DataType sparkSqlType(CassandraBridge.BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createDecimalType(bigNumberConfig.bigDecimalPrecision(), bigNumberConfig.bigDecimalScale());
    }

    @Override
    public AbstractType<?> dataType()
    {
        return DecimalType.instance;
    }

    @Override
    public Object toSparkSqlType(Object o, boolean isFrozen)
    {
        return org.apache.spark.sql.types.Decimal.apply((BigDecimal) o);
    }

    @Override
    protected int compareTo(Object o1, Object o2)
    {
        return DECIMAL_COMPARATOR.compare((org.apache.spark.sql.types.Decimal) o1, (org.apache.spark.sql.types.Decimal) o2);
    }

    @Override
    protected Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getDecimal(pos, CassandraBridge.BigNumberConfig.DEFAULT.bigIntegerPrecision(), CassandraBridge.BigNumberConfig.DEFAULT.bigIntegerScale());
    }

    @Override
    protected Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getDecimal(pos);
    }

    @Override
    public Object toTestRowType(Object value)
    {
        if (value instanceof BigDecimal)
        {
            return value;
        }
        return ((org.apache.spark.sql.types.Decimal) value).toJavaBigDecimal();
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        final BigInteger unscaledVal = new BigInteger(CassandraBridge.BigNumberConfig.DEFAULT.bigDecimalPrecision(), RandomUtils.RANDOM);
        final int scale = RandomUtils.RANDOM.nextInt(CassandraBridge.BigNumberConfig.DEFAULT.bigDecimalScale());
        return new BigDecimal(unscaledVal, scale);
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int pos, Object value)
    {
        udtValue.setDecimal(pos, (BigDecimal) value);
    }

    @Override
    public org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.DataType.decimal();
    }
}
