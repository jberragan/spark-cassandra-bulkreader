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

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.SparkCqlField;
import org.apache.cassandra.spark.data.fourzero.types.spark.FourZeroSparkCqlField;
import org.apache.cassandra.spark.reader.BigNumberConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CqlFrozen extends org.apache.cassandra.spark.data.fourzero.complex.CqlFrozen implements SparkCqlField.ComplexTrait, SparkCqlField.SparkFrozen
{
    public CqlFrozen(CqlField.CqlType frozen)
    {
        super(FourZeroSparkCqlField.decorate(frozen));
    }

    @Override
    public SparkCqlField.SparkCqlType inner()
    {
        return (SparkCqlField.SparkCqlType) super.inner();
    }

    @Override
    public Object toSparkSqlType(@NotNull Object o)
    {
        return inner().toSparkSqlType(o, true);
    }

    @Override
    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return toSparkSqlType(o, false);
    }

    @Override
    public DataType sparkSqlType(BigNumberConfig bigNumberConfig)
    {
        return inner().sparkSqlType(bigNumberConfig);
    }

    @Nullable
    @Override
    public Object deserialize(ByteBuffer buf)
    {
        return inner().deserialize(buf, true);
    }

    @Nullable
    @Override
    public Object deserialize(ByteBuffer buf, boolean isFrozen)
    {
        return deserialize(buf);
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int pos)
    {
        return inner().sparkSqlRowValue(row, pos);
    }

    @Override
    public Object sparkSqlRowValue(Row row, int pos)
    {
        return inner().sparkSqlRowValue(row, pos);
    }

    @Override
    public Object toTestRowType(Object value)
    {
        return inner().toTestRowType(value);
    }

    @Override
    public boolean equals(Object o1, Object o2)
    {
        return inner().equals(o1, o2);
    }

    @Override
    public int compare(Object o1, Object o2)
    {
        return inner().compare(o1, o2);
    }
}
