package org.apache.cassandra.spark.data.fourzero.types;

import com.google.common.primitives.UnsignedBytes;

import org.apache.cassandra.spark.data.fourzero.NativeType;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

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

public abstract class BinaryBased extends NativeType
{
    public static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR = UnsignedBytes.lexicographicalComparator();

    @Override
    protected int compareTo(Object o1, Object o2)
    {
        return BYTE_ARRAY_COMPARATOR.compare((byte[]) o1, (byte[]) o2);
    }

    @Override
    public DataType sparkSqlType(CassandraBridge.BigNumberConfig bigNumberConfig)
    {
        return DataTypes.BinaryType;
    }

    @Override
    protected Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getBinary(pos);
    }

    @Override
    protected Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getAs(pos);
    }
}
