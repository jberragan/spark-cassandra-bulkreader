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

package org.apache.cassandra.spark.data.fourzero.types.spark;

import java.util.Comparator;

import org.apache.cassandra.spark.data.SparkCqlField;
import org.apache.cassandra.spark.reader.BigNumberConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class TinyInt extends org.apache.cassandra.spark.data.fourzero.types.TinyInt implements SparkCqlField.SparkCqlType
{
    public static TinyInt INSTANCE = new TinyInt();
    private static final Comparator<Byte> BYTE_COMPARATOR = TinyInt::compareBytes;

    @Override
    public DataType sparkSqlType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.ByteType;
    }

    @Override
    public Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getByte(pos);
    }

    @Override
    public Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getByte(pos);
    }

    @Override
    public int compareTo(Object o1, Object o2)
    {
        return BYTE_COMPARATOR.compare((Byte) o1, (Byte) o2);
    }

    private static int compareBytes(final byte a, final byte b)
    {
        return a - b; // safe due to restricted range
    }
}
