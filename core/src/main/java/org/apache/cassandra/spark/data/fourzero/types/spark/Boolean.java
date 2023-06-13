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

public class Boolean extends org.apache.cassandra.spark.data.fourzero.types.Boolean implements SparkCqlField.SparkCqlType
{
    public static final Comparator<java.lang.Boolean> BOOLEAN_COMPARATOR = java.lang.Boolean::compareTo;
    public static Boolean INSTANCE = new Boolean();

    @Override
    public DataType sparkSqlType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.BooleanType;
    }

    @Override
    public Object nativeSparkSqlRowValue(final GenericInternalRow row, final int pos)
    {
        return row.getBoolean(pos);
    }

    @Override
    public Object nativeSparkSqlRowValue(Row row, int pos)
    {
        return row.getBoolean(pos);
    }

    @Override
    public int compareTo(Object o1, Object o2)
    {
        return BOOLEAN_COMPARATOR.compare((java.lang.Boolean) o1, (java.lang.Boolean) o2);
    }
}
