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

package org.apache.cassandra.spark.cdc;

import java.nio.ByteBuffer;
import java.util.function.Function;

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkValueWithMetadata extends ValueWithMetadata implements SparkRowSource, SparkRowSink<SparkValueWithMetadata>
{
    private static final String FIELD_NAME = "Name";
    private  static final String FIELD_TYPE = "Type";
    private  static final String FIELD_VALUE = "Value";

    public static final StructType SCHEMA;
    static
    {
        // Given an example table, CREATE TABLE ( k int PRIMARY KEY, b text, c frozen<list<int>> ),
        // The possible values for 'name' are k, b and c.
        // The possible values for 'type' are int, text, and frozen<list<int>>.
        // The value for the 'value' field is the serialized value.

        // Using StructField array is merely to create StructType in one shot, in order to avoid creating new StructType
        // instances on every call of "add". The sequence in the array does not matter.
        StructField[] fieldAttributes = new StructField[3];
        int i = 0;
        // all fields must present
        fieldAttributes[i++] = DataTypes.createStructField(FIELD_NAME, DataTypes.StringType, false);
        fieldAttributes[i++] = DataTypes.createStructField(FIELD_TYPE, DataTypes.StringType, false);
        // note that value field is nullable. A null value indicates the value is deleted.
        fieldAttributes[i++] = DataTypes.createStructField(FIELD_VALUE, DataTypes.BinaryType, true);
        SCHEMA = DataTypes.createStructType(fieldAttributes);
    }
    public static final SparkValueWithMetadata EMPTY = of(null, null, null);

    public static SparkValueWithMetadata of(String columnName, String columnType, ByteBuffer value)
    {
        return new SparkValueWithMetadata(columnName, columnType, value);
    }

    private SparkValueWithMetadata(String columnName, String columnType, ByteBuffer value)
    {
        super(columnName, columnType, value);
    }

    /**
     * Get the corresponding CqlType based on columnType
     * @param typeMapping, a dictionary that maps string type to CqlType
     * @return the CqlType of the
     */
    public CqlField.CqlType getCqlType(Function<String, CqlField.CqlType> typeMapping)
    {
        // The possible complex values of columnType
        // frozen<list<int>>
        // list<frozen<set<int>>>
        // frozen<map<int, int>>
        // map<int, frozen<my_udt>>
        return typeMapping.apply(columnType);
    }

    @Override
    public InternalRow toRow()
    {
        Preconditions.checkState(columnName != null && columnType != null);
        Object[] values = new Object[SCHEMA.size()];
        values[SCHEMA.fieldIndex(FIELD_NAME)] = UTF8String.fromString(columnName);
        values[SCHEMA.fieldIndex(FIELD_TYPE)] = UTF8String.fromString(columnType);
        values[SCHEMA.fieldIndex(FIELD_VALUE)] = getBytes();
        return new GenericInternalRow(values);
    }

    @Override
    public SparkValueWithMetadata fromRow(Row row)
    {
        String name = row.getString(SCHEMA.fieldIndex(FIELD_NAME));
        String type = row.getString(SCHEMA.fieldIndex(FIELD_TYPE));
        Object valueObj = row.get(SCHEMA.fieldIndex(FIELD_VALUE));
        if (valueObj == null)
        {
            return new SparkValueWithMetadata(name, type, null);
        }

        if (valueObj instanceof byte[])
        {
            byte[] bytes = (byte[]) valueObj;
            return new SparkValueWithMetadata(name, type, ByteBuffer.wrap(bytes));
        }

        throw new IllegalArgumentException("Input row has invalid data type for 'value' column. " +
                                           "Expected byte[] but found " + valueObj.getClass().getSimpleName());
    }
}
