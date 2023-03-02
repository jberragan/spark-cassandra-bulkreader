package org.apache.cassandra.spark.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.spark.cdc.SparkRangeTombstone;
import org.apache.cassandra.spark.sparksql.AbstractSparkRowIterator;
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
public enum SchemaFeatureSet implements SchemaFeature
{
    // special column that passes over last modified timestamp for a row
    LAST_MODIFIED_TIMESTAMP
        {
            @Override
            public DataType fieldDataType()
            {
                return DataTypes.TimestampType;
            }

            @Override
            public AbstractSparkRowIterator.RowBuilder decorate(AbstractSparkRowIterator.RowBuilder builder)
            {
                return new AbstractSparkRowIterator.LastModifiedTimestampDecorator(builder, fieldName());
            }

            // the field value must present when it is requested/enabled
            @Override
            public boolean fieldNullable()
            {
                return false;
            }

            @Override
            public boolean isRequestable()
            {
                return true;
            }
        },

    // special column that passes over string field marking the operation type of the CDC event
    // this is only used for CDC
    OPERATION_TYPE
        {
            @Override
            public DataType fieldDataType()
            {
                return DataTypes.StringType;
            }
        },

    // feature column that contains the column_name to a list of keys of the tombstoned values in a complex data type
    // this is only used for CDC
    CELL_DELETION_IN_COMPLEX
        {
            @Override
            public DataType fieldDataType()
            {
                return DataTypes.createMapType(DataTypes.StringType, DataTypes.createArrayType(DataTypes.BinaryType));
            }
        },

    // feature column that encodes the range tombstones
    // this is only used for CDC
    RANGE_DELETION
        {
            @Override
            public DataType fieldDataType()
            {
                return DataTypes.createArrayType(SparkRangeTombstone.SCHEMA);
            }
        },

    // Pass the TTL value from the query along with the CDC event.
    // this is only used for CDC
    TTL
        {
            @Override
            public DataType fieldDataType()
            {
                // array: [ttl, expirationTime]
                // Why using a single tuple is enough for a row with multiple cell?
                // In the case of INSERT, all columns in the row will be updated with the same TTL value.
                // In the case of UPDATE, the updated columns will have the same TTL value.
                return DataTypes.createArrayType(DataTypes.IntegerType);
            }
        };

    public static final List<SchemaFeature> ALL_CDC_FEATURES = Arrays.asList(
        LAST_MODIFIED_TIMESTAMP,
        OPERATION_TYPE,
        CELL_DELETION_IN_COMPLEX,
        RANGE_DELETION,
        TTL
    );

    /**
     * Initialize the requested features from the input options.
     * @param options
     * @return the requested features list. If none is requested, an empty list is returned.
     */
    public static List<SchemaFeature> initializeFromOptions(Map<String, String> options)
    {
        List<SchemaFeature> enabledFeatures = new ArrayList<>();
        for (SchemaFeature f : values())
        {
            // enable the feature only when the feature is requestable and enabled via option.
            if (f.isRequestable() && Boolean.parseBoolean(options.getOrDefault(f.optionName(), "false")))
            {
                enabledFeatures.add(f);
            }
        }
        return enabledFeatures;
    }
}
