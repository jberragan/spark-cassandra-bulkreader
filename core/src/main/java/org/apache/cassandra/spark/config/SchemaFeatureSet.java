package org.apache.cassandra.spark.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    // Note: the order matters!

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
                return new AbstractSparkRowIterator.LastModifiedTimestampDecorator(builder);
            }
        },

    // special column that passes over updated field bitset field indicating which columns are unset and which are tombstones
    // this is only used for CDC
    UPDATED_FIELDS_INDICATOR
        {
            @Override
            public DataType fieldDataType()
            {
                return DataTypes.BinaryType;
            }

            @Override
            public AbstractSparkRowIterator.RowBuilder decorate(AbstractSparkRowIterator.RowBuilder builder)
            {
                return new AbstractSparkRowIterator.UpdatedFieldsIndicatorDecorator(builder);
            }
        },

    // special column that passes over boolean field marking if mutation was an UPDATE or an INSERT
    // this is only used for CDC
    UPDATE_FLAG
        {
            @Override
            public DataType fieldDataType()
            {
                return DataTypes.BooleanType;
            }

            @Override
            public AbstractSparkRowIterator.RowBuilder decorate(AbstractSparkRowIterator.RowBuilder builder)
            {
                return new AbstractSparkRowIterator.UpdateFlagDecorator(builder);
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

            @Override
            public AbstractSparkRowIterator.RowBuilder decorate(AbstractSparkRowIterator.RowBuilder builder)
            {
                return new AbstractSparkRowIterator.CellTombstonesInComplexDecorator(builder);
            }
        };

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
            // todo: some features are only valid for the CDC scenario. Probably reject early
            if (Boolean.parseBoolean(options.getOrDefault(f.optionName(), "false").toLowerCase()))
            {
                enabledFeatures.add(f);
            }
        }
        return enabledFeatures;
    }
}
