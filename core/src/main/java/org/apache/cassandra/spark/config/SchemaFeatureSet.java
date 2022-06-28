package org.apache.cassandra.spark.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.sparksql.AbstractSparkRowIterator;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.cassandra.spark.config.SchemaFeatureSet.RangeDeletionStruct.*;

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
        },

    RANGE_DELETION
        {
            private transient DataType dataType;

            @Override
            public void generateDataType(CqlSchema cqlSchema, StructType sparkSchema)
            {
                // when there is no clustering keys, range deletion won't happen. (since such deletion applies only when there are clustering key(s))
                if (cqlSchema.numClusteringKeys() == 0)
                {
                    dataType = DataTypes.BooleanType; // assign a dummy data type, it won't be reach with such cql scehma.
                    return;
                }

                List<StructField> clusteringKeyFields = cqlSchema.clusteringKeys()
                                                                 .stream()
                                                                 .map(cqlField -> sparkSchema.apply(cqlField.name()))
                                                                 .collect(Collectors.toList());
                StructType clusteringKeys = DataTypes.createStructType(clusteringKeyFields);
                StructField[] rt = new StructField[TotalFields];
                // The array of binaries follows the same seq of the clustering key definition,
                // e.g. for primary key (pk, ck1, ck2), the array value could be [ck1] or [ck1, ck2], but never (ck2) w/o ck1
                rt[StartFieldPos] = DataTypes.createStructField("Start", clusteringKeys, true);
                // default to be inclusive if null
                rt[StartInclusiveFieldPos] = DataTypes.createStructField("StartInclusive", DataTypes.BooleanType, true);
                rt[EndFieldPos] = DataTypes.createStructField("End", clusteringKeys, true);
                // default to be inclusive if null
                rt[EndInclusiveFieldPos] = DataTypes.createStructField("EndInclusive", DataTypes.BooleanType, true);
                dataType = DataTypes.createArrayType(DataTypes.createStructType(rt));
            }

            @Override
            public DataType fieldDataType()
            {
                Preconditions.checkNotNull(dataType, "The dynamic data type is not initialized.");
                return dataType;
            }

            @Override
            public AbstractSparkRowIterator.RowBuilder decorate(AbstractSparkRowIterator.RowBuilder builder)
            {
                return new AbstractSparkRowIterator.RangeTombstoneDecorator(builder);
            }
        };

    public static final List<SchemaFeature> ALL_CDC_FEATURES = Arrays.asList(
        UPDATED_FIELDS_INDICATOR,
        UPDATE_FLAG,
        CELL_DELETION_IN_COMPLEX,
        RANGE_DELETION
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
            // todo: some features are only valid for the CDC scenario. Probably reject early
            if (Boolean.parseBoolean(options.getOrDefault(f.optionName(), "false").toLowerCase()))
            {
                enabledFeatures.add(f);
            }
        }
        return enabledFeatures;
    }

    public static final class RangeDeletionStruct
    {
        public static final int TotalFields = 4;
        public static final int StartFieldPos = 0;
        public static final int StartInclusiveFieldPos = 1;
        public static final int EndFieldPos = 2;
        public static final int EndInclusiveFieldPos = 3;
    }
}
