package org.apache.cassandra.spark.sparksql;

import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

/**
 * Wrapper iterator around SparkCellIterator to normalize cells into Spark SQL rows
 */
public class SparkRowIterator extends AbstractSparkRowIterator implements InputPartitionReader<InternalRow>
{
    @VisibleForTesting
    public SparkRowIterator(@NotNull final DataLayer dataLayer)
    {
        this(dataLayer, null, new ArrayList<>());
    }

    protected SparkRowIterator(@NotNull final DataLayer dataLayer,
                               @Nullable final StructType requiredSchema,
                               @NotNull final List<PartitionKeyFilter> partitionKeyFilters)
    {
        super(dataLayer, requiredSchema, partitionKeyFilters);
    }

    @Override
    RowBuilder newBuilder()
    {
        RowBuilder builder = new FullRowBuilder(cqlTable, noValueColumns);
        if (requestedFeatures.contains(SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP))
        {
            builder = SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP.decorate(builder);
        }
        builder.reset();
        return builder;
    }
}
