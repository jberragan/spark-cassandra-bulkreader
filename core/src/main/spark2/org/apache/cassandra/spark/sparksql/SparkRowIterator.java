package org.apache.cassandra.spark.sparksql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.CustomFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructField;
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
        super(dataLayer, null, new ArrayList<>());
    }

    SparkRowIterator(@NotNull final DataLayer dataLayer,
                     @Nullable final StructType requiredSchema,
                     @NotNull final List<CustomFilter> filters)
    {
        super(dataLayer, requiredSchema, filters);
    }

    @Override
    RowBuilder newBuilder()
    {
        RowBuilder builder = new FullRowBuilder(cqlSchema.numFields(), cqlSchema.numNonValueColumns(), noValueColumns);
        if (addLastModifiedTimestamp)
        {
            builder = builder.withLastModifiedTimestamp();
        }
        builder.reset();
        return builder;
    }
}
