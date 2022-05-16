package org.apache.cassandra.spark.sparksql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
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
public class SparkRowIterator extends AbstractSparkRowIterator implements PartitionReader<InternalRow>
{

    @VisibleForTesting
    public SparkRowIterator(@NotNull final DataLayer dataLayer)
    {
        super(dataLayer, null, new ArrayList<>(), null);
    }

    public SparkRowIterator(@NotNull final DataLayer dataLayer,
                            @Nullable final StructType columnFilter,
                            @NotNull final List<PartitionKeyFilter> partitionKeyFilters)
    {
        this(dataLayer, columnFilter, partitionKeyFilters, null);
    }

    protected SparkRowIterator(@NotNull final DataLayer dataLayer,
                               @Nullable final StructType columnFilter,
                               @NotNull final List<PartitionKeyFilter> partitionKeyFilters,
                               @Nullable final CdcOffsetFilter cdcOffsetFilter)
    {
        super(dataLayer, columnFilter, partitionKeyFilters, cdcOffsetFilter);
    }

    @Override
    RowBuilder newBuilder()
    {
        RowBuilder builder = columnFilter != null
                             ? new PartialRowBuilder(columnFilter, cqlSchema, noValueColumns)
                             : new FullRowBuilder(cqlSchema.numFields(), cqlSchema.numNonValueColumns(), noValueColumns);

        if (requestedFeatures.addLastModifiedTimestamp())
        {
            builder = new LastModifiedTimestampDecorator(builder);
        }

        if (requestedFeatures.addUpdatedFieldsIndicator())
        {
            builder = new UpdatedFieldsIndicatorDecorator(builder);
        }

        if (requestedFeatures.addUpdateFlag())
        {
            builder = new UpdateFlagDecorator(builder);
        }

        if (requestedFeatures.supportCellDeletionInComplex())
        {
            builder = new CellTombstonesInComplexDecorator(builder);
        }

        builder.reset();
        return builder;
    }

    /**
     * PartialRowBuilder that builds row only containing fields in requiredSchema prune-column filter
     * NOTE: Spark 3 changed the contract from Spark 2 and requires us to only return the columns specified in
     * the requiredSchema 'prune column' filter and not a sparse Object[] array with null values for excluded columns
     */
    static class PartialRowBuilder extends FullRowBuilder
    {
        private final int[] posMap;
        private final boolean hasAllNonValueColumns;

        PartialRowBuilder(@NotNull final StructType requiredSchema,
                          final CqlSchema schema,
                          boolean noValueColumns)
        {
            super(schema.numFields(), schema.numNonValueColumns(), noValueColumns);
            final Set<String> requiredColumns = Arrays.stream(requiredSchema.fields())
                                                      .map(StructField::name)
                                                      .collect(Collectors.toSet());
            this.hasAllNonValueColumns = schema.fields().stream()
                                               .filter(CqlField::isNonValueColumn)
                                               .map(CqlField::name)
                                               .allMatch(requiredColumns::contains);

            // map original column position to new position in requiredSchema
            this.posMap = IntStream.range(0, schema.numFields()).map(i -> -1).toArray();
            int pos = 0;
            for (final StructField structField : requiredSchema.fields())
            {
                final CqlField field = schema.getField(structField.name());
                if (field != null) // field might be last modified timestamp
                {
                    this.posMap[field.pos()] = pos++;
                }
            }
        }

        @Override
        public void copyKeys(SparkCellIterator.Cell cell)
        {
            if (hasAllNonValueColumns)
            {
                // optimization if we are returning all primary key/static columns we can use the super method
                super.copyKeys(cell);
                return;
            }

            // otherwise we need to only return columns requested
            // and map to new position in result array
            final int len = noValueColumns || cell.isTombstone()
                            ? cell.values.length
                            : cell.values.length - 1;
            for (int i = 0; i < len; i++)
            {
                final int pos = posMap[i];
                if (pos >= 0)
                {
                    result[pos] = cell.values[i];
                }
            }
            count += len;
        }

        @Override
        public void copyValue(SparkCellIterator.Cell cell)
        {
            // copy the next value column mapping column to new position
            final int pos = posMap[cell.pos];
            if (pos >= 0)
            {
                result[pos] = cell.values[cell.values.length - 1];
            }
            count++;
        }
    }
}
