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
public class SparkRowIterator implements InputPartitionReader<InternalRow>
{
    private final Stats stats;
    private final SparkCellIterator it;
    private final long openTimeNanos;
    private final boolean addLastModifiedTimestamp;
    private final CqlSchema cqlSchema;
    private final boolean noValueColumns;
    private final RowBuilder builder;

    private SparkCellIterator.Cell cell = null;
    private InternalRow row = null;

    @VisibleForTesting
    public SparkRowIterator(@NotNull final DataLayer dataLayer)
    {
        this(dataLayer, null, new ArrayList<>());
    }

    SparkRowIterator(@NotNull final DataLayer dataLayer,
                     @Nullable final StructType requiredSchema,
                     @NotNull final List<CustomFilter> filters)
    {
        this.stats = dataLayer.stats();
        this.cqlSchema = dataLayer.cqlSchema();
        final StructType columnFilter = useColumnFilter(requiredSchema, cqlSchema) ? requiredSchema : null;
        this.it = new SparkCellIterator(dataLayer, columnFilter, filters);
        this.stats.openedSparkRowIterator();
        this.openTimeNanos = System.nanoTime();
        this.addLastModifiedTimestamp = dataLayer.requestedFeatures().addLastModifiedTimestamp();
        this.noValueColumns = it.noValueColumns();
        this.builder = newBuilder();
    }

    private static boolean useColumnFilter(@Nullable StructType requiredSchema, CqlSchema cqlSchema)
    {
        if (requiredSchema == null)
        {
            return false;
        }
        // only use column filter if it excludes any of the CqlSchema fields
        final Set<String> requiredFields = Arrays.stream(requiredSchema.fields()).map(StructField::name).collect(Collectors.toSet());
        return cqlSchema.fields().stream().map(CqlField::name)
                        .anyMatch(field -> !requiredFields.contains(field));
    }

    private RowBuilder newBuilder()
    {
        RowBuilder builder = new FullRowBuilder(cqlSchema.numFields(), cqlSchema.numNonValueColumns(), noValueColumns);
        if (addLastModifiedTimestamp)
        {
            builder = builder.withLastModifiedTimestamp();
        }
        builder.reset();
        return builder;
    }

    @Override
    public InternalRow get()
    {
        return row;
    }

    @Override
    public boolean next()
    {
        // we are finished if not already reading a row (if cell != null, it can happen if previous row was incomplete)
        // and SparkCellIterator has no next value
        if (this.cell == null && !this.it.hasNext())
        {
            return false;
        }

        // pivot values to normalize each cell into single SparkSQL or 'CQL' type row
        do
        {
            if (this.cell == null)
            {
                // read next cell
                this.cell = this.it.next();
            }

            builder.nextCell(cell);

            if (builder.isFirstCell())
            {
                // on first iteration, copy all partition keys, clustering keys, static columns
                assert this.cell.isNewRow;
                builder.copyKeys(cell);
            }
            else if (this.cell.isNewRow)
            // current row is incomplete so we have moved to new row before reaching end
            // break out to return current incomplete row and handle next row in next iteration
            {
                break;
            }

            if (!noValueColumns)
            {
                // if schema has value column then copy across
                builder.copyValue(cell);
            }
            this.cell = null;
            // keep reading more cells until we read the entire row
        } while (builder.hasMoreCells() && this.it.hasNext());

        // build row and reset builder for next row
        this.row = builder.build();
        builder.reset();

        this.stats.nextRow();
        return true;
    }

    @Override
    public void close() throws IOException
    {
        this.stats.closedSparkRowIterator(System.nanoTime() - openTimeNanos);
        this.it.close();
    }

    // RowBuilder

    interface RowBuilder
    {
        default void reset()
        {
            this.reset(0);
        }

        void reset(int extraCells);

        boolean isFirstCell();

        boolean hasMoreCells();

        void nextCell(SparkCellIterator.Cell cell);

        void copyKeys(SparkCellIterator.Cell cell);

        void copyValue(SparkCellIterator.Cell cell);

        Object[] array();

        GenericInternalRow build();

        default WithLastModifiedTimestamp withLastModifiedTimestamp()
        {
            return new WithLastModifiedTimestamp(this);
        }
    }

    /**
     * FullRowBuilder expects all fields in the schema to be returned, i.e. no prune column filter
     */
    static class FullRowBuilder implements RowBuilder
    {
        final int numColumns, numCells;
        final boolean noValueColumns;
        Object[] result;
        int count;

        FullRowBuilder(int numColumns, int numNonValueColumns, boolean noValueColumns)
        {
            this.numColumns = numColumns;
            this.numCells = numNonValueColumns + (noValueColumns ? 0 : 1);
            this.noValueColumns = noValueColumns;
        }

        public void reset(int extraCells)
        {
            this.result = new Object[numColumns + extraCells];
            this.count = 0;
        }

        public boolean isFirstCell()
        {
            return count == 0;
        }

        public void copyKeys(SparkCellIterator.Cell cell)
        {
            // need to handle special case where schema is only partition or clustering keys - i.e. no value columns
            final int len = noValueColumns ? cell.values.length : cell.values.length - 1;
            System.arraycopy(cell.values, 0, result, 0, len);
            count += len;
        }

        public void copyValue(SparkCellIterator.Cell cell)
        {
            // copy the next value column
            result[cell.pos] = cell.values[cell.values.length - 1];
            count++;
        }

        public Object[] array()
        {
            return result;
        }

        public boolean hasMoreCells()
        {
            return this.count < numColumns;
        }

        public void nextCell(SparkCellIterator.Cell cell)
        {
            assert cell.values.length > 0 && cell.values.length <= numCells;
        }

        public GenericInternalRow build()
        {
            return new GenericInternalRow(result);
        }
    }

    /**
     * Wrap a builder to append last modified timestamp
     */
    static class WithLastModifiedTimestamp implements RowBuilder
    {
        final RowBuilder builder;
        long lastModified = 0L;

        WithLastModifiedTimestamp(RowBuilder builder)
        {
            this.builder = builder;
        }

        public void reset()
        {
            // add extra field in result array to store
            // last modified timestamp
            this.reset(1);
        }

        public void reset(int extraCells)
        {
            builder.reset(extraCells);
        }

        public boolean isFirstCell()
        {
            return builder.isFirstCell();
        }

        public void nextCell(SparkCellIterator.Cell cell)
        {
            lastModified = Math.max(lastModified, cell.timestamp);
            builder.nextCell(cell);
        }

        public Object[] array()
        {
            return builder.array();
        }

        public boolean hasMoreCells()
        {
            return builder.hasMoreCells();
        }

        public void copyKeys(SparkCellIterator.Cell cell)
        {
            builder.copyKeys(cell);
        }

        public void copyValue(SparkCellIterator.Cell cell)
        {
            builder.copyValue(cell);
        }

        public GenericInternalRow build()
        {
            // append last modified timestamp
            final Object[] result = builder.array();
            result[result.length - 1] = lastModified;
            return builder.build();
        }
    }
}
