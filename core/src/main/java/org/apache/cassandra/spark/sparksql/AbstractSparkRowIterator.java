package org.apache.cassandra.spark.sparksql;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
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
public abstract class AbstractSparkRowIterator
{
    private final Stats stats;
    private final SparkCellIterator it;
    private final long openTimeNanos;
    private final RowBuilder builder;

    protected final List<SchemaFeature> requestedFeatures;
    protected final CqlTable cqlTable;
    protected final boolean noValueColumns;
    protected final StructType columnFilter;

    private SparkCellIterator.Cell cell = null;
    private InternalRow row = null;

    AbstractSparkRowIterator(@NotNull final DataLayer dataLayer,
                             @Nullable final StructType requiredSchema,
                             @NotNull final List<PartitionKeyFilter> partitionKeyFilters)
    {
        this.stats = dataLayer.stats();
        this.cqlTable = dataLayer.cqlTable();
        this.columnFilter = useColumnFilter(requiredSchema, cqlTable) ? requiredSchema : null;
        this.it = new SparkCellIterator(dataLayer, requiredSchema, partitionKeyFilters);
        this.stats.openedSparkRowIterator();
        this.openTimeNanos = System.nanoTime();
        this.requestedFeatures = dataLayer.requestedFeatures();
        this.noValueColumns = it.noValueColumns();
        this.builder = newBuilder();
    }

    private static boolean useColumnFilter(@Nullable StructType requiredSchema, CqlTable cqlTable)
    {
        if (requiredSchema == null)
        {
            return false;
        }
        // only use column filter if it excludes any of the CqlTable fields
        final Set<String> requiredFields = Arrays.stream(requiredSchema.fields()).map(StructField::name).collect(Collectors.toSet());
        return cqlTable.fields().stream().map(CqlField::name)
                       .anyMatch(field -> !requiredFields.contains(field));
    }

    abstract RowBuilder newBuilder();

    public InternalRow get()
    {
        return row;
    }

    public boolean next() throws IOException
    {
        // we are finished if not already reading a row (if cell != null, it can happen if previous row was incomplete)
        // and SparkCellIterator has no next value
        if (this.cell == null && !this.it.hasNextThrows())
        {
            return false;
        }

        long maxTimestamp = 0L;

        // pivot values to normalize each cell into single SparkSQL or 'CQL' type row
        do
        {
            if (this.cell == null)
            {
                // read next cell
                this.cell = this.it.next();
            }

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

            builder.onCell(cell);
            maxTimestamp = Math.max(maxTimestamp, cell.timestamp);

            if (!noValueColumns)
            {
                // if schema has value column or not a row deletion
                // then copy across
                builder.copyValue(cell);
            }
            this.cell = null;
            // keep reading more cells until we read the entire row
        } while (builder.hasMoreCells() && this.it.hasNextThrows());

        // build row and reset builder for next row
        this.row = builder.build();
        builder.reset();

        if (maxTimestamp != 0L)
        {
            stats.mutationProducedLatency(System.currentTimeMillis() - TimeUnit.MICROSECONDS.toMillis(maxTimestamp));
        }

        this.stats.nextRow();
        return true;
    }

    public void close() throws IOException
    {
        this.stats.closedSparkRowIterator(System.nanoTime() - openTimeNanos);
        this.it.close();
    }

    // RowBuilder

    public interface RowBuilder
    {
        CqlTable getCqlTable();

        void reset();

        boolean isFirstCell();

        boolean hasMoreCells();

        void onCell(SparkCellIterator.Cell cell);

        void copyKeys(SparkCellIterator.Cell cell);

        void copyValue(SparkCellIterator.Cell cell);

        Object[] array();

        int columnsCount();

        boolean hasRegularValueColumn();

        /**
         * Expand the row with more columns. The extra columns are appended to the row.
         * @param extraColumns number of columns to append
         * @return length of row before expanding
         */
        int expandRow(int extraColumns);

        GenericInternalRow build();
    }

    abstract static class RowBuilderDecorator implements RowBuilder
    {
        protected final RowBuilder delegate;

        RowBuilderDecorator(RowBuilder delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public int columnsCount() {
            return delegate.columnsCount();
        }

        @Override
        public boolean hasRegularValueColumn() {
            return delegate.hasRegularValueColumn();
        }

        @Override
        public void reset()
        {
            delegate.reset();
        }

        @Override
        public boolean isFirstCell()
        {
            return delegate.isFirstCell();
        }

        @Override
        public boolean hasMoreCells()
        {
            return delegate.hasMoreCells();
        }

        @Override
        public void onCell(SparkCellIterator.Cell cell)
        {
            delegate.onCell(cell);
        }

        @Override
        public void copyKeys(SparkCellIterator.Cell cell)
        {
            delegate.copyKeys(cell);
        }

        @Override
        public void copyValue(SparkCellIterator.Cell cell)
        {
            delegate.copyValue(cell);
        }

        @Override
        public Object[] array() {
            return delegate.array();
        }

        @Override
        public int expandRow(int extraColumns)
        {
            return delegate.expandRow(extraColumns + extraColumns()) + extraColumns();
        }

        @Override
        public CqlTable getCqlTable()
        {
            return delegate.getCqlTable();
        }

        /**
         * Preferred to call if the decorator is adding extra columns.
         * @return the index of the fist extra column
         */
        protected int internalExpandRow()
        {
            return expandRow(0) - extraColumns();
        }

        protected abstract int extraColumns();

        @Override
        public GenericInternalRow build()
        {
            return delegate.build();
        }
    }

    /**
     * FullRowBuilder expects all fields in the schema to be returned, i.e. no prune column filter
     */
    static class FullRowBuilder implements RowBuilder
    {
        final int numColumns;
        int extraColumns;
        final int numCells;
        final boolean noValueColumns;
        Object[] result;
        int count;
        private final CqlTable cqlTable;

        FullRowBuilder(CqlTable cqlTable, boolean noValueColumns)
        {
            this.cqlTable = cqlTable;
            this.numColumns = cqlTable.numFields();
            this.numCells = cqlTable.numNonValueColumns() + (noValueColumns ? 0 : 1);
            this.noValueColumns = noValueColumns;
        }

        @Override
        public CqlTable getCqlTable()
        {
            return cqlTable;
        }

        @Override
        public void reset() {
            this.count = 0;
            this.result = new Object[numColumns + extraColumns];
        }

        public boolean isFirstCell()
        {
            return count == 0;
        }

        public void copyKeys(SparkCellIterator.Cell cell)
        {
            // need to handle special case where schema is only partition or clustering keys - i.e. no value columns
            final int len = noValueColumns
                            ? cell.values.length
                            : cell.values.length - 1;
            System.arraycopy(cell.values, 0, result, 0, len);
            count += len;
        }

        public void copyValue(SparkCellIterator.Cell cell)
        {
            // copy the next value column
            result[cell.pos] = cell.values[cell.values.length - 1];
            count++; // increment the number of cells visited
        }

        public Object[] array()
        {
            return result;
        }

        @Override
        public int columnsCount() {
            return numColumns;
        }

        @Override
        public boolean hasRegularValueColumn() {
            return !noValueColumns;
        }

        @Override
        public int expandRow(int extraColumns)
        {
            this.extraColumns = extraColumns;
            return numColumns;
        }

        public boolean hasMoreCells()
        {
            return this.count < numColumns;
        }

        public void onCell(SparkCellIterator.Cell cell)
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
    public static class LastModifiedTimestampDecorator extends RowBuilderDecorator
    {
        private final int lmtColumnPos;
        private long lastModified = 0L;

        public LastModifiedTimestampDecorator(RowBuilder delegate)
        {
            super(delegate);
            // last item after this expansion is for the lmt column
            this.lmtColumnPos = internalExpandRow();
        }

        @Override
        public void reset()
        {
            super.reset();
            // reset the lastModified the builder is re-used across rows
            lastModified = 0L;
        }

        @Override
        public void onCell(SparkCellIterator.Cell cell)
        {
            super.onCell(cell);
            lastModified = Math.max(lastModified, cell.timestamp);
        }

        @Override
        protected int extraColumns() {
            return 1;
        }

        @Override
        public GenericInternalRow build()
        {
            // append last modified timestamp
            final Object[] result = array();
            result[lmtColumnPos] = lastModified;
            return super.build();
        }
    }
}
