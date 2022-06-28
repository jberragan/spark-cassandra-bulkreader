package org.apache.cassandra.spark.sparksql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.ClusteringBound;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.config.SchemaFeatureSet.RangeDeletionStruct.EndFieldPos;
import static org.apache.cassandra.spark.config.SchemaFeatureSet.RangeDeletionStruct.EndInclusiveFieldPos;
import static org.apache.cassandra.spark.config.SchemaFeatureSet.RangeDeletionStruct.StartFieldPos;
import static org.apache.cassandra.spark.config.SchemaFeatureSet.RangeDeletionStruct.StartInclusiveFieldPos;
import static org.apache.cassandra.spark.config.SchemaFeatureSet.RangeDeletionStruct.TotalFields;

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
    protected final CqlSchema cqlSchema;
    protected final boolean noValueColumns;
    protected final StructType columnFilter;

    private SparkCellIterator.Cell cell = null;
    private InternalRow row = null;

    AbstractSparkRowIterator(@NotNull final DataLayer dataLayer,
                             @Nullable final StructType requiredSchema,
                             @NotNull final List<PartitionKeyFilter> partitionKeyFilters,
                             @Nullable final CdcOffsetFilter cdcOffsetFilter)
    {
        this.stats = dataLayer.stats();
        this.cqlSchema = dataLayer.cqlSchema();
        this.columnFilter = useColumnFilter(requiredSchema, cqlSchema) ? requiredSchema : null;
        this.it = buildCellIterator(dataLayer, this.columnFilter, partitionKeyFilters, cdcOffsetFilter);
        this.stats.openedSparkRowIterator();
        this.openTimeNanos = System.nanoTime();
        this.requestedFeatures = dataLayer.requestedFeatures();
        this.noValueColumns = it.noValueColumns();
        this.builder = newBuilder();
    }

    protected SparkCellIterator buildCellIterator(@NotNull final DataLayer dataLayer,
                                                  @Nullable final StructType columnFilter,
                                                  @NotNull final List<PartitionKeyFilter> partitionKeyFilters,
                                                  @Nullable final CdcOffsetFilter cdcOffsetFilter)
    {
        return new SparkCellIterator(dataLayer, this.columnFilter, partitionKeyFilters, cdcOffsetFilter);
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

            if (!noValueColumns && !cell.isTombstone())
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
        CqlSchema getCqlSchema();

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
        public CqlSchema getCqlSchema()
        {
            return delegate.getCqlSchema();
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
        private final CqlSchema cqlSchema;

        FullRowBuilder(CqlSchema cqlSchema, boolean noValueColumns)
        {
            this.cqlSchema = cqlSchema;
            this.numColumns = cqlSchema.numFields();
            this.numCells = cqlSchema.numNonValueColumns() + (noValueColumns ? 0 : 1);
            this.noValueColumns = noValueColumns;
        }

        @Override
        public CqlSchema getCqlSchema()
        {
            return cqlSchema;
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
            final int len = noValueColumns || cell.isTombstone()
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

    public static class UpdatedFieldsIndicatorDecorator extends RowBuilderDecorator
    {
        private final int indicatorPos;
        private final BitSet appearedColumns;

        public UpdatedFieldsIndicatorDecorator(RowBuilder delegate)
        {
            super(delegate);
            // last item after this expansion is for the indicator column
            this.indicatorPos = internalExpandRow();
            this.appearedColumns = new BitSet(columnsCount());
        }

        @Override
        protected int extraColumns() {
            return 1;
        }

        @Override
        public void copyKeys(SparkCellIterator.Cell cell)
        {
            super.copyKeys(cell);
            final int len = hasRegularValueColumn() && !cell.isTombstone()
                            ? cell.values.length - 1
                            : cell.values.length;
            appearedColumns.set(0, len);
        }

        @Override
        public void copyValue(SparkCellIterator.Cell cell) {
            super.copyValue(cell);
            appearedColumns.set(cell.pos);
        }

        @Override
        public void reset()
        {
            super.reset();
            appearedColumns.clear();
        }

        @Override
        public GenericInternalRow build()
        {
            // append updated fields indicator
            final Object[] result = array();
            result[indicatorPos] = appearedColumns.toByteArray();
            return super.build();
        }
    }

    public static class UpdateFlagDecorator extends RowBuilderDecorator
    {
        private final int columnPos;
        private boolean isUpdate = false;

        public UpdateFlagDecorator(RowBuilder delegate)
        {
            super(delegate);
            // last item after this expansion is for the liveness column
            this.columnPos = internalExpandRow();
        }

        @Override
        public void reset()
        {
            super.reset();
            isUpdate = false;
        }

        @Override
        public void onCell(SparkCellIterator.Cell cell)
        {
            super.onCell(cell);
            isUpdate = cell.isUpdate;
        }

        @Override
        protected int extraColumns() {
            return 1;
        }

        @Override
        public GenericInternalRow build()
        {
            // append isUpdate flag
            final Object[] result = array();
            result[columnPos] = isUpdate;
            return super.build();
        }
    }

    public static class CellTombstonesInComplexDecorator extends RowBuilderDecorator
    {
        private final int columnPos;
        private final Map<String, ArrayData> tombstonedKeys = new LinkedHashMap<>();

        public CellTombstonesInComplexDecorator(RowBuilder delegate)
        {
            super(delegate);
            // last item after this expansion is for the list of cell tombstones inside a complex data
            this.columnPos = internalExpandRow();
        }

        @Override
        protected int extraColumns()
        {
            return 1;
        }

        @Override
        public void reset()
        {
            super.reset();
            tombstonedKeys.clear();
        }

        @Override
        public void onCell(SparkCellIterator.Cell cell)
        {
            super.onCell(cell);
            if (cell instanceof SparkCellIterator.TombstonesInComplex)
            {
                SparkCellIterator.TombstonesInComplex tombstones = (SparkCellIterator.TombstonesInComplex) cell;

                Object[] keys = tombstones.tombstonedKeys.stream()
                                                         .map(ByteBuffer::array)
                                                         .toArray();
                tombstonedKeys.put(tombstones.columnName, ArrayData.toArrayData(keys));
            }
        }

        @Override
        public GenericInternalRow build()
        {
            // append isUpdate flag
            final Object[] result = array();
            if (tombstonedKeys.isEmpty())
            {
                result[columnPos] = null;
            }
            else
            {
                Object[] cols = new Object[tombstonedKeys.size()];
                Object[] tombstones = new Object[tombstonedKeys.size()];
                int i = 0;
                for (Map.Entry<String, ArrayData> e : tombstonedKeys.entrySet())
                {
                    cols[i] = UTF8String.fromString(e.getKey());
                    tombstones[i] = e.getValue();
                    i++;
                }
                result[columnPos] = ArrayBasedMapData.apply(cols, tombstones);
            }

            return super.build();
        }
    }

    public static class RangeTombstoneDecorator extends RowBuilderDecorator
    {
        private final int columnPos;
        private final List<InternalRow> rangeTombstoneList;

        public RangeTombstoneDecorator(RowBuilder delegate)
        {
            super(delegate);
            // last item after this expansion is for the list of cell tombstones inside a complex data
            this.columnPos = internalExpandRow();
            this.rangeTombstoneList = new ArrayList<>();
        }

        @Override
        protected int extraColumns()
        {
            return 1;
        }

        @Override
        public void reset()
        {
            super.reset();
            rangeTombstoneList.clear();
        }

        @Override
        public void onCell(SparkCellIterator.Cell cell)
        {
            super.onCell(cell);

            if (!(cell instanceof SparkCellIterator.RangeTombstone))
            {
                return;
            }

            SparkCellIterator.RangeTombstone rt = (SparkCellIterator.RangeTombstone) cell;
            List<RangeTombstoneMarker> markers = rt.rangeTombstoneMarkers;
            // see SchemaFeatureSet.RANGE_DELETION#fieldDataType for the schema
            // each range has 4 fields: Start, StartInclusive, End, EndInclusive
            Object[] range = null;
            for (RangeTombstoneMarker marker : markers)
            {
                if (marker.isBoundary())
                {
                    Preconditions.checkState(range != null);
                    ClusteringBound<?> close = marker.closeBound(false);
                    range[EndFieldPos] = buildClusteringKey(close.clustering());
                    range[EndInclusiveFieldPos] = close.isInclusive();
                    rangeTombstoneList.add(new GenericInternalRow(range));
                    ClusteringBound<?> open = marker.openBound(false);
                    range = new Object[TotalFields];
                    range[StartFieldPos] = buildClusteringKey(open.clustering());
                    range[StartInclusiveFieldPos] = open.isInclusive();
                }
                else if (marker.isOpen(false)) // open bound
                {
                    Preconditions.checkState(range == null);
                    range = new Object[TotalFields];
                    ClusteringBound<?> open = marker.openBound(false);
                    range[StartFieldPos] = buildClusteringKey(open.clustering());
                    range[StartInclusiveFieldPos] = open.isInclusive();
                }
                else // close bound
                {
                    Preconditions.checkState(range != null);
                    ClusteringBound<?> close = marker.closeBound(false);
                    range[EndFieldPos] = buildClusteringKey(close.clustering());
                    range[EndInclusiveFieldPos] = close.isInclusive();
                    rangeTombstoneList.add(new GenericInternalRow(range));
                    range = null;
                }
            }
            Preconditions.checkState(range == null, "Tombstone range should be closed");
        }

        @Override
        public GenericInternalRow build()
        {
            final Object[] result = array();
            if (rangeTombstoneList.isEmpty())
            {
                result[columnPos] = null;
            }
            else
            {
                result[columnPos] = ArrayData.toArrayData(rangeTombstoneList.toArray());
            }

            return super.build();
        }

        private GenericInternalRow buildClusteringKey(ClusteringPrefix<?> clustering)
        {
            int i = 0;
            Object[] ckFields = new Object[getCqlSchema().numClusteringKeys()];
            for (CqlField f : getCqlSchema().clusteringKeys())
            {
                ByteBuffer bb = clustering.bufferAt(i);
                if (bb == null)
                {
                    ckFields[i] = null;
                    break; // a valid range bound does not non-null values following a null value.
                }
                ckFields[i] = f.deserialize(bb);
                i++;
            }
            return new GenericInternalRow(ckFields);
        }
    }
}
