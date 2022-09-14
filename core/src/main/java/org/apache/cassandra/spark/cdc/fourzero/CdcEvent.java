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

package org.apache.cassandra.spark.cdc.fourzero;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.cdc.AbstractCdcEvent;
import org.apache.cassandra.spark.cdc.RangeTombstone;
import org.apache.cassandra.spark.cdc.SparkRowSink;
import org.apache.cassandra.spark.cdc.ValueWithMetadata;
import org.apache.cassandra.spark.reader.fourzero.AbstractStreamScanner.ComplexTypeBuffer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DeletionTime;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ListType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Cell;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.CellPath;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.ColumnData;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Row;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Schema;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.google.common.collect.ImmutableList;
import org.apache.cassandra.spark.utils.ColumnTypes;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.mutable.WrappedArray;

import static org.apache.cassandra.spark.config.SchemaFeatureSet.CELL_DELETION_IN_COMPLEX;
import static org.apache.cassandra.spark.config.SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP;
import static org.apache.cassandra.spark.config.SchemaFeatureSet.OPERATION_TYPE;
import static org.apache.cassandra.spark.config.SchemaFeatureSet.RANGE_DELETION;
import static org.apache.cassandra.spark.config.SchemaFeatureSet.TTL;
import static org.apache.cassandra.spark.utils.TimeUtils.toMicros;

// one event for each row
public class CdcEvent extends AbstractCdcEvent
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcEvent.class);

    private final TableMetadata tableMetadata;
    private List<ValueWithMetadata> partitionKeys = null;
    private List<ValueWithMetadata> clusteringKeys = null;
    private List<ValueWithMetadata> staticColumns = null;
    private List<ValueWithMetadata> valueColumns = null;

    // The max timestamp of the cells in the event
    private long maxTimestampMicros = Long.MIN_VALUE;
    // Records the ttl info of the event
    private TimeToLive timeToLive;
    // Records the tombstoned elements/cells in a complex data
    private Map<String, List<ByteBuffer>> tombstonedCellsInComplex = null;
    // Records the range tombstone markers with in the same partition
    private List<RangeTombstone> rangeTombstoneList = null;
    private RangeTombstoneBuilder rangeTombstoneBuilder = null;

    private CdcEvent(Kind kind, UnfilteredRowIterator partition)
    {
        super(kind, partition.metadata().keyspace, partition.metadata().name);
        this.tableMetadata = partition.metadata();
        setPartitionKeys(partition);
        setStaticColumns(partition);
    }

    /**
     * This is the opposite of {@link #toRow()}
     */
    private CdcEvent(Kind kind, String keyspace, String table, org.apache.spark.sql.Row row)
    {
        super(kind, keyspace, table);
        // note that the converted CdcEvent does not have table metadata
        this.tableMetadata = null;
        StructType schema = SCHEMA;
        partitionKeys = arrayToCqlFields(row.get(schema.fieldIndex(PARTITION_KEYS)), false);
        clusteringKeys = arrayToCqlFields(row.get(schema.fieldIndex(CLUSTERING_KEYS)), true);
        staticColumns = arrayToCqlFields(row.get(schema.fieldIndex(STATIC_COLUMNS)), true);
        valueColumns = arrayToCqlFields(row.get(schema.fieldIndex(VALUE_COLUMNS)), true);
        maxTimestampMicros = toMicros(row.getTimestamp(schema.fieldIndex(LAST_MODIFIED_TIMESTAMP.fieldName())));
        unmakeCellDeletionInComplex(row);
        unmakeRangeDeletion(row);
        unmakeTTL(row);
    }

    @Override
    public InternalRow toRow()
    {
        Preconditions.checkState(maxTimestampMicros != Long.MIN_VALUE,
                                 "A valid cdc event should have non-empty timestamp");
        StructType schema = SCHEMA;
        Object[] data = new Object[schema.size()];
        data[schema.fieldIndex(KEYSPACE)] = UTF8String.fromString(keyspace);
        data[schema.fieldIndex(TABLE)] = UTF8String.fromString(table);
        data[schema.fieldIndex(PARTITION_KEYS)] = cqlFieldsToArray(partitionKeys);
        data[schema.fieldIndex(CLUSTERING_KEYS)] = cqlFieldsToArray(clusteringKeys);
        data[schema.fieldIndex(STATIC_COLUMNS)] = cqlFieldsToArray(staticColumns);
        data[schema.fieldIndex(VALUE_COLUMNS)] = cqlFieldsToArray(valueColumns);
        data[schema.fieldIndex(LAST_MODIFIED_TIMESTAMP.fieldName())] = maxTimestampMicros;
        data[schema.fieldIndex(OPERATION_TYPE.fieldName())] = UTF8String.fromString(kind.toString().toLowerCase());
        data[schema.fieldIndex(CELL_DELETION_IN_COMPLEX.fieldName())] = makeCellDeletionInComplex();
        data[schema.fieldIndex(RANGE_DELETION.fieldName())] = makeRangeDeletion();
        data[schema.fieldIndex(TTL.fieldName())] = makeTTL();
        return new GenericInternalRow(data);
    }

    @Override
    public long getTimestamp(TimeUnit timeUnit)
    {
        return timeUnit.convert(maxTimestampMicros, TimeUnit.MICROSECONDS);
    }

    @Override
    public List<ValueWithMetadata> getPartitionKeys()
    {
        return copyList(partitionKeys);
    }

    @Override
    public List<ValueWithMetadata> getClusteringKeys()
    {
        return copyList(clusteringKeys);
    }

    @Override
    public List<ValueWithMetadata> getStaticColumns()
    {
        return copyList(staticColumns);
    }

    @Override
    public List<ValueWithMetadata> getValueColumns()
    {
        return copyList(valueColumns);
    }

    @Override
    public Map<String, List<ByteBuffer>> getTombstonedCellsInComplex()
    {
        if (tombstonedCellsInComplex == null)
        {
            return null;
        }

        return new HashMap<>(tombstonedCellsInComplex);
    }

    @Override
    public List<RangeTombstone> getRangeTombstoneList()
    {
        return copyList(rangeTombstoneList);
    }

    @Override
    public TimeToLive getTtl()
    {
        return timeToLive;
    }

    private <T> List<T> copyList(List<T> input)
    {
        if (input == null)
        {
            return null;
        }
        return new ArrayList<>(input);
    }

    private void setPartitionKeys(UnfilteredRowIterator partition)
    {
        if (kind == Kind.PARTITION_DELETE)
        {
            updateMaxTimestamp(partition.partitionLevelDeletion().markedForDeleteAt());
        }

        ImmutableList<ColumnMetadata> columnMetadatas = partition.metadata().partitionKeyColumns();
        List<ValueWithMetadata> pk = new ArrayList<>(columnMetadatas.size());

        ByteBuffer pkbb = partition.partitionKey().getKey();
        // single partition key
        if (columnMetadatas.size() == 1)
        {
            pk.add(makeValue(pkbb, columnMetadatas.get(0)));
        }
        else // composite partition key
        {
            ByteBuffer[] pkbbs = ColumnTypes.split(pkbb, columnMetadatas.size());
            for (int i = 0; i < columnMetadatas.size(); i++)
            {
                pk.add(makeValue(pkbbs[i], columnMetadatas.get(i)));
            }
        }
        partitionKeys = pk;
    }

    private void setStaticColumns(UnfilteredRowIterator partition)
    {
        Row staticRow = partition.staticRow();

        if (staticRow.isEmpty())
        {
            return;
        }

        List<ValueWithMetadata> sc = new ArrayList<>(staticRow.columnCount());
        for (ColumnData cd : staticRow)
        {
            addColumn(sc, cd);
        }
        staticColumns = sc;
    }

    private void setClusteringKeys(Unfiltered unfiltered, UnfilteredRowIterator partition)
    {
        ImmutableList<ColumnMetadata> columnMetadatas = partition.metadata().clusteringColumns();
        if (columnMetadatas.isEmpty()) // the table has no clustering keys
        {
            return;
        }

        List<ValueWithMetadata> ck = new ArrayList<>(columnMetadatas.size());
        for (ColumnMetadata cm : columnMetadatas)
        {
            ByteBuffer ckbb = unfiltered.clustering().bufferAt(cm.position());
            ck.add(makeValue(ckbb, cm));
        }
        clusteringKeys = ck;
    }

    private void setValueColumns(Row row)
    {
        if (kind == Kind.ROW_DELETE)
        {
            updateMaxTimestamp(row.deletion().time().markedForDeleteAt());
            return;
        }

        // Just a sanity check. An empty row will not be added to the PartitionUpdate/cdc, so not really expect the case
        if (row.isEmpty())
        {
            LOGGER.warn("Encountered an unexpected empty row in CDC. keyspace={}, table={}", keyspace, table);
            return;
        }

        List<ValueWithMetadata> vc = new ArrayList<>(row.columnCount());
        for (ColumnData cd : row)
        {
            addColumn(vc, cd);
        }
        valueColumns = vc;
    }

    // Update the maxTimestamp if the input `timestamp` is larger.
    private void updateMaxTimestamp(long timestamp)
    {
        maxTimestampMicros = Math.max(maxTimestampMicros, timestamp);
    }

    private void addColumn(List<ValueWithMetadata> holder, ColumnData cd)
    {
        ColumnMetadata columnMetadata = cd.column();
        String columnName = columnMetadata.name.toCQLString();
        if (columnMetadata.isComplex()) // multi-cell column
        {
            ComplexColumnData complex = (ComplexColumnData) cd;
            DeletionTime deletionTime = complex.complexDeletion();
            // the complex data is live, but there could be element deletion inside. Check for it later in the block.
            if (deletionTime.isLive())
            {
                ComplexTypeBuffer buffer = ComplexTypeBuffer.newBuffer(complex.column().type, complex.cellsCount());
                boolean allTombstone = true;
                for (Cell<?> cell : complex)
                {
                    updateMaxTimestamp(cell.timestamp());
                    if (cell.isTombstone())
                    {
                        if (cell.column().type instanceof ListType)
                        {
                            LOGGER.warn("Unable to process element deletions inside a List type. Skipping...");
                            return;
                        }

                        CellPath path = cell.path();
                        if (path.size() > 0) // size can either be 0 (EmptyCellPath) or 1 (SingleItemCellPath).
                        {
                            addCellTombstoneInComplex(columnName, path.get(0));
                        }
                    }
                    else // cell is alive
                    {
                        allTombstone = false;
                        buffer.addCell(cell);
                        if (cell.isExpiring())
                        {
                            setTTL(cell.ttl(), cell.localDeletionTime());
                        }
                    }
                }

                // Multi-cell data types are collections and user defined type (UDT).
                // Update to collections does not mix additions with deletions, since updating with 'null' is rejected.
                // However, UDT permits setting 'null' value. It is possible to see tombstone and modification together
                // from the update to UDT
                if (allTombstone)
                {
                    holder.add(makeValue(null, complex.column()));
                }
                else
                {
                    holder.add(makeValue(buffer.build(), complex.column()));
                }
            }
            else // the entire multi-cell collection/UDT is deleted.
            {
                updateMaxTimestamp(deletionTime.markedForDeleteAt());
                holder.add(makeValue(null, complex.column()));
            }
        }
        else // simple column
        {
            Cell<?> cell = (Cell<?>) cd;
            updateMaxTimestamp(cell.timestamp());
            if (cell.isTombstone())
            {
                holder.add(makeValue(null, cell.column()));
            }
            else
            {
                holder.add(makeValue(cell.buffer(), cell.column()));
                if (cell.isExpiring())
                {
                    setTTL(cell.ttl(), cell.localDeletionTime());
                }
            }
        }
    }

    private void setTTL(int ttlInSec, int expirationTimeInSec)
    {
        // Skip updating TTL if it already has been set.
        // For the same row, the upsert query can only set one TTL value.
        if (timeToLive != null)
        {
            return;
        }

        timeToLive = new TimeToLive(ttlInSec, expirationTimeInSec);
    }

    // adds the serialized cellpath to the tombstone
    private void addCellTombstoneInComplex(String columnName, ByteBuffer key)
    {
        if (tombstonedCellsInComplex == null)
        {
            tombstonedCellsInComplex = new HashMap<>();
        }
        List<ByteBuffer> tombstones = tombstonedCellsInComplex.computeIfAbsent(columnName, k -> new ArrayList<>());
        tombstones.add(key);
    }

    private MapData makeCellDeletionInComplex()
    {
        // no tombstones in complex found
        if (tombstonedCellsInComplex == null || tombstonedCellsInComplex.isEmpty())
        {
            return null;
        }

        List<Object> cols = new ArrayList<>(tombstonedCellsInComplex.size());
        List<Object> tombstones = new ArrayList<>(tombstonedCellsInComplex.size());
        tombstonedCellsInComplex.forEach((colName, tombstonedKeys) -> {
            cols.add(UTF8String.fromString(colName)); // string in ArrayData is backed by UTF8String
            Object[] keys = tombstonedKeys.stream().map(bb -> {
                assert bb.hasArray();
                return bb.array();
            }).toArray();
            tombstones.add(ArrayData.toArrayData(keys));
        });
        return ArrayBasedMapData.apply(cols.toArray(), tombstones.toArray());
    }

    /**
     * The opposite of {@link #makeCellDeletionInComplex()}
     */
    private void unmakeCellDeletionInComplex(org.apache.spark.sql.Row row)
    {
        int index = SCHEMA.fieldIndex(CELL_DELETION_IN_COMPLEX.fieldName());
        if (row.isNullAt(index))
        {
            tombstonedCellsInComplex = null;
            return;
        }

        Map<String, WrappedArray<byte[]>> unwrapped = row.getJavaMap(index);
        tombstonedCellsInComplex = new HashMap<>(unwrapped.size());
        unwrapped.forEach((k, v) -> {
            List<ByteBuffer> bbs = new ArrayList<>(v.size());
            for (int i = 0; i < v.size(); i++)
            {
                bbs.add(ByteBuffer.wrap(v.apply(i)));
            }
            tombstonedCellsInComplex.put(k, bbs);
        });
    }

    private void addRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        if (rangeTombstoneList == null)
        {
            rangeTombstoneList = new ArrayList<>();
            rangeTombstoneBuilder = new RangeTombstoneBuilder(tableMetadata);
        }

        if (marker.isBoundary())
        {
            RangeTombstoneBoundaryMarker boundaryMarker = (RangeTombstoneBoundaryMarker) marker;
            updateMaxTimestamp(boundaryMarker.startDeletionTime().markedForDeleteAt());
            updateMaxTimestamp(boundaryMarker.endDeletionTime().markedForDeleteAt());
        }
        else
        {
            updateMaxTimestamp(((RangeTombstoneBoundMarker) marker).deletionTime().markedForDeleteAt());
        }

        rangeTombstoneBuilder.add(marker);

        if (rangeTombstoneBuilder.canBuild())
        {
            rangeTombstoneList.add(rangeTombstoneBuilder.build());
        }
    }

    private void validateRangeTombstoneMarkers()
    {
        if (rangeTombstoneList == null)
        {
            return;
        }

        Preconditions.checkState(!rangeTombstoneBuilder.hasIncompleteRange(),
                                 "The last range tombstone is not closed");
    }

    private ArrayData makeRangeDeletion()
    {
        if (rangeTombstoneList == null || rangeTombstoneList.isEmpty())
        {
            return null;
        }

        // The range tombstones has been validated already. But it does not harm to check for another time.
        validateRangeTombstoneMarkers();
        return ArrayData.toArrayData(rangeTombstoneList.stream()
                                                       .map(RangeTombstone::toRow)
                                                       .toArray());
    }

    /**
     * The opposite of {@link #makeRangeDeletion()}
     */
    private void unmakeRangeDeletion(org.apache.spark.sql.Row row)
    {
        int index = SCHEMA.fieldIndex(RANGE_DELETION.fieldName());
        if (row.isNullAt(index))
        {
            rangeTombstoneList = null;
            return;
        }

        WrappedArray<org.apache.spark.sql.Row> rangeTombstones = row.getAs(index);
        rangeTombstoneList = new ArrayList<>(rangeTombstones.size());
        for (int i = 0; i < rangeTombstones.size(); i++)
        {
            rangeTombstoneList.add(RangeTombstone.EMPTY.fromRow(rangeTombstones.apply(i)));
        }
    }

    private ArrayData makeTTL()
    {
        if (timeToLive == null)
        {
            return null;
        }

        return ArrayData.toArrayData(new int[] { timeToLive.ttlInSec, timeToLive.expirationTimeInSec });
    }

    /**
     * The opposite of {@link #makeTTL()}
     */
    private void unmakeTTL(org.apache.spark.sql.Row row)
    {
        int index = SCHEMA.fieldIndex(TTL.fieldName());
        if (row.isNullAt(index))
        {
            timeToLive = null;
            return;
        }

        WrappedArray<Integer> tuple = row.getAs(index);
        timeToLive = new TimeToLive(tuple.apply(0), tuple.apply(1));
    }

    private ValueWithMetadata makeValue(ByteBuffer value, ColumnMetadata columnMetadata)
    {
        return ValueWithMetadata.of(columnMetadata.name.toCQLString(),
                                    columnMetadata.type.asCQL3Type().toString(),
                                    value);
    }

    public static class Builder implements SparkRowSink<AbstractCdcEvent>
    {
        private CdcEvent event = null;
        private UnfilteredRowIterator partition = null;

        public static final Builder EMPTY = new Builder(null, null);

        public static Builder of(Kind kind, UnfilteredRowIterator partition)
        {
            return new Builder(kind, partition);
        }

        private Builder(Kind kind, UnfilteredRowIterator partition)
        {
            if (kind == null || partition == null)
            {
                // creating an EMPTY builder
                return;
            }
            this.event = new CdcEvent(kind, partition);
            this.partition = partition;
        }

        public Builder withRow(Row row)
        {
            ensureBuilderNonempty();
            event.setClusteringKeys(row, partition);
            event.setValueColumns(row);
            return this;
        }

        public Builder addRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            ensureBuilderNonempty();
            event.addRangeTombstoneMarker(marker);
            return this;
        }

        public CdcEvent build()
        {
            ensureBuilderNonempty();
            event.validateRangeTombstoneMarkers();
            CdcEvent res = event;
            event = null;
            partition = null;
            return res;
        }

        @Override
        public AbstractCdcEvent fromRow(org.apache.spark.sql.Row row)
        {
            Preconditions.checkState(isEmptyBuilder(), "Cannot build from row with a non-empty builder.");
            String keyspace = row.getString(SCHEMA.fieldIndex(KEYSPACE));
            String table = row.getString(SCHEMA.fieldIndex(TABLE));
            String opType = row.getString(SCHEMA.fieldIndex(OPERATION_TYPE.fieldName()));
            Kind kind = Kind.valueOf(opType.toUpperCase());
            return new CdcEvent(kind, keyspace, table, row);
        }

        private boolean isEmptyBuilder()
        {
            return event == null || partition == null;
        }

        private void ensureBuilderNonempty()
        {
            Preconditions.checkState(!isEmptyBuilder(), "Cannot build with an empty builder.");
        }
    }
}
