package org.apache.cassandra.spark.reader.fourzero;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.reader.Rid;
import org.apache.cassandra.spark.reader.common.SSTableStreamException;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Clustering;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DeletionTime;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ListType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.MapType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.SetType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UserType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Cell;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.ColumnData;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Row;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.ByteBufferUtil;
import org.jetbrains.annotations.NotNull;

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

public abstract class AbstractStreamScanner implements IStreamScanner, Closeable
{

    // all partitions in the SSTable
    private UnfilteredPartitionIterator allPartitions;
    // a single partition, containing rows and/or range tombstones
    private UnfilteredRowIterator partition;
    // the static row of the current partition, which may be empty
    @SuppressWarnings("FieldCanBeLocal")
    private Row staticRow;
    // current atom (row or range tombstone) being processed
    private Unfiltered unfiltered;
    // if processing a row this holds the state of iterating that row
    private Iterator<ColumnData> columns;
    // state of processing data for a single column in a row (which may be multi-celled in the case of complex columns)
    protected ColumnDataState columnData;

    @NotNull
    final TableMetadata metadata;

    protected final Rid rid = new Rid();

    AbstractStreamScanner(@NotNull final TableMetadata metadata, @NotNull final Partitioner partitionerType)
    {
        this.metadata = metadata.unbuild().partitioner(partitionerType == Partitioner.Murmur3Partitioner ? new Murmur3Partitioner() : new RandomPartitioner()).build();

        // counter tables are not supported
        if (metadata.isCounter())
        {
            throw new IllegalArgumentException(String.format("Streaming reads of SSTables from counter tables are " + "not supported, rejecting stream of data from %s.%s", metadata.keyspace, metadata.name));
        }
    }

    public Rid rid()
    {
        return rid;
    }

    /* abstract methods */

    abstract UnfilteredPartitionIterator initializePartitions();

    public abstract void close() throws IOException;

    protected abstract void handleRowTombstone(Row row);

    protected abstract void handlePartitionTombstone(UnfilteredRowIterator partition);

    protected abstract void handleCellTombstone();

    @Override
    public void advanceToNextColumn()
    {
        columnData.consume();
    }

    public boolean hasNext() throws IOException
    {
        if (allPartitions == null)
        {
            allPartitions = initializePartitions();
        }

        while (true)
        {
            if (partition == null)
            {
                try
                {
                    // we've exhausted the partition iterator
                    if (allPartitions.hasNext())
                    {
                        // advance to next partition
                        partition = allPartitions.next();

                        if (partition.partitionLevelDeletion().isLive())
                        {
                            // reset rid with new partition key
                            rid.setPartitionKeyCopy(partition.partitionKey().getKey(),
                                                    FourZeroUtils.tokenToBigInteger(partition.partitionKey().getToken()));
                        }
                        else // there's a partition level delete
                        {
                            handlePartitionTombstone(partition);
                            return true;
                        }
                    }
                    else
                    {
                        return false;
                    }
                }
                catch (final SSTableStreamException e)
                {
                    throw e.getIOException();
                }

                // if the partition has a non-empty static row, grab its columns
                // so we process those before moving onto its atoms (the Unfiltered instances)
                staticRow = partition.staticRow();
                if (!staticRow.isEmpty())
                {
                    columns = staticRow.iterator();
                    prepareColumnData();
                    return true;
                }
            }

            // we may be in the midst of processing some multi-cell column data. If so,
            // we'll resume that where we left off
            if (columnData != null && columnData.hasData())
            {
                return true;
            }

            // continue to process columns of the last read row, which may be static
            if (columns != null && columns.hasNext())
            {
                prepareColumnData();
                return true;
            }

            // current row was exhausted (or none were present), so move to the next atom
            columns = null;
            try
            {
                // advance to next unfiltered
                rid.setIsUpdate(false); // reset isUpdate flag
                if (partition.hasNext())
                {
                    unfiltered = partition.next();
                }
                else
                {
                    // current partition is exhausted
                    partition = null;
                    unfiltered = null;
                }
            }
            catch (final SSTableStreamException e)
            {
                throw e.getIOException();
            }

            if (unfiltered != null)
            {
                if (unfiltered.isRow())
                {
                    final Row row = (Row) unfiltered;

                    // there is a CQL row level delete
                    if (!row.deletion().isLive())
                    {
                        handleRowTombstone(row);
                        return true;
                    }

                    // For non-compact tables, setup a ClusteringColumnDataState to emit a Rid that emulates a
                    // pre-3.0 CQL row marker. This is necessary for backwards compatibility with 2.1 & 2.0 output,
                    // and also for tables with only primary key columns defined.
                    // An empty PKLI is the 3.0 equivalent of having no row marker (e.g. row modifications via
                    // UPDATE not INSERT) so we don't emit a fake row marker in that case.
                    final boolean emptyLiveness = row.primaryKeyLivenessInfo().isEmpty();
                    rid.setIsUpdate(emptyLiveness);
                    if (!emptyLiveness)
                    {
                        if (TableMetadata.Flag.isCQLTable(metadata.flags))
                        {
                            columnData = new ClusteringColumnDataState(row.clustering());
                        }
                        columns = row.iterator();
                        return true;
                    }

                    // The row's actual columns may be empty, in which case we'll simply skip over them during the next
                    // iteration and move to the next unfiltered. So then only the row marker and/or row deletion (if
                    // either are present) will get emitted.
                    columns = row.iterator();
                }
                else
                {
                    throw new IllegalStateException("Range tombstone found, it should have been purged in CompactionIterator");
                }
            }
        }
    }

    /**
     * Prepare the columnData to be consumed the next
     */
    private void prepareColumnData()
    {
        final ColumnData data = columns.next();
        if (data.column().isComplex())
        {
            columnData = new ComplexDataState(data.column().isStatic()
                                              ? Clustering.STATIC_CLUSTERING
                                              : unfiltered.clustering(),
                                              (ComplexColumnData) data);
        }
        else
        {
            columnData = new SimpleColumnDataState(data.column().isStatic()
                                                   ? Clustering.STATIC_CLUSTERING
                                                   : unfiltered.clustering(),
                                                   data);
        }
    }

    private interface ColumnDataState
    {
        /**
         * Indicate whether the column has data
         * @return true if it has data to be consumed
         */
        boolean hasData();

        /**
         * Consume the data in the column
         */
        void consume();
    }

    // maps clustering values to column data, to emulate CQL row markers which were removed
    // in C* 3.0, but which we must still emit Rid for in order to preserve
    // backwards compatibility and to handle tables containing only primary key columns
    protected final class ClusteringColumnDataState implements ColumnDataState
    {

        private boolean consumed = false;
        private final ClusteringPrefix clustering;

        ClusteringColumnDataState(final ClusteringPrefix clustering)
        {
            this.clustering = clustering;
        }

        public boolean hasData()
        {
            return !consumed;
        }

        public void consume()
        {
            if (!consumed)
            {
                rid.setColumnNameCopy(FourZeroUtils.encodeCellName(metadata, clustering, ByteBufferUtil.EMPTY_BYTE_BUFFER, null));
                rid.setValueCopy(ByteBufferUtil.EMPTY_BYTE_BUFFER);
                consumed = true;
            }
            else
            {
                throw new UnsupportedOperationException();
            }
        }
    }

    // holds current processing state of any simple column data
    private final class SimpleColumnDataState implements ColumnDataState
    {

        private ClusteringPrefix clustering;
        private final Cell cell;

        private SimpleColumnDataState(final ClusteringPrefix clustering, final ColumnData data)
        {
            Preconditions.checkArgument(data.column().isSimple(), "The type of the ColumnData should be simple");
            this.clustering = clustering;
            this.cell = (Cell) data;
        }

        public boolean hasData()
        {
            return (clustering != null);
        }

        public void consume()
        {
            final boolean isStatic = cell.column().isStatic();
            rid.setColumnNameCopy(FourZeroUtils.encodeCellName(metadata, isStatic ? Clustering.STATIC_CLUSTERING : clustering, cell.column().name.bytes, null));
            if (cell.isTombstone())
            {
                handleCellTombstone();
            }
            else
            {
                rid.setValueCopy(cell.buffer());
            }
            rid.setTimestamp(cell.timestamp());
            // null out clustering so hasData will return false
            clustering = null;
        }
    }

    // holds current processing state of any complex column data
    private final class ComplexDataState implements ColumnDataState
    {

        private final ColumnMetadata column;
        private ClusteringPrefix clustering;
        private final Iterator<Cell<?>> cells;
        private final int cellCount;
        private final DeletionTime deletionTime;

        private ComplexDataState(final ClusteringPrefix clustering, final ComplexColumnData data)
        {
            this.clustering = clustering;
            column = data.column();
            cells = data.iterator();
            cellCount = data.cellsCount();
            this.deletionTime = data.complexDeletion();
            this.clustering = clustering;
        }

        public boolean hasData()
        {
            return clustering != null && cells.hasNext();
        }

        public void consume()
        {
            rid.setColumnNameCopy(FourZeroUtils.encodeCellName(metadata, clustering, column.name.bytes, ByteBufferUtil.EMPTY_BYTE_BUFFER));
            if (deletionTime.isLive())
            {
                Cell cell = cells.next();
                long maxTimestamp = cell.timestamp();
                final ComplexTypeBuffer buffer = ComplexTypeBuffer.build(cell, cellCount);
                while (cells.hasNext())
                {
                    Cell nextCell = cells.next();
                    maxTimestamp = Math.max(maxTimestamp, nextCell.timestamp());
                    buffer.addCell(nextCell);
                }
                rid.setValueCopy(buffer.build());
                rid.setTimestamp(maxTimestamp);

                // if we've exhausted the cell iterator
                Preconditions.checkState(!cells.hasNext(), "Cells should have exhausted!");
            }
            else
            {
                handleCellTombstone();
                rid.setTimestamp(deletionTime.markedForDeleteAt());
            }

            // null out clustering to indicate no data
            clustering = null;
        }
    }

    private static abstract class ComplexTypeBuffer
    {
        private final List<ByteBuffer> bufs;
        private final int cellCount;
        private int len = 0;

        ComplexTypeBuffer(final int cellCount, final int bufferSize)
        {
            this.cellCount = cellCount;
            this.bufs = new ArrayList<>(bufferSize);
        }

        static ComplexTypeBuffer build(final Cell cell, final int cellCount)
        {
            final ComplexTypeBuffer buffer;
            final AbstractType<?> type = cell.column().type;
            if (type instanceof SetType)
            {
                buffer = new SetBuffer(cellCount);
            }
            else if (type instanceof ListType)
            {
                buffer = new ListBuffer(cellCount);
            }
            else if (type instanceof MapType)
            {
                buffer = new MapBuffer(cellCount);
            }
            else if (type instanceof UserType)
            {
                buffer = new UdtBuffer(cellCount);
            }
            else
            {
                throw new IllegalStateException("Unexpected type deserializing CQL Collection: " + type);
            }
            buffer.addCell(cell);
            return buffer;
        }

        void addCell(final Cell cell)
        {
            this.add(cell.buffer()); // copy over value
        }

        void add(final ByteBuffer buf)
        {
            bufs.add(buf);
            len += buf.remaining();
        }

        ByteBuffer build()
        {
            final ByteBuffer result = ByteBuffer.allocate(4 + (bufs.size() * 4) + len);
            result.putInt(cellCount);
            for (final ByteBuffer buf : bufs)
            {
                result.putInt(buf.remaining());
                result.put(buf);
            }
            return (ByteBuffer) result.flip();
        }
    }

    private static class SetBuffer extends ComplexTypeBuffer
    {
        SetBuffer(int cellCount)
        {
            super(cellCount, cellCount);
        }

        @Override
        void addCell(Cell cell)
        {
            this.add(cell.path().get(0)); // set - copy over key
        }
    }

    private static class ListBuffer extends ComplexTypeBuffer
    {
        ListBuffer(int cellCount)
        {
            super(cellCount, cellCount);
        }
    }

    private static class MapBuffer extends ComplexTypeBuffer
    {

        MapBuffer(int cellCount)
        {
            super(cellCount, cellCount * 2);
        }

        @Override
        void addCell(Cell cell)
        {
            this.add(cell.path().get(0)); // map - copy over key and value
            super.addCell(cell);
        }
    }

    private static class UdtBuffer extends ComplexTypeBuffer
    {
        UdtBuffer(int cellCount)
        {
            super(cellCount, cellCount);
        }
    }
}
