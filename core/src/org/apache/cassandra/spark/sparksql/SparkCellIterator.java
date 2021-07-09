package org.apache.cassandra.spark.sparksql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.sparksql.filters.CustomFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.stats.Stats;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.reader.Rid;
import org.apache.cassandra.spark.utils.ByteBufUtils;
import org.apache.cassandra.spark.utils.ColumnTypes;
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
 * Iterate through CompactionIterator, deserializing ByteBuffers and normalizing into Object[] array in column order
 */
public class SparkCellIterator implements Iterator<SparkCellIterator.Cell>, AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkCellIterator.class);

    private final DataLayer dataLayer;
    private final Stats stats;
    private final CqlSchema cqlSchema;
    private final Object[] values;
    private final int numPartitionKeys;
    private final boolean noValueColumns;
    @Nullable
    private final PruneColumnFilter columnFilter;
    private final long startTimeNanos;
    @NotNull
    private final IStreamScanner scanner;
    @NotNull
    private final Rid rid;

    // mutable iterator state
    private boolean skipPartition = false, newRow = false, closed = false;
    private Cell next = null;

    SparkCellIterator(@NotNull final DataLayer dataLayer, @Nullable final StructType requiredSchema, @NotNull final List<CustomFilter> filters)
    {
        this.dataLayer = dataLayer;
        this.stats = dataLayer.stats();
        this.cqlSchema = dataLayer.cqlSchema();
        this.numPartitionKeys = cqlSchema.numPartitionKeys();
        this.columnFilter = buildColumnFilter(requiredSchema, cqlSchema);
        if (this.columnFilter != null)
        {
            LOGGER.info("Adding prune column filter columns='{}'", String.join(",", columnFilter.requiredColumns()));

            // if we are reading only partition/clustering keys or static columns, no value columns
            final Set<String> valueColumns = cqlSchema.valueColumns().stream().map(CqlField::name).collect(Collectors.toSet());
            this.noValueColumns = columnFilter.requiredColumns().stream().noneMatch(valueColumns::contains);
        }
        else
        {
            this.noValueColumns = cqlSchema.numValueColumns() == 0;
        }

        // the value array copies across all the partition/clustering/static columns
        // and the single column value for this cell to the SparkRowIterator
        this.values = new Object[cqlSchema.numNonValueColumns() + (noValueColumns ? 0 : 1)];

        // open compaction scanner
        this.startTimeNanos = System.nanoTime();
        this.scanner = this.dataLayer.openCompactionScanner(filters, columnFilter);
        final long openTimeNanos = System.nanoTime() - this.startTimeNanos;
        LOGGER.info("Opened CompactionScanner runtimeNanos={}", openTimeNanos);
        stats.openedCompactionScanner(openTimeNanos);
        this.rid = this.scanner.getRid();
        stats.openedSparkCellIterator();
    }

    static PruneColumnFilter buildColumnFilter(StructType requiredSchema, CqlSchema cqlSchema)
    {
        final Set<String> requiredColumns = Optional.ofNullable(requiredSchema)
                                                    .map(structType -> Arrays.stream(structType.fields())
                                                                             .map(StructField::name)
                                                                             .filter(cqlSchema::has)
                                                                             .collect(Collectors.toSet()))
                                                    .orElse(null);

        if (requiredColumns == null)
        {
            return null;
        }

        // we can't exclude partition or clustering keys
        requiredColumns.addAll(cqlSchema.partitionKeys().stream().map(CqlField::name).collect(Collectors.toList()));
        requiredColumns.addAll(cqlSchema.clusteringKeys().stream().map(CqlField::name).collect(Collectors.toList()));
        return new PruneColumnFilter(requiredColumns);
    }

    static class Cell
    {
        final Object[] values;
        final int pos;
        final boolean isNewRow;
        final long timestamp;

        Cell(final Object[] values, final int pos, final boolean isNewRow, final long timestamp)
        {
            this.values = values;
            this.pos = pos;
            this.isNewRow = isNewRow;
            this.timestamp = timestamp;
        }
    }

    public boolean noValueColumns()
    {
        return noValueColumns;
    }

    @Override
    public boolean hasNext()
    {
        try
        {
            return hasNextThrows();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public boolean hasNextThrows() throws IOException
    {
        if (this.next != null || closed)
        {
            return !closed;
        }
        return getNext();
    }

    @Override
    public SparkCellIterator.Cell next()
    {
        final Cell result = this.next;
        assert result != null;
        this.next = null;
        this.newRow = false;
        stats.nextCell();
        return result;
    }

    private boolean getNext() throws IOException
    {
        while (this.scanner.hasNext())
        {
            this.scanner.next();

            // deserialize partition keys - if we have moved to a new partition - and update 'values' Object[] array
            maybeRebuildPartition();

            // skip partition e.g. if token is outside of Spark worker token range
            if (this.skipPartition)
            {
                continue;
            }

            // deserialize clustering keys - if moved to new CQL row - and update 'values' Object[] array
            final ByteBuffer columnNameBuf = Objects.requireNonNull(this.rid.getColumnName(), "ColumnName buffer in Rid is null, this is unexpected");
            maybeRebuildClusteringKeys(columnNameBuf);

            // deserialize CQL field column name
            final ByteBuffer component = ColumnTypes.extractComponent(columnNameBuf, cqlSchema.numClusteringKeys());
            final String columnName = component != null ? ByteBufUtils.stringThrowRuntime(component) : null;
            if (StringUtils.isEmpty(columnName))
            {
                if (this.noValueColumns)
                {
                    // special case where schema consists only of partition keys, clustering keys or static columns, no value columns
                    this.next = new Cell(values, 0, newRow, this.rid.getColumnTimestamp());
                    return true;
                }
                continue;
            }

            final CqlField field = cqlSchema.getField(columnName);
            if (field == null)
            {
                LOGGER.warn("Ignoring unknown column columnName='{}'", columnName);
                continue;
            }

            // deserialize value field or static column and update 'values' Object[] array
            deserializeField(field);

            // static column, so continue reading entire CQL row before returning
            if (field.isStaticColumn())
            {
                continue;
            }

            // update next Cell
            this.next = new Cell(values, field.pos(), newRow, this.rid.getColumnTimestamp());
            return true;
        }

        // finished so close
        this.next = null;
        this.close();
        return false;
    }

    @Override
    public void close() throws IOException
    {
        if (!closed)
        {
            this.scanner.close();
            this.closed = true;
            LOGGER.info("Closed CompactionScanner runtimeNanos={}", (System.nanoTime() - this.startTimeNanos));
            stats.closedSparkCellIterator(System.nanoTime() - startTimeNanos);
        }
    }

    /* iterator helpers */

    /**
     * If it is a new partition see if we can skip (e.g. if partition outside Spark worker token range), otherwise re-build partition keys
     */
    private void maybeRebuildPartition()
    {
        if (!this.rid.isNewPartition())
        {
            return;
        }

        // skip partitions not in the token range for this Spark partition
        this.newRow = true;
        this.skipPartition = !this.dataLayer.isInPartition(rid.getToken(), rid.getPartitionKey());
        if (this.skipPartition)
        {
            stats.skippedPartitionInIterator(rid.getPartitionKey(), rid.getToken());
            return;
        }

        // or new partition, so deserialize partition keys and update 'values' array
        final ByteBuffer partitionKey = rid.getPartitionKey();
        if (this.numPartitionKeys == 1)
        {
            // not a composite partition key
            final CqlField field = cqlSchema.partitionKeys().get(0);
            this.values[field.pos()] = field.deserialize(partitionKey);
        }
        else
        {
            // split composite partition keys
            final ByteBuffer[] partitionKeyBufs = ColumnTypes.split(partitionKey, this.numPartitionKeys);
            int idx = 0;
            for (final CqlField field : cqlSchema.partitionKeys())
            {
                this.values[field.pos()] = field.deserialize(partitionKeyBufs[idx++]);
            }
        }
    }

    /**
     * Deserialize clustering key components and update 'values' array if changed. Mark isNewRow true if we move to new CQL row.
     */
    private void maybeRebuildClusteringKeys(@NotNull final ByteBuffer columnNameBuf)
    {
        final List<CqlField> clusteringKeys = cqlSchema.clusteringKeys();
        if (clusteringKeys.isEmpty())
        {
            return;
        }

        int idx = 0;
        for (final CqlField field : clusteringKeys)
        {
            final Object newObj = field.deserialize(ColumnTypes.extractComponent(columnNameBuf, idx++));
            final Object oldObj = this.values[field.pos()];
            if (newRow || oldObj == null || newObj == null || !field.equals(newObj, oldObj))
            {
                newRow = true;
                this.values[field.pos()] = newObj;
            }
        }
    }

    /**
     * Deserialize value field if required and update 'values' array
     */
    private void deserializeField(@NotNull final CqlField field)
    {
        final Object value;
        if (columnFilter == null || this.columnFilter.includeColumn(field.name()))
        {
            // deserialize value
            value = field.deserialize(this.rid.getValue());
        }
        else
        {
            // prune columns push down filter does not contain column so don't need to deserialize
            value = null;
        }

        if (field.isStaticColumn())
        {
            this.values[field.pos()] = value;
            return;
        }

        this.values[this.values.length - 1] = value; // last idx in array always stores the cell value
    }
}
