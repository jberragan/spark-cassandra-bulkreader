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

package org.apache.cassandra.spark.reader.fourzero;

import java.util.Collection;
import java.util.PriorityQueue;
import java.util.Queue;

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.cdc.AbstractCdcEvent;
import org.apache.cassandra.spark.cdc.fourzero.CdcEvent;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Row;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.spark.cdc.AbstractCdcEvent.Kind;

/**
 * A scanner that is backed by a sorted collection of {@link PartitionUpdateWrapper}.
 * Not thread safe. (And not a stream)
 */
public class CdcSortedStreamScanner implements IStreamScanner<AbstractCdcEvent>
{
    private final Queue<PartitionUpdateWrapper> updates;
    private final UnfilteredPartitionIterator partitionIterator;

    private UnfilteredRowIterator currentPartition = null;
    private AbstractCdcEvent event;
    private CdcEvent.Builder rangeDeletionBuilder;

    CdcSortedStreamScanner(@NotNull Collection<PartitionUpdateWrapper> updates)
    {
        this.updates = new PriorityQueue<>(PartitionUpdateWrapper::compareTo);
        this.updates.addAll(updates);
        this.partitionIterator = new HybridUnfilteredPartitionIterator();
    }

    @Override
    public AbstractCdcEvent data()
    {
        Preconditions.checkState(event != null,
                                 "No data available. Make sure hasNext is called before this method!");
        AbstractCdcEvent data = event;
        event = null; // reset to null
        return data;
    }

    /**
     * Prepare the {@link #data()} to be fetched.
     * Briefly, the procedure is to iterate through the rows/rangetombstones in each partition.
     * A CdcEvent is produced for each row.
     * For range tombstones, a CdcEvent is produced for all markers/bounds combined within the same partition.
     * Caller must call this method before calling {@link #data()}.
     * @return true if there are more data; otherwise, false.
     */
    @Override
    public boolean next()
    {
        while (true)
        {
            if (allExhausted())
            {
                return false;
            }

            if (currentPartition == null)
            {
                currentPartition = partitionIterator.next();

                // it is a Cassandra partition deletion
                if (!currentPartition.partitionLevelDeletion().isLive())
                {
                    event = makePartitionTombstone(currentPartition);
                    currentPartition = null;
                    return true;
                }
            }

            if (!currentPartition.hasNext())
            {
                // The current partition is exhausted. Clean up and advance to the next partition by `continue`.
                currentPartition = null; // reset
                // Publish any range deletion for the partition
                if (rangeDeletionBuilder != null)
                {
                    event = rangeDeletionBuilder.build();
                    rangeDeletionBuilder = null; // reset
                    return true;
                }
                else
                {
                    continue;
                }
            }

            // An unfiltered can either be a Row or RangeTombstoneMarker
            Unfiltered unfiltered = currentPartition.next();

            if (unfiltered.isRow())
            {
                Row row = (Row) unfiltered;
                event = makeRow(row, currentPartition);
                return true;
            }
            else if (unfiltered.isRangeTombstoneMarker())
            {
                // Range tombstone can get complicated.
                // - In the most simple case, that is a DELETE statement with a single clustering key range, we expect
                //   the UnfilteredRowIterator with 2 markers, i.e. open and close range tombstone markers
                // - In a slightly more complicated case, it contains IN operator (on prior clustering keys), we expect
                //   the UnfilteredRowIterator with 2 * N markers, where N is the number of values specified for IN.
                // - In the most complicated case, client could comopse a complex partition update with a BATCH statement.
                //   It could have those further scenarios: (only discussing the statements applying to the same partition key)
                //   - Multiple disjoint ranges => we should expect 2 * N markers, where N is the number of ranges.
                //   - Overlapping ranges with the same timestamp => we should expect 2 markers, considering the
                //     overlapping ranges are merged into a single one. (as the boundary is omitted)
                //   - Overlapping ranges with different timestamp ==> we should expect 3 markers, i.e. open bound,
                //     boundary and end bound
                //   - Ranges mixed with INSERT! => The order of the unfiltered (i.e. Row/RangeTombstoneMarker) is determined
                //     by comparing the row clustering with the bounds of the ranges. See o.a.c.d.r.RowAndDeletionMergeIterator
                RangeTombstoneMarker rangeTombstoneMarker = (RangeTombstoneMarker) unfiltered;
                // We encode the ranges within the same spark row. Therefore, it needs to keep the markers when
                // iterating through the partition, and _only_ generate a spark row with range tombstone info when
                // exhausting the partition / UnfilteredRowIterator.
                handleRangeTombstone(rangeTombstoneMarker, currentPartition);
                // continue to consume the next unfiltered row/marker
            }
            else
            {
                // As of Cassandra 4, the unfiltered kind can either be row or range tombstone marker, see o.a.c.db.rows.Unfiltered.Kind
                // Having the else branch only for completeness.
                throw new IllegalStateException("Encountered unknown Unfiltered kind.");
            }
        }
    }

    @Override
    public void advanceToNextColumn()
    {
        throw new UnsupportedOperationException("not implemented!");
    }

    private boolean allExhausted()
    {
        return !partitionIterator.hasNext() // no next partition
               && currentPartition == null // current partition has exhausted
               && rangeDeletionBuilder == null; // no range deletion being built
    }

    /**
     * An {@link UnfilteredPartitionIterator} that is composed of partition data from different tables.
     * Note that the {@link HybridUnfilteredPartitionIterator#metadata()} reflects the metadata of the partition read
     * from {@link HybridUnfilteredPartitionIterator#next()}.
     */
    private class HybridUnfilteredPartitionIterator implements UnfilteredPartitionIterator
    {
        private PartitionUpdateWrapper next;

        /**
         * @return the table metadata of the partition of the next CdcUpdate.
         *         When the next is null, this method returns null too.
         */
        @Override
        public TableMetadata metadata()
        {
            return next == null
                   ? null
                   : next.partitionUpdate().metadata();
        }

        @Override
        public void close()
        {
            // do nothing
        }

        @Override
        public boolean hasNext()
        {
            if (next == null)
            {
                next = updates.poll();
            }
            return next != null;
        }

        // Note: calling it multiple times without calling hasNext does not advance.
        // It is also assumed that hasNext is called before this method.
        @Override
        public UnfilteredRowIterator next()
        {
            PartitionUpdate update = next.partitionUpdate();
            next = null;
            return update.unfilteredIterator();
        }
    }


    @Override
    public void close()
    {
        updates.clear();
    }

    private AbstractCdcEvent makeRow(Row row, UnfilteredRowIterator partition)
    {
        // It is a Cassandra row deletion
        if (!row.deletion().isLive())
        {
            return CdcEvent.Builder.of(Kind.ROW_DELETE, partition)
                                   .withRow(row)
                                   .build();
        }

        // Empty primaryKeyLivenessInfo == update; non-empty == insert
        // The cql row could also be a deletion kind.
        // Here, it only _assumes_ UPDATE/INSERT, and the kind is updated accordingly on build.
        return CdcEvent.Builder.of(row.primaryKeyLivenessInfo().isEmpty()
                                   ? Kind.UPDATE
                                   : Kind.INSERT, partition)
                               .withRow(row)
                               .build();
    }

    private AbstractCdcEvent makePartitionTombstone(UnfilteredRowIterator partition)
    {
        return CdcEvent.Builder.of(Kind.PARTITION_DELETE, partition).build();
    }

    private void handleRangeTombstone(RangeTombstoneMarker marker, UnfilteredRowIterator partition)
    {
        if (rangeDeletionBuilder == null)
        {
            rangeDeletionBuilder = CdcEvent.Builder.of(Kind.RANGE_DELETE, partition);
        }
        rangeDeletionBuilder.addRangeTombstoneMarker(marker);
    }
}
