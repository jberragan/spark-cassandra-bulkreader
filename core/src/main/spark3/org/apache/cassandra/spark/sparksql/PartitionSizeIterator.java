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

package org.apache.cassandra.spark.sparksql;

import java.io.IOException;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.SparkCqlTable;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.reader.IndexEntry;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.jetbrains.annotations.NotNull;

/**
 * Wrapper iterator around IndexIterator to read all Index.db files and return SparkSQL rows containing all partition keys and the associated on-disk uncompressed and compressed sizes.
 */
public class PartitionSizeIterator implements PartitionReader<InternalRow>
{
    private final IStreamScanner<IndexEntry> it;
    private final SparkCqlTable cqlTable;
    private final int numPartitionKeys;
    private final Stats stats;
    private final long startTimeNanos;
    private GenericInternalRow curr = null;

    public PartitionSizeIterator(final int partitionId, @NotNull final DataLayer dataLayer)
    {
        this.cqlTable = dataLayer.cqlTable();
        this.numPartitionKeys = cqlTable.numPartitionKeys();
        this.stats = dataLayer.stats();
        this.startTimeNanos = System.nanoTime();
        this.it = dataLayer.openPartitionSizeIterator(partitionId);
        stats.openedPartitionSizeIterator(System.nanoTime() - startTimeNanos);
    }

    public boolean next() throws IOException
    {
        if (it.next())
        {
            it.advanceToNextColumn();

            final IndexEntry entry = it.data();
            final Object[] values = new Object[numPartitionKeys + 2];

            SparkCellIterator.readPartitionKey(entry.getPartitionKey(), cqlTable, values, stats);
            values[numPartitionKeys] = entry.getUncompressed();
            values[numPartitionKeys + 1] = entry.getCompressed();

            this.curr = new GenericInternalRow(values);
            stats.emitIndexEntry(entry);

            return true;
        }

        return false;
    }

    public InternalRow get()
    {
        return curr;
    }

    public void close() throws IOException
    {
        this.it.close();
        stats.closedPartitionSizeIterator(System.nanoTime() - startTimeNanos);
    }
}
