package org.apache.cassandra.spark.reader.fourzero;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.AbstractCompactionController;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DecoratedKey;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Keyspace;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.compaction.OperationType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Row;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.CompactionParams;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.utils.IOUtils;
import org.apache.cassandra.spark.utils.TimeProvider;
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

public class CompactionStreamScanner extends AbstractStreamScanner
{
    private final Collection<? extends Scannable> toCompact;
    private final UUID taskId;

    private PurgingCompactionController controller;
    private AbstractCompactionStrategy.ScannerList scanners;
    private final TimeProvider timeProvider;
    private CompactionIterator ci;

    CompactionStreamScanner(@NotNull final TableMetadata cfMetaData,
                            @NotNull final Partitioner partitionerType,
                            @NotNull final Collection<? extends Scannable> toCompact)
    {
        this(cfMetaData, partitionerType, TimeProvider.INSTANCE, toCompact);
    }

    CompactionStreamScanner(@NotNull final TableMetadata cfMetaData,
                            @NotNull final Partitioner partitionerType,
                            @NotNull final TimeProvider timeProvider,
                            @NotNull final Collection<? extends Scannable> toCompact)
    {
        super(cfMetaData, partitionerType);
        this.timeProvider = timeProvider;
        this.toCompact = toCompact;
        this.taskId = UUID.randomUUID();
    }

    @Override
    public void close()
    {
        Arrays.asList(controller, scanners, ci)
              .forEach(IOUtils::closeQuietly);
    }

    @Override
    protected void handleRowTombstone(Row row)
    {
        throw new IllegalStateException("Row tombstone found, it should have been purged in CompactionIterator");
    }

    @Override
    protected void handlePartitionTombstone(UnfilteredRowIterator partition)
    {
        throw new IllegalStateException("Partition tombstone found, it should have been purged in CompactionIterator");
    }

    @Override
    UnfilteredPartitionIterator initializePartitions()
    {
        final int nowInSec = timeProvider.now();
        final Keyspace keyspace = Keyspace.openWithoutSSTables(metadata.keyspace);
        final ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore(metadata.name);
        this.controller = new PurgingCompactionController(cfStore, CompactionParams.TombstoneOption.NONE);
        List<ISSTableScanner> scannerList = toCompact.stream()
                                                     .map(Scannable::scanner)
                                                     .collect(Collectors.toList());
        this.scanners = new AbstractCompactionStrategy.ScannerList(scannerList);
        this.ci = new CompactionIterator(OperationType.COMPACTION, scanners.scanners, controller, nowInSec, taskId);
        return this.ci;
    }

    private static class PurgingCompactionController extends AbstractCompactionController implements AutoCloseable
    {
        PurgingCompactionController(final ColumnFamilyStore cfs, final CompactionParams.TombstoneOption tombstoneOption)
        {
            super(cfs, Integer.MAX_VALUE, tombstoneOption);
        }

        public boolean compactingRepaired()
        {
            return false;
        }

        public LongPredicate getPurgeEvaluator(final DecoratedKey key)
        {
            // purge all tombstones
            return (time) -> true;
        }

        public void close()
        {
        }
    }
}
