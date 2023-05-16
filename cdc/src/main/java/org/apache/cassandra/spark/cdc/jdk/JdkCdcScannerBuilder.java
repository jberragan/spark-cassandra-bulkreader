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

package org.apache.cassandra.spark.cdc.jdk;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.cassandra.spark.cdc.AbstractCdcEvent;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.ICassandraSource;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.fourzero.CdcScannerBuilder;
import org.apache.cassandra.spark.reader.fourzero.CdcSortedStreamScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Row;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.stats.CdcStats;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class JdkCdcScannerBuilder extends CdcScannerBuilder<JdkValueMetadata,
                                                           JdkRangeTombstone,
                                                           JdkCdcEvent,
                                                           JdkCdcScannerBuilder.JdkCdcSortedStreamScanner>
{
    public JdkCdcScannerBuilder(@Nullable RangeFilter rangeFilter,
                                @NotNull CdcOffsetFilter offsetFilter,
                                @NotNull final Watermarker watermarker,
                                @NotNull final Function<String, Integer> minimumReplicasFunc,
                                @NotNull final AsyncExecutor executor,
                                @NotNull Map<CassandraInstance, List<CommitLog>> logs,
                                @NotNull final String jobId,
                                final ICassandraSource cassandraSource)
    {
        this(0, Partitioner.Murmur3Partitioner, CdcStats.DoNothingCdcStats.INSTANCE, rangeFilter, offsetFilter, minimumReplicasFunc, watermarker, jobId, executor, false, logs, cassandraSource);
    }

    public JdkCdcScannerBuilder(int partitionId,
                                Partitioner partitioner,
                                CdcStats stats,
                                @Nullable RangeFilter rangeFilter,
                                @NotNull CdcOffsetFilter offsetFilter,
                                Function<String, Integer> minimumReplicasFunc,
                                @NotNull Watermarker jobWatermarker,
                                @NotNull String jobId,
                                @NotNull AsyncExecutor executor,
                                boolean readCommitLogHeader,
                                @NotNull Map<CassandraInstance, List<CommitLog>> logs,
                                final ICassandraSource cassandraSource)
    {
        super(partitionId, partitioner, stats, rangeFilter, offsetFilter, minimumReplicasFunc, jobWatermarker, jobId, executor, readCommitLogHeader, logs, cassandraSource);
    }

    public JdkCdcSortedStreamScanner buildStreamScanner(Collection<PartitionUpdateWrapper> updates)
    {
        return new JdkCdcSortedStreamScanner(updates, cassandraSource);
    }

    public static class JdkCdcSortedStreamScanner extends CdcSortedStreamScanner<JdkValueMetadata, JdkRangeTombstone, JdkCdcEvent>
    {
        private final ICassandraSource cassandraSource;

        JdkCdcSortedStreamScanner(@NotNull Collection<PartitionUpdateWrapper> updates, ICassandraSource cassandraSource)
        {
            super(updates);
            this.cassandraSource = cassandraSource;
        }

        public JdkCdcEvent buildRowDelete(Row row, UnfilteredRowIterator partition)
        {
            return JdkCdcEvent.Builder.of(AbstractCdcEvent.Kind.ROW_DELETE, partition, cassandraSource)
                                      .withRow(row)
                                      .build();
        }

        public JdkCdcEvent buildUpdate(Row row, UnfilteredRowIterator partition)
        {
            return JdkCdcEvent.Builder.of(AbstractCdcEvent.Kind.UPDATE, partition, cassandraSource)
                                      .withRow(row)
                                      .build();
        }

        public JdkCdcEvent buildInsert(Row row, UnfilteredRowIterator partition)
        {
            return JdkCdcEvent.Builder.of(AbstractCdcEvent.Kind.INSERT, partition, cassandraSource)
                                      .withRow(row)
                                      .build();
        }

        public JdkCdcEvent makePartitionTombstone(UnfilteredRowIterator partition)
        {
            return JdkCdcEvent.Builder.of(AbstractCdcEvent.Kind.PARTITION_DELETE, partition, cassandraSource)
                                      .build();
        }

        public void handleRangeTombstone(RangeTombstoneMarker marker, UnfilteredRowIterator partition)
        {
            if (rangeDeletionBuilder == null)
            {
                rangeDeletionBuilder = JdkCdcEvent.Builder.of(AbstractCdcEvent.Kind.RANGE_DELETE, partition, cassandraSource);
            }
            rangeDeletionBuilder.addRangeTombstoneMarker(marker);
        }
    }
}
