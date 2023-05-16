package org.apache.cassandra.spark.reader.fourzero;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.cdc.AbstractCdcEvent;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.ICassandraSource;
import org.apache.cassandra.spark.cdc.SparkCdcEvent;
import org.apache.cassandra.spark.cdc.SparkRangeTombstone;
import org.apache.cassandra.spark.cdc.SparkValueWithMetadata;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Row;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.stats.ICdcStats;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.spark.TaskContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.reader.fourzero.SparkCdcScannerBuilder.SparkCdcSortedStreamScanner;

public class SparkCdcScannerBuilder extends CdcScannerBuilder<SparkValueWithMetadata,
                                                             SparkRangeTombstone,
                                                             SparkCdcEvent,
                                                             SparkCdcSortedStreamScanner>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkCdcScannerBuilder.class);

    public SparkCdcScannerBuilder(int partitionId,
                                  Partitioner partitioner,
                                  ICdcStats stats,
                                  @Nullable RangeFilter rangeFilter,
                                  @NotNull CdcOffsetFilter offsetFilter,
                                  Function<String, Integer> minimumReplicasFunc,
                                  @NotNull Watermarker jobWatermarker,
                                  @NotNull String jobId,
                                  @NotNull AsyncExecutor executor,
                                  boolean readCommitLogHeader,
                                  @NotNull Map<CassandraInstance, List<CommitLog>> logs,
                                  ICassandraSource cassandraSource)
    {
        super(partitionId,
              partitioner,
              stats,
              rangeFilter,
              offsetFilter,
              minimumReplicasFunc,
              jobWatermarker,
              jobId,
              executor,
              readCommitLogHeader,
              logs,
              cassandraSource);
    }

    @Override
    public SparkCdcSortedStreamScanner buildStreamScanner(Collection<PartitionUpdateWrapper> updates)
    {
        return new SparkCdcSortedStreamScanner(updates, cassandraSource);
    }

    @Override
    public void schedulePersist()
    {
        if (TaskContext.get() == null)
        {
            LOGGER.error("Spark Context null, cannot add TaskCompletionListener");
            return;
        }

        TaskContext.get().addTaskCompletionListener(context -> {
            if (context.isCompleted() && context.fetchFailed().isEmpty())
            {
                LOGGER.info("Persisting Watermark on task completion partitionId={}", partitionId);
                watermarker.persist(offsetFilter.maxAgeMicros()); // once we have read all commit logs we can persist the watermark state
            }
            else
            {
                LOGGER.warn("Not persisting Watermark due to task failure partitionId={}", partitionId, context.fetchFailed().get());
            }
        });
    }

    public static class SparkCdcSortedStreamScanner extends CdcSortedStreamScanner<SparkValueWithMetadata, SparkRangeTombstone, SparkCdcEvent>
    {
        private final ICassandraSource cassandraSource;

        SparkCdcSortedStreamScanner(@NotNull Collection<PartitionUpdateWrapper> updates, ICassandraSource cassandraSource)
        {
            super(updates);
            this.cassandraSource = cassandraSource;
        }

        @Override
        public SparkCdcEvent buildRowDelete(Row row, UnfilteredRowIterator partition)
        {
            return SparkCdcEvent.Builder.of(AbstractCdcEvent.Kind.ROW_DELETE, partition, cassandraSource)
                                        .withRow(row)
                                        .build();
        }

        @Override
        public SparkCdcEvent buildUpdate(Row row, UnfilteredRowIterator partition)
        {
            return SparkCdcEvent.Builder.of(AbstractCdcEvent.Kind.UPDATE, partition, cassandraSource)
                                        .withRow(row)
                                        .build();
        }

        @Override
        public SparkCdcEvent buildInsert(Row row, UnfilteredRowIterator partition)
        {
            return SparkCdcEvent.Builder.of(AbstractCdcEvent.Kind.INSERT, partition, cassandraSource)
                                        .withRow(row)
                                        .build();
        }

        @Override
        public SparkCdcEvent makePartitionTombstone(UnfilteredRowIterator partition)
        {
            return SparkCdcEvent.Builder.of(AbstractCdcEvent.Kind.PARTITION_DELETE, partition, cassandraSource).build();
        }

        public void handleRangeTombstone(RangeTombstoneMarker marker, UnfilteredRowIterator partition)
        {
            if (rangeDeletionBuilder == null)
            {
                rangeDeletionBuilder = SparkCdcEvent.Builder.of(AbstractCdcEvent.Kind.RANGE_DELETE, partition, cassandraSource);
            }
            rangeDeletionBuilder.addRangeTombstoneMarker(marker);
        }
    }
}
