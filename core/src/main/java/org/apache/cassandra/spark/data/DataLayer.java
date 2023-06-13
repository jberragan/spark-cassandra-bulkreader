package org.apache.cassandra.spark.data;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.ICassandraSource;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.SparkCdcEvent;
import org.apache.cassandra.spark.cdc.TableIdLookup;
import org.apache.cassandra.spark.cdc.watermarker.DoNothingWatermarker;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.BigNumberConfig;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.reader.EmptyScanner;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.reader.Rid;
import org.apache.cassandra.spark.sparksql.NoMatchFoundException;
import org.apache.cassandra.spark.sparksql.filters.CdcOffset;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.sparksql.filters.SerializableCommitLog;
import org.apache.cassandra.spark.stats.CdcStats;
import org.apache.cassandra.spark.stats.ICdcStats;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.spark.utils.TimeUtils;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.MetadataBuilder;
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
@SuppressWarnings({ "unused", "WeakerAccess" })
public abstract class DataLayer implements Serializable
{

    public static final long serialVersionUID = 42L;
    public static final int DEFAULT_CDC_SUB_MICRO_BATCH_SIZE = 5;

    public DataLayer()
    {
    }

    /**
     * Defines the SparkSQL table schema.
     * In the case of bulk read, it maps from the Cassandra table schema of the table reading from.
     * In the case of CDC, the table schema is generic to represent changes from multiple Cassandra tables.
     *
     * @return SparkSQL table schema
     */
    public StructType structType()
    {
        if (isCdc())
        {
            return SparkCdcEvent.SCHEMA;
        }

        StructType structType = new StructType();
        for (final SparkCqlField field : cqlTable().sparkFields())
        {
            // pass Cassandra field metadata in StructField metadata
            final MetadataBuilder metadata = new MetadataBuilder();
            metadata.putLong("position", field.pos());
            metadata.putString("cqlType", field.cqlTypeName());
            metadata.putBoolean("isPartitionKey", field.isPartitionKey());
            metadata.putBoolean("isPrimaryKey", field.isPrimaryKey());
            metadata.putBoolean("isClusteringKey", field.isClusteringColumn());
            metadata.putBoolean("isStaticColumn", field.isStaticColumn());
            metadata.putBoolean("isValueColumn", field.isValueColumn());

            structType = structType.add(field.name(),
                                        field.type().sparkSqlType(bigNumberConfig(field)),
                                        true,
                                        metadata.build());
        }
        // append the requested feature fields
        for (SchemaFeature f : requestedFeatures())
        {
            structType = structType.add(f.field());
        }

        return structType;
    }

    /**
     * Requested schema feature for bulk reader job.
     *
     * @return the list of requested features.
     */
    public List<SchemaFeature> requestedFeatures()
    {
        return Collections.emptyList();
    }

    /**
     * Determine the mode that spark job operates in. Currently, the job can either run as a CDC job or bulk read job.
     * Therefore, a boolean is used. If the usage is expanded, the api is going to be changed.
     * Implicitly, the job kind of knows the mode without the indicator when the job runs. The CDC job uses the spark
     * stream api and the bulk read job does not.
     * By providing this information explicitly (loading from job options), it helps to construct the job accordingly
     * _before_ starting.
     *
     * @return true if runs in the CDC mode; otherwise it runs bulk read job. The default is false.
     */
    public boolean isCdc()
    {
        return false;
    }

    /**
     * DataLayer can override this method to return the BigInteger/BigDecimal precision/scale values for a given column
     *
     * @param field the cql field
     * @return a BigNumberConfig object that specifies the desired precision/scale for BigDecimal and BigInteger.
     */
    public BigNumberConfig bigNumberConfig(final CqlField field)
    {
        return BigNumberConfig.DEFAULT;
    }

    /**
     * @return Cassandra version (3.0, 4.0 etc)
     */
    public abstract CassandraVersion version();

    /**
     * @return version specific CassandraBridge wrapping shaded packages
     */
    public CassandraBridge bridge()
    {
        return CassandraBridge.get(version());
    }

    public abstract int partitionCount();

    /**
     * @return CqlTable object for table being read, batch/bulk read jobs only.
     */
    public abstract SparkCqlTable cqlTable();

    /**
     * @return set of CqlTable objects for all CDC-enabled tables.
     */
    public abstract Set<CqlTable> cdcTables();

    public abstract boolean isInPartition(final int partitionId, final BigInteger token, final ByteBuffer key);

    public List<PartitionKeyFilter> partitionKeyFiltersInRange(final int partitionId, final List<PartitionKeyFilter> partitionKeyFilters) throws NoMatchFoundException
    {
        return partitionKeyFilters;
    }

    /**
     * @return a CommitLogProvider that lists all commit logs available across all replicas.
     */
    public abstract CommitLogProvider commitLogs();

    public abstract TableIdLookup tableIdLookup();

    public ICassandraSource getCassandraSource()
    {
        return (keySpace, table, columnsToFetch, primaryKeyColumns) -> null;
    }
    /**
     * DataLayer implementation should provide a RangeFilter to filter out partitions and mutations
     * that do not overlap with the Spark worker's token range.
     *
     * @param partitionId the partitionId for the task
     * @return RangeFilter for the Spark worker's token range
     */
    public RangeFilter rangeFilter(int partitionId)
    {
        return null;
    }

    /**
     * DataLayer implementation should provide an ExecutorService for doing blocking i/o when opening SSTable readers or reading CDC CommitLogs.
     * It is the responsibility of the DataLayer implementation to appropriately size and manage this ExecutorService.
     *
     * @return executor service
     */
    protected abstract ExecutorService executorService();

    /**
     * @param partitionId         the partitionId of the task
     * @param rangeFilter         range filter
     * @param partitionKeyFilters the list of partition key filters
     * @return set of SSTables
     */
    public abstract SSTablesSupplier sstables(final int partitionId,
                                              @Nullable final RangeFilter rangeFilter,
                                              @NotNull final List<PartitionKeyFilter> partitionKeyFilters);

    public abstract Partitioner partitioner();

    /**
     * It specifies the minimum number of replicas required for CDC.
     * For example, the minimum number of PartitionUpdates for compaction,
     * and the minimum number of replicas to pull logs from to proceed to compaction.
     *
     * @return the minimum number of replicas for a provided keyspace. The returned value must be 1 or more.
     */
    public Function<String, Integer> minimumReplicasForCdc()
    {
        return (keyspace) -> 1;
    }

    /**
     * @return a string that uniquely identifies this Spark job.
     */
    public abstract String jobId();

    public int cdcSubMicroBatchSize()
    {
        return DEFAULT_CDC_SUB_MICRO_BATCH_SIZE;
    }

    /**
     * Override this method with a Watermarker implementation that persists high and low watermarks per Spark partition between Streaming batches.
     *
     * @return watermarker for persisting high and low watermark and late updates.
     */
    public Watermarker cdcWatermarker()
    {
        return DoNothingWatermarker.INSTANCE;
    }

    /**
     * Override to always read the commit log header, this might be required for compressed or encrypted commit logs.
     *
     * @return true if BufferingCommitLogReader should always read the CommitLog header, or false if it should attempt to skip the header if seeking to highwaterMark position.
     */
    public boolean canSkipReadCdcHeader()
    {
        return false;
    }

    public Duration cdcWatermarkWindow()
    {
        return Duration.ofSeconds(600);
    }

    public CdcOffset initialOffset(long maxAgeMicros)
    {
        return new CdcOffset(TimeUtils.nowMicros() - maxAgeMicros);
    }

    public CdcOffset latestOffset(long minAgeMicros)
    {
        // list commit logs on all instances to build latest CdcOffset
        return new CdcOffset(TimeUtils.nowMicros() - minAgeMicros, commitLogs().logs());
    }

    /**
     * @param partitionId the partitionId for the task
     * @param offset      cdc offset filter that provides the start and end offset to start reading from.
     * @return all the commit logs we need to read in this Spark partition.
     */
    public Map<CassandraInstance, List<CommitLog>> partitionLogs(final int partitionId, @NotNull CdcOffsetFilter offset)
    {
        return offset.allLogs().entrySet().stream()
                     .collect(Collectors.toMap(
                     Map.Entry::getKey,
                     e -> e.getValue().stream().map(log -> toLog(partitionId, e.getKey(), log))
                           .collect(Collectors.toList())));
    }

    /**
     * Converts the {@link SerializableCommitLog} into a {@link CommitLog} that we can read from.
     * The method is called on the Spark executors.
     * The argument partitionId should be preferred over the one read from {@link org.apache.spark.TaskContext}
     *
     * @param partitionId the partitionId for the task
     * @param instance    the Cassandra instance
     * @param commitLog   a @{link org.apache.cassandra.spark.sparksql.filters.CdcOffset.SerializableCommitLog}.
     * @return a {@link org.apache.cassandra.spark.cdc.CommitLog}.
     */
    public abstract CommitLog toLog(final int partitionId, CassandraInstance instance, SerializableCommitLog commitLog);

    public IStreamScanner<SparkCdcEvent> openCdcScanner(final int partitionId,
                                                        @NotNull Set<CqlTable> cdcTables,
                                                        @NotNull CdcOffsetFilter offset)
    {
        return bridge().getCdcScanner(partitionId, cdcTables, partitioner(), tableIdLookup(),
                                      cdcStats(), rangeFilter(partitionId), offset,
                                      minimumReplicasForCdc(), cdcWatermarker(), jobId(),
                                      executorService(), canSkipReadCdcHeader(), partitionLogs(partitionId, offset),
                                      cdcSubMicroBatchSize(), getCassandraSource());
    }

    public IStreamScanner<Rid> openCompactionScanner(final int partitionId, final List<PartitionKeyFilter> partitionKeyFilters)
    {
        return openCompactionScanner(partitionId, partitionKeyFilters, null);
    }

    /**
     * When true the SSTableReader should attempt to find the offset into the Data.db file for the Spark worker's token range.
     * This works by first binary searching the Summary.db file to find offset into Index.db file,
     * then reading the Index.db from the Summary.db offset to find the first offset in the Data.db file that overlaps with the Spark worker's token range.
     * This enables the reader to start reading from the first in-range partition in the Data.db file, and close after reading the last partition.
     * This feature improves scalability as more Spark workers shard the token range into smaller subranges.
     * This avoids wastefully reading the Data.db file for out-of-range partitions.
     *
     * @return true if, the SSTableReader should attempt to read Summary.db and Index.db files to find the start index offset into the Data.db file that overlaps with the Spark workers token range.
     */
    public boolean readIndexOffset()
    {
        return true;
    }

    /**
     * When true the SSTableReader should only read repaired SSTables from a single 'primary repair' replica and read unrepaired SSTables at the user set consistency level.
     *
     * @return true if the SSTableReader should only read repaired SSTables on single 'repair primary' replica.
     */
    public boolean useIncrementalRepair()
    {
        return true;
    }

    /**
     * @return CompactionScanner for iterating over one or more SSTables, compacting data and purging tombstones
     */
    public IStreamScanner<Rid> openCompactionScanner(final int partitionId,
                                                     final List<PartitionKeyFilter> partitionKeyFilters,
                                                     @Nullable PruneColumnFilter columnFilter)
    {
        List<PartitionKeyFilter> filtersInRange;
        try
        {
            filtersInRange = partitionKeyFiltersInRange(partitionId, partitionKeyFilters);
        }
        catch (NoMatchFoundException e)
        {
            return EmptyScanner.INSTANCE;
        }
        final RangeFilter rangeFilter = rangeFilter(partitionId);
        return bridge().getCompactionScanner(cqlTable(), partitioner(), sstables(partitionId, rangeFilter, filtersInRange),
                                             rangeFilter, filtersInRange, columnFilter, timeProvider(),
                                             readIndexOffset(), useIncrementalRepair(), stats());
    }

    /**
     * @return a TimeProvider that returns the time now in seconds. User can override with their own provider.
     */
    public TimeProvider timeProvider()
    {
        return bridge().timeProvider();
    }

    /**
     * @param filters array of push down filters that
     * @return an array of push filters that are *not* supported by this data layer
     */
    public Filter[] unsupportedPushDownFilters(final Filter[] filters)
    {
        Set<String> partitionKeys = cqlTable().partitionKeys().stream()
                                              .map(key -> StringUtils.lowerCase(key.name()))
                                              .collect(Collectors.toSet());

        List<Filter> unsupportedFilters = new ArrayList<>(filters.length);
        for (Filter filter : filters)
        {
            if (filter instanceof EqualTo || filter instanceof In)
            {
                String columnName = StringUtils.lowerCase(filter instanceof EqualTo
                                                          ? ((EqualTo) filter).attribute()
                                                          : ((In) filter).attribute());

                if (partitionKeys.contains(columnName))
                {
                    partitionKeys.remove(columnName);
                }
                else
                {
                    // only partition keys are supported
                    unsupportedFilters.add(filter);
                }
            }
            else
            {
                // push down filters other than EqualTo & In not supported yet
                unsupportedFilters.add(filter);
            }
        }
        // If the partition keys are not in the filter, we disable push down
        return partitionKeys.size() > 0 ? filters : unsupportedFilters.toArray(new Filter[0]);
    }

    public ICdcStats cdcStats()
    {
        return CdcStats.DoNothingCdcStats.INSTANCE;
    }

    /**
     * Override to plug in your own Stats instrumentation for recording internal events.
     *
     * @return Stats implementation to record internal events
     */
    public Stats stats()
    {
        return Stats.DoNothingStats.INSTANCE;
    }
}
