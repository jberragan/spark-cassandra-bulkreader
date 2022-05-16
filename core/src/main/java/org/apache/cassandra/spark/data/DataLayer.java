package org.apache.cassandra.spark.data;

import java.io.InputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.TableIdLookup;
import org.apache.cassandra.spark.cdc.watermarker.DoNothingWatermarker;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.EmptyScanner;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.sparksql.NoMatchFoundException;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.Stats;

import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.DataTypes;
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

    public enum FileType
    {
        DATA("Data.db"),
        INDEX("Index.db"),
        FILTER("Filter.db"),
        STATISTICS("Statistics.db"),
        SUMMARY("Summary.db"),
        COMPRESSION_INFO("CompressionInfo.db"),
        TOC("TOC.txt"),
        DIGEST("Digest.sha1"),
        CRC("CRC.db"),
        CRC32("Digest.crc32"),
        COMMITLOG(".log");

        private final String fileSuffix;

        FileType(final String fileSuffix)
        {
            this.fileSuffix = fileSuffix;
        }

        private static final Map<String, FileType> FILE_TYPE_HASH_MAP = new HashMap<>();

        static
        {
            for (final DataLayer.FileType fileType : FileType.values())
            {
                FILE_TYPE_HASH_MAP.put(fileType.getFileSuffix(), fileType);
            }
        }

        public static DataLayer.FileType fromExtension(final String extension)
        {
            Preconditions.checkArgument(FILE_TYPE_HASH_MAP.containsKey(extension), "Unknown sstable file type: " + extension);
            return FILE_TYPE_HASH_MAP.get(extension);
        }

        @Nullable
        public static Path resolveComponentFile(final FileType fileType, final Path dataFilePath)
        {
            final Path filePath = fileType == FileType.DATA ? dataFilePath : dataFilePath.resolveSibling(dataFilePath.getFileName().toString().replace(FileType.DATA.getFileSuffix(), fileType.getFileSuffix()));
            return Files.exists(filePath) ? filePath : null;
        }

        public String getFileSuffix()
        {
            return fileSuffix;
        }
    }

    public DataLayer()
    {
    }

    /**
     * Map Cassandra CQL table schema to SparkSQL StructType
     *
     * @return StructType representation of CQL table
     */
    public StructType structType()
    {
        StructType structType = new StructType();
        for (final CqlField field : cqlSchema().fields())
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
        if (requestedFeatures().addLastModifiedTimestamp())
        {
            // special column that passes over last modified timestamp for a row
            structType = structType.add(requestedFeatures().lastModifiedTimestampColumnName(), DataTypes.TimestampType);
        }
        if (requestedFeatures().addUpdatedFieldsIndicator())
        {
            // special column that passes over updated field bitset field indicating which columns are unset and which are tombstones
            // this is only used for CDC
            structType = structType.add(requestedFeatures().updatedFieldsIndicatorColumnName(), DataTypes.BinaryType);
        }
        if (requestedFeatures().addUpdateFlag())
        {
            // special column that passes over boolean field marking if mutation was an UPDATE or an INSERT
            // this is only used for CDC
            structType = structType.add(requestedFeatures().updateFlagColumnName(), DataTypes.BooleanType);
        }
        if (requestedFeatures().supportCellDeletionInComplex())
        {
            // feature column that contains the column_name to a list of keys of the tombstoned values in a complex data type
            // this is only used for CDC
            structType = structType.add(requestedFeatures().supportCellDeletionInComplexColumnName(),
                                        DataTypes.createMapType(DataTypes.StringType, DataTypes.createArrayType(DataTypes.BinaryType)));
        }
        return structType;
    }

    public TableFeatures requestedFeatures()
    {
        return new TableFeatures.Default();
    }

    /**
     * DataLayer can override this method to return the BigInteger/BigDecimal precision/scale values for a given column
     *
     * @param field the cql field
     * @return a BigNumberConfig object that specifies the desired precision/scale for BigDecimal and BigInteger.
     */
    public CassandraBridge.BigNumberConfig bigNumberConfig(final CqlField field)
    {
        return CassandraBridge.BigNumberConfig.DEFAULT;
    }

    /**
     * @return Cassandra version (3.0, 4.0 etc)
     */
    public abstract CassandraBridge.CassandraVersion version();

    /**
     * @return version specific CassandraBridge wrapping shaded packages
     */
    public CassandraBridge bridge()
    {
        return CassandraBridge.get(version());
    }

    public abstract int partitionCount();

    public abstract CqlSchema cqlSchema();

    public abstract boolean isInPartition(final BigInteger token, final ByteBuffer key);

    public List<PartitionKeyFilter> partitionKeyFiltersInRange(final List<PartitionKeyFilter> partitionKeyFilters) throws NoMatchFoundException
    {
        return partitionKeyFilters;
    }

    public abstract CommitLogProvider commitLogs();

    public abstract TableIdLookup tableIdLookup();

    /**
     * DataLayer implementation should provide a SparkRangeFilter to filter out partitions and mutations
     * that do not overlap with the Spark worker's token range.
     *
     * @return SparkRangeFilter for the Spark worker's token range
     */
    public SparkRangeFilter sparkRangeFilter()
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
     * @param sparkRangeFilter    spark range filter
     * @param partitionKeyFilters the list of partition key filters
     * @return set of SSTables
     */
    public abstract SSTablesSupplier sstables(@Nullable final SparkRangeFilter sparkRangeFilter,
                                              @NotNull final List<PartitionKeyFilter> partitionKeyFilters);

    public abstract Partitioner partitioner();

    /**
     * It specifies the minimum number of replicas required for CDC.
     * For example, the minimum number of PartitionUpdates for compaction,
     * and the minimum number of replicas to pull logs from to proceed to compaction.
     *
     * @return the minimum number of replicas. The returned value must be 1 or more.
     */
    public int minimumReplicasForCdc()
    {
        return 1;
    }

    /**
     * @return a string that uniquely identifies this Spark job.
     */
    public abstract String jobId();

    /**
     * Override this method with a Watermarker implementation that persists high and low watermarks per Spark partition between Streaming batches.
     *
     * @return watermarker for persisting high and low watermark and late updates.
     */
    public Watermarker cdcWatermarker()
    {
        return DoNothingWatermarker.INSTANCE;
    }

    public Duration cdcWatermarkWindow()
    {
        return Duration.ofSeconds(30);
    }

    public IStreamScanner openCdcScanner(@Nullable CdcOffsetFilter offset)
    {
        return bridge().getCdcScanner(cqlSchema(), partitioner(), commitLogs(), tableIdLookup(),
                                      stats(), sparkRangeFilter(), offset,
                                      minimumReplicasForCdc(), cdcWatermarker(), jobId(),
                                      executorService(), timeProvider());
    }

    public IStreamScanner openCompactionScanner(final List<PartitionKeyFilter> partitionKeyFilters)
    {
        return openCompactionScanner(partitionKeyFilters, null);
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
    public IStreamScanner openCompactionScanner(final List<PartitionKeyFilter> partitionKeyFilters,
                                                @Nullable PruneColumnFilter columnFilter)
    {
        List<PartitionKeyFilter> filtersInRange;
        try
        {
            filtersInRange = partitionKeyFiltersInRange(partitionKeyFilters);
        }
        catch (NoMatchFoundException e)
        {
            return EmptyScanner.INSTANCE;
        }
        final SparkRangeFilter sparkRangeFilter = sparkRangeFilter();
        return bridge().getCompactionScanner(cqlSchema(), partitioner(), sstables(sparkRangeFilter, filtersInRange),
                                             sparkRangeFilter, filtersInRange, columnFilter, timeProvider(),
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
        Set<String> partitionKeys = cqlSchema().partitionKeys().stream()
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

    /**
     * Override to plug in your own Stats instrumentation for recording internal events.
     *
     * @return Stats implementation to record internal events
     */
    public Stats stats()
    {
        return Stats.DoNothingStats.INSTANCE;
    }

    /**
     * Abstract class representing a single SSTable.
     * Implementations must override hashCode and equals methods
     */
    public abstract static class SSTable implements Serializable
    {

        public static final long serialVersionUID = 42L;

        public SSTable()
        {

        }

        @Nullable
        protected abstract InputStream openInputStream(final FileType fileType);

        @Nullable
        public InputStream openCompressionStream()
        {
            return openInputStream(FileType.COMPRESSION_INFO);
        }

        @Nullable
        public InputStream openStatsStream()
        {
            return openInputStream(FileType.STATISTICS);
        }

        @Nullable
        public InputStream openSummaryStream()
        {
            return openInputStream(FileType.SUMMARY);
        }

        @Nullable
        public InputStream openPrimaryIndexStream()
        {
            return openInputStream(FileType.INDEX);
        }

        @Nullable
        public InputStream openFilterStream()
        {
            return openInputStream(FileType.FILTER);
        }

        @NotNull
        public InputStream openDataStream()
        {
            return Objects.requireNonNull(openInputStream(FileType.DATA), "Data.db SSTable file component must exist");
        }

        public abstract boolean isMissing(final FileType fileType);

        public void verify() throws IncompleteSSTableException
        {
            // need Data.db file
            if (isMissing(FileType.DATA))
            {
                throw new IncompleteSSTableException(FileType.DATA);
            }
            // need Statistics.db file to open SerializationHeader
            if (isMissing(FileType.STATISTICS))
            {
                throw new IncompleteSSTableException(FileType.STATISTICS);
            }
            // need Summary.db or Index.db to read first/last partition key
            if (isMissing(FileType.SUMMARY) && isMissing(FileType.INDEX))
            {
                throw new IncompleteSSTableException(FileType.SUMMARY, FileType.INDEX);
            }
        }

        public abstract String getDataFileName();
    }
}
