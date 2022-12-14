package org.apache.cassandra.spark.sparksql;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.sparksql.filters.CdcOffset;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.utils.FilterUtils;
import org.apache.cassandra.spark.utils.TimeUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.partitioning.ClusteredDistribution;
import org.apache.spark.sql.connector.read.partitioning.Distribution;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
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

public abstract class CassandraTableProvider implements TableProvider, DataSourceRegister
{
    public abstract DataLayer getDataLayer(final CaseInsensitiveStringMap options);

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options)
    {
        return getDataLayer(options).structType();
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties)
    {
        return new CassandraTable(getDataLayer(new CaseInsensitiveStringMap(properties)), schema);
    }
}

class CassandraTable implements Table, SupportsRead
{
    private final DataLayer dataLayer;
    private final StructType schema;

    CassandraTable(DataLayer dataLayer, StructType schema)
    {
        this.dataLayer = dataLayer;
        this.schema = schema;
    }

    @Override
    public String name()
    {
        return dataLayer.cqlTable().keyspace() + "." + dataLayer.cqlTable().table();
    }

    @Override
    public StructType schema()
    {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities()
    {
        return new HashSet<>(Arrays.asList(TableCapability.BATCH_READ, TableCapability.MICRO_BATCH_READ));
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options)
    {
        return new CassandraScanBuilder(dataLayer, schema, options);
    }
}

class CassandraScanBuilder implements ScanBuilder, Scan, Batch, SupportsPushDownFilters,
                                      SupportsPushDownRequiredColumns, SupportsReportPartitioning
{
    final DataLayer dataLayer;
    final StructType schema;
    final CaseInsensitiveStringMap options;
    StructType requiredSchema = null;
    Filter[] pushedFilters = new Filter[0];

    CassandraScanBuilder(DataLayer dataLayer, StructType schema, CaseInsensitiveStringMap options)
    {
        this.dataLayer = dataLayer;
        this.schema = schema;
        this.options = options;
    }

    @Override
    public Scan build()
    {
        return this;
    }

    @Override
    public void pruneColumns(StructType requiredSchema)
    {
        this.requiredSchema = requiredSchema;
    }

    @Override
    public Filter[] pushFilters(final Filter[] filters)
    {
        final Filter[] unsupportedFilters = this.dataLayer.unsupportedPushDownFilters(filters);

        final List<Filter> supportedFilters = new ArrayList<>(Arrays.asList(filters));
        supportedFilters.removeAll(Arrays.asList(unsupportedFilters));
        this.pushedFilters = supportedFilters.toArray(new Filter[0]);

        return unsupportedFilters;
    }

    @Override
    public Filter[] pushedFilters()
    {
        return this.pushedFilters;
    }

    @Override
    public StructType readSchema()
    {
        return requiredSchema;
    }

    @Override
    public Batch toBatch()
    {
        return this;
    }

    @Override
    public InputPartition[] planInputPartitions()
    {
        return IntStream.range(0, this.dataLayer.partitionCount())
                        .mapToObj(CassandraInputPartition::new)
                        .toArray(InputPartition[]::new);
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation)
    {
        return new CassandraMicroBatchStream(dataLayer, options);
    }

    @Override
    public PartitionReaderFactory createReaderFactory()
    {
        return new CassandraPartitionReaderFactory(dataLayer, requiredSchema, buildPartitionKeyFilters());
    }

    @Override
    public Partitioning outputPartitioning()
    {
        return new CassandraPartitioning(dataLayer);
    }

    private List<PartitionKeyFilter> buildPartitionKeyFilters()
    {
        final List<String> partitionKeyColumnNames = this.dataLayer.cqlTable().partitionKeys().stream().map(CqlField::name).collect(Collectors.toList());
        final Map<String, List<String>> partitionKeyValues = FilterUtils.extractPartitionKeyValues(pushedFilters, new HashSet<>(partitionKeyColumnNames));
        if (partitionKeyValues.size() > 0)
        {
            final List<List<String>> orderedValues = partitionKeyColumnNames.stream().map(partitionKeyValues::get).collect(Collectors.toList());
            return FilterUtils.cartesianProduct(orderedValues)
                              .stream().map(this::buildFilter)
                              .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    private PartitionKeyFilter buildFilter(List<String> keys)
    {
        final Pair<ByteBuffer, BigInteger> filterKey = this.dataLayer.bridge()
                                                                     .getPartitionKey(this.dataLayer.cqlTable(), this.dataLayer.partitioner(), keys);
        return PartitionKeyFilter.create(filterKey.getLeft(), filterKey.getRight());
    }
}

class CassandraInputPartition implements InputPartition
{
    @Nullable
    private final Map<CassandraInstance, CommitLog.Marker> startMarkers;
    @Nullable
    private final Map<CassandraInstance, List<CdcOffset.SerializableCommitLog>> logs;
    @Nullable
    private final Set<CqlTable> cdcTables;
    @Nullable
    private final Long startTimestampMicros;
    private final int partitionId;

    CassandraInputPartition(int partitionId)
    {
        this.startMarkers = null;
        this.logs = null;
        this.startTimestampMicros = null;
        this.cdcTables = null;
        this.partitionId = partitionId;
    }

    CassandraInputPartition(int partitionId,
                            @NotNull final CdcOffset start,
                            @NotNull final CdcOffset end,
                            @NotNull final Set<CqlTable> cdcTables)
    {
        this.startMarkers = start.getInstanceLogs().entrySet().stream()
                                 .filter(e -> Objects.nonNull(e.getValue().getMarker()))
                                 .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getMarker()));
        this.logs = end.allLogs();
        this.startTimestampMicros = start.getTimestampMicros();
        this.cdcTables = cdcTables;
        this.partitionId = partitionId;
    }

    @Nullable
    public Map<CassandraInstance, CommitLog.Marker> getStartMarkers()
    {
        return startMarkers;
    }

    @Nullable
    public Map<CassandraInstance, List<CdcOffset.SerializableCommitLog>> getLogs()
    {
        return logs;
    }

    @Nullable
    public Set<CqlTable> getCdcTables()
    {
        return cdcTables;
    }

    @Nullable
    public Long getStartTimestampMicros()
    {
        return startTimestampMicros;
    }

    public int getPartitionId()
    {
        return partitionId;
    }
}

class CassandraPartitioning implements Partitioning
{
    final DataLayer dataLayer;

    CassandraPartitioning(final DataLayer dataLayer)
    {
        this.dataLayer = dataLayer;
    }

    @Override
    public int numPartitions()
    {
        return this.dataLayer.partitionCount();
    }

    @Override
    public boolean satisfy(Distribution distribution)
    {
        if (distribution instanceof ClusteredDistribution)
        {
            String[] clusteredCols = ((ClusteredDistribution) distribution).clusteredColumns;
            List<String> partitionKeys = this.dataLayer.cqlTable().partitionKeys().stream().map(CqlField::name).collect(Collectors.toList());
            return Arrays.asList(clusteredCols).containsAll(partitionKeys);
        }
        return false;
    }
}

class CassandraPartitionReaderFactory implements PartitionReaderFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraPartitionReaderFactory.class);
    final DataLayer dataLayer;
    final StructType requiredSchema;
    final List<PartitionKeyFilter> partitionKeyFilters;

    CassandraPartitionReaderFactory(DataLayer dataLayer,
                                    StructType requiredSchema,
                                    List<PartitionKeyFilter> partitionKeyFilters)
    {
        this.dataLayer = dataLayer;
        this.requiredSchema = requiredSchema;
        this.partitionKeyFilters = partitionKeyFilters;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition)
    {
        final int partitionId;
        if (partition instanceof CassandraInputPartition)
        {
            partitionId = ((CassandraInputPartition) partition).getPartitionId();
        }
        else
        {
            partitionId = TaskContext.getPartitionId();
            LOGGER.warn("InputPartition is not of CassandraInputPartition type. Using TaskContext to determine the partitionId type={}, partitionId={}",
                        partition.getClass().getName(), partitionId);
        }
        return new SparkRowIterator(partitionId, dataLayer, requiredSchema, partitionKeyFilters);
    }
}

// spark streaming

class CassandraMicroBatchStream implements MicroBatchStream, Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraMicroBatchStream.class);
    private static final int DEFAULT_MIN_MUTATION_AGE_SECS = 0;

    private final DataLayer dataLayer;
    private final long minAgeMicros;
    private final CdcOffset initial;

    CassandraMicroBatchStream(DataLayer dataLayer,
                              CaseInsensitiveStringMap options)
    {
        this.dataLayer = dataLayer;
        this.minAgeMicros = TimeUtils.secsToMicros(options.getLong("minMutationAgeSeconds", DEFAULT_MIN_MUTATION_AGE_SECS));
        // initial batch: [now - (minAgeMicros + cdc_window), now - minAgeMicros]
        this.initial = dataLayer.initialOffset(minAgeMicros + TimeUtils.toMicros(dataLayer.cdcWatermarkWindow()));
    }

    // Runs on driver
    @Override
    public Offset initialOffset()
    {
        return initial;
    }

    // Runs on driver
    @Override
    public Offset latestOffset()
    {
        return dataLayer.latestOffset(minAgeMicros);
    }

    // Runs on driver
    @Override
    public Offset deserializeOffset(String json)
    {
        return CdcOffset.fromJson(json);
    }

    // Runs on driver
    @Override
    public void commit(Offset end)
    {
        final CdcOffset cdcEnd = (CdcOffset) end;
        LOGGER.info("Commit CassandraMicroBatchStream end end='{}'", cdcEnd.getTimestampMicros());
    }

    // Runs on driver
    @Override
    public void stop()
    {
        LOGGER.info("Stopping CassandraMicroBatchStream");
    }

    // Runs on driver
    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end)
    {
        final int numPartitions = this.dataLayer.partitionCount();
        final CdcOffset cdcStart = (CdcOffset) start;
        final CdcOffset cdcEnd = (CdcOffset) end;
        final Set<CqlTable> cdcTables = this.dataLayer.cdcTables();
        LOGGER.info("Planning CDC input partitions numPartitions={} start='{}' end='{}'",
                    numPartitions, cdcStart.getTimestampMicros(), cdcEnd.getTimestampMicros());
        return IntStream.range(0, numPartitions)
                        .mapToObj(partitionId -> new CassandraInputPartition(partitionId, cdcStart, cdcEnd, cdcTables))
                        .toArray(InputPartition[]::new);
    }

    // Runs on driver. The returned PartitionReaderFactory runs on the executors.
    @Override
    public PartitionReaderFactory createReaderFactory()
    {
        return this::createCdcRowIteratorForPartition;
    }

    // Runs on the executors
    private PartitionReader<InternalRow> createCdcRowIteratorForPartition(InputPartition partition)
    {
        Preconditions.checkNotNull(partition, "Null InputPartition");
        if (partition instanceof CassandraInputPartition)
        {
            CassandraInputPartition cassandraInputPartition = (CassandraInputPartition) partition;
            final Map<CassandraInstance, CommitLog.Marker> startMarkers = cassandraInputPartition.getStartMarkers();
            final Map<CassandraInstance, List<CdcOffset.SerializableCommitLog>> logs = cassandraInputPartition.getLogs();
            final Long startTimestampMicros = cassandraInputPartition.getStartTimestampMicros();
            final Set<CqlTable> cdcTables = cassandraInputPartition.getCdcTables();
            Preconditions.checkNotNull(startMarkers, "Cdc start markers were not set");
            Preconditions.checkNotNull(logs, "Cdc commit logs were not set");
            Preconditions.checkNotNull(startTimestampMicros, "Cdc start timestamp was not set");
            Preconditions.checkNotNull(cdcTables, "Cdc tables were not set");
            LOGGER.info("Opening CdcRowIterator startMarkers='{}' logs='{}' startTimestampMicros={} partitionId={}",
                        startMarkers, logs, startTimestampMicros, cassandraInputPartition.getPartitionId());
            return new CdcRowIterator(cassandraInputPartition.getPartitionId(), this.dataLayer, cdcTables,
                                      CdcOffsetFilter.of(startMarkers, logs, startTimestampMicros, dataLayer.cdcWatermarkWindow()));
        }
        throw new UnsupportedOperationException("Unexpected InputPartition type: " + (partition.getClass().getName()));
    }
}
