package org.apache.cassandra.spark.sparksql;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.CustomFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.utils.FilterUtils;
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
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

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
        return dataLayer.cqlSchema().keyspace() + "." + dataLayer.cqlSchema().table();
    }

    @Override
    public StructType schema()
    {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities()
    {
        return new HashSet<>(Collections.singletonList(TableCapability.BATCH_READ));
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options)
    {
        return new CassandraScanBuilder(dataLayer, schema);
    }
}

class CassandraScanBuilder implements ScanBuilder, Scan, Batch, SupportsPushDownFilters,
                                      SupportsPushDownRequiredColumns, SupportsReportPartitioning
{
    final DataLayer dataLayer;
    final StructType schema;
    StructType requiredSchema = null;
    Filter[] pushedFilters = new Filter[0];

    CassandraScanBuilder(DataLayer dataLayer, StructType schema)
    {
        this.dataLayer = dataLayer;
        this.schema = schema;
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
                        .mapToObj(i -> new CassandraInputPartition())
                        .toArray(InputPartition[]::new);
    }

    @Override
    public PartitionReaderFactory createReaderFactory()
    {
        return new CassandraPartitionReaderFactory(dataLayer, requiredSchema, buildFilters());
    }

    @Override
    public Partitioning outputPartitioning()
    {
        return new CassandraPartitioning(dataLayer);
    }

    private List<CustomFilter> buildFilters()
    {
        final List<String> partitionKeyColumnNames = this.dataLayer.cqlSchema().partitionKeys().stream().map(CqlField::name).collect(Collectors.toList());
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
        final String compositeKey = String.join(":", keys);
        final Pair<ByteBuffer, BigInteger> filterKey = this.dataLayer.bridge()
                                                                     .getPartitionKey(this.dataLayer.cqlSchema(), this.dataLayer.partitioner(), compositeKey);
        return PartitionKeyFilter.create(filterKey.getLeft(), filterKey.getRight());
    }
}

class CassandraInputPartition implements InputPartition
{

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
            List<String> partitionKeys = this.dataLayer.cqlSchema().partitionKeys().stream().map(CqlField::name).collect(Collectors.toList());
            return Arrays.asList(clusteredCols).containsAll(partitionKeys);
        }
        return false;
    }
}

class CassandraPartitionReaderFactory implements PartitionReaderFactory
{
    final DataLayer dataLayer;
    final StructType requiredSchema;
    final List<CustomFilter> filters;

    CassandraPartitionReaderFactory(DataLayer dataLayer,
                                    StructType requiredSchema,
                                    List<CustomFilter> filters)
    {
        this.dataLayer = dataLayer;
        this.requiredSchema = requiredSchema;
        this.filters = filters;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition)
    {
        return new SparkRowIterator(dataLayer, requiredSchema, filters);
    }
}