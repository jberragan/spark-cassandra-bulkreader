package org.apache.cassandra.spark.sparksql;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.utils.FilterUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.v2.reader.partitioning.Distribution;
import org.apache.spark.sql.sources.v2.reader.partitioning.Partitioning;
import org.apache.spark.sql.types.StructType;
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

public abstract class CassandraDataSource implements DataSourceV2, ReadSupport, DataSourceRegister
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraDataSource.class);

    public abstract DataLayer getDataLayer(final DataSourceOptions options);

    @Override
    public DataSourceReader createReader(final DataSourceOptions options)
    {
        return new SSTableSourceReader(getDataLayer(options));
    }

    public static class SSTableSourceReader implements DataSourceReader, Serializable, SupportsPushDownFilters, SupportsPushDownRequiredColumns, Partitioning
    {

        private Filter[] pushedFilters = new Filter[0];
        private final DataLayer dataLayer;
        private StructType requiredSchema = null;

        SSTableSourceReader(@NotNull final DataLayer dataLayer)
        {
            this.dataLayer = dataLayer;
        }

        @Override
        public StructType readSchema()
        {
            return this.dataLayer.structType();
        }

        @Override
        public List<InputPartition<InternalRow>> planInputPartitions()
        {
            final List<PartitionKeyFilter> partitionKeyFilters = new ArrayList<>();

            final List<String> partitionKeyColumnNames = this.dataLayer.cqlTable().partitionKeys().stream().map(CqlField::name).collect(Collectors.toList());
            final Map<String, List<String>> partitionKeyValues = FilterUtils.extractPartitionKeyValues(pushedFilters, new HashSet<>(partitionKeyColumnNames));
            if (partitionKeyValues.size() > 0)
            {
                final List<List<String>> orderedValues = partitionKeyColumnNames.stream().map(partitionKeyValues::get).collect(Collectors.toList());
                FilterUtils.cartesianProduct(orderedValues).forEach(keys ->
                {
                    final Pair<ByteBuffer, BigInteger> filterKey = this.dataLayer.bridge().getPartitionKey(this.dataLayer.cqlTable(), this.dataLayer.partitioner(), keys);
                    partitionKeyFilters.add(PartitionKeyFilter.create(filterKey.getLeft(), filterKey.getRight()));
                });
            }
            LOGGER.info("Creating data reader factories numPartitions={}", this.dataLayer.partitionCount());
            return IntStream.range(0, this.dataLayer.partitionCount())
                            .mapToObj(partitionId -> (InputPartition<InternalRow>) () -> new SparkRowIterator(partitionId, this.dataLayer, this.requiredSchema, partitionKeyFilters))
                            .collect(Collectors.toList());
        }

        /**
         * Pushes down filters, and returns filters that need to be evaluated after scanning.
         *
         * @param filters the filters in the query
         * @return filters that need to be evaluated after scanning
         */
        @Override
        public Filter[] pushFilters(final Filter[] filters)
        {
            final Filter[] unsupportedFilters = this.dataLayer.unsupportedPushDownFilters(filters);

            final List<Filter> supportedFilters = Lists.newArrayList(filters);
            supportedFilters.removeAll(Arrays.asList(unsupportedFilters));
            this.pushedFilters = supportedFilters.toArray(Filter[]::new);

            return unsupportedFilters;
        }

        @Override
        public Filter[] pushedFilters()
        {
            return this.pushedFilters;
        }

        @Override
        public void pruneColumns(final StructType requiredSchema)
        {
            this.requiredSchema = requiredSchema;
        }

        @Override
        public int numPartitions()
        {
            return this.dataLayer.partitionCount();
        }

        @Override
        public boolean satisfy(final Distribution distribution)
        {
            return true;
        }
    }
}
