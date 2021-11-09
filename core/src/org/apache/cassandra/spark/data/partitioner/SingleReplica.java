package org.apache.cassandra.spark.data.partitioner;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.PartitionedDataLayer;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.apache.cassandra.spark.reader.common.SSTableStreamException;
import org.apache.cassandra.spark.stats.Stats;
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

/**
 * Return a set of SSTables for a single Cassandra Instance
 */
public class SingleReplica extends SSTablesSupplier
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleReplica.class);

    private final PartitionedDataLayer dataLayer;
    private final CassandraInstance instance;
    private final Range<BigInteger> range;
    private final int partitionId;
    private final ExecutorService executor;
    private final Stats stats;

    public SingleReplica(@NotNull final CassandraInstance instance,
                         @NotNull final PartitionedDataLayer dataLayer,
                         @NotNull final Range<BigInteger> range,
                         final int partitionId,
                         @NotNull final ExecutorService executor)
    {
        this(instance, dataLayer, range, partitionId, executor, Stats.DoNothingStats.INSTANCE);
    }

    public SingleReplica(@NotNull final CassandraInstance instance,
                         @NotNull final PartitionedDataLayer dataLayer,
                         @NotNull final Range<BigInteger> range,
                         final int partitionId,
                         @NotNull final ExecutorService executor,
                         @NotNull final Stats stats)
    {
        this.dataLayer = dataLayer;
        this.instance = instance;
        this.range = range;
        this.partitionId = partitionId;
        this.executor = executor;
        this.stats = stats;
    }

    public CassandraInstance instance()
    {
        return this.instance;
    }

    public Range<BigInteger> range() { return this.range; }

    /**
     * Open all SparkSSTableReaders for all SSTables for this replica
     *
     * @param readerOpener provides function to open SparkSSTableReader using SSTable
     * @return set of SparkSSTableReader to pass over to the CompactionIterator
     */
    @Override
    public <T extends SparkSSTableReader> Set<T> openAll(ReaderOpener<T> readerOpener)
    {
        try
        {
            return openReplicaAsync(readerOpener).get();
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (final ExecutionException e)
        {
            throw new RuntimeException(e.getCause() == null ? e.getCause() : e);
        }
    }


    <T extends SparkSSTableReader> CompletableFuture<Set<T>> openReplicaAsync(@NotNull final ReaderOpener<T> readerOpener)
    {
        // list SSTables and open sstable readers
        try
        {
            final long timeNanos = System.nanoTime();
            return dataLayer.listInstance(partitionId, range, instance)
                            .thenApply(stream -> {
                                stats.timeToListSnapshot(this, System.nanoTime() - timeNanos);
                                return stream;
                            })
                            .thenCompose(stream -> openAll(stream, readerOpener));
        }
        catch (final Throwable t)
        {
            LOGGER.warn("Unexpected error attempting to open SSTable readers for replica node={} token={} dc={}", instance().nodeName(), instance().token(), instance().dataCenter(), t);
            final CompletableFuture<Set<T>> exceptionally = new CompletableFuture<>();
            exceptionally.completeExceptionally(t);
            return exceptionally;
        }
    }

    private <T extends SparkSSTableReader> CompletableFuture<Set<T>> openAll(@NotNull final Stream<DataLayer.SSTable> stream,
                                                                             @NotNull final ReaderOpener<T> readerOpener)
    {
        final Set<T> result = ConcurrentHashMap.newKeySet();
        final CompletableFuture[] futures = stream
                                            // verify all the required SSTable file components are available
                                            .peek((DataLayer.SSTable::verify))
                                            // open SSTable readers in parallel using executor
                                            .map(ssTable -> CompletableFuture.runAsync(() -> openReader(readerOpener, ssTable, result), executor))
                                            .toArray(CompletableFuture[]::new);

        // all futures must complete non-exceptionally for the resulting future to complete
        return CompletableFuture.allOf(futures).thenApply(aVoid -> ImmutableSet.copyOf(result));
    }

    private <T extends SparkSSTableReader> void openReader(@NotNull final ReaderOpener<T> readerOpener, @NotNull final DataLayer.SSTable ssTable, @NotNull final Set<T> result)
    {
        try
        {
            final T reader = readerOpener.openReader(ssTable);
            if (!reader.ignore())
            {
                result.add(reader);
            }
        }
        catch (final IOException e)
        {
            throw new SSTableStreamException(e);
        }
    }
}
