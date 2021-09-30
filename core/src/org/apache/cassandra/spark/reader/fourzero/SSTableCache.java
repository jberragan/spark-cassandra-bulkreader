package org.apache.cassandra.spark.reader.fourzero;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.tuple.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DecoratedKey;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.BloomFilter;
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
 * Basic cache to reduce wasteful requests on the DataLayer for cacheable SSTable meta-data
 * Useful when running many Spark tasks on the same Spark worker
 */
@SuppressWarnings("UnstableApiUsage")
public class SSTableCache
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableCache.class);

    public static final SSTableCache INSTANCE = new SSTableCache();
    private final Cache<DataLayer.SSTable, SummaryDbUtils.Summary> summary = buildCache(propOrDefault("sbr.cache.summary.maxEntries", 4096), propOrDefault("sbr.cache.summary.expireAfterMins", 15));
    private final Cache<DataLayer.SSTable, Pair<DecoratedKey, DecoratedKey>> index = buildCache(propOrDefault("sbr.cache.index.maxEntries", 128), propOrDefault("sbr.cache.index.expireAfterMins", 60));
    private final Cache<DataLayer.SSTable, Map<MetadataType, MetadataComponent>> stats = buildCache(propOrDefault("sbr.cache.stats.maxEntries", 16384), propOrDefault("sbr.cache.stats.expireAfterMins", 60));
    private final Cache<DataLayer.SSTable, BloomFilter> filter = buildCache(propOrDefault("sbr.cache.filter.maxEntries", 16384), propOrDefault("sbr.cache.filter.expireAfterMins", 60));

    private static int propOrDefault(String name, int defaultValue)
    {
        final String str = System.getProperty(name);
        if (str != null)
        {
            try
            {
                return Integer.parseInt(str);
            }
            catch (NumberFormatException e)
            {
                LOGGER.error("NumberFormatException for prop {} ", name, e);
            }
        }
        return defaultValue;
    }

    private <T> Cache<DataLayer.SSTable, T> buildCache(final int size, int expireAfterMins)
    {
        return CacheBuilder.newBuilder()
                           .expireAfterAccess(expireAfterMins, TimeUnit.MINUTES)
                           .maximumSize(size)
                           .build();
    }

    public SummaryDbUtils.Summary keysFromSummary(@NotNull final TableMetadata metadata, @NotNull final DataLayer.SSTable ssTable) throws IOException
    {
        return get(summary, ssTable, () -> SummaryDbUtils.readSummary(metadata, ssTable));
    }

    public Pair<DecoratedKey, DecoratedKey> keysFromIndex(@NotNull final TableMetadata metadata, @NotNull final DataLayer.SSTable ssTable) throws IOException
    {
        return get(index, ssTable, () -> FourZeroUtils.keysFromIndex(metadata, ssTable));
    }

    public Map<MetadataType, MetadataComponent> componentMapFromStats(@NotNull final DataLayer.SSTable ssTable, final Descriptor descriptor) throws IOException
    {
        return get(stats, ssTable, () -> FourZeroUtils.deserializeStatsMetadata(ssTable, descriptor));
    }

    public BloomFilter bloomFilter(@NotNull final DataLayer.SSTable ssTable, final Descriptor descriptor) throws IOException
    {
        return get(filter, ssTable, () -> FourZeroUtils.readFilter(ssTable, descriptor.version.hasOldBfFormat()));
    }

    boolean containsSummary(@NotNull final DataLayer.SSTable ssTable)
    {
        return contains(summary, ssTable);
    }

    boolean containsIndex(@NotNull final DataLayer.SSTable ssTable)
    {
        return contains(index, ssTable);
    }

    boolean containsStats(@NotNull final DataLayer.SSTable ssTable)
    {
        return contains(stats, ssTable);
    }

    boolean containsFilter(@NotNull final DataLayer.SSTable ssTable)
    {
        return contains(filter, ssTable);
    }

    private static <T> boolean contains(@NotNull final Cache<DataLayer.SSTable, T> cache, @NotNull final DataLayer.SSTable ssTable)
    {
        return cache.getIfPresent(ssTable) != null;
    }

    private static <T> T get(@NotNull final Cache<DataLayer.SSTable, T> cache, @NotNull final DataLayer.SSTable ssTable, @NotNull final Callable<T> callable) throws IOException
    {
        try
        {
            return cache.get(ssTable, callable);
        }
        catch (ExecutionException e)
        {
            throw toIOException(e);
        }
    }

    private static IOException toIOException(final Throwable e)
    {
        if (e.getCause() instanceof IOException)
        {
            return (IOException) e.getCause();
        }
        return new IOException(e.getCause() != null ? e.getCause() : e);
    }
}
