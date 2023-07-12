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

package org.apache.cassandra.spark.reader.fourzero;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.collect.Range;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.SparkTestUtils;
import org.apache.cassandra.spark.TestDataLayer;
import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.reader.IndexConsumer;
import org.apache.cassandra.spark.reader.IndexEntry;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.streaming.CassandraFile;

import static org.apache.cassandra.spark.SparkTestUtils.getFileType;
import static org.apache.cassandra.spark.SparkTestUtils.runTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IndexReaderTests
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexReaderTests.class);

    @Test
    public void testIndexReaderWithCompression()
    {
        testIndexReader(true);
    }

    @Test
    public void testIndexReaderWithoutCompression()
    {
        testIndexReader(false);
    }

    public void testIndexReader(boolean withCompression)
    {
        runTest((partitioner, dir, bridge) -> {
            final int numPartitions = 50000;
            final BigInteger eighth = partitioner.maxToken().divide(BigInteger.valueOf(8));
            final RangeFilter rangeFilter = RangeFilter.create(Range.closed(partitioner.minToken().add(eighth), partitioner.maxToken().subtract(eighth)));
            final TestSchema schema = TestSchema.builder()
                                                .withPartitionKey("a", bridge.aInt())
                                                .withColumn("b", bridge.blob())
                                                .withCompression(withCompression)
                                                .build();
            final TableMetadata metaData = schema.schemaBuilder(partitioner).tableMetaData();

            // write an SSTable
            final Map<Integer, Integer> expected = new HashMap<>();
            SparkTestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < numPartitions; i++)
                {
                    final BigInteger token = BaseFourZeroUtils.tokenToBigInteger(metaData.partitioner.decorateKey(Int32Serializer.instance.serialize(i)).getToken());
                    final byte[] lowEntropyData = TestUtils.randomLowEntropyData();
                    if (rangeFilter.overlaps(token))
                    {
                        expected.put(i, lowEntropyData.length);
                    }
                    writer.write(i, ByteBuffer.wrap(lowEntropyData));
                }
            });
            assertFalse(expected.isEmpty());
            assertTrue(expected.size() < numPartitions);

            final TestDataLayer dataLayer = new TestDataLayer(bridge, getFileType(dir, CassandraFile.FileType.DATA).collect(Collectors.toList()));
            final List<SSTable> ssTables = dataLayer.listSSTables().collect(Collectors.toList());
            assertFalse(ssTables.isEmpty());
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final CountDownLatch latch = new CountDownLatch(ssTables.size());
            final AtomicInteger rowCount = new AtomicInteger(0);

            final IndexConsumer consumer = new IndexConsumer()
            {
                public void onFailure(Throwable t)
                {
                    LOGGER.warn("Error reading index file", t);
                    if (error.get() == null)
                    {
                        error.compareAndSet(null, t);
                    }
                }

                public void onFinished(long runtimeNanos)
                {
                    latch.countDown();
                }

                public void accept(IndexEntry indexEntry)
                {
                    // we should only read in-range partition keys
                    rowCount.getAndIncrement();
                    final int pk = indexEntry.partitionKey.getInt();
                    final int blobSize = expected.get(pk);
                    assertTrue(expected.containsKey(pk));
                    assertTrue(indexEntry.compressed > 0);
                    assertTrue(withCompression ? indexEntry.compressed < indexEntry.uncompressed * 0.1 : indexEntry.compressed == indexEntry.uncompressed);
                    assertTrue((int) indexEntry.uncompressed > blobSize);
                    assertTrue(((int) indexEntry.uncompressed - blobSize) < 40); // uncompressed size should be proportional to the blob size, with some serialization overhead
                }
            };

            ssTables
            .forEach(ssTable -> CompletableFuture.runAsync(() -> new FourZeroIndexReader(ssTable, metaData, rangeFilter, Stats.DoNothingStats.INSTANCE, consumer), LocalDataLayer.EXECUTOR));

            try
            {
                latch.await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            assertNull(error.get());
            assertEquals(expected.size(), rowCount.get());
        });
    }
}
