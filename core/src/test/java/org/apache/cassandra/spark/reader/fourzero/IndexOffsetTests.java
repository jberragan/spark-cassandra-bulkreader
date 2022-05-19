package org.apache.cassandra.spark.reader.fourzero;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import com.google.common.collect.Range;
import org.apache.commons.lang.mutable.MutableInt;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Schema;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.Stats;

import static org.apache.cassandra.spark.TestUtils.countSSTables;
import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.booleans;

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

public class IndexOffsetTests
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexOffsetTests.class);

    @Test
    public void testReadIndexOffsets()
    {
        qt().forAll(booleans().all())
            .checkAssert(enableCompression ->
                         runTest((partitioner, dir, bridge) -> test(bridge, dir, partitioner, enableCompression)));
    }

    private static void test(final CassandraBridge bridge,
                             final Path dir,
                             final Partitioner partitioner,
                             final boolean enableCompression) throws IOException
    {
        final int numKeys = 500000;
        final int sparkPartitions = 128;
        final TestSchema schema = TestSchema.basicBuilder(bridge)
                                            .withCompression(enableCompression).build();

        TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
            for (int i = 0; i < numKeys; i++)
            {
                writer.write(i, 0, i);
            }
        });
        assertEquals(1, countSSTables(dir));

        final TableMetadata metadata = Schema.instance.getTableMetadata(schema.keyspace, schema.table);
        if (metadata == null)
        {
            throw new NullPointerException("Could not find table");
        }

        final LocalDataLayer dataLayer = new LocalDataLayer(CassandraBridge.CassandraVersion.FOURZERO, partitioner,
                                                            schema.keyspace, schema.createStmt, Collections.emptyList(),
                                                            Collections.emptySet(), true, null, dir.toString());
        final DataLayer.SSTable ssTable = dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find sstable"));

        final Integer[] counts = IntStream.range(0, numKeys).map(i -> 0).boxed().toArray(Integer[]::new);
        final CassandraRing ring = TestUtils.createRing(partitioner, 32);

        // use TokenPartitioner to simulate Spark worker tokens partitions
        final TokenPartitioner tokenPartitioner = new TokenPartitioner(ring, 1, sparkPartitions);
        final List<Range<BigInteger>> ranges = tokenPartitioner.subRanges();
        LOGGER.info("Testing index offsets numKeys={} sparkPartitions={} partitioner={} enableCompression={}", numKeys, ranges.size(), partitioner.name(), enableCompression);

        final MutableInt skipped = new MutableInt(0);
        for (Range<BigInteger> range : ranges)
        {
            final FourZeroSSTableReader reader = FourZeroSSTableReader.builder(metadata, ssTable)
                                                                      .withSparkRangeFilter(SparkRangeFilter.create(range))
                                                                      .withStats(new Stats()
                                                                      {
                                                                          public void skippedPartition(ByteBuffer key, BigInteger token)
                                                                          {
                                                                              skipped.add(1);
                                                                          }
                                                                      })
                                                                      .build();
            if (reader.ignore())
            {
                // we can skip this range entirely, it doesn't overlap with sstable
                continue;
            }

            // iterate through SSTable partitions
            // each scanner should only read tokens within it's own token range
            try (final ISSTableScanner scanner = reader.scanner())
            {
                while (scanner.hasNext())
                {
                    final UnfilteredRowIterator rowIterator = scanner.next();
                    final int key = rowIterator.partitionKey().getKey().getInt();
                    // count how many times we read a key across all 'spark' token partitions
                    counts[key] = counts[key] + 1;
                    while (rowIterator.hasNext())
                    {
                        rowIterator.next();
                    }
                }
            }
        }

        // verify we read each key exactly once across all Spark partitions
        assertEquals(counts.length, numKeys);
        int idx = 0;
        for (Integer count : counts)
        {
            if (count == 0)
            {
                LOGGER.error("Missing key key={} token={} partitioner={}",
                             idx,
                             FourZeroUtils.tokenToBigInteger(FourZero.getPartitioner(partitioner).decorateKey((ByteBuffer) ByteBuffer.allocate(4).putInt(idx).flip()).getToken()),
                             partitioner.name());
            }
            else if (count > 1)
            {
                LOGGER.error("Key read by more than 1 Spark partition key={} token={} partitioner={}",
                             idx,
                             FourZeroUtils.tokenToBigInteger(FourZero.getPartitioner(partitioner).decorateKey((ByteBuffer) ByteBuffer.allocate(4).putInt(idx).flip()).getToken()),
                             partitioner.name());
            }
            assertEquals(count == 0 ? "Key not found: " + idx : "Key " + idx + " read " + count + " times",
                         1, count.intValue());
            idx++;
        }
        LOGGER.info("Success skippedKeys={} partitioner={}",
                    skipped.intValue(),
                    partitioner.name());
    }
}
