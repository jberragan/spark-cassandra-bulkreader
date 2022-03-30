package org.apache.cassandra.spark.reader.fourzero;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;

import org.junit.Test;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.IPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Schema;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;

import static org.apache.cassandra.spark.TestUtils.countSSTables;
import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

public class SummaryDbTests
{

    @Test
    public void testSearchSummary()
    {
        runTest((partitioner, dir, bridge) -> {
            final TestSchema schema = TestSchema.basicBuilder(bridge).withCompression(false).build();
            final IPartitioner iPartitioner = FourZero.getPartitioner(partitioner);
            final int numRows = 1000;

            // write an sstable and record token
            final List<BigInteger> tokens = new ArrayList<>(numRows);
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < numRows; i++)
                {
                    final ByteBuffer key = (ByteBuffer) ByteBuffer.allocate(4).putInt(i).flip();
                    final BigInteger token = FourZeroUtils.tokenToBigInteger(iPartitioner.decorateKey(key).getToken());
                    tokens.add(token);
                    writer.write(i, 0, i);
                }
            });
            assertEquals(1, countSSTables(dir));
            Collections.sort(tokens);

            final TableMetadata metadata = Schema.instance.getTableMetadata(schema.keyspace, schema.table);
            if (metadata == null)
            {
                throw new NullPointerException("Could not find table");
            }

            final Path summaryDb = TestUtils.getFirstFileType(dir, DataLayer.FileType.SUMMARY);
            assertNotNull(summaryDb);
            final LocalDataLayer dataLayer = new LocalDataLayer(CassandraBridge.CassandraVersion.FOURZERO, partitioner,
                                                                schema.keyspace, schema.createStmt, false, false, false,
                                                                Collections.emptySet(), true, null, dir.toString());
            final DataLayer.SSTable ssTable = dataLayer.listSSTables().findFirst().orElseThrow(() -> new RuntimeException("Could not find sstable"));

            // binary search Summary.db file in token order and verify offsets are ordered
            final SummaryDbUtils.Summary summary = SummaryDbUtils.readSummary(metadata, ssTable);
            long prev = -1;
            for (final BigInteger token : tokens)
            {
                final long offset = SummaryDbUtils.findIndexOffsetInSummary(summary.summary(), iPartitioner, token);
                if (prev == -1)
                {
                    assertEquals(offset, 0);
                }
                else
                {
                    assertTrue(prev <= offset);
                }
                prev = offset;
            }
        });
    }

    public static class ArrayTokenList implements SummaryDbUtils.TokenList
    {
        private final BigInteger[] tokens;

        public ArrayTokenList(Long... tokens)
        {
            this.tokens = Arrays.stream(tokens).map(BigInteger::valueOf).toArray(BigInteger[]::new);
        }

        public ArrayTokenList(BigInteger... tokens)
        {
            this.tokens = tokens;
        }

        public int size()
        {
            return tokens.length;
        }

        public BigInteger tokenAt(int idx)
        {
            return tokens[idx];
        }
    }

    @Test
    public void testSummaryBinarySearch()
    {
        final SummaryDbUtils.TokenList list = new ArrayTokenList(LongStream.range(5, 10000).boxed().toArray(Long[]::new));
        assertEquals(148, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(154L)));
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(-500L)));
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(4L)));
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(3L)));
        for (int i = 5; i < 10000; i++)
        {
            final int idx = SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(i));
            assertEquals(Math.max(0, i - 6), idx);
        }
        assertEquals(9994, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(10000L)));
        assertEquals(9994, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(10001L)));
    }

    @Test
    public void testSummaryBinarySearchSparse()
    {
        final SummaryDbUtils.TokenList list = new ArrayTokenList(5L, 10L, 15L, 20L, 25L);
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(-500L)));
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(3L)));
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(5L)));
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(6L)));
        assertEquals(0, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(10L)));
        assertEquals(1, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(11L)));
        assertEquals(1, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(13L)));
        assertEquals(1, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(15L)));
        assertEquals(2, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(16L)));
        assertEquals(3, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(25L)));
        assertEquals(4, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(26L)));
        assertEquals(4, SummaryDbUtils.binarySearchSummary(list, BigInteger.valueOf(100L)));
    }
}
