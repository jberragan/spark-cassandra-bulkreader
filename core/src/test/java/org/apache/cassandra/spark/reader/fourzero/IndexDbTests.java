package org.apache.cassandra.spark.reader.fourzero;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.junit.Test;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.IPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Schema;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.spark.TestUtils.countSSTables;
import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

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

public class IndexDbTests
{
    @Test
    public void testSearchIndex()
    {
        runTest((partitioner, dir, bridge) -> {
            final TestSchema schema = TestSchema.basicBuilder(bridge).withCompression(false).build();
            final IPartitioner iPartitioner = FourZero.getPartitioner(partitioner);
            final int numRows = 5000;

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

            final int rowSize = 39;
            final int sample = 4;
            // sample the token list and read offset in Index.db for sampled list & verify the offset matches the expected
            // we sample the list as IndexDbUtils.findStartOffset(...) returns the previous offset, so we want to test
            // it correctly skips tokens less than the token we are looking for before returning.
            final List<BigInteger> sparseList = IntStream.range(0, tokens.size()).filter(i -> i > 0 && i % sample == 0).mapToObj(tokens::get).collect(Collectors.toList());
            assertEquals((numRows / 4) - 1, sparseList.size());
            try (final DataInputStream in = new DataInputStream(Objects.requireNonNull(ssTable.openPrimaryIndexStream())))
            {
                try
                {
                    for (int idx = 0; idx < sparseList.size(); idx++)
                    {
                        final BigInteger token = sparseList.get(idx);
                        final long expectedOffset = (((idx + 1L) * sample) - 1) * rowSize;
                        final long offset = IndexDbUtils.findStartOffset(in, iPartitioner, Range.closed(token, token), Stats.DoNothingStats.INSTANCE);
                        assertEquals(expectedOffset, offset);
                        FourZeroUtils.skipRowIndexEntry(in);
                    }
                }
                catch (final EOFException ignore)
                {

                }
            }
        });
    }

    public static class IndexRow implements Comparable<IndexRow>
    {
        final BigInteger token;
        final int value;
        int position = 0;

        public IndexRow(IPartitioner iPartitioner, int value)
        {
            this.token = token(iPartitioner, value);
            this.value = value;
        }

        public int compareTo(@NotNull IndexRow o)
        {
            return token.compareTo(o.token);
        }
    }

    @Test
    public void testFindStartEndOffset()
    {
        qt()
        .forAll(TestUtils.partitioners())
        .checkAssert((partitioner) ->
                     {
                         final IPartitioner iPartitioner = FourZero.getPartitioner(partitioner);
                         final int rowSize = 256;
                         final int numValues = 5000;

                         // generate random index row values and sort by token
                         final IndexRow[] rows = IntStream.range(0, numValues)
                                                          .mapToObj(i -> new IndexRow(iPartitioner, RandomUtils.randomPositiveInt(100000)))
                                                          .sorted()
                                                          .toArray(IndexRow[]::new);
                         IntStream.range(0, rows.length).forEach(i -> rows[i].position = i * rowSize); // update position offset
                         final int startPos = rows.length >> 1;
                         final IndexRow startRow = rows[startPos];
                         final int[] valuesAndOffsets = Arrays.stream(rows)
                                                              .map(i -> new int[]{ i.value, i.position })
                                                              .flatMapToInt(Arrays::stream)
                                                              .toArray();

                         try (final DataInputStream in = mockDataInputStream(valuesAndOffsets))
                         {
                             final long startOffset = IndexDbUtils.findStartOffset(in, iPartitioner, Range.closed(startRow.token, startRow.token), Stats.DoNothingStats.INSTANCE);
                             assertEquals(rows[startPos - 1].position, startOffset);
                             FourZeroUtils.skipRowIndexEntry(in);
                         }
                         catch (IOException e)
                         {
                             throw new RuntimeException(e);
                         }
                     });
    }

    @Test
    public void testReadToken()
    {
        qt()
        .withExamples(500)
        .forAll(TestUtils.partitioners(), integers().all())
        .checkAssert((partitioner, value) ->
                     {
                         final IPartitioner iPartitioner = FourZero.getPartitioner(partitioner);
                         final BigInteger expectedToken = token(iPartitioner, value);
                         try (final DataInputStream in = mockDataInputStream(value, 0))
                         {
                             IndexDbUtils.readNextToken(iPartitioner, in, new Stats()
                             {
                                 public void readPartitionIndexDb(ByteBuffer key, BigInteger token)
                                 {
                                     assertEquals(value.intValue(), key.getInt());
                                     assertEquals(expectedToken, token);
                                 }
                             });
                         }
                         catch (IOException e)
                         {
                             throw new RuntimeException(e);
                         }
                     }
        );
    }

    private static BigInteger token(IPartitioner iPartitioner, int value)
    {
        return FourZeroUtils.tokenToBigInteger(iPartitioner.decorateKey((ByteBuffer) ByteBuffer.allocate(4).putInt(value).flip()).getToken());
    }

    // creates an in-memory DataInputStream mocking Index.db bytes, with len (short), key (int), position (vint)
    private static DataInputStream mockDataInputStream(int... valuesAndOffsets) throws IOException
    {
        Preconditions.checkArgument(valuesAndOffsets.length % 2 == 0);
        final int numValues = valuesAndOffsets.length / 2;

        int size = (numValues * 7); // 2 bytes short len, 4 bytes partition key value, 1 byte promoted index
        size += IntStream.range(0, valuesAndOffsets.length) // variable int for position offset
                         .filter(i -> (i + 1) % 2 == 0)
                         .map(i -> valuesAndOffsets[i])
                         .map(FourZeroUtils::vIntSize)
                         .sum();

        final ByteBuffer buf = ByteBuffer.allocate(size);
        for (int i = 0; i < valuesAndOffsets.length; i = i + 2)
        {
            buf.putShort((short) 4)
               .putInt(valuesAndOffsets[i]); // value
            FourZeroUtils.writePosition(valuesAndOffsets[i + 1], buf); // write variable int position offset
            FourZeroUtils.writePosition(0L, buf); // promoted index
        }

        buf.flip();
        final byte[] ar = new byte[buf.remaining()];
        buf.get(ar);

        return new DataInputStream(new ByteArrayInputStream(ar));
    }


    @Test
    public void testLessThan() {
        assertTrue(IndexDbUtils.isLessThan(BigInteger.valueOf(4L), Range.open(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));
        assertTrue(IndexDbUtils.isLessThan(BigInteger.valueOf(4L), Range.openClosed(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));
        assertTrue(IndexDbUtils.isLessThan(BigInteger.valueOf(4L), Range.closed(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));
        assertTrue(IndexDbUtils.isLessThan(BigInteger.valueOf(4L), Range.closedOpen(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));

        assertTrue(IndexDbUtils.isLessThan(BigInteger.valueOf(5L), Range.open(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));
        assertTrue(IndexDbUtils.isLessThan(BigInteger.valueOf(5L), Range.openClosed(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));
        assertFalse(IndexDbUtils.isLessThan(BigInteger.valueOf(5L), Range.closed(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));
        assertFalse(IndexDbUtils.isLessThan(BigInteger.valueOf(5L), Range.closedOpen(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));

        assertFalse(IndexDbUtils.isLessThan(BigInteger.valueOf(6L), Range.open(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));
        assertFalse(IndexDbUtils.isLessThan(BigInteger.valueOf(6L), Range.openClosed(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));
        assertFalse(IndexDbUtils.isLessThan(BigInteger.valueOf(6L), Range.closed(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));
        assertFalse(IndexDbUtils.isLessThan(BigInteger.valueOf(6L), Range.closedOpen(BigInteger.valueOf(5L), BigInteger.valueOf(10L))));
    }
}
