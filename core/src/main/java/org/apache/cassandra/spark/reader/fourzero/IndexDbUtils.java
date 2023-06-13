package org.apache.cassandra.spark.reader.fourzero;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.IPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.ByteBufUtils;
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

/**
 * Helper methods for reading the Index.db SSTable file component
 */
public class IndexDbUtils
{
    @Nullable
    public static Long findDataDbOffset(@NotNull final IndexSummary indexSummary,
                                        @NotNull final Range<BigInteger> range,
                                        @NotNull final IPartitioner partitioner,
                                        @NotNull final SSTable ssTable,
                                        @NotNull final Stats stats) throws IOException
    {
        final long searchStartOffset = SummaryDbUtils.findIndexOffsetInSummary(indexSummary, partitioner, range.lowerEndpoint());

        // open the Index.db, skip to nearest offset found in Summary.db and find start & end offset for the Data.db file
        return findDataDbOffset(range, partitioner, ssTable, stats, searchStartOffset);
    }

    @Nullable
    public static Long findDataDbOffset(@NotNull final Range<BigInteger> range,
                                        @NotNull final IPartitioner partitioner,
                                        @NotNull final SSTable ssTable,
                                        @NotNull final Stats stats,
                                        final long searchStartOffset) throws IOException
    {
        try (final InputStream is = ssTable.openPrimaryIndexStream())
        {
            return findIndexOffset(is, partitioner, range, stats, searchStartOffset);
        }
    }

    /**
     * Find the first Data.db offset in the Index.db file for a given token range,
     * using the approximate start offset found in the Summary.db file to seek ahead to the nearest position in the Index.db file.
     *
     * @param is                the input stream on the Index.db file
     * @param partitioner       Cassandra partitioner
     * @param range             the range we are trying to find
     * @param stats             stats instance
     * @param searchStartOffset the Index.db approximate start offset read from the Summary.db sample file
     * @return the index offset into the Data.db file for the first partition greater than or equal to the token, or null if cannot find
     * @throws IOException IOException reading Index.db file
     */
    @Nullable
    static Long findIndexOffset(@Nullable final InputStream is,
                                @NotNull IPartitioner partitioner,
                                @NotNull final Range<BigInteger> range,
                                @NotNull final Stats stats,
                                final long searchStartOffset) throws IOException
    {
        if (is == null)
        {
            return null;
        }

        try
        {
            // skip to Index.db offset found in Summary.db file
            final DataInputStream in = new DataInputStream(is);
            ByteBufUtils.skipFully(in, searchStartOffset);

            return findStartOffset(in, partitioner, range, stats);
        }
        catch (EOFException ignore)
        {
            // we can possibly reach EOF before start has been found, which is fine
        }

        return null;
    }

    /**
     * Find and return Data.db offset for first overlapping partition.
     *
     * @param in          Index.db DataInputStream
     * @param partitioner partitioner
     * @param range       Spark worker token range
     * @param stats       stats instance
     * @return start offset into the Data.db file for the first overlapping partition.
     * @throws IOException IOException reading Index.db file
     */
    static long findStartOffset(@NotNull final DataInputStream in,
                                @NotNull final IPartitioner partitioner,
                                @NotNull final Range<BigInteger> range,
                                @NotNull final Stats stats) throws IOException
    {
        BigInteger keyToken;
        long prev = 0L;
        while (isLessThan((keyToken = readNextToken(partitioner, in, stats)), range))
        {
            // keep skipping until we find first partition overlapping with Spark token range
            prev = FourZeroUtils.readPosition(in);
            FourZeroUtils.skipPromotedIndex(in);
        }
        assert range.lowerEndpoint().compareTo(keyToken) <= 0;
        // found first token that overlaps with Spark token range
        // because we passed the target by skipping the promoted index,
        // we use the previously-read position as start
        return prev;
    }

    /**
     * @param keyToken key token read from Index.db
     * @param range    spark worker token range
     * @return true if keyToken is less than the range lower bound
     */
    static boolean isLessThan(BigInteger keyToken, Range<BigInteger> range)
    {
        if (range.lowerBoundType() == BoundType.CLOSED)
        {
            return keyToken.compareTo(range.lowerEndpoint()) < 0;
        }
        return keyToken.compareTo(range.lowerEndpoint()) <= 0;
    }

    /**
     * Read partition key, use partitioner to hash and return token as BigInteger.
     *
     * @param partitioner partitioner
     * @param in          Index.db DataInputStream
     * @param stats       stats instance
     * @return token as BigInteger
     * @throws IOException IOException reading Index.db file
     */
    static BigInteger readNextToken(@NotNull final IPartitioner partitioner,
                                    @NotNull final DataInputStream in,
                                    @NotNull final Stats stats) throws IOException
    {
        final ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
        final BigInteger token = BaseFourZeroUtils.tokenToBigInteger(partitioner.decorateKey(key).getToken());
        stats.readPartitionIndexDb((ByteBuffer) key.rewind(), token);
        return token;
    }
}
