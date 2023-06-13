package org.apache.cassandra.spark.reader.fourzero;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DecoratedKey;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.IPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.ByteBufferUtil;
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
 * Helper methods for reading the Summary.db SSTable file component
 */
public class SummaryDbUtils
{
    static class Summary
    {
        private final IndexSummary indexSummary;
        private final DecoratedKey firstKey, lastKey;

        Summary(final IndexSummary indexSummary,
                final DecoratedKey firstKey,
                final DecoratedKey lastKey)
        {
            this.indexSummary = indexSummary;
            this.firstKey = firstKey;
            this.lastKey = lastKey;
        }

        public IndexSummary summary()
        {
            return indexSummary;
        }

        public DecoratedKey first()
        {
            return firstKey;
        }

        public DecoratedKey last()
        {
            return lastKey;
        }
    }

    static Summary readSummary(@NotNull final TableMetadata metadata, @NotNull final SSTable ssTable) throws IOException
    {
        try (final InputStream in = ssTable.openSummaryStream())
        {
            return readSummary(in, metadata.partitioner, metadata.params.minIndexInterval, metadata.params.maxIndexInterval);
        }
    }

    /**
     * Read and deserialize the Summary.db file.
     *
     * @param summaryStream    input stream for Summary.db file
     * @param partitioner      token partitioner
     * @param minIndexInterval min index interval
     * @param maxIndexInterval max index interval
     * @return Summary object
     * @throws IOException io exception
     */
    static Summary readSummary(final InputStream summaryStream,
                               final IPartitioner partitioner,
                               final int minIndexInterval,
                               final int maxIndexInterval) throws IOException
    {
        if (summaryStream == null)
        {
            return null;
        }

        try (final DataInputStream is = new DataInputStream(summaryStream))
        {
            final IndexSummary indexSummary = IndexSummary.serializer.deserialize(is, partitioner, minIndexInterval, maxIndexInterval);
            final DecoratedKey firstKey = partitioner.decorateKey(ByteBufferUtil.readWithLength(is));
            final DecoratedKey lastKey = partitioner.decorateKey(ByteBufferUtil.readWithLength(is));
            return new Summary(indexSummary, firstKey, lastKey);
        }
    }

    public interface TokenList
    {
        int size();

        BigInteger tokenAt(int idx);
    }

    /**
     * Binary search Summary.db to find nearest offset in Index.db that precedes the token we are looking for.
     *
     * @param summary     IndexSummary from Summary.db file.
     * @param partitioner Cassandra partitioner to hash partition keys to token.
     * @param token       the token we are trying to find.
     * @return offset into the Index.db file for the closest to partition in the Summary.db file that precedes the token we are looking for.
     */
    public static long findIndexOffsetInSummary(IndexSummary summary, IPartitioner partitioner, BigInteger token)
    {
        return summary.getPosition(binarySearchSummary(summary, partitioner, token));
    }

    public static class IndexSummaryTokenList implements TokenList
    {

        final IPartitioner partitioner;
        final IndexSummary summary;

        IndexSummaryTokenList(final IPartitioner partitioner,
                              final IndexSummary summary)
        {
            this.partitioner = partitioner;
            this.summary = summary;
        }

        public int size()
        {
            return summary.size();
        }

        public BigInteger tokenAt(int idx)
        {
            return BaseFourZeroUtils.tokenToBigInteger(partitioner.decorateKey(ByteBuffer.wrap(summary.getKey(idx))).getToken());
        }
    }

    public static int binarySearchSummary(IndexSummary summary, IPartitioner partitioner, BigInteger token)
    {
        return binarySearchSummary(new IndexSummaryTokenList(partitioner, summary), token);
    }

    /**
     * Binary search the Summary.db file to find nearest index offset in Index.db for a given token.
     * Method lifted from org.apache.cassandra.io.sstable.IndexSummary.binarySearch(PartitionPosition key) and reworked for tokens.
     *
     * @param tokenList list of tokens to binary search
     * @param token     token to find
     * @return closest offset in Index.db preceding token.
     */
    public static int binarySearchSummary(TokenList tokenList, BigInteger token)
    {
        int low = 0, mid = tokenList.size(), high = mid - 1, result = -1;
        while (low <= high)
        {
            mid = low + high >> 1;
            result = token.compareTo(tokenList.tokenAt(mid));
            if (result > 0)
            {
                low = mid + 1;
            }
            else if (result < 0)
            {
                high = mid - 1;
            }
            else
            {
                break;  // exact match
            }
        }

        /* If:
               1) result < 0: the token is less than nearest sampled token found at mid, so we need to start from mid - 1.
               2) result == 0: we found an exact match for the token in the sample, but there may be token collisions in Data.db so start from mid -1 to be safe.
               3) result > 0: the nearest sample token at mid is less than the token so we can start from that position.
         */
        return result <= 0 ? Math.max(0, mid - 1) : mid;
    }
}
