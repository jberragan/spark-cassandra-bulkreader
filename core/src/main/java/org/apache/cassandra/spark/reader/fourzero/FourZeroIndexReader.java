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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.IncompleteSSTableException;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.reader.IndexConsumer;
import org.apache.cassandra.spark.reader.IndexEntry;
import org.apache.cassandra.spark.reader.common.AbstractCompressionMetadata;
import org.apache.cassandra.spark.reader.common.IndexReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DecoratedKey;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.IPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.format.Version;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.vint.VIntCoding;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.ByteBufUtils;
import org.apache.cassandra.spark.utils.streaming.CassandraFile;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.reader.fourzero.FourZeroUtils.readPosition;
import static org.apache.cassandra.spark.reader.fourzero.FourZeroUtils.skipPromotedIndex;

public class FourZeroIndexReader implements IndexReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FourZeroIndexReader.class);

    private Range<BigInteger> ssTableRange = null;

    public FourZeroIndexReader(@NotNull final SSTable ssTable,
                               @NotNull final TableMetadata metadata,
                               @Nullable final RangeFilter rangeFilter,
                               @NotNull final Stats stats,
                               @NotNull final IndexConsumer consumer)
    {
        long now = System.nanoTime();
        final long startTimeNanos = now;
        try
        {
            final File file = FourZeroSSTableReader.constructFilename(metadata.keyspace, metadata.name, ssTable.getDataFileName());
            final Descriptor descriptor = Descriptor.fromFilename(file);
            final Version version = descriptor.version;

            // if there is a range filter we can use the Summary.db file to seek to approximate start token range location in Index.db file
            long skipAhead = -1;
            now = System.nanoTime();
            if (rangeFilter != null)
            {
                final SummaryDbUtils.Summary summary = SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable);
                if (summary != null)
                {
                    this.ssTableRange = Range.closed(FourZeroUtils.tokenToBigInteger(summary.first().getToken()), FourZeroUtils.tokenToBigInteger(summary.last().getToken()));
                    if (!rangeFilter.overlaps(this.ssTableRange))
                    {
                        LOGGER.info("Skipping non-overlapping Index.db file rangeFilter='[{},{}]' sstableRange='[{},{}]'",
                                    rangeFilter.tokenRange().lowerEndpoint(), rangeFilter.tokenRange().upperEndpoint(),
                                    this.ssTableRange.lowerEndpoint(), this.ssTableRange.upperEndpoint());
                        stats.indexFileSkipped();
                        return;
                    }

                    skipAhead = summary.summary().getPosition(SummaryDbUtils.binarySearchSummary(summary.summary(), metadata.partitioner, rangeFilter.tokenRange().lowerEndpoint()));
                    stats.indexSummaryFileRead(System.nanoTime() - now);
                    now = System.nanoTime();
                }
            }

            // read CompressionMetadata if it exists
            final CompressionMetadata compressionMetadata = getCompressionMetadata(ssTable, version.hasMaxCompressedLength());
            if (compressionMetadata != null)
            {
                stats.indexCompressionFileRead(System.nanoTime() - now);
                now = System.nanoTime();
            }

            // read through Index.db and consume Partition keys
            try (final InputStream is = ssTable.openPrimaryIndexStream())
            {
                if (is == null)
                {
                    consumer.onFailure(new IncompleteSSTableException(CassandraFile.FileType.INDEX));
                    return;
                }

                consumePrimaryIndex(metadata.partitioner,
                                    is,
                                    ssTable.length(CassandraFile.FileType.INDEX),
                                    compressionMetadata,
                                    rangeFilter,
                                    stats,
                                    skipAhead,
                                    ssTable.length(CassandraFile.FileType.DATA),
                                    consumer);
                stats.indexFileRead(System.nanoTime() - now);
            }
        }
        catch (Throwable t)
        {
            consumer.onFailure(t);
        }
        finally
        {
            consumer.onFinished(System.nanoTime() - startTimeNanos);
        }
    }

    @Nullable
    public static CompressionMetadata getCompressionMetadata(final SSTable ssTable,
                                                             boolean hasMaxCompressedLength) throws IOException
    {
        try (final InputStream cis = ssTable.openCompressionStream())
        {
            if (cis != null)
            {
                return CompressionMetadata.fromInputStream(cis, hasMaxCompressedLength);
            }
        }
        return null;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    static void consumePrimaryIndex(@NotNull final IPartitioner partitioner,
                                    @NotNull final InputStream primaryIndex,
                                    final long primaryIndexLength,
                                    @Nullable final CompressionMetadata compressionMetadata,
                                    @Nullable final RangeFilter range,
                                    @NotNull final Stats stats,
                                    final long skipBytes,
                                    final long dataDbFileLength,
                                    @NotNull final IndexConsumer consumer) throws IOException
    {
        try (final DataInputStream dis = new DataInputStream(primaryIndex))
        {
            if (skipBytes > 0)
            {
                ByteBufUtils.skipFully(dis, skipBytes);
                stats.indexBytesSkipped(skipBytes);
            }

            ByteBuffer prevKey = null;
            long prevPos = 0;
            BigInteger prevToken = null;
            boolean started = false;

            long totalBytesRead = 0;
            try
            {
                while (true)
                {
                    // read partition key length
                    final int len = dis.readUnsignedShort();

                    // read partition key & decorate
                    final byte[] buf = new byte[len];
                    dis.readFully(buf);
                    final ByteBuffer key = ByteBuffer.wrap(buf);
                    final DecoratedKey decoratedKey = partitioner.decorateKey(key);
                    final BigInteger token = FourZeroUtils.tokenToBigInteger(decoratedKey.getToken());

                    // read position & skip promoted index
                    final long pos = readPosition(dis);
                    final int promotedIndex = skipPromotedIndex(dis);
                    totalBytesRead += 2 + len + VIntCoding.computeUnsignedVIntSize(pos) + promotedIndex;

                    if (prevKey != null && (range == null || range.overlaps(prevToken)))
                    {
                        // previous key overlaps with range filter, so consume
                        started = true;
                        final long uncompressed = pos - prevPos;
                        final long compressed = compressionMetadata == null ? uncompressed : calculateCompressedSize(compressionMetadata, dataDbFileLength, prevPos, pos - 1);
                        consumer.accept(new IndexEntry(prevKey, prevToken, uncompressed, compressed));
                    }
                    else if (started)
                    {
                        // we have gone passed the range we care about so exit early
                        stats.indexBytesSkipped(primaryIndexLength - totalBytesRead - skipBytes);
                        return;
                    }

                    prevPos = pos;
                    prevKey = key;
                    prevToken = token;
                }
            }
            catch (final EOFException ignored)
            {
                // finished
            }
            finally
            {
                stats.indexBytesRead(totalBytesRead);
            }

            if (prevKey != null && (range == null || range.overlaps(prevToken)))
            {
                // we reached the end of the file, so consume last key if overlaps
                final long end = (compressionMetadata == null ? dataDbFileLength : compressionMetadata.getDataLength());
                final long uncompressed = end - prevPos;
                final long compressed = compressionMetadata == null ? uncompressed : calculateCompressedSize(compressionMetadata, dataDbFileLength, prevPos, end - 1);
                consumer.accept(new IndexEntry(prevKey, prevToken, uncompressed, compressed));
            }
        }
    }

    /**
     * @param compressionMetadata  SSTable Compression Metadata
     * @param compressedDataLength full compressed length of the Data.db file
     * @param start                uncompressed start position.
     * @param end                  uncompressed end position.
     * @return the compressed size of a partition using the uncompressed start and end offset in the Data.db file to calculate.
     */
    public static long calculateCompressedSize(@NotNull final CompressionMetadata compressionMetadata,
                                               final long compressedDataLength,
                                               final long start,
                                               final long end)
    {
        final int startIdx = compressionMetadata.chunkIdx(start);
        final int endIdx = compressionMetadata.chunkIdx(end);
        final AbstractCompressionMetadata.Chunk startChunk = compressionMetadata.chunkAtIndex(startIdx);
        final long startLen = chunkCompressedLength(startChunk, compressedDataLength);
        final long uncompressedChunkLen = compressionMetadata.chunkLength(); // compressed chunk sizes vary, but uncompressed chunk length is the same for all chunks

        if (startIdx == endIdx)
        {
            // within the same chunk, so take % of uncompressed length and apply to compressed length
            final float perc = (end - start) / (float) uncompressedChunkLen;
            return Math.round(perc * startLen);
        }

        long size = partialCompressedSizeWithinChunk(start, uncompressedChunkLen, startLen, true);
        final AbstractCompressionMetadata.Chunk endChunk = compressionMetadata.chunkAtIndex(endIdx);
        final long endLen = chunkCompressedLength(endChunk, compressedDataLength);

        size += partialCompressedSizeWithinChunk(end, uncompressedChunkLen, endLen, false);

        for (int idx = startIdx + 1; idx < endIdx; idx++)
        {
            // add compressed size of whole intermediate chunks
            size += chunkCompressedLength(compressionMetadata.chunkAtIndex(idx), compressedDataLength);
        }

        return size;
    }

    private static long chunkCompressedLength(final AbstractCompressionMetadata.Chunk chunk, final long compressedDataLength)
    {
        // chunk.length < 0 means it is the last chunk so use compressedDataLength to calculate compressed size
        return chunk.length >= 0 ? chunk.length : compressedDataLength - chunk.offset;
    }

    /**
     * Returns the partial compressed size of a partition whose start or end overlaps with a compressed chunk.
     * This is an estimate because of the variable compressibility of partitions within the chunk.
     *
     * @param uncompressedPos      uncompressed position in Data.db file
     * @param uncompressedChunkLen fixed size uncompressed chunk size
     * @param compressedChunkLen   compressed chunk size of this chunk
     * @param start                true if uncompressedPos is start position of partition and false if end position of partition
     * @return the estimated compressed size of partition start or end that overlaps with this chunk.
     */
    public static int partialCompressedSizeWithinChunk(final long uncompressedPos,
                                                       final long uncompressedChunkLen,
                                                       final long compressedChunkLen,
                                                       final boolean start)
    {
        final long mod = uncompressedPos % uncompressedChunkLen;
        final long usedBytes = start ? (uncompressedChunkLen - mod) : mod; // if start position then it occupies remaining bytes to end of chunk, if end position it occupies bytes from start of chunk
        final float perc = usedBytes / (float) uncompressedChunkLen; // percentage of uncompressed bytes that it occupies in the chunk
        return Math.round(perc * compressedChunkLen); // apply percentage to compressed chunk length to give compressed bytes occupied
    }

    public BigInteger firstToken()
    {
        return ssTableRange != null ? ssTableRange.lowerEndpoint() : null;
    }

    public BigInteger lastToken()
    {
        return ssTableRange != null ? ssTableRange.upperEndpoint() : null;
    }

    public boolean ignore()
    {
        return false;
    }
}
