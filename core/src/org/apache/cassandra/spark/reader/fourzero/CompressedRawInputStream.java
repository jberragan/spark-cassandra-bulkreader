package org.apache.cassandra.spark.reader.fourzero;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.reader.common.AbstractCompressionMetadata;
import org.apache.cassandra.spark.reader.common.ChunkCorruptException;
import org.apache.cassandra.spark.reader.common.RawInputStream;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.ChecksumType;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.streaming.SkippableDataInputStream;
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

public class CompressedRawInputStream extends RawInputStream
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CompressedRawInputStream.class);

    @Nullable
    private final DataLayer.SSTable ssTable; // only used for logging/stats
    private final CompressionMetadata metadata;

    // used by reBuffer() to escape creating lots of temporary buffers
    private byte[] compressed;
    private long currentCompressed = 0;

    // re-use single checksum object
    private final Checksum checksum;

    // raw checksum bytes
    private final byte[] checksumBytes = new byte[4];

    private CompressedRawInputStream(@Nullable final DataLayer.SSTable ssTable,
                                     final DataInputStream source,
                                     final CompressionMetadata metadata,
                                     Stats stats)
    {
        super(source, new byte[metadata.chunkLength()], stats);
        this.ssTable = ssTable;
        this.metadata = metadata;
        this.checksum = ChecksumType.CRC32.newInstance();
        this.compressed = new byte[metadata.compressor().initialCompressedBufferLength(metadata.chunkLength())];
    }

    @VisibleForTesting
    static CompressedRawInputStream fromInputStream(final InputStream in, final InputStream compressionInfoInputStream, final boolean hasCompressedLength) throws IOException
    {
        return fromInputStream(null, SkippableDataInputStream.of(in), compressionInfoInputStream, hasCompressedLength, Stats.DoNothingStats.INSTANCE);
    }

    static CompressedRawInputStream fromInputStream(@Nullable final DataLayer.SSTable ssTable,
                                                    final DataInputStream dataInputStream,
                                                    final InputStream compressionInfoInputStream,
                                                    final boolean hasCompressedLength,
                                                    Stats stats) throws IOException
    {
        return new CompressedRawInputStream(ssTable, dataInputStream, CompressionMetadata.fromInputStream(compressionInfoInputStream, hasCompressedLength), stats);
    }

    @Override
    public boolean isEOF()
    {
        return current >= metadata.getDataLength();
    }

    private void assertChunkPos(CompressionMetadata.Chunk chunk)
    {
        //
        // We may be asked to skip ahead by more than one block
        //
        assert chunk.offset >= currentCompressed :
        String.format("Requested chunk at input offset %d is less than current compressed position at %d",
                      chunk.offset,
                      currentCompressed);
    }

    private void decompressChunk(final CompressionMetadata.Chunk chunk, final double crcChance) throws IOException
    {
        final int checkSumFromChunk;

        assertChunkPos(chunk);

        source.skipBytes((int) (chunk.offset - currentCompressed));
        currentCompressed = chunk.offset;

        if (compressed.length < chunk.length)
        {
            compressed = new byte[chunk.length];
        }

        if (chunk.length > 0)
        {
            try
            {
                source.readFully(compressed, 0, chunk.length);
            }
            catch (final EOFException eOFE)
            {
                throw new IOException(String.format("Failed to read %d bytes from offset %d.", chunk.length, chunk.offset), eOFE);
            }

            checkSumFromChunk = source.readInt();
            stats.readBytes(chunk.length + checksumBytes.length); // 4 bytes for crc
        }
        else
        {
            //
            // last block; we don't have the length of the last chunk;
            // try to read full buffer length; this will almost certainly end up reading all of the compressed data;
            // update current chunk length to the number of the bytes read minus 4 to accommodate for the chunk length field
            //

            int lastBytesLength = 0;
            for (; ; )
            {
                if (lastBytesLength >= compressed.length)
                {
                    final byte[] buf = new byte[lastBytesLength * 2];
                    System.arraycopy(compressed, 0, buf, 0, lastBytesLength);
                    compressed = buf;
                }
                final int readLen = source.read(compressed, lastBytesLength, compressed.length - lastBytesLength);
                if (readLen == -1)
                {
                    break;
                }
                stats.readBytes(readLen);
                lastBytesLength += readLen;
            }

            chunk.setLength(lastBytesLength - 4);

            //
            // we inadvertently also read the checksum; we need to grab it from the end of the buffer
            //
            checkSumFromChunk = ByteBufferUtil.toInt(ByteBuffer.wrap(compressed, lastBytesLength - 4, 4));
        }

        validBufferBytes = metadata.compressor().uncompress(compressed, 0, chunk.length, buffer, 0);
        stats.decompressedBytes(chunk.length, validBufferBytes);

        if (crcChance > ThreadLocalRandom.current().nextDouble())
        {
            checksum.update(compressed, 0, chunk.length);

            if (checkSumFromChunk != (int) checksum.getValue())
            {
                throw new ChunkCorruptException("bad chunk " + chunk);
            }

            // reset checksum object back to the original (blank) state
            checksum.reset();
        }

        currentCompressed += chunk.length + checksumBytes.length;

        alignBufferOffset();
    }

    /**
     * Buffer offset is always aligned
     */
    private void alignBufferOffset()
    {
        // see: https://en.wikipedia.org/wiki/Data_structure_alignment#Computing_padding
        bufferOffset = current & -buffer.length;
    }

    @Override
    protected void reBuffer() throws IOException
    {
        try
        {
            decompressChunk(metadata.chunkAtPosition(current), metadata.crcCheckChance());
        }
        catch (IOException e)
        {
            if (ssTable != null)
            {
                LOGGER.warn("IOException decompressing SSTable position={} sstable='{}'", current, ssTable, e);
                stats.decompressionException(ssTable, e);
            }
            throw e;
        }
    }

    /**
     * For Compressed input, we can only efficiently skip entire compressed chunks.
     * Therefore, we:
     * 1) Skip any uncompressed bytes already buffered. If that's enough to satisfy the skip request, we return.
     * 2) Count how many whole compressed chunks we can skip to satisfy the uncompressed skip request,
     * then skip the compressed bytes at the source InputStream, decrementing the remaining bytes by the equivalent uncompressed bytes that have been skipped.
     * 3) Then we continue to skip any remaining bytes in-memory (decompress the next chunk and skip in-memory until we've satisfied the skip request)
     *
     * @param n number of uncompressed bytes to skip
     */
    @Override
    public long skip(long n) throws IOException
    {
        final long precheck = maybeStandardSkip(n);
        if (precheck >= 0)
        {
            return precheck;
        }

        // skip any buffered bytes
        long remaining = n - skipBuffered();

        // we can efficiently skip ahead by 0 or more whole compressed chunks
        // by passing down to the source InputStream
        long totalCompressedBytes = 0L;
        long totalDecompressedBytes = 0L;
        final long startCurrent = current, startCompressed = currentCompressed, startBufferOffset = bufferOffset;
        while (totalDecompressedBytes + buffer.length < remaining) // we can only skip whole chunks
        {
            final AbstractCompressionMetadata.Chunk chunk = metadata.chunkAtPosition(current);
            if (chunk.length < 0)
            {
                // for the last chunk we don't know the length so reset positions & skip as normal
                current = startCurrent;
                currentCompressed = startCompressed;
                bufferOffset = startBufferOffset;
                return standardSkip(remaining);
            }

            assertChunkPos(chunk);

            // sum total compressed & decompressed bytes we can skip
            final int chunkLen = chunk.length + checksumBytes.length;
            totalCompressedBytes += (int) (chunk.offset - currentCompressed);
            currentCompressed = chunk.offset;
            totalCompressedBytes += chunkLen;
            currentCompressed += chunkLen;
            totalDecompressedBytes += buffer.length;

            alignBufferOffset();
            current += buffer.length;
        }

        // skip compressed chunks at the source
        final long skipped = source.skip(totalCompressedBytes);
        assert skipped == totalCompressedBytes : "Bytes skipped should equal compressed length";
        remaining -= totalDecompressedBytes; // decrement decompressed bytes we have skipped

        // skip any remaining bytes as normal
        remaining -= standardSkip(remaining);

        final long total = n - remaining;
        stats.skippedBytes(total);
        return total;
    }
}
