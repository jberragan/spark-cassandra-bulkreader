package org.apache.cassandra.spark.reader.fourzero;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.Checksum;

import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.ChecksumType;
import org.apache.cassandra.spark.reader.common.ChunkCorruptException;
import org.apache.cassandra.spark.reader.common.RawInputStream;

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
    private final CompressionMetadata metadata;

    // used by reBuffer() to escape creating lots of temporary buffers
    private byte[] compressed;
    private long currentCompressed = 0;

    // re-use single checksum object
    private final Checksum checksum;

    // raw checksum bytes
    private final byte[] checksumBytes = new byte[4];

    private CompressedRawInputStream(final DataInputStream source, final CompressionMetadata metadata)
    {
        super(source, new byte[metadata.chunkLength()]);
        this.metadata = metadata;
        this.checksum = ChecksumType.CRC32.newInstance();
        this.compressed = new byte[metadata.compressor().initialCompressedBufferLength(metadata.chunkLength())];
    }

    static CompressedRawInputStream fromInputStream(final InputStream dataInputStream, final InputStream compressionInfoInputStream, final boolean hasCompressedLength) throws IOException
    {
        return new CompressedRawInputStream(new DataInputStream(dataInputStream), CompressionMetadata.fromInputStream(compressionInfoInputStream, hasCompressedLength));
    }

    @Override
    public boolean isEOF()
    {
        return current >= metadata.getDataLength();
    }

    private void decompressChunk(final CompressionMetadata.Chunk chunk, final double crcChance) throws IOException
    {
        final int checkSumFromChunk;

        //
        // We may be asked to skip ahead by more than one block
        //
        assert chunk.offset >= currentCompressed :
        String.format("Requested chunk at input offset %d is less than current compressed position at %d",
                      chunk.offset,
                      currentCompressed);

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
                lastBytesLength += readLen;
            }

            chunk.setLength(lastBytesLength - 4);

            //
            // we inadvertently also read the checksum; we need to grab it from the end of the buffer
            //
            checkSumFromChunk = ByteBufferUtil.toInt(ByteBuffer.wrap(compressed, lastBytesLength - 4, 4));
        }

        validBufferBytes = metadata.compressor().uncompress(compressed, 0, chunk.length, buffer, 0);

        if (crcChance > ThreadLocalRandom.current().nextDouble())
        {
            checksum.update(compressed, 0, chunk.length);

            if (checkSumFromChunk != (int) checksum.getValue())
            {
                throw new ChunkCorruptException("bad chunk " + chunk.toString());
            }

            // reset checksum object back to the original (blank) state
            checksum.reset();
        }

        currentCompressed += chunk.length + checksumBytes.length;

        // buffer offset is always aligned
        bufferOffset = current & -buffer.length;
    }

    @Override
    protected void reBuffer() throws IOException
    {
        decompressChunk(metadata.chunkAtPosition(current), metadata.crcCheckChance());
    }
}
