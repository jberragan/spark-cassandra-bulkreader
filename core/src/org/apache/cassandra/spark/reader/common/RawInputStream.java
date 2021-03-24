package org.apache.cassandra.spark.reader.common;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.ByteBufUtils;
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

public class RawInputStream extends InputStream
{
    protected final DataInputStream source;

    protected final byte[] buffer;

    // `current` as current position in source
    // `bufferOffset` is the offset of the beginning of the buffer
    protected long bufferOffset;
    protected long current = 0;
    // `validBufferBytes` is the number of bytes in the buffer that are actually valid;
    //  this will be LESS than buffer capacity if buffer is not full!
    protected int validBufferBytes = 0;
    private boolean endOfStream = false;
    protected final Stats stats;

    public RawInputStream(final DataInputStream source, final byte[] buffer, Stats stats)
    {
        this.source = source;
        this.buffer = buffer;
        this.stats = stats;
        this.stats.openedDataInputStream();
    }

    public boolean isEOF()
    {
        return endOfStream && (current >= bufferOffset + validBufferBytes);
    }

    private int bufferCursor()
    {
        return (int) (current - bufferOffset);
    }

    private boolean bufferInit = false;

    protected void reBuffer() throws IOException
    {
        if (endOfStream)
        {
            throw new RuntimeException("Shouldn't be reading from a known EOF stream.");
        }

        if (bufferInit)
        {
            bufferOffset += buffer.length;
        }
        else
        {
            bufferInit = true;
        }

        validBufferBytes = ByteBufUtils.readFully(source, buffer, buffer.length);
        stats.readBytes(validBufferBytes);

        if (validBufferBytes < buffer.length)
        {
            endOfStream = true;
        }
    }

    @Override
    public long skip(long n) throws IOException {
        final long actual = super.skip(n);
        stats.skippedBytes(actual);
        return actual;
    }

    @Override
    public int read() throws IOException
    {
        if (buffer == null)
        {
            throw new IOException();
        }

        if (isEOF())
        {
            return -1;
        }

        if (current >= bufferOffset + validBufferBytes || validBufferBytes == -1)
        {
            reBuffer();
        }

        assert current >= bufferOffset && current < bufferOffset + validBufferBytes;

        return ((int) buffer[(int) (current++ - bufferOffset)]) & 0xff;
    }

    @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read(@NotNull final byte[] buff, final int offset, final int length) throws IOException
    {
        if (buffer == null)
        {
            throw new IOException();
        }

        if (length == 0)
        {
            return 0;
        }

        if (isEOF())
        {
            return -1;
        }

        if (current >= bufferOffset + validBufferBytes || validBufferBytes == -1)
        {
            reBuffer();
        }

        assert current >= bufferOffset && current < bufferOffset + validBufferBytes
        : String.format("Current offset %d, buffer offset %d, buffer limit %d",
                        current,
                        bufferOffset,
                        validBufferBytes);

        final int toCopy = Math.min(length, validBufferBytes - bufferCursor());

        System.arraycopy(buffer, bufferCursor(), buff, offset, toCopy);
        current += toCopy;

        return toCopy;
    }

    @Override
    public void close() throws IOException
    {
        source.close();
        stats.closedDataInputStream();
    }
}
