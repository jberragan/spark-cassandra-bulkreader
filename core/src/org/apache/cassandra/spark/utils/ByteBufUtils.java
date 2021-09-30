package org.apache.cassandra.spark.utils;

import io.netty.util.concurrent.FastThreadLocal;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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
public class ByteBufUtils
{

    private final static String EMPTY_STR = "";
    private final static FastThreadLocal<CharsetDecoder> UTF8_DECODER = new FastThreadLocal<CharsetDecoder>()
    {
        @Override
        protected CharsetDecoder initialValue()
        {
            return StandardCharsets.UTF_8.newDecoder();
        }
    };

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static void skipBytesFully(final DataInput in, final int bytes) throws IOException
    {
        int n = 0;
        while (n < bytes)
        {
            final int skipped = in.skipBytes(bytes - n);
            if (skipped == 0)
            {
                throw new EOFException("EOF after " + n + " bytes out of " + bytes);
            }
            n += skipped;
        }
    }

    public static byte[] readRemainingBytes(final InputStream in, final int size) throws IOException
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream(size);
        final byte[] ar = new byte[size];
        int len;
        while ((len = in.read(ar)) != -1)
        {
            out.write(ar, 0, len);
        }
        return out.toByteArray();
    }

    public static byte[] getArray(final ByteBuffer buffer)
    {
        final int length = buffer.remaining();

        if (buffer.hasArray())
        {
            final int boff = buffer.arrayOffset() + buffer.position();
            return Arrays.copyOfRange(buffer.array(), boff, boff + length);
        }
        // else, DirectByteBuffer.get() is the fastest route
        final byte[] bytes = new byte[length];
        buffer.duplicate().get(bytes);

        return bytes;
    }

    public static String stringThrowRuntime(final ByteBuffer buffer)
    {
        try
        {
            return ByteBufUtils.string(buffer);
        }
        catch (final CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String string(final ByteBuffer buffer) throws CharacterCodingException
    {
        if (buffer.remaining() <= 0)
        {
            return EMPTY_STR;
        }
        return UTF8_DECODER.get().decode(buffer.duplicate()).toString();
    }

    private static String toHexString(final byte[] bytes, final int length)
    {
        return toHexString(bytes, 0, length);
    }

    static String toHexString(final byte[] bytes, final int offset, final int length)
    {
        final char[] hexCharacters = new char[length << 1];

        int decimalValue;
        for (int i = offset; i < offset + length; i++)
        {
            // Calculate the int value represented by the byte
            decimalValue = bytes[i] & 0xFF;
            // Retrieve hex character for 4 upper bits
            hexCharacters[(i - offset) << 1] = HEX_ARRAY[decimalValue >> 4];
            // Retrieve hex character for 4 lower bits
            hexCharacters[((i - offset) << 1) + 1] = HEX_ARRAY[decimalValue & 0xF];
        }

        return new String(hexCharacters);
    }

    public static String toHexString(final ByteBuffer buffer)
    {
        if (buffer == null)
        {
            return "null";
        }

        if (buffer.isReadOnly())
        {
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.slice().get(bytes);
            return ByteBufUtils.toHexString(bytes, bytes.length);
        }

        return ByteBufUtils.toHexString(buffer.array(),
                                        buffer.arrayOffset() + buffer.position(),
                                        buffer.remaining());
    }

    public static int readFully(final InputStream in, final byte[] b, final int len) throws IOException
    {
        if (len < 0)
        {
            throw new IndexOutOfBoundsException();
        }

        int n = 0;
        while (n < len)
        {
            final int count = in.read(b, n, len - n);
            if (count < 0)
            {
                break;
            }
            n += count;
        }

        return n;
    }

    // changes bb position
    public static ByteBuffer readBytesWithShortLength(final ByteBuffer bb)
    {
        return readBytes(bb, readShortLength(bb));
    }

    // changes bb position
    static void writeShortLength(final ByteBuffer bb, final int length)
    {
        bb.put((byte) ((length >> 8) & 0xFF));
        bb.put((byte) (length & 0xFF));
    }

    // Doesn't change bb position
    static int peekShortLength(final ByteBuffer bb, final int position)
    {
        final int length = (bb.get(position) & 0xFF) << 8;
        return length | (bb.get(position + 1) & 0xFF);
    }

    // changes bb position
    static int readShortLength(final ByteBuffer bb)
    {
        final int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }

    // changes bb position
    @SuppressWarnings("RedundantCast")
    public static ByteBuffer readBytes(final ByteBuffer bb, final int length)
    {
        final ByteBuffer copy = bb.duplicate();
        ((Buffer) copy).limit(copy.position() + length);
        ((Buffer) bb).position(bb.position() + length);
        return copy;
    }

    public static void skipFully(InputStream is, long len) throws IOException
    {
        final long skipped = is.skip(len);
        if (skipped != len)
        {
            throw new EOFException("EOF after " + skipped + " bytes out of " + len);
        }
    }
}
