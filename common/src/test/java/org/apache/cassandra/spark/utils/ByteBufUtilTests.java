package org.apache.cassandra.spark.utils;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
@SuppressWarnings("UnstableApiUsage")
public class ByteBufUtilTests
{
    @Test
    public void testSkipBytesFully() throws IOException
    {
        testSkipBytesFully("abc".getBytes(StandardCharsets.UTF_8));
        testSkipBytesFully("abcdefghijklm".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testReadRemainingBytes() throws IOException
    {
        testReadRemainingBytes("");
        testReadRemainingBytes("abc");
        testReadRemainingBytes("abcdefghijklm");
    }

    @Test
    public void testGetArray()
    {
        testGetArray("");
        testGetArray("abc");
        testGetArray("abcdefghijklm");
    }

    @Test
    public void testHexString()
    {
        assertEquals("00000000000001F4", ByteBufUtils.toHexString((ByteBuffer) ByteBuffer.allocate(8).putLong(500L).flip()));
        assertEquals("616263", ByteBufUtils.toHexString(ByteBuffer.wrap(new byte[]{ 'a', 'b', 'c' })));
        assertEquals("000000000588C164", ByteBufUtils.toHexString((ByteBuffer) ByteBuffer.allocate(8).putLong(92848484L).asReadOnlyBuffer().flip()));
        assertEquals("null", ByteBufUtils.toHexString(null));

        assertEquals("616263", ByteBufUtils.toHexString(new byte[]{ 'a', 'b', 'c' }, 0, 3));
        assertEquals("63", ByteBufUtils.toHexString(new byte[]{ 'a', 'b', 'c' }, 2, 1));
    }

    private static void testGetArray(final String str)
    {
        assertEquals(str, new String(ByteBufUtils.getArray(ByteBuffer.wrap(str.getBytes())), StandardCharsets.UTF_8));
    }

    private static void testReadRemainingBytes(final String str) throws IOException
    {
        assertEquals(str, new String(ByteBufUtils.readRemainingBytes(new ByteArrayInputStream(str.getBytes()), str.length()), StandardCharsets.UTF_8));
    }

    private static void testSkipBytesFully(final byte[] ar) throws IOException
    {
        final int len = ar.length;
        final ByteArrayDataInput in = ByteStreams.newDataInput(ar, 0);
        ByteBufUtils.skipBytesFully(in, 1);
        ByteBufUtils.skipBytesFully(in, len - 2);
        assertEquals(ar[len - 1], in.readByte());
        try
        {
            ByteBufUtils.skipBytesFully(in, 1);
            fail("EOFException should have been thrown");
        }
        catch (final EOFException ignore)
        {
        }
    }
}
