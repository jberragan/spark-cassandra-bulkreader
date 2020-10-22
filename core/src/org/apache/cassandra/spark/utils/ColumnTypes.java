package org.apache.cassandra.spark.utils;

import java.nio.ByteBuffer;
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

public class ColumnTypes
{
    private static final int STATIC_MARKER = 0xFFFF;

    public static ByteBuffer build(final boolean isStatic, final ByteBuffer... buffers)
    {
        int totalLength = isStatic ? 2 : 0;
        for (final ByteBuffer bb : buffers)
        {
            totalLength += 2 + bb.remaining() + 1;
        }

        final ByteBuffer out = ByteBuffer.allocate(totalLength);
        if (isStatic)
        {
            out.putShort((short) STATIC_MARKER);
        }

        for (final ByteBuffer bb : buffers)
        {
            ByteBufUtils.writeShortLength(out, bb.remaining());
            out.put(bb.duplicate());
            out.put((byte) 0);
        }
        out.flip();
        return out;
    }

    // Extract component idx from bb. Return null if there is not enough component.
    public static ByteBuffer extractComponent(ByteBuffer bb, final int idx)
    {
        bb = bb.duplicate();
        readStatic(bb);
        int i = 0;
        while (bb.remaining() > 0)
        {
            final ByteBuffer c = ByteBufUtils.readBytesWithShortLength(bb);
            if (i == idx)
            {
                return c;
            }

            bb.get(); // skip end-of-component
            ++i;
        }
        return null;
    }

    public static ByteBuffer[] split(final ByteBuffer name, final int numKeys)
    {
        // Assume all components, we'll trunk the array afterwards if need be, but
        // most names will be complete.
        final ByteBuffer[] l = new ByteBuffer[numKeys];
        final ByteBuffer bb = name.duplicate();
        ColumnTypes.readStatic(bb);
        int i = 0;
        while (bb.remaining() > 0)
        {
            l[i++] = ByteBufUtils.readBytesWithShortLength(bb);
            bb.get(); // skip end-of-component
        }
        return i == l.length ? l : Arrays.copyOfRange(l, 0, i);
    }

    private static void readStatic(final ByteBuffer bb)
    {
        if (bb.remaining() < 2)
        {
            return;
        }

        final int header = ByteBufUtils.peekShortLength(bb, bb.position());
        if ((header & 0xFFFF) != STATIC_MARKER)
        {
            return;
        }

        ByteBufUtils.readShortLength(bb); // Skip header
    }
}

