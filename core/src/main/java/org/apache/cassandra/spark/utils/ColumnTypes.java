package org.apache.cassandra.spark.utils;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.spark.data.CqlField;

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
    private final static String EMPTY_STR = "";
    private final static FastThreadLocal<CharsetDecoder> UTF8_DECODER = new FastThreadLocal<CharsetDecoder>()
    {
        @Override
        protected CharsetDecoder initialValue()
        {
            return StandardCharsets.UTF_8.newDecoder();
        }
    };

    public static ByteBuffer buildPartitionKey(List<CqlField> partitionKeys, Object... values)
    {
        if (partitionKeys.size() == 1)
        {
            // only 1 partition key
            final CqlField key = partitionKeys.get(0);
            return key.serialize(values[key.pos()]);
        }

        // composite partition key
        final ByteBuffer[] buffers = partitionKeys.stream()
                                                  .map(f -> f.serialize(values[f.pos()]))
                                                  .toArray(ByteBuffer[]::new);
        return ByteBufUtils.build(false, buffers);
    }

    // Extract component idx from bb. Return null if there is not enough component.
    public static ByteBuffer extractComponent(ByteBuffer bb, final int idx)
    {
        bb = bb.duplicate();
        ByteBufUtils.readStatic(bb);
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

    public static String string(final ByteBuffer buffer) throws CharacterCodingException
    {
        if (buffer.remaining() <= 0)
        {
            return ColumnTypes.EMPTY_STR;
        }
        return ColumnTypes.UTF8_DECODER.get().decode(buffer.duplicate()).toString();
    }

    public static String stringThrowRuntime(final ByteBuffer buffer)
    {
        try
        {
            return string(buffer);
        }
        catch (final CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }
}

