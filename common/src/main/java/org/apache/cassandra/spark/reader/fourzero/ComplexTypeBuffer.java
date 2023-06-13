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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ListType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.MapType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.SetType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UserType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Cell;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.transport.ProtocolVersion;

public abstract class ComplexTypeBuffer
{
    private final List<ByteBuffer> bufs;
    private final int cellCount;
    private int len = 0;

    ComplexTypeBuffer(final int cellCount, final int bufferSize)
    {
        this.cellCount = cellCount;
        this.bufs = new ArrayList<>(bufferSize);
    }

    public static ComplexTypeBuffer newBuffer(final AbstractType<?> type, final int cellCount)
    {
        final ComplexTypeBuffer buffer;
        if (type instanceof SetType)
        {
            buffer = new SetBuffer(cellCount);
        }
        else if (type instanceof ListType)
        {
            buffer = new ListBuffer(cellCount);
        }
        else if (type instanceof MapType)
        {
            buffer = new MapBuffer(cellCount);
        }
        else if (type instanceof UserType)
        {
            buffer = new UdtBuffer(cellCount);
        }
        else
        {
            throw new IllegalStateException("Unexpected type deserializing CQL Collection: " + type);
        }
        return buffer;
    }

    public void addCell(final Cell cell)
    {
        this.add(cell.buffer()); // copy over value
    }

    void add(final ByteBuffer buf)
    {
        bufs.add(buf);
        len += buf.remaining();
    }

    public ByteBuffer build()
    {
        final ByteBuffer result = ByteBuffer.allocate(4 + (bufs.size() * 4) + len);
        result.putInt(cellCount);
        for (final ByteBuffer buf : bufs)
        {
            result.putInt(buf.remaining());
            result.put(buf);
        }
        return (ByteBuffer) result.flip();
    }

    /**
     * Pack the cell ByteBuffers into a single ByteBuffer using Cassandra's packing algorithm.
     * It is similar to {@link #build()}, but encoding the data differently.
     *
     * @return a single ByteBuffer with all cell ByteBuffers encoded.
     */
    public ByteBuffer pack()
    {
        // See CollectionSerializer.deserialize for why using the protocol v3 variant is the right thing to do.
        return CollectionSerializer.pack(bufs, ByteBufferAccessor.instance, elements(), ProtocolVersion.V3);
    }

    protected int elements()
    {
        return bufs.size();
    }
}
