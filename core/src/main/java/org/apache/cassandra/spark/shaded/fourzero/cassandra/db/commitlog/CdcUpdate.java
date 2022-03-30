package org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog;

import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.reader.fourzero.CdcScannerBuilder;
import org.apache.cassandra.spark.reader.fourzero.Scannable;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DecoratedKey;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Digest;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.net.MessagingService;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Schema;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
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

public class CdcUpdate implements Scannable, Comparable<CdcUpdate>
{
    private final TableMetadata tableMetadata;
    private final PartitionUpdate update;
    private final ByteBuffer digestBytes;
    private final long maxTimestampMicros;
    private final int dateSize;

    public CdcUpdate(TableMetadata tableMetadata,
                     PartitionUpdate update,
                     long maxTimestampMicros)
    {
        this.tableMetadata = tableMetadata;
        this.update = update;
        this.maxTimestampMicros = maxTimestampMicros;
        final Digest digest = Digest.forReadResponse();
        UnfilteredRowIterators.digest(update.unfilteredIterator(), digest, MessagingService.current_version);
        this.digestBytes = ByteBuffer.wrap(digest.digest());
        this.dateSize = 18 /* = 8 + 4 + 2 + 4 */ + digestBytes.remaining() + update.dataSize();
    }

    // for deserialization
    private CdcUpdate(TableMetadata tableMetadata,
                      PartitionUpdate update,
                      long maxTimestampMicros,
                      ByteBuffer digestBytes,
                      int dateSize)
    {
        this.tableMetadata = tableMetadata;
        this.update = update;
        this.maxTimestampMicros = maxTimestampMicros;
        this.digestBytes = digestBytes;
        this.dateSize = dateSize;
    }

    public DecoratedKey partitionKey()
    {
        return update.partitionKey();
    }

    public long maxTimestampMicros()
    {
        return maxTimestampMicros;
    }

    public PartitionUpdate partitionUpdate()
    {
        return update;
    }

    public ByteBuffer digest()
    {
        return digestBytes;
    }

    public int dataSize()
    {
        return dateSize;
    }

    @Override
    public ISSTableScanner scanner()
    {
        return new CdcScannerBuilder.CDCScanner(tableMetadata, update);
    }

    // todo: add proper equals and hashCode impl for PartitionUpdate in OSS.
    @Override
    public int hashCode()
    {
        return digestBytes.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }

        if (obj instanceof CdcUpdate)
        {
            return digestBytes.equals(((CdcUpdate) obj).digestBytes);
        }

        return false;
    }

    public int compareTo(@NotNull CdcUpdate o)
    {
        return Long.compare(this.maxTimestampMicros, o.maxTimestampMicros);
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CdcUpdate>
    {
        final TableMetadata metadata;

        public Serializer(String keyspace, String table)
        {
            this.metadata = Schema.instance.getTableMetadata(keyspace, table);
        }

        public Serializer(TableMetadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public CdcUpdate read(final Kryo kryo, final Input in, final Class type)
        {
            final long maxTimestampMicros = in.readLong();
            final int size = in.readInt();

            // read digest
            final ByteBuffer digest = ByteBuffer.wrap(in.readBytes(in.readShort()));

            // read partition update
            final PartitionUpdate partitionUpdate = PartitionUpdate.fromBytes(ByteBuffer.wrap(in.readBytes(in.readInt())), MessagingService.current_version);

            return new CdcUpdate(metadata, partitionUpdate, maxTimestampMicros, digest, size);
        }

        @Override
        public void write(final Kryo kryo, final Output out, final CdcUpdate update)
        {
            out.writeLong(update.maxTimestampMicros); // 8 bytes
            out.writeInt(update.dateSize); // 4 bytes

            // write digest
            byte[] ar = new byte[update.digestBytes.remaining()];
            update.digestBytes.get(ar);
            out.writeShort(ar.length); // 2 bytes
            out.writeBytes(ar);  // variable bytes
            update.digestBytes.clear();

            // write partition update
            final ByteBuffer buf = PartitionUpdate.toBytes(update.update, MessagingService.current_version);
            ar = new byte[buf.remaining()];
            buf.get(ar);
            out.writeInt(ar.length); // 4 bytes
            out.writeBytes(ar); // variable bytes
        }
    }
}
