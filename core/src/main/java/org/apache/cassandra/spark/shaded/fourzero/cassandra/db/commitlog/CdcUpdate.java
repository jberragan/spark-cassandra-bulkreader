package org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.data.LocalDataLayer;
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
import org.apache.cassandra.spark.utils.FutureUtils;
import org.jetbrains.annotations.NotNull;
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

public class CdcUpdate implements Scannable, Comparable<CdcUpdate>
{
    private final TableMetadata tableMetadata;
    @Nullable
    private final PartitionUpdate update;
    private final CompletableFuture<byte[]> digest;
    private final long maxTimestampMicros;
    private final CompletableFuture<Integer> dateSize;

    public CdcUpdate(@NotNull TableMetadata tableMetadata,
                     @NotNull PartitionUpdate update,
                     long maxTimestampMicros,
                     @Nullable ExecutorService executor)
    {
        this.tableMetadata = tableMetadata;
        this.update = update;
        this.maxTimestampMicros = maxTimestampMicros;
        if (executor != null)
        {
            // use provided executor service to avoid CPU usage on BufferingCommitLogReader thread
            this.digest = CompletableFuture.supplyAsync(() -> CdcUpdate.digest(update), executor);
        }
        else
        {
            this.digest = CompletableFuture.completedFuture(CdcUpdate.digest(update));
        }
        this.dateSize = this.digest.thenApply(ar -> 18 /* = 8 + 4 + 2 + 4 */ + ar.length + update.dataSize());
    }

    // for deserialization
    private CdcUpdate(@NotNull TableMetadata tableMetadata,
                      @Nullable PartitionUpdate update,
                      long maxTimestampMicros,
                      @NotNull byte[] digest,
                      int dateSize)
    {
        this.tableMetadata = tableMetadata;
        this.update = update;
        this.maxTimestampMicros = maxTimestampMicros;
        this.digest = CompletableFuture.completedFuture(digest);
        this.dateSize = CompletableFuture.completedFuture(dateSize);
    }

    public static byte[] digest(PartitionUpdate update)
    {
        final Digest digest = Digest.forReadResponse();
        UnfilteredRowIterators.digest(update.unfilteredIterator(), digest, MessagingService.current_version);
        return digest.digest();
    }

    @Nullable
    public DecoratedKey partitionKey()
    {
        return update == null ? null : update.partitionKey();
    }

    public long maxTimestampMicros()
    {
        return maxTimestampMicros;
    }

    @NotNull
    public PartitionUpdate partitionUpdate()
    {
        if (update == null)
        {
            throw new NullPointerException("PartitionUpdate is null, cannot read partition update from deserialized CdcUpdate");
        }
        return update;
    }

    public byte[] digest()
    {
        return FutureUtils.get(digest);
    }

    public int dataSize()
    {
        return FutureUtils.get(dateSize);
    }

    @Override
    public ISSTableScanner scanner()
    {
        if (update == null)
        {
            throw new NullPointerException("PartitionUpdate is null, cannot create scanner from deserialized CdcUpdate");
        }
        return new CdcScannerBuilder.CDCScanner(tableMetadata, update);
    }

    // todo: add proper equals and hashCode impl for PartitionUpdate in OSS.
    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(107, 109)
               .append(digest())
               .append(dataSize())
               .toHashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof CdcUpdate))
        {
            return false;
        }

        final CdcUpdate rhs = (CdcUpdate) obj;
        return new EqualsBuilder()
               .append(digest(), rhs.digest())
               .append(dataSize(), rhs.dataSize())
               .isEquals();
    }

    public int compareTo(@NotNull CdcUpdate o)
    {
        return Long.compare(this.maxTimestampMicros, o.maxTimestampMicros);
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CdcUpdate>
    {
        final TableMetadata metadata;
        final boolean includePartitionUpdate;

        public Serializer(String keyspace, String table)
        {
            this(keyspace, table, false);
        }

        public Serializer(String keyspace, String table, boolean includePartitionUpdate)
        {
            this.metadata = Schema.instance.getTableMetadata(keyspace, table);
            this.includePartitionUpdate = includePartitionUpdate;
        }

        public Serializer(TableMetadata metadata)
        {
            this(metadata, false);
        }

        public Serializer(TableMetadata metadata, boolean includePartitionUpdate)
        {
            this.metadata = metadata;
            this.includePartitionUpdate = includePartitionUpdate;
        }

        @Override
        public CdcUpdate read(final Kryo kryo, final Input in, final Class type)
        {
            final long maxTimestampMicros = in.readLong();
            final int size = in.readInt();

            // read digest
            final byte[] digest = in.readBytes(in.readShort());

            PartitionUpdate partitionUpdate = null;
            if (includePartitionUpdate)
            {
                // read partition update
                partitionUpdate = PartitionUpdate.fromBytes(ByteBuffer.wrap(in.readBytes(in.readInt())), MessagingService.current_version);
            }

            return new CdcUpdate(metadata, partitionUpdate, maxTimestampMicros, digest, size);
        }

        @Override
        public void write(final Kryo kryo, final Output out, final CdcUpdate update)
        {
            out.writeLong(update.maxTimestampMicros); // 8 bytes
            out.writeInt(update.dataSize()); // 4 bytes

            // write digest
            final byte[] digest = update.digest();
            out.writeShort(digest.length);
            out.writeBytes(digest);

            // write partition update
            if (includePartitionUpdate)
            {
                final ByteBuffer buf = PartitionUpdate.toBytes(update.update, MessagingService.current_version);
                byte[] ar = new byte[buf.remaining()];
                buf.get(ar);
                out.writeInt(ar.length); // 4 bytes
                out.writeBytes(ar); // variable bytes
            }
        }
    }
}
