package org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.reader.fourzero.BaseFourZeroUtils;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DecoratedKey;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Digest;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.net.MessagingService;
import org.apache.cassandra.spark.utils.AsyncExecutor;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.apache.cassandra.spark.utils.KryoUtils;
import org.apache.cassandra.spark.utils.RangeUtils;
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
public class PartitionUpdateWrapper implements Comparable<PartitionUpdateWrapper>
{
    public final String keyspace;
    public final String table;
    @Nullable
    private final PartitionUpdate update;
    private final CompletableFuture<byte[]> digest;
    private final long maxTimestampMicros;
    private final CompletableFuture<Integer> dataSize;
    private final BigInteger token;

    public PartitionUpdateWrapper(@NotNull PartitionUpdate update,
                                  long maxTimestampMicros,
                                  @Nullable AsyncExecutor executor)
    {
        this.update = update;
        this.keyspace = update.metadata().keyspace;
        this.table = update.metadata().name;
        this.maxTimestampMicros = maxTimestampMicros;
        this.token = BaseFourZeroUtils.tokenToBigInteger(update.partitionKey().getToken());
        final int tokenByteLen = RangeUtils.bigIntegerByteArraySize(this.token);
        if (executor != null)
        {
            // use provided executor service to avoid CPU usage on BufferingCommitLogReader thread
            this.digest = executor.submit(() -> PartitionUpdateWrapper.digest(update));
        }
        else
        {
            this.digest = CompletableFuture.completedFuture(PartitionUpdateWrapper.digest(update));
        }
        this.dataSize = this.digest.thenApply(ar -> 18 /* = 8 + 4 + 2 + 4 */ + tokenByteLen + ar.length + update.dataSize());
    }

    // for deserialization
    public PartitionUpdateWrapper(@Nullable PartitionUpdate update,
                                  @NotNull String keyspace,
                                  @NotNull String table,
                                  long maxTimestampMicros,
                                  @NotNull byte[] digest,
                                  int dataSize,
                                  BigInteger token)
    {
        this.update = update;
        this.keyspace = keyspace;
        this.table = table;
        this.maxTimestampMicros = maxTimestampMicros;
        this.token = token;
        this.digest = CompletableFuture.completedFuture(digest);
        this.dataSize = CompletableFuture.completedFuture(dataSize);
    }

    public String keyspace()
    {
        return keyspace;
    }

    public String table()
    {
        return table;
    }

    public BigInteger token()
    {
        return token;
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
            throw new UnsupportedOperationException("PartitionUpdate is null, cannot read partition update from deserialized CdcUpdate");
        }
        return update;
    }

    public byte[] digest()
    {
        return FutureUtils.get(digest);
    }

    public int dataSize()
    {
        return FutureUtils.get(dataSize);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(107, 109)
               .append(keyspace)
               .append(table)
               .append(digest())
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
        if (!(obj instanceof PartitionUpdateWrapper))
        {
            return false;
        }

        final PartitionUpdateWrapper rhs = (PartitionUpdateWrapper) obj;
        return new EqualsBuilder()
               .append(keyspace, rhs.keyspace)
               .append(table, rhs.table)
               .append(digest(), rhs.digest())
               .isEquals();
    }

    public int compareTo(@NotNull PartitionUpdateWrapper o)
    {
        return Long.compare(this.maxTimestampMicros, o.maxTimestampMicros);
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<PartitionUpdateWrapper>
    {
        final boolean includePartitionUpdate;

        public Serializer()
        {
            this(false);
        }

        public Serializer(boolean includePartitionUpdate)
        {
            this.includePartitionUpdate = includePartitionUpdate;
        }

        @Override
        public PartitionUpdateWrapper read(final Kryo kryo, final Input in, final Class type)
        {
            final long maxTimestampMicros = in.readLong();
            final int size = in.readInt();

            final BigInteger token = KryoUtils.readBigInteger(in);

            // read digest
            final byte[] digest = in.readBytes(in.readShort());

            PartitionUpdate partitionUpdate = null;
            if (includePartitionUpdate)
            {
                // read partition update
                partitionUpdate = PartitionUpdate.fromBytes(ByteBuffer.wrap(in.readBytes(in.readInt())), MessagingService.current_version);
            }

            String keyspace = in.readString();
            String table = in.readString();

            return new PartitionUpdateWrapper(partitionUpdate, keyspace, table, maxTimestampMicros, digest, size, token);
        }

        @Override
        public void write(final Kryo kryo, final Output out, final PartitionUpdateWrapper update)
        {
            out.writeLong(update.maxTimestampMicros); // 8 bytes
            out.writeInt(update.dataSize()); // 4 bytes

            KryoUtils.writeBigInteger(out, update.token); // max 16 bytes

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

            out.writeString(update.keyspace);
            out.writeString(update.table);
        }
    }
}
