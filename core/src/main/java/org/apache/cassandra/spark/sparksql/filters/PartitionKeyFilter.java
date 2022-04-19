package org.apache.cassandra.spark.sparksql.filters;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;

import org.apache.spark.util.SerializableBuffer;
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

public class PartitionKeyFilter implements Serializable
{
    private final SerializableBuffer key;
    private final BigInteger token;

    private PartitionKeyFilter(@NotNull final ByteBuffer filterKey,
                               @NotNull final BigInteger filterKeyTokenValue)
    {
        this.key = new SerializableBuffer(filterKey);
        this.token = filterKeyTokenValue;
    }

    public Range<BigInteger> tokenRange()
    {
        return Range.closed(token, token);
    }

    public ByteBuffer key()
    {
        return this.key.buffer();
    }

    public BigInteger token()
    {
        return this.token;
    }

    public boolean overlaps(final Range<BigInteger> tokenRange)
    {
        return tokenRange.contains(this.token);
    }

    public boolean matches(final ByteBuffer key)
    {
        return key.compareTo(this.key.buffer()) == 0;
    }

    public boolean filter(final ByteBuffer key)
    {
        return this.key.buffer().compareTo(key) == 0;
    }

    public static PartitionKeyFilter create(@NotNull final ByteBuffer filterKey, @NotNull final BigInteger filterKeyTokenValue)
    {
        Preconditions.checkArgument(filterKey.capacity() != 0);
        return new PartitionKeyFilter(filterKey, filterKeyTokenValue);
    }

    public static Range<BigInteger> mergeRanges(Range<BigInteger> r1, Range<BigInteger> r2)
    {
        return Range.closed(r1.lowerEndpoint().min(r2.lowerEndpoint()),
                            r1.upperEndpoint().max(r2.upperEndpoint()));
    }
}
