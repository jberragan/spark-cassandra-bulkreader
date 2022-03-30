package org.apache.cassandra.spark.sparksql.filters;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;

import org.apache.cassandra.spark.reader.SparkSSTableReader;
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

/**
 * Cdc Offset filter used to checkpoint Streaming queries and only read mutations within watermark window.
 */
public class CdcOffsetFilter implements CustomFilter
{
    private final CdcOffset start;
    private final long maxAgeMicros;

    private CdcOffsetFilter(@NotNull final CdcOffset start,
                            @NotNull final Duration watermarkWindow)
    {
        Preconditions.checkNotNull(start, "Start offset cannot be null");
        this.start = start;
        this.maxAgeMicros = start.getTimestampMicros() - (watermarkWindow.toNanos() / 1000);
    }

    /**
     * Return true if mutation timestamp overlaps with watermark window.
     *
     * @param timestampMicros mutation timestamp in micros
     * @return true if timestamp overlaps with range
     */
    public boolean overlaps(final long timestampMicros)
    {
        return timestampMicros >= maxAgeMicros;
    }

    public static CdcOffsetFilter of(@NotNull final CdcOffset start,
                                     @NotNull final Duration watermarkWindow)
    {
        return new CdcOffsetFilter(start, watermarkWindow);
    }

    public long maxAgeMicros()
    {
        return maxAgeMicros;
    }

    public CdcOffset start()
    {
        return start;
    }

    public boolean overlaps(Range<BigInteger> tokenRange)
    {
        return false;
    }

    public Range<BigInteger> tokenRange()
    {
        return null;
    }

    public boolean skipPartition(ByteBuffer key, BigInteger token)
    {
        return false;
    }

    public boolean canFilterByKey()
    {
        return false;
    }

    public boolean filter(ByteBuffer key)
    {
        return false;
    }

    public boolean filter(SparkSSTableReader reader)
    {
        return false;
    }

    public boolean isSpecificRange()
    {
        return false;
    }
}
