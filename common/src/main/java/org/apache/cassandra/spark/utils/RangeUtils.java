package org.apache.cassandra.spark.utils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BoundType;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;

import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;


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
 * Common Cassandra range operations on Guava ranges. Assumes ranges are not wrapped around.
 * It's the responsibility of caller to unwrap ranges. For example, [100, 1] should become
 * [100, MAX] and [MIN, 1]. MIN and MAX depend on {@link Partitioner}.
 */
public final class RangeUtils
{
    private RangeUtils()
    {
    }

    public static boolean isOppositeSign(final long l1, final long l2)
    {
        return ((l1 ^ l2) < 0);
    }

    private static BigInteger sizeOf(final Range<BigInteger> range)
    {
        Preconditions.checkArgument(range.lowerEndpoint().compareTo(range.upperEndpoint()) <= 0,
                                    "RangeUtils assume ranges are not wrap-around");

        if (range.isEmpty())
        {
            return BigInteger.ZERO;
        }

        BigInteger size = range.upperEndpoint().subtract(range.lowerEndpoint()).add(BigInteger.ONE);

        if (range.lowerBoundType() == BoundType.OPEN)
        {
            size = size.subtract(BigInteger.ONE);
        }

        if (range.upperBoundType() == BoundType.OPEN)
        {
            size = size.subtract(BigInteger.ONE);
        }

        return size;
    }

    /**
     * Splits the given range into equal sized small ranges. Number of splits can be controlled by
     * nrSplits. If nrSplits are smaller than size of the range, split size would be set to 1.
     * <p>
     * This is best effort scheme, nrSplits not necessarily as promised and not all splits may not be
     * exact same size.
     */
    public static List<Range<BigInteger>> split(final Range<BigInteger> range, final int nrSplits)
    {
        Preconditions.checkArgument(range.lowerEndpoint().compareTo(range.upperEndpoint()) <= 0,
                                    "RangeUtils assume ranges are not wrap-around");

        if (range.isEmpty())
        {
            return Collections.emptyList();
        }

        // Make sure split size is not 0.
        final BigInteger splitSize = sizeOf(range).divide(BigInteger.valueOf(nrSplits)).max(BigInteger.ONE);

        // Start from range lower endpoint and spit ranges of size splitSize, until we cross the range.
        BigInteger nextLowerEndpoint = range.lowerBoundType() == BoundType.CLOSED ? range.lowerEndpoint() : range.lowerEndpoint().add(BigInteger.ONE);
        final List<Range<BigInteger>> splits = new ArrayList<>();
        while (range.contains(nextLowerEndpoint))
        {
            final BigInteger upperEndpoint = nextLowerEndpoint.add(splitSize);
            splits.add(range.intersection(Range.closedOpen(nextLowerEndpoint, upperEndpoint)));
            nextLowerEndpoint = upperEndpoint;
        }

        return splits;
    }

    public static Multimap<CassandraInstance, Range<BigInteger>> calculateTokenRanges(final List<CassandraInstance> instances, final int rf, final Partitioner partitioner)
    {
        Preconditions.checkArgument(rf != 0, "RF cannot be 0");
        Preconditions.checkArgument((instances.size() == 0) || (rf <= instances.size()), String.format("RF (%d) cannot be greater than the number of Cassandra instances (%d)", rf, instances.size()));
        final Multimap<CassandraInstance, Range<BigInteger>> tokenRanges = ArrayListMultimap.create();
        for (int i = 0; i < instances.size(); i++)
        {
            final CassandraInstance instance = instances.get(i);
            final int disjointReplica = ((instances.size() + i) - rf) % instances.size();
            final BigInteger rangeStart = new BigInteger(instances.get(disjointReplica).token());
            final BigInteger rangeEnd = new BigInteger(instance.token());

            // If start token is not strictly smaller than end token we are looking at a wrap around range, split it.
            if (rangeStart.compareTo(rangeEnd) >= 0)
            {
                tokenRanges.put(instance, Range.range(rangeStart, BoundType.OPEN, partitioner.maxToken(), BoundType.CLOSED));
                tokenRanges.put(instance, Range.range(partitioner.minToken(), BoundType.CLOSED, rangeEnd, BoundType.CLOSED));
            }
            else
            {
                tokenRanges.put(instance, Range.openClosed(rangeStart, rangeEnd));
            }
        }

        return tokenRanges;
    }
}
