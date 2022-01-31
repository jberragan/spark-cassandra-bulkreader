package org.apache.cassandra.spark.sparksql.filters;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.function.Function;

import com.google.common.collect.Range;

import org.apache.cassandra.spark.reader.SparkSSTableReader;

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

public interface CustomFilter
{
    boolean overlaps(final Range<BigInteger> tokenRange);

    default <T extends CustomFilter> boolean matchFound(final Function<CustomFilter, Boolean> applyMatch, final Function<T, Boolean> match)
    {
        if (!applyMatch.apply(this))
        {
            return false;
        }
        return match.apply((T) this);
    }

    Range<BigInteger> tokenRange();

    boolean skipPartition(final ByteBuffer key, final BigInteger token);

    boolean canFilterByKey();

    boolean filter(final ByteBuffer key);

    boolean filter(SparkSSTableReader reader);

    /**
     * If true, filter requests reading a specific token range e.g. PartitionKeyFilter.
     * If false, filter is a general token range that can be overridden by specific range filters e.g. a SparkRangeFilter.
     *
     * @return true if a specific range.
     */
    boolean isSpecificRange();

    static Range<BigInteger> mergeRanges(Range<BigInteger> r1, Range<BigInteger> r2)
    {
        return Range.closed(r1.lowerEndpoint().min(r2.lowerEndpoint()),
                            r1.upperEndpoint().max(r2.upperEndpoint()));
    }
}
