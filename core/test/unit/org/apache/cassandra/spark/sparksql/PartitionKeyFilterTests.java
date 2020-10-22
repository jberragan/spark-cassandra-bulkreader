package org.apache.cassandra.spark.sparksql;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Range;
import org.junit.Test;

import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.Murmur3Partitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

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

public class PartitionKeyFilterTests
{
    @Test
    public void testValidFilter()
    {
        final ByteBuffer key = Int32Type.instance.fromString("10");
        final BigInteger token = BigInteger.valueOf((long) Murmur3Partitioner.instance.getToken(key).getTokenValue());
        final PartitionKeyFilter filter = PartitionKeyFilter.create(key, token);

        final ByteBuffer diffKey = Int32Type.instance.fromString("11");
        final BigInteger diffToken = BigInteger.valueOf((long) Murmur3Partitioner.instance.getToken(diffKey).getTokenValue());
        final Range<BigInteger> inRange = Range.closed(token, token);
        final Range<BigInteger> notInRange = Range.closed(token.subtract(BigInteger.ONE), token.subtract(BigInteger.ONE));
        final SparkSSTableReader reader = mock(SparkSSTableReader.class);
        when(reader.range()).thenReturn(Range.closed(token, token));

        assertTrue(filter.filter(key));
        assertFalse(filter.filter(diffKey));
        assertTrue(filter.overlaps(inRange));
        assertFalse(filter.overlaps(notInRange));
        assertFalse(filter.skipPartition(key, token));
        assertTrue(filter.skipPartition(diffKey, diffToken));
        assertTrue(filter.filter(reader));

        final Function<CustomFilter, Boolean> canApply = testFilter -> testFilter instanceof PartitionKeyFilter;
        final Function<CustomFilter, Boolean> cannotApply = testFilter -> testFilter instanceof SparkRangeFilter;
        final Function<PartitionKeyFilter, Boolean> matchFunc = keyFilter -> Boolean.TRUE;
        assertTrue(filter.matchFound(canApply, matchFunc));
        assertFalse(filter.matchFound(cannotApply, matchFunc));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyKey()
    {
        final PartitionKeyFilter filter = PartitionKeyFilter.create(ByteBuffer.wrap(new byte[0]), BigInteger.ZERO);
    }

    @Test
    public void testTokenRing()
    {
        qt().forAll(TestUtils.partitioners(), arbitrary().pick(Arrays.asList(1, 3, 6, 12, 128)))
            .checkAssert((partitioner, numInstances) -> {
                final CassandraRing ring = TestUtils.createRing(partitioner, numInstances);
                final TokenPartitioner tokenPartitioner = new TokenPartitioner(ring, 24, 24);
                final List<BigInteger> boundaryTokens = IntStream.range(0, tokenPartitioner.numPartitions())
                                                                 .mapToObj(tokenPartitioner::getTokenRange)
                                                                 .map(r -> Arrays.asList(r.lowerEndpoint(), midPoint(r), r.upperEndpoint()))
                                                                 .flatMap(Collection::stream).collect(Collectors.toList());
                for (final BigInteger token : boundaryTokens) {
                    // check boundary tokens only match 1 Spark token range
                    final PartitionKeyFilter filter = PartitionKeyFilter.create(Int32Type.instance.fromString("11"), token);
                    assertEquals(1, tokenPartitioner.subRanges().stream().filter(filter::overlaps).count());
                }
            });
    }

    private static BigInteger midPoint(final Range<BigInteger> range) {
        return range.upperEndpoint().subtract(range.lowerEndpoint()).divide(BigInteger.valueOf(2L)).add(range.lowerEndpoint());
    }
}
