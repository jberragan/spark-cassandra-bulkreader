package org.apache.cassandra.spark.data.partitioner;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Range;
import org.junit.Test;

import org.apache.cassandra.spark.TestUtils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
public class TokenPartitionerTests
{
    private static final int NUM_TOKEN_TESTS = 100;

    @Test
    public void testTokenPartitioner()
    {
        qt().forAll(TestUtils.partitioners(), arbitrary().pick(Arrays.asList(1, 3, 6, 12, 104, 208, 416)), arbitrary().pick(Arrays.asList(1, 2, 4, 16, 128, 1024)))
            .checkAssert(this::runTest);
    }

    private void runTest(final Partitioner partitioner, final int numInstances, final int numCores)
    {
        final TokenPartitioner tokenPartitioner = new TokenPartitioner(TestUtils.createRing(partitioner, numInstances), 1, numCores);
        assertTrue(tokenPartitioner.numPartitions() > 1);

        // generate some random tokens and verify they only exist in a single token partition
        final Map<BigInteger, Integer> tokens = IntStream.range(0, NUM_TOKEN_TESTS).mapToObj(i -> TestUtils.randomBigInteger(partitioner)).collect(Collectors.toMap(Function.identity(), i -> 0));

        for (int i = 0; i < tokenPartitioner.numPartitions(); i++)
        {
            final Range<BigInteger> range = tokenPartitioner.getTokenRange(i);
            for (final BigInteger token : tokens.keySet())
            {
                if (range.contains(token))
                {
                    tokens.put(token, tokens.get(token) + 1);
                    assertTrue(tokenPartitioner.isInPartition(token, ByteBuffer.wrap("not important".getBytes()), i));
                }
            }
        }

        for (final Map.Entry<BigInteger, Integer> entry : tokens.entrySet())
        {
            assertFalse("Token not found in any token partitions: " + entry.getKey(), entry.getValue() < 1);
            assertFalse("Token exists in more than one token partition: " + entry.getKey(), entry.getValue() > 1);
        }
    }
}
