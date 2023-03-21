package org.apache.cassandra.spark.sparksql;

import java.math.BigInteger;

import com.google.common.collect.Range;
import org.junit.Test;

import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

public class RangeFilterTests
{
    @Test
    public void testValidFilter()
    {
        final Range<BigInteger> connected = Range.closed(BigInteger.ONE, BigInteger.TWO);
        final Range<BigInteger> notConnected = Range.greaterThan(BigInteger.TEN);

        final RangeFilter filter = RangeFilter.create(Range.closed(BigInteger.ZERO, BigInteger.ONE));
        final SparkSSTableReader reader = mock(SparkSSTableReader.class);
        when(reader.range()).thenReturn(connected);

        assertTrue(filter.overlaps(connected));
        assertFalse(filter.overlaps(notConnected));
        assertTrue(filter.skipPartition(BigInteger.TEN));
        assertFalse(filter.skipPartition(BigInteger.ONE));
        assertTrue(SparkSSTableReader.overlaps(reader, filter.tokenRange()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidRange()
    {
        final RangeFilter filter = RangeFilter.create(Range.atLeast(BigInteger.TEN));
    }
}
