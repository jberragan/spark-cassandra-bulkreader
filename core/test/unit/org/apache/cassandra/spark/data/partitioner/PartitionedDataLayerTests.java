package org.apache.cassandra.spark.data.partitioner;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.PartitionedDataLayer;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.EmptyScanner;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.sparksql.CustomFilter;
import org.apache.cassandra.spark.sparksql.PartitionKeyFilter;
import org.apache.spark.TaskContext;


import static org.apache.cassandra.spark.data.PartitionedDataLayer.AvailabilityHint.DOWN;
import static org.apache.cassandra.spark.data.PartitionedDataLayer.AvailabilityHint.UNKNOWN;
import static org.apache.cassandra.spark.data.PartitionedDataLayer.AvailabilityHint.UP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
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
public class PartitionedDataLayerTests extends VersionRunner
{

    public PartitionedDataLayerTests(CassandraBridge.CassandraVersion version)
    {
        super(version);
    }

    @Test
    public void testSplitQuorumAllUp()
    {
        runSplitTests(1, UP);
        runSplitTests(2, UP, UP);
        runSplitTests(2, UP, UP, UP);
        runSplitTests(3, UP, UP, UP, UP, UP);
    }

    @Test
    public void testSplitQuorumOneDown()
    {
        runSplitTests(1, DOWN);
        runSplitTests(2, DOWN, UP);
        runSplitTests(2, DOWN, UP, UP);
        runSplitTests(3, UP, DOWN, UP, UP, UP);
    }

    @Test
    public void testSplitQuorumTwoDown()
    {
        runSplitTests(2, DOWN, DOWN);
        runSplitTests(2, DOWN, UP, DOWN);
        runSplitTests(3, UP, DOWN, UP, UP, DOWN);
    }

    @Test
    public void testSplitAll()
    {
        runSplitTests(1, DOWN);
        runSplitTests(1, UNKNOWN);
        runSplitTests(3, UP, UP, DOWN);
        runSplitTests(5, UP, UP, DOWN, UNKNOWN, UP);
    }

    @Test
    public void testValidReplicationFactor()
    {
        PartitionedDataLayer.validateReplicationFactor(ConsistencyLevel.ANY, TestUtils.simpleStrategy(), null);
        PartitionedDataLayer.validateReplicationFactor(ConsistencyLevel.ANY, TestUtils.networkTopologyStrategy(), null);
        PartitionedDataLayer.validateReplicationFactor(ConsistencyLevel.ANY, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3)), null);
        PartitionedDataLayer.validateReplicationFactor(ConsistencyLevel.ANY, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3)), "PV");
        PartitionedDataLayer.validateReplicationFactor(ConsistencyLevel.LOCAL_QUORUM, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3)), "PV");
        PartitionedDataLayer.validateReplicationFactor(ConsistencyLevel.ALL, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)), null);
        PartitionedDataLayer.validateReplicationFactor(ConsistencyLevel.EACH_QUORUM, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)), null);
        PartitionedDataLayer.validateReplicationFactor(ConsistencyLevel.ANY, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplicationFactorDCRequired()
    {
        // dc required for dc local consistency level
        PartitionedDataLayer.validateReplicationFactor(ConsistencyLevel.LOCAL_QUORUM, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplicationFactorUnknownDC()
    {
        PartitionedDataLayer.validateReplicationFactor(ConsistencyLevel.LOCAL_QUORUM, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)), "ST");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplicationFactorRF0()
    {
        PartitionedDataLayer.validateReplicationFactor(ConsistencyLevel.LOCAL_QUORUM, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 0)), "MR");
    }

    @Test
    public void testSSTableSupplier()
    {
        final CassandraRing ring = TestUtils.createRing(Partitioner.Murmur3Partitioner, 3);
        final CqlSchema schema = TestSchema.basic(bridge).buildSchema();
        final JDKSerializationTests.TestPartitionedDataLayer dataLayer = new JDKSerializationTests.TestPartitionedDataLayer(4, 32, null, ring, schema);
        final SSTablesSupplier supplier = dataLayer.sstables(new ArrayList<>());
        final Set<MultipleReplicasTests.TestSSTableReader> ssTableReaders = supplier.openAll(MultipleReplicasTests.TestSSTableReader::new);
        assertNotNull(ssTableReaders);
    }

    @Test
    public void testSSTableSupplierWithMatchingFilters()
    {
        final CassandraRing ring = TestUtils.createRing(Partitioner.Murmur3Partitioner, 3);
        final CqlSchema schema = TestSchema.basic(bridge).buildSchema();
        final JDKSerializationTests.TestPartitionedDataLayer dataLayer = new JDKSerializationTests.TestPartitionedDataLayer(4, 32, null, ring, schema);

        final PartitionKeyFilter filter = PartitionKeyFilter.create(ByteBuffer.wrap(RandomUtils.nextBytes(10)), BigInteger.valueOf(-9223372036854775808L));
        final SSTablesSupplier supplier = dataLayer.sstables(Collections.singletonList(filter));
        final Set<MultipleReplicasTests.TestSSTableReader> ssTableReaders = supplier.openAll(MultipleReplicasTests.TestSSTableReader::new);
        assertNotNull(ssTableReaders);
    }

    @Test(expected = NotEnoughReplicasException.class)
    public void testSSTableSupplierWithNonMatchingFilters()
    {
        final CassandraRing ring = TestUtils.createRing(Partitioner.Murmur3Partitioner, 3);
        final CqlSchema schema = TestSchema.basic(bridge).buildSchema();
        final JDKSerializationTests.TestPartitionedDataLayer dataLayer = new JDKSerializationTests.TestPartitionedDataLayer(4, 32, null, ring, schema);

        final PartitionKeyFilter filter = PartitionKeyFilter.create(ByteBuffer.wrap(RandomUtils.nextBytes(10)), BigInteger.valueOf(6917529027641081853L));
        final SSTablesSupplier supplier = dataLayer.sstables(Collections.singletonList(filter));
    }

    @Test
    public void testFiltersInRange() throws Exception
    {
        final Map<Integer, Range<BigInteger>> reversePartitionMap = Collections.singletonMap(TaskContext.getPartitionId(), Range.closed(BigInteger.ONE, BigInteger.valueOf(2L)));
        final TokenPartitioner mockPartitioner = mock(TokenPartitioner.class);
        when(mockPartitioner.reversePartitionMap()).thenReturn(reversePartitionMap);

        final PartitionedDataLayer dataLayer = mock(PartitionedDataLayer.class, CALLS_REAL_METHODS);
        when(dataLayer.tokenPartitioner()).thenReturn(mockPartitioner);

        final PartitionKeyFilter filterInRange = PartitionKeyFilter.create(ByteBuffer.wrap(new byte[10]), BigInteger.valueOf(2L));
        final PartitionKeyFilter filterOutsideRange = PartitionKeyFilter.create(ByteBuffer.wrap(new byte[10]), BigInteger.TEN);
        final CustomFilter randomFilter = mock(CustomFilter.class);
        when(randomFilter.overlaps(any())).thenReturn(true);

        assertFalse(dataLayer.filtersInRange(Collections.singletonList(randomFilter)).isEmpty());
        assertEquals(3, dataLayer.filtersInRange(Arrays.asList(filterInRange, randomFilter)).size());
        assertEquals(3, dataLayer.filtersInRange(Arrays.asList(filterInRange, filterOutsideRange, randomFilter)).size());

        // filter does not fall in spark token range
        final IStreamScanner scanner = dataLayer.openCompactionScanner(Collections.singletonList(filterOutsideRange));
        assertTrue(scanner instanceof EmptyScanner);
    }

    private static void runSplitTests(final int minReplicas, final PartitionedDataLayer.AvailabilityHint... availabilityHint)
    {
        final int numInstances = availabilityHint.length;
        TestUtils.runTest((partitioner, dir, bridge) -> {
            final List<CassandraInstance> instances = new ArrayList<>(TestUtils.createRing(partitioner, numInstances).instances());
            instances.sort(Comparator.comparing(CassandraInstance::nodeName));
            final Map<CassandraInstance, PartitionedDataLayer.AvailabilityHint> availableMap = new HashMap<>(numInstances);
            for (int i = 0; i < numInstances; i++)
            {
                availableMap.put(instances.get(i), availabilityHint[i]);
            }

            final Pair<Set<CassandraInstance>, Set<CassandraInstance>> split = PartitionedDataLayer.splitReplicas(instances, availableMap::get, minReplicas);
            assertEquals(minReplicas, split.getLeft().size());
            assertEquals(numInstances - minReplicas, split.getRight().size());

            final List<CassandraInstance> sortedInstances = new ArrayList<>(instances);
            sortedInstances.sort(Comparator.comparing(availableMap::get));
            for (int i = 0; i < sortedInstances.size(); i++)
            {
                if (i < minReplicas)
                {
                    assertTrue(split.getLeft().contains(sortedInstances.get(i)));
                }
                else
                {
                    assertTrue(split.getRight().contains(sortedInstances.get(i)));
                }
            }
        });
    }
}
