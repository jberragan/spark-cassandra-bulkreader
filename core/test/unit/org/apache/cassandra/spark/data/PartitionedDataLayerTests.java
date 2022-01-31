package org.apache.cassandra.spark.data;

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
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.cassandra.spark.data.partitioner.JDKSerializationTests;
import org.apache.cassandra.spark.data.partitioner.MultipleReplicasTests;
import org.apache.cassandra.spark.data.partitioner.NotEnoughReplicasException;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.EmptyScanner;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.sparksql.filters.CustomFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.spark.TaskContext;

import static org.apache.cassandra.spark.data.PartitionedDataLayer.AvailabilityHint.DOWN;
import static org.apache.cassandra.spark.data.PartitionedDataLayer.AvailabilityHint.UNKNOWN;
import static org.apache.cassandra.spark.data.PartitionedDataLayer.AvailabilityHint.UP;
import static org.apache.cassandra.spark.data.partitioner.ConsistencyLevel.ALL;
import static org.apache.cassandra.spark.data.partitioner.ConsistencyLevel.ANY;
import static org.apache.cassandra.spark.data.partitioner.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.spark.data.partitioner.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.spark.data.partitioner.ConsistencyLevel.ONE;
import static org.apache.cassandra.spark.data.partitioner.ConsistencyLevel.TWO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.Generate.pick;

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
        PartitionedDataLayer.validateReplicationFactor(ANY, TestUtils.simpleStrategy(), null);
        PartitionedDataLayer.validateReplicationFactor(ANY, TestUtils.networkTopologyStrategy(), null);
        PartitionedDataLayer.validateReplicationFactor(ANY, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3)), null);
        PartitionedDataLayer.validateReplicationFactor(ANY, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3)), "PV");
        PartitionedDataLayer.validateReplicationFactor(LOCAL_QUORUM, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3)), "PV");
        PartitionedDataLayer.validateReplicationFactor(ALL, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)), null);
        PartitionedDataLayer.validateReplicationFactor(EACH_QUORUM, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)), null);
        PartitionedDataLayer.validateReplicationFactor(ANY, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplicationFactorDCRequired()
    {
        // dc required for dc local consistency level
        PartitionedDataLayer.validateReplicationFactor(LOCAL_QUORUM, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplicationFactorUnknownDC()
    {
        PartitionedDataLayer.validateReplicationFactor(LOCAL_QUORUM, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)), "ST");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReplicationFactorRF0()
    {
        PartitionedDataLayer.validateReplicationFactor(LOCAL_QUORUM, TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 0)), "MR");
    }

    @Test
    public void testSSTableSupplier()
    {
        final CassandraRing ring = TestUtils.createRing(Partitioner.Murmur3Partitioner, 3);
        final CqlSchema schema = TestSchema.basic(bridge).buildSchema();
        final JDKSerializationTests.TestPartitionedDataLayer dataLayer = new JDKSerializationTests.TestPartitionedDataLayer(4, 32, null, ring, schema);
        final SSTablesSupplier supplier = dataLayer.sstables(new ArrayList<>());
        final Set<MultipleReplicasTests.TestSSTableReader> ssTableReaders = supplier.openAll((ssTable, isRepairPrimary) -> new MultipleReplicasTests.TestSSTableReader(ssTable));
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
        final Set<MultipleReplicasTests.TestSSTableReader> ssTableReaders = supplier.openAll((ssTable, isRepairPrimary) -> new MultipleReplicasTests.TestSSTableReader(ssTable));
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

    @SuppressWarnings("UnstableApiUsage")
    private static void runSplitTests(final int minReplicas, final PartitionedDataLayer.AvailabilityHint... availabilityHint)
    {
        final int numInstances = availabilityHint.length;
        TestUtils.runTest((partitioner, dir, bridge) -> {
            final CassandraRing ring = TestUtils.createRing(partitioner, numInstances);
            final List<CassandraInstance> instances = new ArrayList<>(ring.instances());
            instances.sort(Comparator.comparing(CassandraInstance::nodeName));
            final TokenPartitioner tokenPartitioner = new TokenPartitioner(ring, 1, 32);
            final Map<CassandraInstance, PartitionedDataLayer.AvailabilityHint> availableMap = new HashMap<>(numInstances);
            for (int i = 0; i < numInstances; i++)
            {
                availableMap.put(instances.get(i), availabilityHint[i]);
            }

            final Map<Range<BigInteger>, List<CassandraInstance>> ranges = ring.getSubRanges(tokenPartitioner.getTokenRange(0)).asMapOfRanges();
            final PartitionedDataLayer.ReplicaSet replicaSet = PartitionedDataLayer.splitReplicas(instances, ranges, availableMap::get, minReplicas, 0);
            assertEquals(minReplicas, replicaSet.primary().size());
            assertEquals(numInstances - minReplicas, replicaSet.backup().size());

            final List<CassandraInstance> sortedInstances = new ArrayList<>(instances);
            sortedInstances.sort(Comparator.comparing(availableMap::get));
            for (int i = 0; i < sortedInstances.size(); i++)
            {
                if (i < minReplicas)
                {
                    assertTrue(replicaSet.primary().contains(sortedInstances.get(i)));
                }
                else
                {
                    assertTrue(replicaSet.backup().contains(sortedInstances.get(i)));
                }
            }
        });
    }

    @Test
    public void testSplitReplicas()
    {
        final ReplicationFactor rf = TestUtils.networkTopologyStrategy();
        TestUtils.runTest((partitioner, dir, bridge) ->
                          qt().forAll(pick(Arrays.asList(3, 32, 1024)),
                                      pick(Arrays.asList(LOCAL_QUORUM, ONE, ALL, TWO)),
                                      pick(Arrays.asList(1, 32, 1024)),
                                      pick(Arrays.asList(1, 32, 1024)))
                              .checkAssert((numInstances, consistencyLevel, numCores, defaultParallelism) ->
                                           PartitionedDataLayerTests.testSplitReplicas(TestUtils.createRing(partitioner, numInstances), consistencyLevel, defaultParallelism, numCores, rf, "DC1")));
    }

    @SuppressWarnings("UnstableApiUsage")
    private static void testSplitReplicas(final CassandraRing ring,
                                          final ConsistencyLevel consistencyLevel,
                                          final int defaultParallelism,
                                          final int numCores,
                                          final ReplicationFactor rf,
                                          final String dc)
    {
        final TokenPartitioner tokenPartitioner = new TokenPartitioner(ring, defaultParallelism, numCores);

        for (int partition = 0; partition < tokenPartitioner.numPartitions(); partition++)
        {
            final Range<BigInteger> range = tokenPartitioner.getTokenRange(partition);
            final Map<Range<BigInteger>, List<CassandraInstance>> subRanges = ring.getSubRanges(range).asMapOfRanges();
            final Set<CassandraInstance> replicas = PartitionedDataLayer.rangesToReplicas(consistencyLevel, dc, subRanges);
            final Function<CassandraInstance, PartitionedDataLayer.AvailabilityHint> availability = (instances) -> UP;
            final int minReplicas = consistencyLevel.blockFor(rf, dc);
            final PartitionedDataLayer.ReplicaSet replicaSet = PartitionedDataLayer.splitReplicas(consistencyLevel, dc, subRanges, replicas, availability, minReplicas, 0);
            assertNotNull(replicaSet);
            assertTrue(Collections.disjoint(replicaSet.primary(), replicaSet.backup()));
            assertEquals(replicas.size(), replicaSet.primary().size() + replicaSet.backup().size());
        }
    }
}
