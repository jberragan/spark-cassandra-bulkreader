package org.apache.cassandra.spark.data.partitioner;

import java.util.Arrays;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.spark.TestUtils;

import static org.apache.cassandra.spark.TestUtils.deserialize;
import static org.apache.cassandra.spark.TestUtils.serialize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public class CommonJDKSerializationTests
{
    @Test
    public void testCassandraRing()
    {
        qt().forAll(TestUtils.partitioners(), arbitrary().pick(Arrays.asList(1, 3, 6, 12, 128)))
            .checkAssert(((partitioner, numInstances) -> {
                final CassandraRing ring;
                if (numInstances > 4)
                {
                    ring = TestUtils.createRing(partitioner, ImmutableMap.of("DC1", numInstances / 2, "DC2", numInstances / 2));
                }
                else
                {
                    ring = TestUtils.createRing(partitioner, numInstances);
                }
                final byte[] ar = serialize(ring);
                final CassandraRing deserialized = deserialize(ar, CassandraRing.class);
                assertNotNull(deserialized);
                assertNotNull(deserialized.rangeMap());
                assertNotNull(deserialized.tokenRanges());
                assertEquals(ring, deserialized);
            }));
    }

    @Test
    public void testTokenPartitioner()
    {
        qt().forAll(TestUtils.partitioners(), arbitrary().pick(Arrays.asList(1, 3, 6, 12, 128)), arbitrary().pick(Arrays.asList(1, 4, 8, 16, 32, 1024)))
            .checkAssert(((partitioner, numInstances, numCores) -> {
                final CassandraRing ring = TestUtils.createRing(partitioner, numInstances);
                final TokenPartitioner tokenPartitioner = new TokenPartitioner(ring, 4, numCores);
                final byte[] ar = serialize(tokenPartitioner);
                final TokenPartitioner deserialized = deserialize(ar, TokenPartitioner.class);
                assertEquals(tokenPartitioner.ring(), deserialized.ring());
                assertEquals(tokenPartitioner.numPartitions(), deserialized.numPartitions());
                assertEquals(tokenPartitioner.subRanges(), deserialized.subRanges());
                assertEquals(tokenPartitioner.partitionMap(), deserialized.partitionMap());
                assertEquals(tokenPartitioner.reversePartitionMap(), deserialized.reversePartitionMap());
                for (int i = 0; i < tokenPartitioner.numPartitions(); i++)
                {
                    assertEquals(tokenPartitioner.getTokenRange(i), deserialized.getTokenRange(i));
                }
            }));
    }
}
