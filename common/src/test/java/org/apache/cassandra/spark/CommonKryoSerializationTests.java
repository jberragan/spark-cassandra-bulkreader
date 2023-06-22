package org.apache.cassandra.spark;

import java.math.BigInteger;
import java.util.Arrays;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.cdc.Marker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.utils.KryoUtils;

import static org.apache.cassandra.spark.utils.KryoUtils.deserialize;
import static org.apache.cassandra.spark.utils.KryoUtils.serialize;
import static org.apache.cassandra.spark.utils.KryoUtils.serializeToBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public class CommonKryoSerializationTests
{
    private static final ThreadLocal<Kryo> KRYO = ThreadLocal.withInitial(() -> {
        final Kryo kryo = new Kryo();
        new BaseKryoRegister().registerClasses(kryo);
        return kryo;
    });

    public static Kryo kryo()
    {
        return KRYO.get();
    }

    @Test
    public void testCassandraInstance()
    {
        final CassandraInstance instance = new CassandraInstance("-9223372036854775807", "local1-i1", "DC1");
        final Output out = serialize(kryo(), instance);
        final CassandraInstance deserialized = deserialize(kryo(), out, CassandraInstance.class);
        assertNotNull(deserialized);
        assertEquals(instance, deserialized);
    }

    @Test
    public void testCassandraRing()
    {
        qt().forAll(TestUtils.partitioners())
            .checkAssert(partitioner -> {
                final CassandraRing ring = TestUtils.createRing(partitioner, ImmutableMap.of("DC1", 3, "DC2", 3));
                final Output out = serialize(kryo(), ring);
                final CassandraRing deserialized = deserialize(kryo(), out, CassandraRing.class);
                assertNotNull(deserialized);
                assertEquals(ring, deserialized);
                assertEquals(partitioner, deserialized.partitioner());
            });
    }

    @Test
    public void testTokenPartitioner()
    {
        qt().forAll(TestUtils.partitioners(), arbitrary().pick(Arrays.asList(3, 16, 128)), arbitrary().pick(Arrays.asList(1, 4, 16)), arbitrary().pick(Arrays.asList(4, 16, 64)))
            .checkAssert((partitioner, numInstances, defaultParallelism, numCores) -> {
                final CassandraRing ring = TestUtils.createRing(partitioner, numInstances);
                final TokenPartitioner tokenPartitioner = new TokenPartitioner(ring, defaultParallelism, numCores);
                final Output out = serialize(kryo(), tokenPartitioner);
                final TokenPartitioner deserialized = deserialize(kryo(), out, TokenPartitioner.class);
                assertNotNull(deserialized);
                assertEquals(tokenPartitioner.numPartitions(), deserialized.numPartitions());
                assertEquals(tokenPartitioner.subRanges().size(), deserialized.subRanges().size());
                for (int i = 0; i < tokenPartitioner.subRanges().size(); i++)
                {
                    assertEquals(tokenPartitioner.subRanges().get(i), deserialized.subRanges().get(i));
                }
                assertEquals(tokenPartitioner.ring(), deserialized.ring());
            });
    }

    @Test
    public void testCommitLogMarker()
    {
        final Marker marker = new Marker(new CassandraInstance("0", "local1-i1", "DC1"), 500L, 200);
        final byte[] ar = serializeToBytes(kryo(), marker);
        final Marker deserialized = deserialize(kryo(), ar, Marker.class);
        assertNotNull(deserialized);
        assertEquals(marker, deserialized);
    }

    @Test
    public void testRange()
    {
        testRange(Range.closed(BigInteger.valueOf(500), BigInteger.valueOf(99999)));
        testRange(Range.closed(BigInteger.valueOf(-999999), BigInteger.valueOf(99999)));
        testRange(Range.closed(BigInteger.ZERO, Partitioner.Murmur3Partitioner.maxToken()));
        testRange(Range.closed(Partitioner.Murmur3Partitioner.minToken(), Partitioner.Murmur3Partitioner.maxToken()));
        testRange(Range.closed(Partitioner.Murmur3Partitioner.minToken(), BigInteger.ZERO));
        testRange(Range.closed(BigInteger.ZERO, Partitioner.RandomPartitioner.maxToken()));
        testRange(Range.closed(Partitioner.RandomPartitioner.minToken(), Partitioner.RandomPartitioner.maxToken()));
        testRange(Range.closed(Partitioner.RandomPartitioner.minToken(), BigInteger.ZERO));
        testRange(null);
    }

    private static void testRange(Range<BigInteger> expected)
    {
        final byte[] ar;
        try (Output out = new Output(512))
        {
            KryoUtils.writeRange(out, expected);
            ar = out.getBuffer();
        }

        try (Input in = new Input(ar))
        {
            assertEquals(expected, KryoUtils.readRange(in));
        }
    }
}
