package org.apache.cassandra.spark;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.quicktheories.core.Gen;

import static org.junit.Assert.assertTrue;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public class TestUtils
{
    public static Gen<Partitioner> partitioners()
    {
        return arbitrary().enumValues(Partitioner.class);
    }

    public static BigInteger randomBigInteger(final Partitioner partitioner)
    {
        final BigInteger range = partitioner.maxToken().subtract(partitioner.minToken());
        final int len = partitioner.maxToken().bitLength();
        BigInteger result = new BigInteger(len, RandomUtils.RANDOM);
        if (result.compareTo(partitioner.minToken()) < 0)
        {
            result = result.add(partitioner.minToken());
        }
        if (result.compareTo(range) >= 0)
        {
            result = result.mod(range).add(partitioner.minToken());
        }
        return result;
    }

    public static String randomLowEntropyString()
    {
        return new String(randomLowEntropyData(), StandardCharsets.UTF_8);
    }

    public static byte[] randomLowEntropyData()
    {
        return randomLowEntropyData(RandomUtils.randomPositiveInt(16384 - 512) + 512);
    }

    public static byte[] randomLowEntropyData(int size)
    {
        return randomLowEntropyData("Hello world!", size);
    }

    public static byte[] randomLowEntropyData(String str, int size)
    {
        return StringUtils.repeat(str, size / str.length() + 1)
                          .substring(0, size)
                          .getBytes(StandardCharsets.UTF_8);
    }

    public static CassandraRing createRing(final Partitioner partitioner, int numInstances)
    {
        return createRing(partitioner, ImmutableMap.of("DC1", numInstances));
    }

    public static CassandraRing createRing(final Partitioner partitioner, final Map<String, Integer> numInstances)
    {
        final Collection<CassandraInstance> instances = numInstances.entrySet().stream().map(e -> TestUtils.createInstances(partitioner, e.getValue(), e.getKey())).flatMap(Collection::stream).collect(Collectors.toList());
        final Map<String, Integer> dcs = numInstances.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, a -> Math.min(a.getValue(), 3)));
        return new CassandraRing(partitioner, "test", new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, dcs), instances);
    }

    public static Collection<CassandraInstance> createInstances(final Partitioner partitioner, final int numInstances, final String dc)
    {
        Preconditions.checkArgument(numInstances > 0, "NumInstances must be greater than zero");
        final BigInteger split = partitioner.maxToken().subtract(partitioner.minToken()).divide(BigInteger.valueOf(numInstances));
        final Collection<CassandraInstance> instances = new ArrayList<>(numInstances);
        BigInteger token = partitioner.minToken();
        for (int i = 0; i < numInstances; i++)
        {
            instances.add(new CassandraInstance(token.toString(), "local-i" + i, dc));
            token = token.add(split);
            assertTrue(token.compareTo(partitioner.maxToken()) <= 0);
        }
        return instances;
    }

    public static ReplicationFactor simpleStrategy()
    {
        return new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, ImmutableMap.of("DC1", 3));
    }

    public static ReplicationFactor networkTopologyStrategy(final Map<String, Integer> options)
    {
        return new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, options);
    }

    public static ReplicationFactor networkTopologyStrategy()
    {
        return networkTopologyStrategy(ImmutableMap.of("DC1", 3));
    }

    static List<CassandraVersion> testableVersions()
    {
        return new ArrayList<>(Collections.singletonList(CassandraVersion.FOURZERO));
    }

    static List<CassandraVersion> tombstoneTestableVersions()
    {
        return Collections.singletonList(CassandraVersion.FOURZERO);
    }

    public static Gen<CassandraVersion> tombstoneVersions()
    {
        return arbitrary().pick(tombstoneTestableVersions());
    }

    // jdk serialization

    public static <T> T deserialize(final byte[] ar, final Class<T> cType)
    {
        final ObjectInputStream in;
        try
        {
            in = new ObjectInputStream(new ByteArrayInputStream(ar));
            return cType.cast(in.readObject());
        }
        catch (final IOException | ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static byte[] serialize(final Serializable serializable)
    {
        try
        {
            final ByteArrayOutputStream arOut = new ByteArrayOutputStream(512);
            try (final ObjectOutputStream out = new ObjectOutputStream(arOut))
            {
                out.writeObject(serializable);
            }
            return arOut.toByteArray();
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
