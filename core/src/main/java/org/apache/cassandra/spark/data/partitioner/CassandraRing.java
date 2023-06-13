package org.apache.cassandra.spark.data.partitioner;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.utils.RangeUtils;

import static org.apache.cassandra.spark.data.ReplicationFactor.ReplicationStrategy.SimpleStrategy;

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
 * CassandraRing is designed to have one unique way of handling Cassandra token/topology information across all Cassandra
 * tooling. This class is made Serializable so it's easy to use it from Hadoop/Spark. As Cassandra token ranges are
 * dependent on Replication strategy, ring makes sense for a specific keyspace only. It is made to be immutable for the
 * sake of simplicity.
 * <p>
 * Token ranges are calculated assuming Cassandra racks are not being used, but controlled by assigning tokens properly.
 * <p>
 * {@link #equals(Object)} and {@link #hashCode()} don't take {@link #replicas} and {@link #tokenRangeMap} into
 * consideration as they are just derived fields.
 */
@SuppressWarnings({ "UnstableApiUsage", "unused", "WeakerAccess" })
public class CassandraRing implements Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRing.class);
    public static final Serializer SERIALIZER = new Serializer();

    private Partitioner partitioner;
    private String keyspace;
    private ReplicationFactor replicationFactor;
    private ArrayList<CassandraInstance> instances;

    private transient RangeMap<BigInteger, List<CassandraInstance>> replicas;
    private transient Multimap<CassandraInstance, Range<BigInteger>> tokenRangeMap;

    /**
     * Add a replica with given range to replicaMap (RangeMap pointing to replicas).
     * <p>
     * replicaMap starts with full range (representing complete ring) with empty list of replicas. So, it is
     * guaranteed that range will match one or many ranges in replicaMap.
     * <p>
     * Scheme to add a new replica for a range
     * * Find overlapping rangeMap entries from replicaMap
     * * For each overlapping range, create new replica list by adding new replica to the existing list and add it
     * back to replicaMap
     */
    private static void addReplica(final CassandraInstance replica, final Range<BigInteger> range, final RangeMap<BigInteger, List<CassandraInstance>> replicaMap)
    {
        Preconditions.checkArgument(range.lowerEndpoint().compareTo(range.upperEndpoint()) <= 0,
                                    "Range calculations assume range is not wrapped");

        final RangeMap<BigInteger, List<CassandraInstance>> replicaRanges = replicaMap.subRangeMap(range);
        final RangeMap<BigInteger, List<CassandraInstance>> mappingsToAdd = TreeRangeMap.create();

        replicaRanges.asMapOfRanges().forEach((key, value) -> {
            final List<CassandraInstance> replicas = new ArrayList<>(value);
            replicas.add(replica);
            mappingsToAdd.put(key, replicas);
        });
        replicaMap.putAll(mappingsToAdd);
    }

    public CassandraRing(final Partitioner partitioner, final String keyspace, final ReplicationFactor replicationFactor, final Collection<CassandraInstance> instances)
    {
        this.partitioner = partitioner;
        this.keyspace = keyspace;
        this.replicationFactor = replicationFactor;
        this.instances = instances.stream().sorted(Comparator.comparing(o -> new BigInteger(o.token()))).collect(Collectors.toCollection(ArrayList::new));
        this.init();
    }

    private void init()
    {
        // setup token range map
        this.replicas = TreeRangeMap.create();
        this.tokenRangeMap = ArrayListMultimap.create();

        // Calculate instance to token ranges mapping
        switch (this.replicationFactor.getReplicationStrategy())
        {
            case SimpleStrategy:
                this.tokenRangeMap.putAll(RangeUtils.calculateTokenRanges(this.instances, this.replicationFactor.getTotalReplicationFactor(), this.partitioner));
                break;
            case NetworkTopologyStrategy:
                for (final String dc : this.dataCenters())
                {
                    final int rf = this.replicationFactor.getOptions().get(dc);
                    if (rf == 0)
                    {
                        continue;
                    }
                    final List<CassandraInstance> dcInstances = this.instances.stream()
                                                                              .filter(i -> i.dataCenter().matches(dc))
                                                                              .collect(Collectors.toList());
                    tokenRangeMap.putAll(RangeUtils.calculateTokenRanges(dcInstances,
                                                                         this.replicationFactor.getOptions().get(dc),
                                                                         this.partitioner));
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported replication strategy");
        }

        // Calculate token range to replica mapping
        this.replicas.put(Range.closed(this.partitioner.minToken(), this.partitioner.maxToken()), Collections.emptyList());
        this.tokenRangeMap.asMap().forEach((inst, ranges) -> ranges.forEach(range -> addReplica(inst, range, this.replicas)));
    }

    public Partitioner partitioner()
    {
        return partitioner;
    }

    public String keyspace()
    {
        return keyspace;
    }

    public Collection<CassandraInstance> instances()
    {
        return instances;
    }

    public Collection<CassandraInstance> getReplicas(final BigInteger token)
    {
        return replicas.get(token);
    }

    public RangeMap<BigInteger, List<CassandraInstance>> rangeMap()
    {
        return this.replicas;
    }

    public ReplicationFactor replicationFactor()
    {
        return this.replicationFactor;
    }

    public RangeMap<BigInteger, List<CassandraInstance>> getSubRanges(final Range<BigInteger> tokenRange)
    {
        return replicas.subRangeMap(tokenRange);
    }

    public Multimap<CassandraInstance, Range<BigInteger>> tokenRanges()
    {
        return this.tokenRangeMap;
    }

    private Collection<String> dataCenters()
    {
        return (this.replicationFactor.getReplicationStrategy() == SimpleStrategy)
               ? Collections.emptySet()
               : this.replicationFactor.getOptions().keySet();
    }

    public Collection<BigInteger> tokens()
    {
        return instances.stream()
                        .map(CassandraInstance::token)
                        .map(BigInteger::new)
                        .sorted()
                        .collect(Collectors.toList());
    }

    public Collection<BigInteger> tokens(final String dataCenter)
    {
        Preconditions.checkArgument(this.replicationFactor.getReplicationStrategy() != SimpleStrategy,
                                    "Datacenter tokens doesn't make sense for SimpleStrategy");
        return instances.stream()
                        .filter(inst -> inst.dataCenter().matches(dataCenter))
                        .map(CassandraInstance::token)
                        .map(BigInteger::new)
                        .collect(Collectors.toList());
    }

    @Override
    public boolean equals(final Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }
        if (obj.getClass() != getClass())
        {
            return false;
        }

        final CassandraRing rhs = (CassandraRing) obj;
        return new EqualsBuilder()
               .append(partitioner, rhs.partitioner)
               .append(keyspace, rhs.keyspace)
               .append(replicationFactor, rhs.replicationFactor)
               .append(instances, rhs.instances)
               .append(replicas, rhs.replicas)
               .append(tokenRangeMap, rhs.tokenRangeMap)
               .isEquals();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(41, 43)
               .append(partitioner)
               .append(keyspace)
               .append(replicationFactor)
               .append(instances)
               .append(replicas)
               .append(tokenRangeMap)
               .toHashCode();
    }

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        LOGGER.warn("Falling back to JDK deserialization");
        this.partitioner = in.readByte() == 0 ? Partitioner.RandomPartitioner : Partitioner.Murmur3Partitioner;
        this.keyspace = in.readUTF();

        final ReplicationFactor.ReplicationStrategy strategy = ReplicationFactor.ReplicationStrategy.valueOf(in.readByte());
        final int optionCount = in.readByte();
        final Map<String, Integer> options = new HashMap<>(optionCount);
        for (int i = 0; i < optionCount; i++)
        {
            options.put(in.readUTF(), (int) in.readByte());
        }
        this.replicationFactor = new ReplicationFactor(strategy, options);

        final int numInstances = in.readShort();
        this.instances = new ArrayList<>(numInstances);
        for (int i = 0; i < numInstances; i++)
        {
            this.instances.add(new CassandraInstance(in.readUTF(), in.readUTF(), in.readUTF()));
        }
        this.init();
    }

    private void writeObject(final ObjectOutputStream out) throws IOException, ClassNotFoundException
    {
        LOGGER.warn("Falling back to JDK serialization");
        out.writeByte(this.partitioner == Partitioner.RandomPartitioner ? 0 : 1);
        out.writeUTF(this.keyspace);

        out.writeByte(this.replicationFactor.getReplicationStrategy().value);
        final Map<String, Integer> options = this.replicationFactor.getOptions();
        out.writeByte(options.size());
        for (final Map.Entry<String, Integer> option : options.entrySet())
        {
            out.writeUTF(option.getKey());
            out.writeByte(option.getValue());
        }

        out.writeShort(this.instances.size());
        for (final CassandraInstance instance : this.instances)
        {
            out.writeUTF(instance.token());
            out.writeUTF(instance.nodeName());
            out.writeUTF(instance.dataCenter());
        }
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CassandraRing>
    {
        @Override
        public void write(final Kryo kryo, final Output out, final CassandraRing ring)
        {
            out.writeByte(ring.partitioner == Partitioner.RandomPartitioner ? 1 : 0);
            out.writeString(ring.keyspace);
            kryo.writeObject(out, ring.replicationFactor);
            kryo.writeObject(out, ring.instances);
        }

        @SuppressWarnings("unchecked")
        @Override
        public CassandraRing read(final Kryo kryo, final Input in, final Class<CassandraRing> type)
        {
            return new CassandraRing(in.readByte() == 1 ? Partitioner.RandomPartitioner : Partitioner.Murmur3Partitioner, in.readString(), kryo.readObject(in, ReplicationFactor.class), kryo.readObject(in, ArrayList.class));
        }
    }
}
