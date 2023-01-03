package org.apache.cassandra.spark.data.partitioner;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeMap;
import com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.utils.ByteBufUtils;
import org.apache.cassandra.spark.utils.RangeUtils;

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
 * Util class for partitioning Spark workers across the token ring.
 */
@SuppressWarnings("UnstableApiUsage")
public class TokenPartitioner implements Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TokenPartitioner.class);

    private List<Range<BigInteger>> subRanges;
    private CassandraRing ring;
    private transient RangeMap<BigInteger, Integer> partitionMap;
    private transient Map<Integer, Range<BigInteger>> reversePartitionMap;

    protected TokenPartitioner(final List<Range<BigInteger>> subRanges, final CassandraRing ring)
    {
        this.subRanges = subRanges;
        this.ring = ring;
        this.partitionMap = TreeRangeMap.create();
        this.reversePartitionMap = new HashMap<>();
        calculateTokenRangeMap();
    }

    public TokenPartitioner(final CassandraRing ring, final int defaultParallelism, final int numCores)
    {
        this(ring, defaultParallelism, numCores, false);
    }

    public TokenPartitioner(final CassandraRing ring, final int defaultParallelism, final int numCores, final boolean shuffle)
    {
        LOGGER.info("Creating TokenPartitioner defaultParallelism={} numCores={}", defaultParallelism, numCores);
        this.partitionMap = TreeRangeMap.create();
        this.reversePartitionMap = new HashMap<>();
        this.ring = ring;

        final int numSplits = TokenPartitioner.calculateSplits(ring, defaultParallelism, numCores);
        this.subRanges = ring.rangeMap().asMapOfRanges().keySet().stream()
                             .flatMap(tr -> RangeUtils.split(tr, numSplits).stream()).collect(Collectors.toList());

        // shuffle off by default to avoid every spark worker
        // connecting to every Cassandra instance
        if (shuffle)
        {
            // Spark executes workers in partition order so here we shuffle
            // the sub-ranges before assigning to a Spark partition
            // so the job executes more evenly across the token ring
            Collections.shuffle(subRanges);
        }

        calculateTokenRangeMap();
    }

    private void calculateTokenRangeMap()
    {
        int nextPartitionId = 0;
        for (final Range<BigInteger> tr : subRanges)
        {
            final int partitionId = nextPartitionId;
            partitionMap.put(tr, partitionId);
            reversePartitionMap.put(partitionId, tr);
            nextPartitionId++;
        }

        validateMapSizes();
        validateCompleteRangeCoverage();
        validateRangesDoNotOverlap();

        LOGGER.info("Number of partitions {}", reversePartitionMap.size());
        LOGGER.info("Partition map" + partitionMap);
        LOGGER.info("Reverse partition map " + reversePartitionMap);
    }

    private static int calculateSplits(final CassandraRing ring, final int defaultParallelism, final Integer cores)
    {
        final int tasksToRun = Math.max(cores, defaultParallelism);
        LOGGER.info("Tasks to run: {}", tasksToRun);
        final Map<Range<BigInteger>, List<CassandraInstance>> rangeListMap = ring.rangeMap().asMapOfRanges();
        LOGGER.info("Initial ranges: {}", rangeListMap);
        final int ranges = rangeListMap.size();
        LOGGER.info("Number of ranges: {}", ranges);
        final int calculatedSplits = TokenPartitioner.divCeil(tasksToRun, ranges);
        LOGGER.info("Calculated number of splits as {}", calculatedSplits);
        return calculatedSplits;
    }

    public CassandraRing ring()
    {
        return ring;
    }

    public List<Range<BigInteger>> subRanges()
    {
        return subRanges;
    }

    public RangeMap<BigInteger, Integer> partitionMap()
    {
        return partitionMap;
    }

    public Map<Integer, Range<BigInteger>> reversePartitionMap()
    {
        return reversePartitionMap;
    }

    private static int divCeil(final int a, final int b)
    {
        return (a + b - 1) / b;
    }

    public int numPartitions()
    {
        return reversePartitionMap.size();
    }

    @SuppressWarnings("ConstantConditions")
    public boolean isInPartition(final BigInteger token, final ByteBuffer key, final int partitionId)
    {
        final boolean isInPartition = partitionId == partitionMap.get(token);
        if (LOGGER.isDebugEnabled() && !isInPartition)
        {
            final Range<BigInteger> range = getTokenRange(partitionId);
            LOGGER.debug("Filtering out partition key key='{}' token={} rangeLower={} rangeUpper={}", ByteBufUtils.toHexString(key), token, range.lowerEndpoint(), range.upperEndpoint());
        }
        return isInPartition;
    }

    public Range<BigInteger> getTokenRange(final int partitionId)
    {
        return reversePartitionMap.get(partitionId);
    }

    // validation

    private void validateRangesDoNotOverlap()
    {
        final List<Range<BigInteger>> sortedRanges = partitionMap.asMapOfRanges().keySet()
                                                                 .stream()
                                                                 .sorted(Comparator.comparing(Range::lowerEndpoint))
                                                                 .collect(Collectors.toList());
        Range<BigInteger> prev = null;
        for (final Range<BigInteger> current : sortedRanges)
        {
            if (prev != null)
            {
                Preconditions.checkState(!current.isConnected(prev) || current.intersection(prev).isEmpty(),
                                         String.format("Two ranges in partition map are overlapping %s %s",
                                                       prev.toString(), current.toString()));
            }

            prev = current;
        }
    }

    private void validateCompleteRangeCoverage()
    {
        final RangeSet<BigInteger> missingRangeSet = TreeRangeSet.create();
        missingRangeSet.add(Range.closed(ring.partitioner().minToken(),
                                         ring.partitioner().maxToken()));

        partitionMap.asMapOfRanges().keySet().forEach(missingRangeSet::remove);

        final List<Range<BigInteger>> missingRanges = missingRangeSet.asRanges().stream().filter(Range::isEmpty).collect(Collectors.toList());
        Preconditions.checkState(missingRanges.isEmpty(),
                                 "There should be no missing ranges, but found " + missingRanges.toString());
    }

    private void validateMapSizes()
    {
        final int nrPartitions = numPartitions();
        Preconditions.checkState(nrPartitions == partitionMap.asMapOfRanges().keySet().size(),
                                 String.format("Number of partitions %d not matching with partition map size %d",
                                               nrPartitions, partitionMap.asMapOfRanges().keySet().size()));
        Preconditions.checkState(nrPartitions == reversePartitionMap.keySet().size(),
                                 String.format("Number of partitions %d not matching with reverse partition map size %d",
                                               nrPartitions, reversePartitionMap.keySet().size()));
        Preconditions.checkState(nrPartitions >= ring.rangeMap().asMapOfRanges().keySet().size(),
                                 String.format("Number of partitions %d supposed to be more than number of token ranges %d",
                                               nrPartitions, ring.rangeMap().asMapOfRanges().keySet().size()));
        Preconditions.checkState(nrPartitions >= ring.tokenRanges().keySet().size(),
                                 String.format("Number of partitions %d supposed to be more than number of instances %d",
                                               nrPartitions, ring.tokenRanges().keySet().size()));
        Preconditions.checkState(partitionMap.asMapOfRanges().keySet().size() == reversePartitionMap.keySet().size(),
                                 String.format("You must be kidding me! Partition map %d and reverse map %d are not of same size",
                                               partitionMap.asMapOfRanges().keySet().size(),
                                               reversePartitionMap.keySet().size()));
    }

    @SuppressWarnings("unchecked")
    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        LOGGER.warn("Falling back to JDK deserialization");
        this.partitionMap = TreeRangeMap.create();
        this.reversePartitionMap = new HashMap<>();
        this.ring = (CassandraRing) in.readObject();
        this.subRanges = (List<Range<BigInteger>>) in.readObject();
        this.calculateTokenRangeMap();
    }

    private void writeObject(final ObjectOutputStream out) throws IOException, ClassNotFoundException
    {
        LOGGER.warn("Falling back to JDK serialization");
        out.writeObject(this.ring);
        out.writeObject(this.subRanges);
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<TokenPartitioner>
    {
        @Override
        public void write(final Kryo kryo, final Output out, final TokenPartitioner partitioner)
        {
            out.writeInt(partitioner.subRanges.size());
            for (final Range<BigInteger> subRange : partitioner.subRanges)
            {
                out.writeByte(subRange.lowerBoundType() == BoundType.OPEN ? 1 : 0);
                out.writeString(subRange.lowerEndpoint().toString());
                out.writeByte(subRange.upperBoundType() == BoundType.OPEN ? 1 : 0);
                out.writeString(subRange.upperEndpoint().toString());
            }
            kryo.writeObject(out, partitioner.ring);
        }

        @Override
        public TokenPartitioner read(final Kryo kryo, final Input in, final Class<TokenPartitioner> type)
        {
            final int numRanges = in.readInt();
            final List<Range<BigInteger>> subRanges = new ArrayList<>(numRanges);
            for (int i = 0; i < numRanges; i++)
            {
                final BoundType lowerBoundType = in.readByte() == 1 ? BoundType.OPEN : BoundType.CLOSED;
                final BigInteger lowerBound = new BigInteger(in.readString());
                final BoundType upperBoundType = in.readByte() == 1 ? BoundType.OPEN : BoundType.CLOSED;
                final BigInteger upperBound = new BigInteger(in.readString());
                subRanges.add(Range.range(lowerBound, lowerBoundType, upperBound, upperBoundType));
            }
            return new TokenPartitioner(subRanges, kryo.readObject(in, CassandraRing.class));
        }
    }
}
