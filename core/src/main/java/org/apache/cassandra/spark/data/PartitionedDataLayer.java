package org.apache.cassandra.spark.data;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.cassandra.spark.data.partitioner.MultipleReplicas;
import org.apache.cassandra.spark.data.partitioner.NotEnoughReplicasException;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.partitioner.SingleReplica;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.sparksql.NoMatchFoundException;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.FutureUtils;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.spark.TaskContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
 * DataLayer that partitions token range by the number of Spark partitions and only lists SSTables overlapping with range
 */
@SuppressWarnings("WeakerAccess")
public abstract class PartitionedDataLayer extends DataLayer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedDataLayer.class);
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM;
    public static final ConcurrentHashMap<String, Integer> RF_CACHE = new ConcurrentHashMap<>(16);

    @NotNull
    protected final ConsistencyLevel consistencyLevel;
    protected final String dc;

    public enum AvailabilityHint
    {
        // 0 means high priority
        UP(0), MOVING(1), LEAVING(1), UNKNOWN(2), JOINING(2), DOWN(2);

        private final int priority;

        AvailabilityHint(int priority)
        {
            this.priority = priority;
        }

        public static final Comparator<AvailabilityHint> AVAILABILITY_HINT_COMPARATOR = Comparator.comparingInt((AvailabilityHint o) -> o.priority).reversed();

        public static Enum fromState(String status, String state)
        {
            if (status.equalsIgnoreCase(AvailabilityHint.DOWN.name()))
            {
                return AvailabilityHint.DOWN;
            }

            if (status.equalsIgnoreCase(AvailabilityHint.UNKNOWN.name()))
            {
                return AvailabilityHint.UNKNOWN;
            }

            if (state.equalsIgnoreCase("NORMAL"))
            {
                return AvailabilityHint.valueOf(status);
            }
            if (state.equalsIgnoreCase(AvailabilityHint.MOVING.name()))
            {
                return AvailabilityHint.MOVING;
            }
            if (state.equalsIgnoreCase(AvailabilityHint.LEAVING.name()))
            {
                return AvailabilityHint.LEAVING;
            }
            if (state.equalsIgnoreCase("STARTING"))
            {
                return AvailabilityHint.valueOf(status);
            }
            if (state.equalsIgnoreCase(AvailabilityHint.JOINING.name()))
            {
                return AvailabilityHint.JOINING;
            }

            return AvailabilityHint.UNKNOWN;
        }
    }

    public PartitionedDataLayer(@Nullable final ConsistencyLevel consistencyLevel, @Nullable final String dc)
    {
        this.consistencyLevel = consistencyLevel == null ? DEFAULT_CONSISTENCY_LEVEL : consistencyLevel;
        this.dc = dc == null ? null : dc.toUpperCase();

        if (consistencyLevel == ConsistencyLevel.SERIAL || consistencyLevel == ConsistencyLevel.LOCAL_SERIAL)
        {
            throw new IllegalArgumentException("SERIAL or LOCAL_SERIAL are invalid consistency levels for the bulk reader");
        }
        if (consistencyLevel == ConsistencyLevel.EACH_QUORUM)
        {
            throw new NotImplementedException("EACH_QUORUM has not been implemented yet");
        }
        PartitionedDataLayer.clearRfCache();
    }

    protected void validateReplicationFactor(@NotNull final ReplicationFactor rf)
    {
        validateReplicationFactor(consistencyLevel, rf, dc);
    }

    @VisibleForTesting
    public static void validateReplicationFactor(@NotNull final ConsistencyLevel consistencyLevel,
                                                 @NotNull final ReplicationFactor rf,
                                                 @Nullable final String dc)
    {
        if (rf.getReplicationStrategy() != ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy)
        {
            return;
        }
        // single DC and no DC specified so use only DC in replication factor
        if (dc == null && rf.getOptions().size() == 1)
        {
            return;
        }
        Preconditions.checkArgument(dc != null || !consistencyLevel.isDCLocal, "A DC must be specified for DC local consistency level " + consistencyLevel.name());
        if (dc == null)
        {
            return;
        }
        Preconditions.checkArgument(rf.getOptions().containsKey(dc), "DC %s not found in replication factor %s", dc, rf.getOptions().keySet());
        Preconditions.checkArgument(rf.getOptions().get(dc) > 0, "Cannot read from DC %s with non-positive replication factor %d", dc, rf.getOptions().get(dc));
    }

    public abstract CompletableFuture<Stream<SSTable>> listInstance(final int partitionId, @NotNull final Range<BigInteger> range, @NotNull final CassandraInstance instance);

    public abstract CassandraRing ring();

    public abstract TokenPartitioner tokenPartitioner();

    @Override
    public int partitionCount()
    {
        return tokenPartitioner().numPartitions();
    }

    @Override
    public Partitioner partitioner()
    {
        return ring().partitioner();
    }

    @Override
    public boolean isInPartition(final BigInteger token, final ByteBuffer key)
    {
        return tokenPartitioner().isInPartition(token, key, TaskContext.getPartitionId());
    }

    @Override
    public SparkRangeFilter sparkRangeFilter()
    {
        final int partitionId = TaskContext.getPartitionId();
        Map<Integer, Range<BigInteger>> reversePartitionMap = tokenPartitioner().reversePartitionMap();
        final Range<BigInteger> sparkTokenRange = reversePartitionMap.get(partitionId);
        if (sparkTokenRange == null)
        {
            LOGGER.error("Unable to find the sparkTokenRange for partitionId={} in reversePartitionMap={}",
                    partitionId, reversePartitionMap);
            throw new IllegalStateException(String.format("Unable to find sparkTokenRange for partitionId=%d in the reverse partition map",
                    partitionId));
        }
        return SparkRangeFilter.create(sparkTokenRange);
    }

    @Override
    public List<PartitionKeyFilter> partitionKeyFiltersInRange(final List<PartitionKeyFilter> filters) throws NoMatchFoundException
    {
        // we only need to worry about Partition key filters that overlap with this Spark workers token range
        final SparkRangeFilter rangeFilter = sparkRangeFilter();
        final Range<BigInteger> sparkTokenRange = rangeFilter.tokenRange();

        final List<PartitionKeyFilter> filtersInRange = filters.stream()
                                                               .filter(filter -> filter.overlaps(sparkTokenRange))
                                                               .collect(Collectors.toList());

        if (!filters.isEmpty() && filtersInRange.isEmpty())
        {
            LOGGER.info("None of the partition key filters overlap with Spark partition token range firstToken={} lastToken{}", sparkTokenRange.lowerEndpoint(), sparkTokenRange.upperEndpoint());
            throw new NoMatchFoundException();
        }
        return filterNonIntersectingSSTables() ? filtersInRange : filters;
    }

    public ConsistencyLevel consistencylevel()
    {
        return this.consistencyLevel;
    }

    @Override
    public SSTablesSupplier sstables(@Nullable final SparkRangeFilter sparkRangeFilter,
                                     @NotNull final List<PartitionKeyFilter> partitionKeyFilters)
    {
        // get token range for Spark partition
        final TokenPartitioner tokenPartitioner = tokenPartitioner();
        final int partitionId = TaskContext.getPartitionId();
        if (partitionId < 0 || partitionId >= tokenPartitioner.numPartitions())
        {
            throw new IllegalStateException("PartitionId outside expected range: " + partitionId);
        }

        // get all replicas overlapping partition token range
        final Range<BigInteger> range = tokenPartitioner.getTokenRange(partitionId);
        final CassandraRing ring = ring();
        final ReplicationFactor rf = ring.replicationFactor();
        validateReplicationFactor(rf);
        final Map<Range<BigInteger>, List<CassandraInstance>> instRanges;
        final Map<Range<BigInteger>, List<CassandraInstance>> subRanges = ring().getSubRanges(range).asMapOfRanges();
        if (partitionKeyFilters.isEmpty())
        {
            instRanges = subRanges;
        }
        else
        {
            instRanges = new HashMap<>();
            subRanges.keySet().forEach(instRange -> {
                if (partitionKeyFilters.stream().anyMatch(filter -> filter.overlaps(instRange)))
                {
                    instRanges.putIfAbsent(instRange, subRanges.get(instRange));
                }
            });
        }

        final Set<CassandraInstance> replicas = PartitionedDataLayer.rangesToReplicas(consistencyLevel, dc, instRanges);
        LOGGER.info("Creating partitioned SSTablesSupplier for Spark partition partitionId={} rangeLower={} rangeUpper={} numReplicas={}", partitionId, range.lowerEndpoint(), range.upperEndpoint(), replicas.size());

        // use consistency level and replication factor to calculate min number of replicas required to satisfy consistency level
        // split replicas into 'primary' and 'backup' replicas, attempt on primary replicas and use backups to retry in the event of a failure
        final int minReplicas = consistencyLevel.blockFor(rf, dc);
        final ReplicaSet replicaSet = PartitionedDataLayer.splitReplicas(consistencyLevel, dc, instRanges, replicas, this::getAvailability, minReplicas, partitionId);
        if (replicaSet.primary().size() < minReplicas)
        {
            // could not find enough primary replicas to meet consistency level
            assert replicaSet.backup.isEmpty();
            throw new NotEnoughReplicasException(consistencyLevel, range, minReplicas, replicas.size(), dc);
        }

        final ExecutorService executor = executorService();
        final Stats stats = stats();
        final Set<SingleReplica> primaryReplicas = replicaSet.primary().stream().map(inst -> new SingleReplica(inst, this, range, partitionId, executor, stats, replicaSet.isRepairPrimary(inst))).collect(Collectors.toSet());
        final Set<SingleReplica> backupReplicas = replicaSet.backup().stream().map(inst -> new SingleReplica(inst, this, range, partitionId, executor, stats, true)).collect(Collectors.toSet());

        return new MultipleReplicas(primaryReplicas, backupReplicas, stats);
    }

    /**
     * Overridable method setting whether the PartitionedDataLayer should filter out SSTables that do not intersect with the Spark partition token range
     *
     * @return true if we should filter
     */
    public boolean filterNonIntersectingSSTables()
    {
        return true;
    }

    /**
     * Data Layer can override this method to hint availability of a Cassandra instance so
     * bulk reader attempts UP instances first, and avoids instances known to be down e.g. if create snapshot request already failed
     *
     * @param instance a cassandra instance
     * @return availability hint
     */
    protected AvailabilityHint getAvailability(final CassandraInstance instance)
    {
        return AvailabilityHint.UNKNOWN;
    }

    static Set<CassandraInstance> rangesToReplicas(@NotNull final ConsistencyLevel consistencyLevel,
                                                   @Nullable final String dc,
                                                   @NotNull final Map<Range<BigInteger>, List<CassandraInstance>> ranges)
    {
        return ranges.values().stream()
                     .flatMap(Collection::stream)
                     .filter(inst -> !consistencyLevel.isDCLocal || dc == null || inst.dataCenter().equalsIgnoreCase(dc))
                     .collect(Collectors.toSet());
    }

    /**
     * Split the replicas overlapping with the Spark worker's token range based on availability hint so that we
     * achieve consistency.
     *
     * @param consistencyLevel user set consistency level.
     * @param dc               data center to read from.
     * @param ranges           all the token ranges owned by this Spark worker, and associated replicas.
     * @param replicas         all the replicas we can read from.
     * @param availability     availability hint provider for each CassandraInstance.
     * @param minReplicas      minimum number of replicas to achieve consistency.
     * @param partitionId      Spark worker partitionId
     * @return a set of primary and backup replicas to read from.
     * @throws NotEnoughReplicasException thrown when insufficient primary replicas selected to achieve consistency level for any sub-range of the Spark worker's token range.
     */
    static ReplicaSet splitReplicas(@NotNull final ConsistencyLevel consistencyLevel,
                                    @Nullable final String dc,
                                    @NotNull Map<Range<BigInteger>, List<CassandraInstance>> ranges,
                                    @NotNull final Set<CassandraInstance> replicas,
                                    @NotNull final Function<CassandraInstance, AvailabilityHint> availability,
                                    final int minReplicas,
                                    final int partitionId) throws NotEnoughReplicasException
    {
        final ReplicaSet split = splitReplicas(replicas, ranges, availability, minReplicas, partitionId);
        validateConsistency(consistencyLevel, dc, ranges, split.primary(), minReplicas);
        return split;
    }

    /**
     * Validate we have achieved consistency for all sub-ranges owned by the Spark worker.
     *
     * @param consistencyLevel consistency level.
     * @param dc               data center.
     * @param workerRanges     token sub-ranges owned by this Spark worker.
     * @param primaryReplicas  set of primary replicas selected.
     * @param minReplicas      minimum number of replicas required to meet consistency level.
     * @throws NotEnoughReplicasException thrown when insufficient primary replicas selected to achieve consistency level for any sub-range of the Spark worker's token range.
     */
    private static void validateConsistency(@NotNull final ConsistencyLevel consistencyLevel,
                                            @Nullable final String dc,
                                            @NotNull final Map<Range<BigInteger>, List<CassandraInstance>> workerRanges,
                                            @NotNull final Set<CassandraInstance> primaryReplicas,
                                            final int minReplicas) throws NotEnoughReplicasException
    {
        for (Map.Entry<Range<BigInteger>, List<CassandraInstance>> range : workerRanges.entrySet())
        {
            final int count = (int) range.getValue().stream().filter(primaryReplicas::contains).count();
            if (count < minReplicas)
            {
                throw new NotEnoughReplicasException(consistencyLevel, range.getKey(), minReplicas, count, dc);
            }
        }
    }

    /**
     * Return a set of primary and backup CassandraInstances to satisfy the consistency level.
     * NOTE: this method current assumes that each Spark token worker owns a single replica set.
     *
     * @param instances    replicas that overlap with the Spark worker's token range.
     * @param ranges       all the token ranges owned by this Spark worker, and associated replicas.
     * @param availability availability hint provider for each CassandraInstance.
     * @param minReplicas  minimum number of replicas to achieve consistency.
     * @param partitionId  Spark worker partitionId
     * @return a set of primary and backup replicas to read from.
     */
    static ReplicaSet splitReplicas(final Collection<CassandraInstance> instances,
                                    @NotNull Map<Range<BigInteger>, List<CassandraInstance>> ranges,
                                    final Function<CassandraInstance, AvailabilityHint> availability,
                                    final int minReplicas,
                                    int partitionId)
    {
        final ReplicaSet replicaSet = new ReplicaSet(minReplicas, partitionId);

        // sort instances by status hint so we attempt available instances first (e.g. we already know which instances are probably up from create snapshot request)
        instances.stream()
                 .sorted(Comparator.comparing(availability, AvailabilityHint.AVAILABILITY_HINT_COMPARATOR))
                 .forEach(replicaSet::add);

        if (ranges.size() != 1)
        {
            // currently we don't support using incremental repair when Spark worker owns multiple replica sets
            // but for current implementation of the TokenPartitioner it returns a single replica set per Spark worker/partition
            LOGGER.warn("Cannot use incremental repair awareness when Spark partition owns more than one replica set, performance will be degraded numRanges={}", ranges.size());
            replicaSet.incrementalRepairPrimary = null;
        }

        return replicaSet;
    }

    public static class ReplicaSet
    {
        private final Set<CassandraInstance> primary, backup;
        private final int minReplicas, partitionId;
        private CassandraInstance incrementalRepairPrimary;

        ReplicaSet(int minReplicas,
                   int partitionId)
        {
            this.minReplicas = minReplicas;
            this.partitionId = partitionId;
            this.primary = new HashSet<>();
            this.backup = new HashSet<>();
        }

        public ReplicaSet add(CassandraInstance instance)
        {
            if (primary.size() < minReplicas)
            {
                LOGGER.info("Selecting instance as primary replica nodeName={} token={} dc={} partitionId={}", instance.nodeName(), instance.token(), instance.dataCenter(), partitionId);
                return addPrimary(instance);
            }
            return addBackup(instance);
        }

        public boolean isRepairPrimary(final CassandraInstance instance)
        {
            return incrementalRepairPrimary == null || incrementalRepairPrimary.equals(instance);
        }

        public Set<CassandraInstance> primary()
        {
            return this.primary;
        }

        public ReplicaSet addPrimary(final CassandraInstance instance)
        {
            if (this.incrementalRepairPrimary == null)
            {
                // pick the first primary replica as a 'repair primary' to read repaired SSTables at CL ONE
                this.incrementalRepairPrimary = instance;
            }
            this.primary.add(instance);
            return this;
        }

        public Set<CassandraInstance> backup()
        {
            return this.backup;
        }

        public ReplicaSet addBackup(final CassandraInstance instance)
        {
            LOGGER.info("Selecting instance as backup replica nodeName={} token={} dc={} partitionId={}", instance.nodeName(), instance.token(), instance.dataCenter(), partitionId);
            this.backup.add(instance);
            return this;
        }
    }

    // cdc

    public abstract CompletableFuture<List<CommitLog>> listCommitLogs(CassandraInstance instance);

    @SuppressWarnings("UnstableApiUsage")
    public Map<CassandraInstance, List<CommitLog>> partitionLogs(@NotNull CdcOffsetFilter offset)
    {
        final TokenPartitioner tokenPartitioner = tokenPartitioner();
        final int partitionId = TaskContext.getPartitionId();
        final Range<BigInteger> range = tokenPartitioner.getTokenRange(partitionId);

        // for CDC we read from all available replicas that overlap with Spark token range
        final Map<Range<BigInteger>, List<CassandraInstance>> subRanges = ring().getSubRanges(range).asMapOfRanges();
        final Set<CassandraInstance> replicas = subRanges.values().stream()
                                                         .flatMap(Collection::stream)
                                                         .filter(inst -> dc == null || inst.dataCenter().equalsIgnoreCase(dc))
                                                         .collect(Collectors.toSet());

        final Map<CassandraInstance, List<CommitLog>> replicaLogs = replicas.stream().collect(Collectors.toMap(
        Function.identity(),
        inst -> offset.logs(inst).stream().map(log -> toLog(inst, log)).collect(Collectors.toList())
        ));
        final Function<String, Integer> minimumReplicasFunc = minimumReplicasForCdc();
        final int requiredReplicas = this.cdcTables().stream()
                                         .map(CqlTable::keyspace)
                                         .mapToInt(minimumReplicasFunc::apply)
                                         .max()
                                         .orElse(1);
        if (replicaLogs.size() < requiredReplicas)
        {
            // we need *at least* local quorum for CDC to work, but if all nodes are up then read from LOCAL ALL
            throw new NotEnoughReplicasException(String.format("CDC requires at least %d replicas but only %d responded", requiredReplicas, replicaLogs.size()));
        }

        return replicaLogs;
    }

    @Override
    public CommitLogProvider commitLogs()
    {
        return () -> {
            final Set<CassandraInstance> replicas = ring().instances().stream()
                                                          .filter(inst -> dc == null || inst.dataCenter().equalsIgnoreCase(dc))
                                                          .collect(Collectors.toSet());
            final List<CompletableFuture<List<CommitLog>>> futures = replicas
                                                                     .stream()
                                                                     .map(this::listCommitLogs)
                                                                     .collect(Collectors.toList());

            // block to list replicas
            final List<List<CommitLog>> replicaLogs = FutureUtils
                                                      .awaitAll(futures,
                                                                true,
                                                                (throwable -> LOGGER.warn("Failed to list CDC commit logs on instance",
                                                                                          (ThrowableUtils.rootCause(throwable)))));

            return replicaLogs.stream()
                              .flatMap(Collection::stream);
        };
    }

    public abstract ReplicationFactor rf(String keyspace);

    @Override
    public Function<String, Integer> minimumReplicasForCdc()
    {
        return (keyspace) -> RF_CACHE.computeIfAbsent(keyspace, (ks) -> {
            ReplicationFactor rf = rf(keyspace);
            validateReplicationFactor(rf);
            return consistencyLevel.blockFor(rf, dc);
        });
    }

    public static void clearRfCache()
    {
        RF_CACHE.clear();
    }
    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(59, 61)
               .append(dc)
               .toHashCode();
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

        final PartitionedDataLayer rhs = (PartitionedDataLayer) obj;
        return new EqualsBuilder()
               .append(dc, rhs.dc)
               .isEquals();
    }
}
