package org.apache.cassandra.spark.reader;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.ICassandraSource;
import org.apache.cassandra.spark.cdc.SparkCdcEvent;
import org.apache.cassandra.spark.cdc.TableIdLookup;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.data.SparkCqlField;
import org.apache.cassandra.spark.data.SparkCqlTable;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.fourzero.FourZero;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.CellPath;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.stats.ICdcStats;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.TimeProvider;
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
 * Provides an abstract interface for all calls to shaded Cassandra code
 */
@SuppressWarnings({ "WeakerAccess", "unused" })
public abstract class CassandraBridge
{

    @VisibleForTesting // Used to indicate if a column is unset. Used in generating mutations for commit log.
    public static final Object UNSET_MARKER = new Object();

    public static final class CollectionElement
    {
        // the path to store the value in the collection. Consider it as the key
        public final CellPath cellPath;
        // the value to be stored in the collection.
        public final Object value;

        private CollectionElement(CellPath cellPath, Object value)
        {
            this.cellPath = cellPath;
            this.value = value;
        }

        public static CollectionElement living(CellPath cellPath, Object value)
        {
            return new CollectionElement(cellPath, value);
        }

        public static CollectionElement deleted(CellPath cellPath)
        {
            return new CollectionElement(cellPath, null);
        }
    }

    public static final Set<CassandraVersion> SUPPORTED_VERSIONS = new HashSet<>(Arrays.asList(CassandraVersion.THREEZERO, CassandraVersion.FOURZERO));
    private static final AtomicReference<FourZero> FOUR_ZERO = new AtomicReference<>();

    public static CassandraBridge get(final CassandraVersion version)
    {
        switch (version)
        {
            case THREEZERO:
            case FOURZERO:
                final FourZero fourZero = FOUR_ZERO.get();
                if (fourZero != null)
                {
                    return fourZero;
                }
                FOUR_ZERO.compareAndSet(null, new FourZero());
                return FOUR_ZERO.get();
        }
        throw new UnsupportedOperationException("Cassandra " + version.name + " is unsupported");
    }

    public abstract Pair<ByteBuffer, BigInteger> getPartitionKey(@NotNull final CqlTable schema,
                                                                 @NotNull final Partitioner partitioner,
                                                                 @NotNull final List<String> keys);

    public abstract TimeProvider timeProvider();

    public abstract IStreamScanner<SparkCdcEvent> getCdcScanner(final int partitionId,
                                                                @NotNull final Set<CqlTable> cdcTables,
                                                                @NotNull final Partitioner partitioner,
                                                                @NotNull final TableIdLookup tableIdLookup,
                                                                @NotNull final ICdcStats stats,
                                                                @Nullable final RangeFilter rangeFilter,
                                                                @NotNull final CdcOffsetFilter offset,
                                                                final Function<String, Integer> minimumReplicasFunc,
                                                                @NotNull final Watermarker watermarker,
                                                                @NotNull final String jobId,
                                                                @NotNull final ExecutorService executorService,
                                                                final boolean readCommitLogHeader,
                                                                @NotNull final Map<CassandraInstance, List<CommitLog>> logs,
                                                                final int cdcSubMicroBatchSize,
                                                                ICassandraSource cassandraSource);

    // Compaction Stream Scanner
    public abstract IStreamScanner<Rid> getCompactionScanner(@NotNull final CqlTable schema,
                                                             @NotNull final Partitioner partitionerType,
                                                             @NotNull final SSTablesSupplier ssTables,
                                                             @Nullable final RangeFilter rangeFilter,
                                                             @NotNull final Collection<PartitionKeyFilter> partitionKeyFilters,
                                                             @Nullable final PruneColumnFilter columnFilter,
                                                             @NotNull final TimeProvider timeProvider,
                                                             final boolean readIndexOffset,
                                                             final boolean useIncrementalRepair,
                                                             @NotNull final Stats stats);

    public abstract CassandraVersion getVersion();

    public abstract BigInteger hash(final Partitioner partitioner, final ByteBuffer key);

    public UUID getTimeUUID()
    {
        return types().getTimeUUID();
    }

    // CQL Schema

    public CqlTable buildSchema(final String keyspace,
                                final String createStmt,
                                final ReplicationFactor rf,
                                final Partitioner partitioner)
    {
        return buildSchema(keyspace, createStmt, rf, partitioner, Collections.emptySet(), null);
    }

    public CqlTable buildSchema(final String keyspace,
                                final String createStmt,
                                final ReplicationFactor rf,
                                final Partitioner partitioner,
                                final Set<String> udts)
    {
        return buildSchema(keyspace, createStmt, rf, partitioner, udts, null);

    }

    public CqlTable buildSchema(final String keyspace,
                                final String createStmt,
                                final ReplicationFactor rf,
                                final Partitioner partitioner,
                                final Set<String> udts,
                                @Nullable final UUID tableId)
    {
        return buildSchema(keyspace, createStmt, rf, partitioner, udts, tableId, false);
    }

    public CqlTable buildSchema(final String keyspace,
                                final String createStmt,
                                final ReplicationFactor rf,
                                final Partitioner partitioner,
                                final Set<String> udts,
                                @Nullable final UUID tableId,
                                final boolean enableCdc)
    {
        return decorate(types().buildSchema(keyspace, createStmt, rf, partitioner, udts, tableId, enableCdc));
    }

    // cql type parsing

    public abstract CassandraTypes types();

    public List<SparkCqlField.SparkCqlType> allTypes()
    {
        return types().allTypes().stream().map(this::decorate).collect(Collectors.toList());
    }

    public Map<String, ? extends CqlField.NativeType> nativeTypeNames()
    {
        return types().nativeTypeNames();
    }

    public SparkCqlField.SparkCqlType nativeType(String name)
    {
        return decorate(types().nativeType(name));
    }

    public List<SparkCqlField.SparkCqlType> supportedTypes()
    {
        return allTypes().stream()
                         .filter(SparkCqlField.SparkCqlType::isSupported)
                         .map(this::decorate)
                         .collect(Collectors.toList());
    }

    public SparkCqlTable decorate(CqlTable table)
    {
        return new SparkCqlTable(
        table.keyspace(),
        table.table(),
        table.createStmt(),
        table.replicationFactor(),
        decorate(table.fields()),
        decorateUdts(table.udts())
        );
    }


    public Set<CqlField.CqlUdt> decorateUdts(Set<CqlField.CqlUdt> udts)
    {
        return udts.stream()
                   .map(udt -> (CqlField.CqlUdt) decorate(udt))
                   .collect(Collectors.toSet());
    }

    public List<CqlField> decorate(List<CqlField> fields)
    {
        return fields.stream()
                     .map(this::decorate)
                     .collect(Collectors.toList());
    }

    public abstract SparkCqlField decorate(CqlField field);

    public abstract SparkCqlField.SparkCqlType decorate(CqlField.CqlType field);

    // native

    public SparkCqlField.SparkCqlType ascii()
    {
        return decorate(types().ascii());
    }

    public SparkCqlField.SparkCqlType blob()
    {
        return decorate(types().blob());
    }

    public SparkCqlField.SparkCqlType bool()
    {
        return decorate(types().bool());
    }

    public SparkCqlField.SparkCqlType counter()
    {
        return decorate(types().counter());
    }

    public SparkCqlField.SparkCqlType bigint()
    {
        return decorate(types().bigint());
    }

    public SparkCqlField.SparkCqlType date()
    {
        return decorate(types().date());
    }

    public SparkCqlField.SparkCqlType decimal()
    {
        return decorate(types().decimal());
    }

    public SparkCqlField.SparkCqlType aDouble()
    {
        return decorate(types().aDouble());
    }

    public SparkCqlField.SparkCqlType duration()
    {
        return decorate(types().duration());
    }

    public SparkCqlField.SparkCqlType empty()
    {
        return decorate(types().empty());
    }

    public SparkCqlField.SparkCqlType aFloat()
    {
        return decorate(types().aFloat());
    }

    public SparkCqlField.SparkCqlType inet()
    {
        return decorate(types().inet());
    }

    public SparkCqlField.SparkCqlType aInt()
    {
        return decorate(types().aInt());
    }

    public SparkCqlField.SparkCqlType smallint()
    {
        return decorate(types().smallint());
    }

    public SparkCqlField.SparkCqlType text()
    {
        return decorate(types().text());
    }

    public SparkCqlField.SparkCqlType time()
    {
        return decorate(types().time());
    }

    public SparkCqlField.SparkCqlType timestamp()
    {
        return decorate(types().timestamp());
    }

    public SparkCqlField.SparkCqlType timeuuid()
    {
        return decorate(types().timeuuid());
    }

    public SparkCqlField.SparkCqlType tinyint()
    {
        return decorate(types().tinyint());
    }

    public SparkCqlField.SparkCqlType uuid()
    {
        return decorate(types().uuid());
    }

    public SparkCqlField.SparkCqlType varchar()
    {
        return decorate(types().varchar());
    }

    public SparkCqlField.SparkCqlType varint()
    {
        return decorate(types().varint());
    }

    // complex

    public SparkCqlField.SparkCqlType collection(final String name, final CqlField.CqlType... types)
    {
        return decorate(types().collection(name, types));
    }

    public SparkCqlField.SparkList list(final CqlField.CqlType type)
    {
        return (SparkCqlField.SparkList) decorate(types().list(type));
    }

    public SparkCqlField.SparkSet set(final CqlField.CqlType type)
    {
        return (SparkCqlField.SparkSet) decorate(types().set(type));
    }

    public SparkCqlField.SparkMap map(final CqlField.CqlType keyType, final CqlField.CqlType valueType)
    {
        return (SparkCqlField.SparkMap) decorate(types().map(keyType, valueType));
    }

    public SparkCqlField.SparkTuple tuple(final CqlField.CqlType... types)
    {
        return (SparkCqlField.SparkTuple) decorate(types().tuple(types));
    }

    public SparkCqlField.SparkFrozen frozen(final CqlField.CqlType type)
    {
        return (SparkCqlField.SparkFrozen) decorate(types().frozen(type));
    }

    public abstract SparkCqlField.SparkUdtBuilder udt(final String keyspace, final String name);

    public CqlField.CqlType parseType(final String type)
    {
        return decorate(types().parseType(type));
    }

    public SparkCqlField.SparkCqlType parseType(final String type, final Map<String, CqlField.CqlUdt> udts)
    {
        return decorate(types().parseType(type, udts));
    }

    // sstable writer

    public interface IWriter
    {
        void write(Object... values);
    }

    public void writeSSTable(final Partitioner partitioner, final String keyspace, final Path dir, final String createStmt, final String insertStmt, final Consumer<IWriter> writer)
    {
        writeSSTable(partitioner, keyspace, dir, createStmt, insertStmt, null, false, Collections.emptySet(), writer);
    }

    public abstract void writeSSTable(final Partitioner partitioner, final String keyspace, final Path dir,
                                      final String createStmt, final String insertStmt, final String updateStmt,
                                      final boolean upsert, final Set<CqlField.CqlUdt> udts, final Consumer<IWriter> writer);


    // CommitLog

    public interface IMutation
    {

    }

    public interface IRow
    {
        Object get(int pos);

        /**
         * Indicate whether the entire row is deleted
         */
        default boolean isDeleted()
        {
            return false;
        }

        /**
         * Indicate whether the row is from an INSERT statement
         */
        default boolean isInsert()
        {
            return true;
        }

        /**
         * Get the range tombstones for this partition (todo: IRow is used as a partition. Semantically, it does not fit)
         *
         * @return null if no range tombstones exist. Otherwise, return a list of range tombstones.
         */
        default List<RangeTombstoneData> rangeTombstones()
        {
            return null;
        }

        /**
         * TTL in second. 0 means no ttl.
         *
         * @return ttl
         */
        default int ttl()
        {
            return CqlField.NO_TTL;
        }
    }

    @VisibleForTesting // It is used to generated test data.
    public static class RangeTombstoneData
    {
        public final Bound open;
        public final Bound close;

        public RangeTombstoneData(Bound open, Bound close)
        {
            this.open = open;
            this.close = close;
        }

        public static class Bound
        {
            public final Object[] values;
            public final boolean inclusive;

            public Bound(Object[] values, boolean inclusive)
            {
                this.values = values;
                this.inclusive = inclusive;
            }
        }
    }

    public interface ICommitLog
    {
        void start();

        void stop();

        void clear();

        void add(IMutation mutation);

        void sync();
    }

    /**
     * Cassandra version specific implementation for logging a row mutation to commit log. Used for CDC unit test framework.
     *
     * @param schema    cql schema
     * @param log       commit log instance
     * @param row       row instance
     * @param timestamp mutation timestamp
     */
    @VisibleForTesting
    public abstract void log(CqlTable schema, ICommitLog log, IRow row, long timestamp);

    /**
     * Determine whether a row is a partition deletion.
     * It is a partition deletion, when all fields except the partition keys are null
     *
     * @param schema cql schema
     * @param row    row instance
     * @return true if it is a partition deletion
     */
    protected abstract boolean isPartitionDeletion(CqlTable schema, IRow row);

    /**
     * Determine whether a row is a row deletion
     * It is a row deletion, when all fields except the primary keys are null
     *
     * @param schema cql schema
     * @param row    row instance
     * @return true if it is a row deletion
     */
    protected abstract boolean isRowDeletion(CqlTable schema, IRow row);
}
