package org.apache.cassandra.spark.reader;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.esotericsoftware.kryo.io.Input;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.TableIdLookup;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.fourzero.FourZero;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
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
    public enum CassandraVersion
    {
        TWOONE("2.1"), THREEZERO("3.0"), FOURZERO("4.0");
        private final String name;

        CassandraVersion(final String name)
        {
            this.name = name;
        }

        public String versionName()
        {
            return name;
        }
    }

    public interface BigNumberConfig
    {
        BigNumberConfig DEFAULT = new BigNumberConfig()
        {
            public int bigIntegerPrecision()
            {
                return 38;
            }

            public int bigIntegerScale()
            {
                return 0;
            }

            public int bigDecimalPrecision()
            {
                return 38;
            }

            public int bigDecimalScale()
            {
                return 19;
            }
        };

        int bigIntegerPrecision();

        int bigIntegerScale();

        int bigDecimalPrecision();

        int bigDecimalScale();
    }

    @VisibleForTesting // Used to indicate if a column is unset. Used in generating mutations for commit log.
    public static final Object UNSET_MARKER = new Object();

    public static final Pattern COLLECTIONS_PATTERN = Pattern.compile("^(set|list|map|tuple)<(.+)>$", Pattern.CASE_INSENSITIVE);
    public static final Pattern FROZEN_PATTERN = Pattern.compile("^frozen<(.*)>$", Pattern.CASE_INSENSITIVE);

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

    public abstract Pair<ByteBuffer, BigInteger> getPartitionKey(@NotNull final CqlSchema schema,
                                                                 @NotNull final Partitioner partitioner,
                                                                 @NotNull final List<String> keys);

    public abstract TimeProvider timeProvider();

    public abstract IStreamScanner getCdcScanner(@NotNull final CqlSchema schema,
                                                 @NotNull final Partitioner partitioner,
                                                 @NotNull final CommitLogProvider commitLogProvider,
                                                 @NotNull final TableIdLookup tableIdLookup,
                                                 @NotNull final Stats stats,
                                                 @Nullable final SparkRangeFilter sparkRangeFilter,
                                                 @Nullable final CdcOffsetFilter offset,
                                                 final int minimumReplicasPerMutation,
                                                 @NotNull final Watermarker watermarker,
                                                 @NotNull final String jobId,
                                                 @NotNull final ExecutorService executorService);

    // Compaction Stream Scanner
    public abstract IStreamScanner getCompactionScanner(@NotNull final CqlSchema schema,
                                                        @NotNull final Partitioner partitionerType,
                                                        @NotNull final SSTablesSupplier ssTables,
                                                        @Nullable final SparkRangeFilter sparkRangeFilter,
                                                        @NotNull final Collection<PartitionKeyFilter> partitionKeyFilters,
                                                        @Nullable final PruneColumnFilter columnFilter,
                                                        @NotNull final TimeProvider timeProvider,
                                                        final boolean readIndexOffset,
                                                        final boolean useIncrementalRepair,
                                                        @NotNull final Stats stats);

    public abstract CassandraBridge.CassandraVersion getVersion();

    public abstract BigInteger hash(final Partitioner partitioner, final ByteBuffer key);

    public abstract UUID getTimeUUID();

    // CQL Schema

    public abstract CqlSchema buildSchema(final String keyspace,
                                          final String createStmt,
                                          final ReplicationFactor rf,
                                          final Partitioner partitioner,
                                          final Set<String> udts,
                                          @Nullable final UUID tableId);

    // cql type parsing

    public abstract CqlField.CqlType readType(CqlField.CqlType.InternalType type, Input input);

    public List<CqlField.NativeType> allTypes()
    {
        return Arrays.asList(ascii(), bigint(), blob(), bool(), counter(), date(), decimal(), aDouble(), duration(), empty(), aFloat(),
                             inet(), aInt(), smallint(), text(), time(), timestamp(), timeuuid(), tinyint(), uuid(), varchar(), varint());
    }

    public abstract Map<String, ? extends CqlField.NativeType> nativeTypeNames();

    public CqlField.NativeType nativeType(String name)
    {
        return nativeTypeNames().get(name.toLowerCase());
    }

    public List<CqlField.NativeType> supportedTypes()
    {
        return allTypes().stream().filter(CqlField.NativeType::isSupported).collect(Collectors.toList());
    }

    // native

    public abstract CqlField.NativeType ascii();

    public abstract CqlField.NativeType blob();

    public abstract CqlField.NativeType bool();

    public abstract CqlField.NativeType counter();

    public abstract CqlField.NativeType bigint();

    public abstract CqlField.NativeType date();

    public abstract CqlField.NativeType decimal();

    public abstract CqlField.NativeType aDouble();

    public abstract CqlField.NativeType duration();

    public abstract CqlField.NativeType empty();

    public abstract CqlField.NativeType aFloat();

    public abstract CqlField.NativeType inet();

    public abstract CqlField.NativeType aInt();

    public abstract CqlField.NativeType smallint();

    public abstract CqlField.NativeType text();

    public abstract CqlField.NativeType time();

    public abstract CqlField.NativeType timestamp();

    public abstract CqlField.NativeType timeuuid();

    public abstract CqlField.NativeType tinyint();

    public abstract CqlField.NativeType uuid();

    public abstract CqlField.NativeType varchar();

    public abstract CqlField.NativeType varint();

    // complex

    public abstract CqlField.CqlType collection(final String name, final CqlField.CqlType... types);

    public abstract CqlField.CqlList list(final CqlField.CqlType type);

    public abstract CqlField.CqlSet set(final CqlField.CqlType type);

    public abstract CqlField.CqlMap map(final CqlField.CqlType keyType, final CqlField.CqlType valueType);

    public abstract CqlField.CqlTuple tuple(final CqlField.CqlType... types);

    public abstract CqlField.CqlType frozen(final CqlField.CqlType type);

    public abstract CqlField.CqlUdtBuilder udt(final String keyspace, final String name);

    public CqlField.CqlType parseType(final String type)
    {
        return parseType(type, Collections.emptyMap());
    }

    public CqlField.CqlType parseType(final String type, final Map<String, CqlField.CqlUdt> udts)
    {
        if (StringUtils.isEmpty(type))
        {
            return null;
        }
        final Matcher matcher = COLLECTIONS_PATTERN.matcher(type);
        if (matcher.find())
        {
            // cql collection
            final String[] types = splitInnerTypes(matcher.group(2));
            return collection(matcher.group(1), Stream.of(types).map(t -> parseType(t, udts)).toArray(CqlField.CqlType[]::new));
        }
        final Matcher frozenMatcher = FROZEN_PATTERN.matcher(type);
        if (frozenMatcher.find())
        {
            // frozen collections
            return frozen(parseType(frozenMatcher.group(1), udts));
        }

        if (udts.containsKey(type))
        {
            // user defined type
            return udts.get(type);
        }

        // native cql 3 type
        return nativeType(type);
    }

    @VisibleForTesting
    public static String[] splitInnerTypes(final String str)
    {
        final List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int parentheses = 0;
        for (int i = 0; i < str.length(); i++)
        {
            final char c = str.charAt(i);
            switch (c)
            {
                case ' ':
                    if (parentheses == 0)
                    {
                        continue;
                    }
                    break;
                case ',':
                    if (parentheses == 0)
                    {
                        if (current.length() > 0)
                        {
                            result.add(current.toString());
                            current = new StringBuilder();
                        }
                        continue;
                    }
                    break;
                case '<':
                    parentheses++;
                    break;
                case '>':
                    parentheses--;
                    break;
            }
            current.append(c);
        }

        if (current.length() > 0 || result.isEmpty())
        {
            result.add(current.toString());
        }

        return result.toArray(new String[0]);
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
    public abstract void log(CqlSchema schema, ICommitLog log, IRow row, long timestamp);

    /**
     * Determine whether a row is a partition deletion.
     * It is a partition deletion, when all fields except the partition keys are null
     *
     * @param schema cql schema
     * @param row    row instance
     * @return true if it is a partition deletion
     */
    protected abstract boolean isPartitionDeletion(CqlSchema schema, IRow row);

    /**
     * Determine whether a row is a row deletion
     * It is a row deletion, when all fields except the parimary keys are null
     *
     * @param schema cql schema
     * @param row    row instance
     * @return true if it is a row deletion
     */
    protected abstract boolean isRowDeletion(CqlSchema schema, IRow row);
}
