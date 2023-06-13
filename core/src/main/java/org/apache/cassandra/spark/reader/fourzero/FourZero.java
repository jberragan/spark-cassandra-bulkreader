package org.apache.cassandra.spark.reader.fourzero;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.ICassandraSource;
import org.apache.commons.lang3.tuple.Pair;

import com.esotericsoftware.kryo.io.Input;
import org.apache.cassandra.spark.cdc.SparkCdcEvent;
import org.apache.cassandra.spark.cdc.TableIdLookup;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.data.fourzero.FourZeroCqlType;
import org.apache.cassandra.spark.data.fourzero.complex.CqlCollection;
import org.apache.cassandra.spark.data.fourzero.complex.CqlFrozen;
import org.apache.cassandra.spark.data.fourzero.complex.CqlList;
import org.apache.cassandra.spark.data.fourzero.complex.CqlMap;
import org.apache.cassandra.spark.data.fourzero.complex.CqlSet;
import org.apache.cassandra.spark.data.fourzero.complex.CqlTuple;
import org.apache.cassandra.spark.data.fourzero.complex.CqlUdt;
import org.apache.cassandra.spark.data.fourzero.types.Ascii;
import org.apache.cassandra.spark.data.fourzero.types.BigInt;
import org.apache.cassandra.spark.data.fourzero.types.Blob;
import org.apache.cassandra.spark.data.fourzero.types.Boolean;
import org.apache.cassandra.spark.data.fourzero.types.Counter;
import org.apache.cassandra.spark.data.fourzero.types.Date;
import org.apache.cassandra.spark.data.fourzero.types.Decimal;
import org.apache.cassandra.spark.data.fourzero.types.Double;
import org.apache.cassandra.spark.data.fourzero.types.Duration;
import org.apache.cassandra.spark.data.fourzero.types.Empty;
import org.apache.cassandra.spark.data.fourzero.types.Float;
import org.apache.cassandra.spark.data.fourzero.types.Inet;
import org.apache.cassandra.spark.data.fourzero.types.Int;
import org.apache.cassandra.spark.data.fourzero.types.SmallInt;
import org.apache.cassandra.spark.data.fourzero.types.Text;
import org.apache.cassandra.spark.data.fourzero.types.Time;
import org.apache.cassandra.spark.data.fourzero.types.TimeUUID;
import org.apache.cassandra.spark.data.fourzero.types.Timestamp;
import org.apache.cassandra.spark.data.fourzero.types.TinyInt;
import org.apache.cassandra.spark.data.fourzero.types.VarChar;
import org.apache.cassandra.spark.data.fourzero.types.VarInt;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.reader.Rid;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.config.Config;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.config.ParameterizedClass;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Clustering;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DecoratedKey;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DeletionTime;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Keyspace;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.LivenessInfo;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Mutation;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.CommitLogSegmentManagerCDC;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Row;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Rows;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.IPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Schema;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableId;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.security.EncryptionContext;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.UUIDGen;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.ColumnTypes;
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

public class FourZero extends CassandraBridge
{
    private static volatile boolean setup = false;

    private final Map<String, CqlField.NativeType> nativeTypes;

    static
    {
        setup();
    }

    public synchronized static void setup()
    {
        if (setup)
        {
            return;
        }

        Config.setClientMode(true);
        // When we create a TableStreamScanner, we will set the partitioner directly on the table metadata
        // using the supplied IIndexStreamScanner.Partitioner. CFMetaData::compile requires a partitioner to
        // be set in DatabaseDescriptor before we can do that though, so we set one here in preparation.
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        DatabaseDescriptor.clientInitialization();
        final Config config = DatabaseDescriptor.getRawConfig();
        config.memtable_flush_writers = 8;
        config.diagnostic_events_enabled = false;
        config.max_mutation_size_in_kb = config.commitlog_segment_size_in_mb * 1024 / 2;
        config.concurrent_compactors = 4;
        final Path tmpDir;
        try
        {
            tmpDir = Files.createTempDirectory(UUID.randomUUID().toString());
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
        config.data_file_directories = new String[]{ tmpDir.toString() };
        DatabaseDescriptor.setEndpointSnitch(new SimpleSnitch());
        Keyspace.setInitialized();
        setup = true;
    }

    public FourZero()
    {
        this.nativeTypes = allTypes().stream().collect(Collectors.toMap(CqlField.CqlType::name, Function.identity()));
    }

    public static void setCommitLogPath(Path path)
    {
        DatabaseDescriptor.getRawConfig().commitlog_directory = path + "/commitlog";
        DatabaseDescriptor.getRawConfig().hints_directory = path + "/hints";
        DatabaseDescriptor.getRawConfig().saved_caches_directory = path + "/saved_caches";
    }

    public static void setCDC(Path path, final int commitLogSegmentSize)
    {
        DatabaseDescriptor.getRawConfig().cdc_raw_directory = path + "/cdc";
        DatabaseDescriptor.setCDCEnabled(true);
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.periodic);
        DatabaseDescriptor.setCommitLogCompression(new ParameterizedClass("LZ4Compressor", ImmutableMap.of()));
        DatabaseDescriptor.setEncryptionContext(new EncryptionContext());
        DatabaseDescriptor.setCommitLogSyncPeriod(30);
        DatabaseDescriptor.setCommitLogMaxCompressionBuffersPerPool(3);
        DatabaseDescriptor.setCommitLogSyncGroupWindow(30);
        DatabaseDescriptor.setCommitLogSegmentSize(commitLogSegmentSize);
        DatabaseDescriptor.getRawConfig().commitlog_total_space_in_mb = 1024;
        DatabaseDescriptor.setCommitLogSegmentMgrProvider((commitLog -> new CommitLogSegmentManagerCDC(commitLog, path + "/commitlog")));
    }

    @Override
    public Pair<ByteBuffer, BigInteger> getPartitionKey(@NotNull final CqlTable schema,
                                                        @NotNull final Partitioner partitioner,
                                                        @NotNull final List<String> keys)
    {
        Preconditions.checkArgument(schema.partitionKeys().size() > 0);
        final ByteBuffer partitionKey = buildPartitionKey(schema, keys);
        final BigInteger partitionKeyTokenValue = hash(partitioner, partitionKey);
        return Pair.of(partitionKey, partitionKeyTokenValue);
    }

    public static ByteBuffer buildPartitionKey(@NotNull final CqlTable schema,
                                               @NotNull final List<String> keys)
    {
        final List<AbstractType<?>> partitionKeyColumnTypes = partitionKeyColumnTypes(schema);
        if (schema.partitionKeys().size() == 1)
        {
            // single partition key
            return partitionKeyColumnTypes.get(0).fromString(keys.get(0));
        }
        else
        {
            // composite partition key
            final ByteBuffer[] bufs = new ByteBuffer[keys.size()];
            for (int i = 0; i < bufs.length; i++)
            {
                bufs[i] = partitionKeyColumnTypes.get(i).fromString(keys.get(i));
            }
            return CompositeType.build(ByteBufferAccessor.instance, bufs);
        }
    }

    public static List<AbstractType<?>> partitionKeyColumnTypes(CqlTable schema)
    {
        return schema.partitionKeys()
                     .stream()
                     .map(CqlField::type)
                     .map(type -> (FourZeroCqlType) type)
                     .map(type -> type.dataType(true))
                     .collect(Collectors.toList());
    }

    @Override
    public TimeProvider timeProvider()
    {
        return TimeProvider.INSTANCE;
    }

    @Override
    public IStreamScanner<SparkCdcEvent> getCdcScanner(final int partitionId,
                                                       @NotNull final Set<CqlTable> cdcTables,
                                                       @NotNull final Partitioner partitioner,
                                                       @NotNull final TableIdLookup tableIdLookup,
                                                       @NotNull final Stats stats,
                                                       @Nullable final RangeFilter rangeFilter,
                                                       @NotNull final CdcOffsetFilter offset,
                                                       final Function<String, Integer> minimumReplicasFunc,
                                                       @NotNull final Watermarker watermarker,
                                                       @NotNull final String jobId,
                                                       @NotNull final ExecutorService executorService,
                                                       final boolean readCommitLogHeader,
                                                       @NotNull final Map<CassandraInstance, List<CommitLog>> logs,
                                                       final int cdcSubMicroBatchSize,
                                                       ICassandraSource cassandraSource)
    {
        updateCdcSchema(Schema.instance, cdcTables, partitioner, tableIdLookup);

        //NOTE: need to use SchemaBuilder to init keyspace if not already set in C* Schema instance
        return new SparkCdcScannerBuilder(partitionId, partitioner,
                                          stats, rangeFilter,
                                          offset, minimumReplicasFunc,
                                          watermarker, jobId,
                                          executorService, readCommitLogHeader, logs, cdcSubMicroBatchSize, cassandraSource).build();
    }

    public static void updateCdcSchema(@NotNull final Schema schema,
                                       @NotNull final Set<CqlTable> cdcTables,
                                       @NotNull final Partitioner partitioner,
                                       @NotNull final TableIdLookup tableIdLookup)
    {
        final Map<String, Set<String>> cdcEnabledTables = SchemaUtils.cdcEnabledTables(schema);
        for (final CqlTable table : cdcTables)
        {
            final UUID tableId = tableIdLookup.lookup(table.keyspace(), table.table());
            if (cdcEnabledTables.containsKey(table.keyspace()) && cdcEnabledTables.get(table.keyspace()).contains(table.table()))
            {
                // table has cdc enabled already, update schema if it has changed
                cdcEnabledTables.get(table.keyspace()).remove(table.table());
                SchemaUtils.maybeUpdateSchema(schema, partitioner, table, tableId, true);
                continue;
            }

            if (SchemaUtils.has(schema, table))
            {
                // update schema if changed for existing table
                SchemaUtils.maybeUpdateSchema(schema, partitioner, table, tableId, true);
                continue;
            }

            // new table so initialize table with cdc = true
            new FourZeroSchemaBuilder(table, partitioner, tableId, true);
            if (tableId != null)
            {
                // verify TableMetadata and ColumnFamilyStore initialized in Schema
                final TableId tableIdAfter = TableId.fromUUID(tableId);
                Preconditions.checkNotNull(schema.getTableMetadata(tableIdAfter), "Table not initialized in the schema");
                Preconditions.checkArgument(Objects.requireNonNull(schema.getKeyspaceInstance(table.keyspace())).hasColumnFamilyStore(tableIdAfter),
                                            "ColumnFamilyStore not initialized in the schema");
            }
        }
        // existing table no longer with cdc = true, so disable
        cdcEnabledTables.forEach((ks, tables) -> tables.forEach(table -> SchemaUtils.disableCdc(schema, ks, table)));
    }

    @Override
    public IStreamScanner<Rid> getCompactionScanner(@NotNull final CqlTable schema,
                                                    @NotNull final Partitioner partitioner,
                                                    @NotNull final SSTablesSupplier ssTables,
                                                    @Nullable final RangeFilter rangeFilter,
                                                    @NotNull final Collection<PartitionKeyFilter> partitionKeyFilters,
                                                    @Nullable final PruneColumnFilter columnFilter,
                                                    @NotNull final TimeProvider timeProvider,
                                                    final boolean readIndexOffset,
                                                    final boolean useIncrementalRepair,
                                                    @NotNull final Stats stats)
    {
        //NOTE: need to use SchemaBuilder to init keyspace if not already set in C* Schema instance
        final FourZeroSchemaBuilder schemaBuilder = new FourZeroSchemaBuilder(schema, partitioner);
        final TableMetadata metadata = schemaBuilder.tableMetaData();
        return new CompactionStreamScanner(metadata, partitioner, timeProvider, ssTables.openAll(
        ((ssTable, isRepairPrimary) -> FourZeroSSTableReader.builder(metadata, ssTable)
                                                            .withRangeFilter(rangeFilter)
                                                            .withPartitionKeyFilters(partitionKeyFilters)
                                                            .withColumnFilter(columnFilter)
                                                            .withReadIndexOffset(readIndexOffset)
                                                            .withStats(stats)
                                                            .useIncrementalRepair(useIncrementalRepair)
                                                            .isRepairPrimary(isRepairPrimary)
                                                            .build())
        ));
    }

    @Override
    public CassandraVersion getVersion()
    {
        return CassandraVersion.FOURZERO;
    }

    @Override
    public BigInteger hash(final Partitioner partitioner, final ByteBuffer key)
    {
        switch (partitioner)
        {
            case RandomPartitioner:
                return RandomPartitioner.instance.getToken(key).getTokenValue();
            case Murmur3Partitioner:
                return BigInteger.valueOf((long) Murmur3Partitioner.instance.getToken(key).getTokenValue());
        }
        throw new UnsupportedOperationException("Unexpected partitioner: " + partitioner);
    }

    @Override
    public UUID getTimeUUID()
    {
        return UUIDGen.getTimeUUID();
    }

    @Override
    public CqlTable buildSchema(final String keyspace,
                                final String createStmt,
                                final ReplicationFactor rf,
                                final Partitioner partitioner,
                                final Set<String> udts,
                                @Nullable final UUID tableId,
                                final boolean enableCdc)
    {
        return new FourZeroSchemaBuilder(createStmt, keyspace, rf, partitioner, udts, tableId, enableCdc).build();
    }

    // cql type parser

    @Override
    public Map<String, ? extends CqlField.NativeType> nativeTypeNames()
    {
        return nativeTypes;
    }

    @Override
    public CqlField.CqlType readType(CqlField.CqlType.InternalType type, Input input)
    {
        switch (type)
        {
            case NativeCql:
                return nativeType(input.readString());
            case Set:
            case List:
            case Map:
            case Tuple:
                return CqlCollection.read(type, input);
            case Frozen:
                return CqlFrozen.build(CqlField.CqlType.read(input));
            case Udt:
                return CqlUdt.read(input);
            default:
                throw new IllegalStateException("Unknown cql type, cannot deserialize");
        }
    }

    @Override
    public Ascii ascii()
    {
        return Ascii.INSTANCE;
    }

    @Override
    public Blob blob()
    {
        return Blob.INSTANCE;
    }

    @Override
    public Boolean bool()
    {
        return Boolean.INSTANCE;
    }

    @Override
    public Counter counter()
    {
        return Counter.INSTANCE;
    }

    @Override
    public BigInt bigint()
    {
        return BigInt.INSTANCE;
    }

    @Override
    public Date date()
    {
        return Date.INSTANCE;
    }

    @Override
    public Decimal decimal()
    {
        return Decimal.INSTANCE;
    }

    @Override
    public Double aDouble()
    {
        return Double.INSTANCE;
    }

    @Override
    public Duration duration()
    {
        return Duration.INSTANCE;
    }

    @Override
    public Empty empty()
    {
        return Empty.INSTANCE;
    }

    @Override
    public Float aFloat()
    {
        return Float.INSTANCE;
    }

    @Override
    public Inet inet()
    {
        return Inet.INSTANCE;
    }

    @Override
    public Int aInt()
    {
        return Int.INSTANCE;
    }

    @Override
    public SmallInt smallint()
    {
        return SmallInt.INSTANCE;
    }

    @Override
    public Text text()
    {
        return Text.INSTANCE;
    }

    @Override
    public Time time()
    {
        return Time.INSTANCE;
    }

    @Override
    public Timestamp timestamp()
    {
        return Timestamp.INSTANCE;
    }

    @Override
    public TimeUUID timeuuid()
    {
        return TimeUUID.INSTANCE;
    }

    @Override
    public TinyInt tinyint()
    {
        return TinyInt.INSTANCE;
    }

    @Override
    public org.apache.cassandra.spark.data.fourzero.types.UUID uuid()
    {
        return org.apache.cassandra.spark.data.fourzero.types.UUID.INSTANCE;
    }

    @Override
    public VarChar varchar()
    {
        return VarChar.INSTANCE;
    }

    @Override
    public VarInt varint()
    {
        return VarInt.INSTANCE;
    }

    @Override
    public CqlField.CqlType collection(String name, CqlField.CqlType... types)
    {
        return CqlCollection.build(name, types);
    }

    @Override
    public CqlList list(CqlField.CqlType type)
    {
        return CqlCollection.list(type);
    }

    @Override
    public CqlSet set(CqlField.CqlType type)
    {
        return CqlCollection.set(type);
    }

    @Override
    public CqlMap map(CqlField.CqlType keyType, CqlField.CqlType valueType)
    {
        return CqlCollection.map(keyType, valueType);
    }

    @Override
    public CqlTuple tuple(CqlField.CqlType... types)
    {
        return CqlCollection.tuple(types);
    }

    @Override
    public CqlField.CqlType frozen(CqlField.CqlType type)
    {
        return CqlFrozen.build(type);
    }

    @Override
    public CqlField.CqlUdtBuilder udt(final String keyspace, final String name)
    {
        return CqlUdt.builder(keyspace, name);
    }

    @Override
    public synchronized void writeSSTable(final Partitioner partitioner,
                                          final String keyspace,
                                          final Path dir,
                                          final String createStmt,
                                          final String insertStmt,
                                          final String updateStmt,
                                          final boolean upsert,
                                          final Set<CqlField.CqlUdt> udts,
                                          final Consumer<IWriter> writer)
    {
        final CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                                 .inDirectory(dir.toFile())
                                                                 .forTable(createStmt)
                                                                 .withPartitioner(getPartitioner(partitioner))
                                                                 .using(upsert ? updateStmt : insertStmt)
                                                                 .withBufferSizeInMB(128);

        for (final CqlField.CqlUdt udt : udts)
        {
            // add user defined types to CQL writer
            builder.withType(udt.createStmt(keyspace));
        }

        try (final CQLSSTableWriter sstable = builder.build())
        {
            writer.accept(values -> {
                try
                {
                    sstable.addRow(values);
                }
                catch (final IOException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static IPartitioner getPartitioner(final Partitioner partitioner)
    {
        return partitioner == Partitioner.Murmur3Partitioner ? Murmur3Partitioner.instance : RandomPartitioner.instance;
    }

    // CommitLog

    @VisibleForTesting
    public static class FourZeroMutation implements IMutation
    {
        public final Mutation mutation;

        private FourZeroMutation(Mutation mutation)
        {
            this.mutation = mutation;
        }

        static FourZeroMutation wrap(Mutation mutation)
        {
            return new FourZeroMutation(mutation);
        }
    }

    @Override
    @VisibleForTesting
    public void log(CqlTable cqlTable, ICommitLog log, IRow row, long timestamp)
    {
        final Mutation mutation = makeMutation(cqlTable, row, timestamp);
        log.add(FourZeroMutation.wrap(mutation));
    }

    @NotNull
    @VisibleForTesting
    public Mutation makeMutation(CqlTable cqlTable, IRow row, long timestamp)
    {
        final TableMetadata table = Schema.instance.getTableMetadata(cqlTable.keyspace(), cqlTable.table());
        assert table != null;

        final Row.Builder rowBuilder = BTreeRow.sortedBuilder();
        if (row.isInsert())
        {
            rowBuilder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, timeProvider().now()));
        }
        Row staticRow = Rows.EMPTY_STATIC_ROW;

        // build partition key
        final List<CqlField> partitionKeys = cqlTable.partitionKeys();
        final ByteBuffer partitionKey = ColumnTypes.buildPartitionKey(partitionKeys,
                                                                      partitionKeys.stream()
                                                                                   .map(f -> row.get(f.pos()))
                                                                                   .toArray());

        final DecoratedKey decoratedPartitionKey = table.partitioner.decorateKey(partitionKey);
        // create a mutation and return early
        if (isPartitionDeletion(cqlTable, row))
        {
            PartitionUpdate delete = PartitionUpdate.fullPartitionDelete(table, partitionKey, timestamp, timeProvider().now());
            return new Mutation(delete);
        }

        final List<CqlField> clusteringKeys = cqlTable.clusteringKeys();

        // create a mutation with rangetombstones
        if (row.rangeTombstones() != null && !row.rangeTombstones().isEmpty())
        {
            PartitionUpdate.SimpleBuilder pub = PartitionUpdate.simpleBuilder(table, decoratedPartitionKey)
                                                               .timestamp(timestamp)
                                                               .nowInSec(timeProvider().now());
            for (RangeTombstoneData rt : row.rangeTombstones())
            {
                // range tombstone builder is built when partition update builder builds
                PartitionUpdate.SimpleBuilder.RangeTombstoneBuilder rtb = pub.addRangeTombstone();
                rtb = rt.open.inclusive ? rtb.inclStart() : rtb.exclStart(); // returns the same ref. just to make compiler happy
                Object[] startValues = clusteringKeys.stream()
                                                     .map(f -> {
                                                         Object v = rt.open.values[f.pos() - cqlTable.numPartitionKeys()];
                                                         return v == null ? null : f.serialize(v);
                                                     })
                                                     .filter(Objects::nonNull)
                                                     .toArray(ByteBuffer[]::new);
                rtb.start(startValues);
                rtb = rt.close.inclusive ? rtb.inclEnd() : rtb.exclEnd();
                Object[] endValues = clusteringKeys.stream()
                                                   .map(f -> {
                                                       Object v = rt.close.values[f.pos() - cqlTable.numPartitionKeys()];
                                                       return v == null ? null : f.serialize(v);
                                                   })
                                                   .filter(Objects::nonNull)
                                                   .toArray(ByteBuffer[]::new);
                rtb.end(endValues);
            }
            return new Mutation(pub.build());
        }

        // build clustering key
        if (clusteringKeys.isEmpty())
        {
            rowBuilder.newRow(Clustering.EMPTY);
        }
        else
        {
            rowBuilder.newRow(Clustering.make(
                              clusteringKeys.stream()
                                            .map(f -> f.serialize(row.get(f.pos())))
                                            .toArray(ByteBuffer[]::new))
            );
        }

        if (row.isDeleted())
        {
            rowBuilder.addRowDeletion(Row.Deletion.regular(new DeletionTime(timestamp, timeProvider().now())));
        }
        else
        {
            BiConsumer<Row.Builder, CqlField> rowBuildFunc = (builder, field) -> {
                final FourZeroCqlType type = (FourZeroCqlType) field.type();
                final ColumnMetadata cd = table.getColumn(new ColumnIdentifier(field.name(), false));
                Object value = row.get(field.pos());
                if (value == CassandraBridge.UNSET_MARKER)
                {
                    // do not add the cell, a.k.a. unset
                }
                else if (value == null)
                {
                    if (cd.isComplex())
                    {
                        type.addComplexTombstone(builder, cd, timestamp);
                    }
                    else
                    {
                        type.addTombstone(builder, cd, timestamp);
                    }
                }
                else if (value instanceof CollectionElement)
                {
                    CollectionElement ce = (CollectionElement) value;
                    if (ce.value == null)
                    {
                        type.addTombstone(builder, cd, timestamp, ce.cellPath);
                    }
                    else
                    {
                        type.addCell(builder, cd, timestamp, row.ttl(), timeProvider().now(), ce.value, ce.cellPath);
                    }
                }
                else
                {
                    type.addCell(builder, cd, timestamp, row.ttl(), timeProvider().now(), value);
                }
            };

            if (!cqlTable.staticColumns().isEmpty())
            {
                Row.Builder staticRowBuilder = BTreeRow.sortedBuilder();
                staticRowBuilder.newRow(Clustering.STATIC_CLUSTERING);
                for (final CqlField field : cqlTable.staticColumns())
                {
                    rowBuildFunc.accept(staticRowBuilder, field);
                }
                staticRow = staticRowBuilder.build(); // replace the empty row with the new static row built
            }

            // build value cells
            for (final CqlField field : cqlTable.valueColumns())
            {
                rowBuildFunc.accept(rowBuilder, field);
            }
        }

        return new Mutation(PartitionUpdate.singleRowUpdate(table, decoratedPartitionKey, rowBuilder.build(), staticRow));
    }

    @Override
    @VisibleForTesting
    protected boolean isPartitionDeletion(CqlTable cqlTable, IRow row)
    {
        final List<CqlField> clusteringKeys = cqlTable.clusteringKeys();
        final List<CqlField> valueFields = cqlTable.valueColumns();
        final List<CqlField> staticFields = cqlTable.staticColumns();
        for (CqlField f : Iterables.concat(clusteringKeys, valueFields, staticFields))
        {
            if (row.get(f.pos()) != null)
            {
                return false;
            }
        }
        return true;
    }

    @Override
    @VisibleForTesting
    protected boolean isRowDeletion(CqlTable schema, IRow row)
    {
        return row.isDeleted();
    }
}
