package org.apache.cassandra.spark.reader.fourzero;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;

import com.esotericsoftware.kryo.io.Input;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
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
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.config.Config;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Keyspace;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.IPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.UUIDGen;
import org.apache.cassandra.spark.sparksql.filters.CustomFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.stats.Stats;
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
        FourZero.setup();
    }

    public synchronized static void setup()
    {
        if (FourZero.setup)
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

    @Override
    public Pair<ByteBuffer, BigInteger> getPartitionKey(@NotNull final CqlSchema schema,
                                                        @NotNull final Partitioner partitioner,
                                                        @NotNull final String key)
    {
        Preconditions.checkArgument(schema.partitionKeys().size() > 0);
        final List<AbstractType<?>> partitionKeyColumnTypes = schema.partitionKeys()
                                                                    .stream()
                                                                    .map(CqlField::type)
                                                                    .map(type -> (FourZeroCqlType) type)
                                                                    .map(type -> type.dataType(true))
                                                                    .collect(Collectors.toList());
        final AbstractType<?> keyValidator = schema.partitionKeys().size() == 1
                                             ? partitionKeyColumnTypes.get(0)
                                             : CompositeType.getInstance(partitionKeyColumnTypes);
        final ByteBuffer partitionKey = keyValidator.fromString(key);
        final BigInteger partitionKeyTokenValue = hash(partitioner, partitionKey);
        return Pair.of(partitionKey, partitionKeyTokenValue);
    }

    @Override
    public IStreamScanner getCompactionScanner(@NotNull final CqlSchema schema,
                                               @NotNull final Partitioner partitioner,
                                               @NotNull final SSTablesSupplier ssTables,
                                               @NotNull final List<CustomFilter> filters,
                                               @Nullable final PruneColumnFilter columnFilter,
                                               final boolean readIndexOffset,
                                               final boolean useIncrementalRepair,
                                               @NotNull final Stats stats)
    {
        //NOTE: need to use SchemaBuilder to init keyspace if not already set in C* Schema instance
        final FourZeroSchemaBuilder schemaBuilder = new FourZeroSchemaBuilder(schema, partitioner);
        final TableMetadata metadata = schemaBuilder.tableMetaData();
        return new CompactionStreamScanner(metadata, partitioner, ssTables.openAll(
        ((ssTable, isRepairPrimary) -> FourZeroSSTableReader.builder(metadata, ssTable)
                                         .withFilters(filters)
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

    public UUID getTimeUUID()
    {
        return UUIDGen.getTimeUUID();
    }

    @Override
    public CqlSchema buildSchema(final String keyspace, final String createStmt, final ReplicationFactor rf, final Partitioner partitioner, final Set<String> udts)
    {
        return new FourZeroSchemaBuilder(createStmt, keyspace, rf, partitioner, udts).build();
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
                                                                 .withPartitioner(FourZero.getPartitioner(partitioner))
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
}
