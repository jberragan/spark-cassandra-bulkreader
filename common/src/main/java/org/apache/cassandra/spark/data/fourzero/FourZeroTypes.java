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

package org.apache.cassandra.spark.data.fourzero;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import com.esotericsoftware.kryo.io.Input;
import org.apache.cassandra.spark.cdc.TableIdLookup;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
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
import org.apache.cassandra.spark.reader.fourzero.FourZeroSchemaBuilder;
import org.apache.cassandra.spark.reader.fourzero.SchemaUtils;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.config.Config;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Keyspace;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.IPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Schema;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableId;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.UUIDGen;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FourZeroTypes extends CassandraTypes
{
    public static final FourZeroTypes INSTANCE = new FourZeroTypes();
    private static volatile boolean setup = false;

    @SuppressWarnings("deprecation")
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

    private final Map<String, CqlField.NativeType> nativeTypes;

    public FourZeroTypes()
    {
        this.nativeTypes = allTypes().stream().collect(Collectors.toMap(CqlField.CqlType::name, Function.identity()));
    }

    public static IPartitioner getPartitioner(final Partitioner partitioner)
    {
        return partitioner == Partitioner.Murmur3Partitioner ? Murmur3Partitioner.instance : RandomPartitioner.instance;
    }

    public static void updateCdcSchema(@NotNull final Set<CqlTable> cdcTables,
                                       @NotNull final Partitioner partitioner,
                                       @NotNull final TableIdLookup tableIdLookup)
    {
        updateCdcSchema(Schema.instance, cdcTables, partitioner, tableIdLookup);
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

    @Override
    public Map<String, ? extends CqlField.NativeType> nativeTypeNames()
    {
        return nativeTypes;
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

    public UUID getTimeUUID()
    {
        return UUIDGen.getTimeUUID();
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
}
