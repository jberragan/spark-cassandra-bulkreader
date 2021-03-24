package org.apache.cassandra.spark.reader.fourzero;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.CqlUdt;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.config.Config;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Keyspace;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ByteType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.BytesType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.FloatType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ListType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.LongType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.MapType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.SetType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ShortType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TimeType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TupleType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.IPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Schema;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.ListSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.MapSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.SetSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TupleSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.UUIDGen;
import org.apache.cassandra.spark.sparksql.CustomFilter;
import org.apache.cassandra.spark.utils.ByteBufUtils;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;

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
        try
        {
            final Field field = DatabaseDescriptor.class.getDeclaredField("conf");
            field.setAccessible(true);
            final Config config = (Config) field.get(null);
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
        }
        catch (final NoSuchFieldException | IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        Keyspace.setInitialized();
        setup = true;
    }

    @Override
    public Pair<ByteBuffer, BigInteger> getPartitionKey(@NotNull final CqlSchema schema,
                                                        @NotNull final Partitioner partitioner,
                                                        @NotNull final String key)
    {
        Preconditions.checkArgument(schema.partitionKeys().size() > 0);
        final List<AbstractType<?>> partitionKeyColumnTypes = schema.partitionKeys()
                                                                    .stream()
                                                                    .map(cqlField -> dataType(cqlField.type()))
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
                                               @NotNull final Stats stats)
    {
        //NOTE: need to use SchemaBuilder to init keyspace if not already set in C* Schema instance
        final SchemaBuilder schemaBuilder = new SchemaBuilder(schema, partitioner);
        final TableMetadata metadata = schemaBuilder.tableMetaData();
        return new CompactionStreamScanner(metadata, partitioner, ssTables.openAll((ssTable -> new FourZeroSSTableReader(metadata, ssTable, filters, stats))));
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
        return new SchemaBuilder(createStmt, keyspace, rf, partitioner, udts).build();
    }

    @Override
    public Object deserialize(final CqlField.CqlType type, final ByteBuffer buf)
    {
        return deserialize(type, buf, false);
    }

    private Object deserialize(final CqlField.CqlType type, final ByteBuffer buf, final boolean isFrozen)
    {
        switch (type.internalType())
        {
            case NativeCql:
                final CqlField.NativeCql3Type nativeCql3Type = (CqlField.NativeCql3Type) type;
                return toSparkSqlType(nativeCql3Type, serializer(nativeCql3Type).deserialize(buf));
            case Set:
            case List:
                return collectionToSparkSqlType((CqlField.CqlCollection) type, serializer(type).deserialize(buf));
            case Map:
                return mapToSparkSqlType((CqlField.CqlMap) type, serializer(type).deserialize(buf));
            case Frozen:
                return deserialize(((CqlField.CqlFrozen) type).inner(), buf, true);
            case Udt:
                final CqlUdt udt = (CqlUdt) type;
                return udtToSparkSqlType(udt, deserializeUdt(udt, buf, isFrozen));
            case Tuple:
                final CqlField.CqlTuple tuple = (CqlField.CqlTuple) type;
                return tupleToSparkSqlType(deserializeTuple(tuple, buf, isFrozen));
            default:
                throw unknownType(type);
        }
    }

    public ByteBuffer serializeUdt(final CqlUdt udt, final Map<String, Object> values)
    {
        final List<ByteBuffer> bufs = udt.fields().stream().map(f -> serialize(f.type(), values.get(f.name()))).collect(Collectors.toList());

        final ByteBuffer result = ByteBuffer.allocate(4 + bufs.stream().map(Buffer::remaining).map(a -> a + 4).reduce(Integer::sum).orElse(0));
        result.putInt(bufs.size()); // num fields
        for (final ByteBuffer buf : bufs)
        {
            result.putInt(buf.remaining()); // len
            result.put(buf.duplicate()); // value
        }
        return (ByteBuffer) result.flip();
    }

    public Map<String, Object> deserializeUdt(final CqlUdt udt, final ByteBuffer buf, final boolean isFrozen)
    {
        if (!isFrozen)
        {
            final int fieldCount = buf.getInt();
            Preconditions.checkArgument(fieldCount == udt.size(),
                                        String.format("Unexpected number of fields deserializing UDT '%s', expected %d fields but %d found", udt.cqlName(), udt.size(), fieldCount));
        }

        final Map<String, Object> result = new HashMap<>(udt.size());
        for (final CqlField field : udt.fields())
        {
            if (buf.remaining() < 4)
            {
                break;
            }
            final int len = buf.getInt();
            result.put(field.name(), len <= 0 ? null : deserialize(field.type(), ByteBufUtils.readBytes(buf, len), isFrozen));
        }

        return result;
    }

    public ByteBuffer serializeTuple(final CqlField.CqlTuple udt, final Object[] values)
    {
        final List<ByteBuffer> bufs = IntStream.range(0, udt.size())
                                               .mapToObj(i -> serialize(udt.type(i), values[i]))
                                               .collect(Collectors.toList());
        final ByteBuffer result = ByteBuffer.allocate(bufs.stream().map(Buffer::remaining).map(a -> a + 4).reduce(Integer::sum).orElse(0));
        for (final ByteBuffer buf : bufs)
        {
            result.putInt(buf.remaining()); // len
            result.put(buf.duplicate()); // value
        }
        return (ByteBuffer) result.flip();
    }

    public Object[] deserializeTuple(final CqlField.CqlTuple tuple, final ByteBuffer buf, final boolean isFrozen)
    {
        final Object[] result = new Object[tuple.size()];
        int pos = 0;
        for (final CqlField.CqlType type : tuple.types())
        {
            if (buf.remaining() < 4)
            {
                break;
            }
            final int len = buf.getInt();
            result[pos++] = len <= 0 ? null : deserialize(type, ByteBufUtils.readBytes(buf, len), isFrozen);
        }
        return result;
    }

    private Object toSparkSqlType(final CqlField.CqlType type, final Object o)
    {
        return toSparkSqlType(type, o, false);
    }

    private Object toSparkSqlType(final CqlField.CqlType type, final Object o, boolean isFrozen)
    {
        switch (type.internalType())
        {
            case NativeCql:
                return toSparkSqlType(((CqlField.NativeCql3Type) type), o);
            case Set:
            case List:
                return collectionToSparkSqlType((CqlField.CqlCollection) type, o);
            case Map:
                return mapToSparkSqlType((CqlField.CqlMap) type, o);
            case Frozen:
                return toSparkSqlType(((CqlField.CqlFrozen) type).inner(), o, true);
            case Udt:
                return udtToSparkSqlType((CqlUdt) type, o, isFrozen);
            case Tuple:
                return tupleToSparkSqlType((CqlField.CqlTuple) type, o);
            default:
                throw unknownType(type);
        }
    }

    private GenericInternalRow tupleToSparkSqlType(final CqlField.CqlTuple tuple, final Object o)
    {
        if (o instanceof ByteBuffer)
        {
            // need to deserialize first, e.g. if tuple is frozen inside collections
            return (GenericInternalRow) deserialize(tuple, (ByteBuffer) o);
        }
        return tupleToSparkSqlType((Object[]) o);
    }

    private GenericInternalRow tupleToSparkSqlType(final Object[] o)
    {
        return new GenericInternalRow(o);
    }

    @SuppressWarnings("unchecked")
    private GenericInternalRow udtToSparkSqlType(final CqlUdt udt, final Object o, final boolean isFrozen)
    {
        if (o instanceof ByteBuffer)
        {
            // need to deserialize first, e.g. if udt is frozen inside collections
            return udtToSparkSqlType(udt, deserializeUdt(udt, (ByteBuffer) o, isFrozen));
        }
        return udtToSparkSqlType(udt, (Map<String, Object>) o);
    }

    private static GenericInternalRow udtToSparkSqlType(final CqlUdt udt, final Map<String, Object> o)
    {
        final Object[] ar = new Object[udt.size()];
        for (int i = 0; i < udt.size(); i++)
        {
            ar[i] = o.getOrDefault(udt.field(i).name(), null);
        }
        return new GenericInternalRow(ar);
    }

    @SuppressWarnings("unchecked")
    private ArrayData collectionToSparkSqlType(final CqlField.CqlCollection collection, final Object o)
    {
        return ArrayData.toArrayData(((Collection<Object>) o).stream().map(a -> toSparkSqlType(collection.type(), a)).toArray());
    }

    @SuppressWarnings("unchecked")
    private ArrayBasedMapData mapToSparkSqlType(final CqlField.CqlMap mapType, final Object o)
    {
        return mapToSparkSqlType(mapType, (Map<Object, Object>) o);
    }

    private ArrayBasedMapData mapToSparkSqlType(final CqlField.CqlMap mapType, final Map<Object, Object> map)
    {
        final Object[] keys = new Object[map.size()];
        final Object[] values = new Object[map.size()];
        int pos = 0;
        for (final Map.Entry<Object, Object> entry : map.entrySet())
        {
            keys[pos] = toSparkSqlType(mapType.keyType(), entry.getKey());
            values[pos] = toSparkSqlType(mapType.valueType(), entry.getValue());
            pos++;
        }
        return new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values));
    }

    private static Object toSparkSqlType(final CqlField.NativeCql3Type type, final Object o)
    {
        // map values to SparkSQL type
        switch (type)
        {
            case DECIMAL:
                return Decimal.apply((BigDecimal) o); // org.apache.spark.sql.types.Decimal
            case INET:
                return ((InetAddress) o).getAddress(); // byte[]
            case TIMESTAMP:
                return ((Date) o).getTime() * 1000L; // long
            case BLOB:
                return ByteBufUtils.getArray((ByteBuffer) o); // byte[]
            case VARINT:
                return Decimal.apply((BigInteger) o); // org.apache.spark.sql.types.Decimal
            case TIMEUUID:
            case UUID:
            case ASCII:
            case TEXT:
            case VARCHAR:
                return UTF8String.fromString(o.toString()); // UTF8String
            default:
                return o; // all other data types work as ordinary Java data types.
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public ByteBuffer serialize(final CqlField.CqlType type, final Object value)
    {
        switch (type.internalType())
        {
            case Frozen:
                return serialize(((CqlField.CqlFrozen) type).inner(), value);
            case Udt:
                return serializeUdt((CqlUdt) type, (Map<String, Object>) value);
            case Tuple:
                return serializeTuple((CqlField.CqlTuple) type, (Object[]) value);
            default:
                return serializer(type).serialize(value);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> TypeSerializer<T> serializer(final CqlField.CqlType type)
    {
        switch (type.internalType())
        {
            case NativeCql:
                return serializer((CqlField.NativeCql3Type) type);
            case Set:
                final CqlField.CqlSet setType = (CqlField.CqlSet) type;
                return (TypeSerializer<T>) SetSerializer.getInstance(serializer(setType.type()), dataType(setType.type()));
            case List:
                return (TypeSerializer<T>) ListSerializer.getInstance(serializer(((CqlField.CqlList) type).type()));
            case Map:
                final CqlField.CqlMap mapType = (CqlField.CqlMap) type;
                return (TypeSerializer<T>) MapSerializer.getInstance(serializer(mapType.keyType()), serializer(mapType.valueType()), dataType(mapType.keyType()));
            case Frozen:
                return serializer(((CqlField.CqlFrozen) type).inner());
            case Udt:
                final CqlUdt udt = (CqlUdt) type;
                // get UserTypeSerializer from Schema instance to ensure fields are deserialized in correct order
                return (TypeSerializer<T>) Schema.instance.getKeyspaceMetadata(udt.keyspace()).types
                                           .get(UTF8Serializer.instance.serialize(udt.name()))
                                           .orElseThrow(() -> new RuntimeException(String.format("UDT '%s' not initialized", udt.name())))
                                           .getSerializer();
            case Tuple:
                final CqlField.CqlTuple tuple = (CqlField.CqlTuple) type;
                return (TypeSerializer<T>) new TupleSerializer(tuple.types().stream().map(FourZero::serializer).collect(Collectors.toList()));
            default:
                throw unknownType(type);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> TypeSerializer<T> serializer(final CqlField.NativeCql3Type type)
    {
        return (TypeSerializer<T>) dataType(type).getSerializer();
    }

    private static AbstractType<?> dataType(final CqlField.CqlType type)
    {
        return dataType(type, true);
    }

    private static AbstractType<?> dataType(final CqlField.CqlType type, final boolean isMultiCell)
    {
        switch (type.internalType())
        {
            case NativeCql:
                return dataType((CqlField.NativeCql3Type) type);
            case Set:
                return SetType.getInstance(dataType(((CqlField.CqlSet) type).type()), isMultiCell);
            case List:
                return ListType.getInstance(dataType(((CqlField.CqlList) type).type()), isMultiCell);
            case Map:
                final CqlField.CqlMap map = (CqlField.CqlMap) type;
                return MapType.getInstance(dataType(map.keyType()), dataType(map.valueType()), isMultiCell);
            case Frozen:
                return dataType(((CqlField.CqlFrozen) type).inner(), false); // if frozen collection then isMultiCell = false
            case Udt:
                final CqlUdt udt = (CqlUdt) type;
                return Schema.instance.getKeyspaceMetadata(udt.keyspace()).types
                       .get(UTF8Serializer.instance.serialize(udt.name()))
                       .orElseThrow(() -> new RuntimeException(String.format("UDT '%s' not initialized", udt.name())));
            case Tuple:
                final CqlField.CqlTuple tuple = (CqlField.CqlTuple) type;
                return new TupleType(tuple.types().stream().map(FourZero::dataType).collect(Collectors.toList()));
            default:
                throw unknownType(type);
        }
    }

    private static AbstractType<?> dataType(final CqlField.NativeCql3Type type)
    {
        switch (type)
        {
            case VARINT:
                return IntegerType.instance;
            case INT:
                return Int32Type.instance;
            case BOOLEAN:
                return BooleanType.instance;
            case TIMEUUID:
                return TimeUUIDType.instance;
            case UUID:
                return UUIDType.instance;
            case BIGINT:
                return LongType.instance;
            case DECIMAL:
                return DecimalType.instance;
            case FLOAT:
                return FloatType.instance;
            case DOUBLE:
                return DoubleType.instance;
            case ASCII:
                return AsciiType.instance;
            case TEXT:
            case VARCHAR:
                return UTF8Type.instance;
            case INET:
                return InetAddressType.instance;
            case DATE:
                return SimpleDateType.instance;
            case TIME:
                return TimeType.instance;
            case TIMESTAMP:
                return TimestampType.instance;
            case BLOB:
                return BytesType.instance;
            case EMPTY:
                return EmptyType.instance;
            case SMALLINT:
                return ShortType.instance;
            case TINYINT:
                return ByteType.instance;
            default:
                throw unknownType(type);
        }
    }

    @Override
    public synchronized void writeSSTable(final Partitioner partitioner,
                                          final String keyspace,
                                          final Path dir,
                                          final String createStmt,
                                          final String insertStmt,
                                          final Set<CqlUdt> udts,
                                          final Consumer<IWriter> writer)
    {
        final CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                                 .inDirectory(dir.toFile())
                                                                 .forTable(createStmt)
                                                                 .withPartitioner(FourZero.getPartitioner(partitioner))
                                                                 .using(insertStmt)
                                                                 .withBufferSizeInMB(128);

        for (final CqlUdt udt : udts)
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

    private static NotImplementedException unknownType(final CqlField.CqlType type)
    {
        return new NotImplementedException(type.toString() + " type not implemented yet");
    }
}
