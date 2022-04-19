package org.apache.cassandra.spark.reader.fourzero;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.CRC32;


import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Clustering;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DecoratedKey;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.SerializationHeader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.IPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.Token;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.Component;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.format.Version;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.BloomFilter;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.BloomFilterSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.vint.VIntCoding;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.utils.ByteBufUtils;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.FBUtilities.updateChecksumInt;

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

@SuppressWarnings("WeakerAccess")
public class FourZeroUtils
{
    private static final int CHECKSUM_LENGTH = 4; // CRC32
    private static final Constructor<?> SERIALIZATION_HEADER = Arrays.stream(SerializationHeader.Component.class.getDeclaredConstructors()).filter(a -> a.getParameterCount() == 5).findFirst().orElseThrow(() -> new RuntimeException("Could not find SerializationHeader.Component constructor"));
    public static final ByteBuffer SUPER_COLUMN_MAP_COLUMN = ByteBufferUtil.EMPTY_BYTE_BUFFER;

    static
    {
        SERIALIZATION_HEADER.setAccessible(true);
    }

    public static long tokenToLong(final Token token)
    {
        if (token instanceof Murmur3Partitioner.LongToken)
        {
            return (long) token.getTokenValue();
        }
        if (token instanceof RandomPartitioner.BigIntegerToken)
        {
            return ((RandomPartitioner.BigIntegerToken) token).getTokenValue().longValue();
        }

        throw new UnsupportedOperationException("Unexpected token type: " + token.getClass().getName());
    }

    public static BigInteger tokenToBigInteger(final Token token)
    {
        if (token instanceof Murmur3Partitioner.LongToken)
        {
            return BigInteger.valueOf((long) token.getTokenValue());
        }
        if (token instanceof RandomPartitioner.BigIntegerToken)
        {
            return ((RandomPartitioner.BigIntegerToken) token).getTokenValue();
        }

        throw new UnsupportedOperationException("Unexpected token type: " + token.getClass().getName());
    }

    static ByteBuffer encodeCellName(final TableMetadata metadata,
                                     final ClusteringPrefix clustering,
                                     final ByteBuffer columnName,
                                     final ByteBuffer collectionElement)
    {
        final boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;

        if (!TableMetadata.Flag.isCompound(metadata.flags))
        {
            if (isStatic)
            {
                return columnName;
            }

            assert clustering.size() == 1 : "Expected clustering size to be 1, but was " + clustering.size();
            return clustering.bufferAt(0);
        }

        // We use comparator.size() rather than clustering.size() because of static clusterings
        final int clusteringSize = metadata.comparator.size();
        int size = clusteringSize + (TableMetadata.Flag.isDense(metadata.flags) ? 0 : 1) + (collectionElement == null ? 0 : 1);
        if (TableMetadata.Flag.isSuper(metadata.flags))
        {
            size = clusteringSize + 1;
        }

        final ByteBuffer[] values = new ByteBuffer[size];
        for (int i = 0; i < clusteringSize; i++)
        {
            if (isStatic)
            {
                values[i] = ByteBufferUtil.EMPTY_BYTE_BUFFER;
                continue;
            }

            final ByteBuffer v = clustering.bufferAt(i);
            // we can have null (only for dense compound tables for backward compatibility reasons) but that
            // means we're done and should stop there as far as building the composite is concerned.
            if (v == null)
            {
                return CompositeType.build(ByteBufferAccessor.instance, Arrays.copyOfRange(values, 0, i));
            }

            values[i] = v;
        }

        if (TableMetadata.Flag.isSuper(metadata.flags))
        {
            // We need to set the "column" (in thrift terms) name, i.e. the value corresponding to the subcomparator.
            // What it is depends if this a cell for a declared "static" column or a "dynamic" column part of the
            // super-column internal map.
            assert columnName != null; // This should never be null for supercolumns, see decodeForSuperColumn() above
            values[clusteringSize] = columnName.equals(SUPER_COLUMN_MAP_COLUMN)
                                     ? collectionElement
                                     : columnName;
        }
        else
        {
            if (!TableMetadata.Flag.isDense(metadata.flags))
            {
                values[clusteringSize] = columnName;
            }
            if (collectionElement != null)
            {
                values[clusteringSize + 1] = collectionElement;
            }
        }

        return CompositeType.build(ByteBufferAccessor.instance, isStatic, values);
    }

    static Pair<DecoratedKey, DecoratedKey> keysFromIndex(@NotNull final TableMetadata metadata, @NotNull final DataLayer.SSTable ssTable) throws IOException
    {
        try (final InputStream primaryIndex = ssTable.openPrimaryIndexStream())
        {
            if (primaryIndex != null)
            {
                final IPartitioner partitioner = metadata.partitioner;
                final Pair<ByteBuffer, ByteBuffer> keys = FourZeroUtils.readPrimaryIndex(primaryIndex, true, Collections.emptyList());
                return Pair.of(partitioner.decorateKey(keys.getLeft()), partitioner.decorateKey(keys.getRight()));
            }
        }
        return Pair.of(null, null);
    }

    static boolean anyFilterKeyInIndex(@NotNull final DataLayer.SSTable ssTable,
                                       @NotNull final List<PartitionKeyFilter> filters) throws IOException
    {
        if (filters.isEmpty())
        {
            return false;
        }

        try (final InputStream primaryIndex = ssTable.openPrimaryIndexStream())
        {
            if (primaryIndex != null)
            {
                final Pair<ByteBuffer, ByteBuffer> keys = FourZeroUtils.readPrimaryIndex(primaryIndex, false, filters);
                if (keys.getLeft() != null || keys.getRight() != null)
                {
                    return false;
                }
            }
        }
        return true;
    }

    static Map<MetadataType, MetadataComponent> deserializeStatsMetadata(final DataLayer.SSTable ssTable, final Descriptor descriptor) throws IOException
    {
        try (final InputStream statsStream = ssTable.openStatsStream())
        {
            return FourZeroUtils.deserializeStatsMetadata(statsStream, EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER), descriptor);
        }
    }

    /**
     * Deserialize Statistics.db file to pull out meta-data components needed for SSTable deserialization
     *
     * @param is            input stream for Statistics.db file
     * @param selectedTypes enum of MetadataType to deserialize
     * @param descriptor    SSTable file descriptor
     * @return map of MetadataComponent for each requested MetadataType
     * @throws IOException
     */
    static Map<MetadataType, MetadataComponent> deserializeStatsMetadata(final InputStream is,
                                                                         final EnumSet<MetadataType> selectedTypes,
                                                                         final Descriptor descriptor) throws IOException
    {
        final DataInputStream in = new DataInputPlus.DataInputStreamPlus(is);
        final boolean isChecksummed = descriptor.version.hasMetadataChecksum();
        final CRC32 crc = new CRC32();

        final int count = in.readInt();
        updateChecksumInt(crc, count);
        FourZeroUtils.maybeValidateChecksum(crc, in, descriptor);

        final int[] ordinals = new int[count];
        final int[] offsets = new int[count];
        final int[] lengths = new int[count];

        for (int i = 0; i < count; i++)
        {
            ordinals[i] = in.readInt();
            updateChecksumInt(crc, ordinals[i]);

            offsets[i] = in.readInt();
            updateChecksumInt(crc, offsets[i]);
        }
        FourZeroUtils.maybeValidateChecksum(crc, in, descriptor);

        for (int i = 0; i < count - 1; i++)
        {
            lengths[i] = offsets[i + 1] - offsets[i];
        }

        final MetadataType[] allMetadataTypes = MetadataType.values();
        final Map<MetadataType, MetadataComponent> components = new EnumMap<>(MetadataType.class);
        for (int i = 0; i < count - 1; i++)
        {
            final MetadataType type = allMetadataTypes[ordinals[i]];

            if (!selectedTypes.contains(type))
            {
                in.skipBytes(lengths[i]);
                continue;
            }

            final byte[] buffer = new byte[isChecksummed ? lengths[i] - FourZeroUtils.CHECKSUM_LENGTH : lengths[i]];
            in.readFully(buffer);

            crc.reset();
            crc.update(buffer);
            FourZeroUtils.maybeValidateChecksum(crc, in, descriptor);

            components.put(type, FourZeroUtils.deserializeMetadataComponent(descriptor.version, buffer, type));
        }

        final MetadataType type = allMetadataTypes[ordinals[count - 1]];
        if (!selectedTypes.contains(type))
        {
            return components;
        }

        // we do not have in.bytesRemaining() (as in FileDataInput), so need to read remaining bytes to get final component
        final byte[] remainingBytes = ByteBufUtils.readRemainingBytes(in, 256);
        final byte[] buffer;
        if (descriptor.version.hasMetadataChecksum())
        {
            final ByteBuffer buf = ByteBuffer.wrap(remainingBytes);
            final int len = buf.remaining() - 4;
            buffer = new byte[len];
            buf.get(buffer, 0, len);
            crc.reset();
            crc.update(buffer);
            FourZeroUtils.validateChecksum(crc, buf.getInt(), descriptor);
        }
        else
        {
            buffer = remainingBytes;
        }

        components.put(type, FourZeroUtils.deserializeMetadataComponent(descriptor.version, buffer, type));

        return components;
    }

    private static void maybeValidateChecksum(final CRC32 crc, final DataInputStream in, final Descriptor descriptor) throws IOException
    {
        if (!descriptor.version.hasMetadataChecksum())
        {
            return;
        }
        validateChecksum(crc, in.readInt(), descriptor);
    }

    private static void validateChecksum(final CRC32 crc, final int expectedChecksum, final Descriptor descriptor)
    {
        final int actualChecksum = (int) crc.getValue();

        if (actualChecksum != expectedChecksum)
        {
            final String filename = descriptor.filenameFor(Component.STATS);
            throw new CorruptSSTableException(new IOException("Checksums do not match for " + filename), filename);
        }
    }

    private static MetadataComponent deserializeValidationMetaData(@NotNull final DataInputBuffer in) throws IOException
    {
        return new ValidationMetadata(FourZeroSchemaBuilder.convertToShadedPackages(in.readUTF()), in.readDouble());
    }

    private static MetadataComponent deserializeMetadataComponent(@NotNull final Version version,
                                                                  @NotNull final byte[] buffer,
                                                                  @NotNull final MetadataType type) throws IOException
    {
        final DataInputBuffer in = new DataInputBuffer(buffer);
        if (type == MetadataType.HEADER)
        {
            return deserializeSerializationHeader(in);
        }
        else if (type == MetadataType.VALIDATION)
        {
            return deserializeValidationMetaData(in);
        }
        return type.serializer.deserialize(version, in);
    }

    private static MetadataComponent deserializeSerializationHeader(@NotNull final DataInputBuffer in) throws IOException
    {
        // we need to deserialize data type class names using shaded package names
        final EncodingStats stats = EncodingStats.serializer.deserialize(in);
        final AbstractType<?> keyType = FourZeroUtils.readType(in);
        final int size = (int) in.readUnsignedVInt();
        final List<AbstractType<?>> clusteringTypes = new ArrayList<>(size);

        for (int i = 0; i < size; ++i)
        {
            clusteringTypes.add(FourZeroUtils.readType(in));
        }

        final Map<ByteBuffer, AbstractType<?>> staticColumns = new LinkedHashMap<>();
        final Map<ByteBuffer, AbstractType<?>> regularColumns = new LinkedHashMap<>();
        FourZeroUtils.readColumnsWithType(in, staticColumns);
        FourZeroUtils.readColumnsWithType(in, regularColumns);

        try
        {
            return (SerializationHeader.Component) SERIALIZATION_HEADER.newInstance(keyType, clusteringTypes, staticColumns, regularColumns, stats);
        }
        catch (final InstantiationException | IllegalAccessException | InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void readColumnsWithType(@NotNull final DataInputPlus in,
                                            @NotNull final Map<ByteBuffer, AbstractType<?>> typeMap) throws IOException
    {
        final int length = (int) in.readUnsignedVInt();
        for (int i = 0; i < length; i++)
        {
            final ByteBuffer name = ByteBufferUtil.readWithVIntLength(in);
            typeMap.put(name, readType(in));
        }
    }

    private static AbstractType<?> readType(@NotNull final DataInputPlus in) throws IOException
    {
        return TypeParser.parse(FourZeroSchemaBuilder.convertToShadedPackages(UTF8Type.instance.compose(ByteBufferUtil.readWithVIntLength(in))));
    }

    /**
     * Read primary Index.db file, read through all partitions to get first and last partition key
     *
     * @param primaryIndex input stream for Index.db file
     * @return pair of first and last decorated keys
     * @throws IOException
     */
    @SuppressWarnings("InfiniteLoopStatement")
    static Pair<ByteBuffer, ByteBuffer> readPrimaryIndex(@NotNull final InputStream primaryIndex,
                                                         final boolean readFirstLastKey,
                                                         @NotNull final List<PartitionKeyFilter> filters) throws IOException
    {
        ByteBuffer firstKey = null, lastKey = null;
        try (final DataInputStream dis = new DataInputStream(primaryIndex))
        {
            byte[] last = null;
            try
            {
                while (true)
                {
                    final int len = dis.readUnsignedShort();
                    final byte[] buf = new byte[len];
                    dis.readFully(buf);
                    if (firstKey == null)
                    {
                        firstKey = ByteBuffer.wrap(buf);
                    }
                    last = buf;
                    final ByteBuffer key = ByteBuffer.wrap(last);
                    if (!readFirstLastKey && filters.stream().anyMatch(filter -> filter.filter(key)))
                    {
                        return Pair.of(null, null);
                    }

                    // read position & skip promoted index
                    skipRowIndexEntry(dis);
                }
            }
            catch (final EOFException ignored)
            {
            }

            if (last != null)
            {
                lastKey = ByteBuffer.wrap(last);
            }
        }

        return Pair.of(firstKey, lastKey);
    }


    static void skipRowIndexEntry(final DataInputStream dis) throws IOException
    {
        readPosition(dis);
        skipPromotedIndex(dis);
    }

    static int vIntSize(final long value)
    {
        return VIntCoding.computeUnsignedVIntSize(value);
    }

    static void writePosition(final long value, ByteBuffer buf) throws IOException
    {
        VIntCoding.writeUnsignedVInt(value, buf);
    }

    static long readPosition(final DataInputStream dis) throws IOException
    {
        return VIntCoding.readUnsignedVInt(dis);
    }

    static void skipPromotedIndex(final DataInputStream dis) throws IOException
    {
        final int size = (int) VIntCoding.readUnsignedVInt(dis);
        if (size > 0)
        {
            ByteBufUtils.skipBytesFully(dis, size);
        }
    }

    static List<PartitionKeyFilter> filterKeyInBloomFilter(@NotNull final DataLayer.SSTable ssTable,
                                                           @NotNull final IPartitioner partitioner,
                                                           final Descriptor descriptor,
                                                           @NotNull final List<PartitionKeyFilter> partitionKeyFilters) throws IOException
    {
        try
        {
            final BloomFilter bloomFilter = SSTableCache.INSTANCE.bloomFilter(ssTable, descriptor);
            return partitionKeyFilters.stream()
                                      .filter(filter -> bloomFilter.isPresent(partitioner.decorateKey(filter.key())))
                                      .collect(Collectors.toList());
        }
        catch (Exception e)
        {
            if (e instanceof FileNotFoundException)
            {
                return partitionKeyFilters;
            }
            throw e;
        }
    }

    static BloomFilter readFilter(@NotNull final DataLayer.SSTable ssTable,
                                  final boolean hasOldBfFormat) throws IOException
    {
        try (final InputStream filterStream = ssTable.openFilterStream())
        {
            if (filterStream != null)
            {
                try (final DataInputStream dis = new DataInputStream(filterStream))
                {
                    return BloomFilterSerializer.deserialize(dis, hasOldBfFormat);
                }
            }
        }
        throw new FileNotFoundException();
    }
}
