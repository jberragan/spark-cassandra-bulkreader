package org.apache.cassandra.spark.reader.fourzero;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.apache.cassandra.spark.reader.common.RawInputStream;
import org.apache.cassandra.spark.reader.common.SSTableStreamException;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DecoratedKey;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DeletionTime;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.SerializationHeader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Row;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.UnfilteredDeserializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.SSTableSimpleIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.format.Version;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.net.MessagingService;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.service.ActiveRepairService;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.spark.sparksql.CustomFilter;
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

@SuppressWarnings("unused")
public class FourZeroSSTableReader implements SparkSSTableReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FourZeroSSTableReader.class);

    @NotNull
    private final TableMetadata metadata;
    @NotNull
    private final DataLayer.SSTable ssTable;
    @NotNull
    private final StatsMetadata statsMetadata;
    @NotNull
    private final Version version;
    @NotNull
    private final DecoratedKey first, last;
    @NotNull
    private final BigInteger firstToken, lastToken;
    @NotNull
    private final SerializationHeader header;
    @NotNull
    private final DeserializationHelper helper;
    @NotNull
    private final AtomicReference<SSTableStreamReader> reader = new AtomicReference<>(null);
    @NotNull
    private final List<CustomFilter> filters;
    @NotNull
    private final Stats stats;
    private Long openedNanos = null;

    FourZeroSSTableReader(@NotNull final TableMetadata metadata, @NotNull final DataLayer.SSTable ssTable, @NotNull List<CustomFilter> filters, @NotNull final Stats stats) throws IOException
    {
        this.metadata = metadata;
        this.ssTable = ssTable;
        this.stats = stats;
        final Descriptor descriptor = Descriptor.fromFilename(new File(String.format("./%s/%s", metadata.keyspace, metadata.name), ssTable.getDataFileName()));
        this.version = descriptor.version;

        // read first and last partition key from Summary.db file
        Pair<DecoratedKey, DecoratedKey> keys = SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable);

        if (keys.getLeft() == null || keys.getRight() == null)
        {
            LOGGER.warn("Could not load first and last key from Summary.db file, so attempting Index.db fileName={}", ssTable.getDataFileName());
            keys = SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable);
        }

        if (keys.getLeft() == null || keys.getRight() == null)
        {
            throw new IOException("Could not load SSTable first or last tokens");
        }

        this.first = keys.getLeft();
        this.last = keys.getRight();
        this.firstToken = FourZeroUtils.tokenToBigInteger(first.getToken());
        this.lastToken = FourZeroUtils.tokenToBigInteger(last.getToken());

        final List<CustomFilter> matchingFilters = filters.stream().filter(filter -> filter.filter(this)).collect(Collectors.toList());
        if (matchingFilters.isEmpty() && !filters.isEmpty())
        {
            this.filters = ImmutableList.of();
            stats.skipedSSTable(filters, firstToken, lastToken);
            LOGGER.info("Ignoring SSTableReader with firstToken={} lastToken={}, does not overlap with any filter", this.firstToken, this.lastToken);
            statsMetadata = null;
            header = null;
            helper = null;
            return;
        }

        if (matchingFilters.stream().anyMatch(CustomFilter::canFilterByKey))
        {
            final List<CustomFilter> matchInBloomFilter = FourZeroUtils.filterKeyInBloomFilter(ssTable, metadata.partitioner, descriptor, matchingFilters);
            this.filters = ImmutableList.copyOf(matchInBloomFilter);

            // check if required keys are actually present
            if (matchInBloomFilter.isEmpty() || !FourZeroUtils.anyFilterKeyInIndex(ssTable, matchInBloomFilter))
            {
                if (matchInBloomFilter.isEmpty())
                {
                    stats.missingInBloomFilter();
                }
                else
                {
                    stats.missingInIndex();
                }
                LOGGER.info("Ignoring SSTable {}, no match found in index file for key filters", this.ssTable.getDataFileName());
                statsMetadata = null;
                header = null;
                helper = null;
                return;
            }
        }
        else
        {
            this.filters = ImmutableList.copyOf(filters);
        }

        final Map<MetadataType, MetadataComponent> componentMap = SSTableCache.INSTANCE.componentMapFromStats(ssTable, descriptor);

        final ValidationMetadata validation = (ValidationMetadata) componentMap.get(MetadataType.VALIDATION);
        if (validation != null && !validation.partitioner.equals(metadata.partitioner.getClass().getName()))
        {
            throw new IllegalStateException("Partitioner in ValidationMetadata does not match TableMetaData: " + validation.partitioner + " vs. " + metadata.partitioner.getClass().getName());
        }

        this.statsMetadata = (StatsMetadata) componentMap.get(MetadataType.STATS);
        final SerializationHeader.Component headerComp = (SerializationHeader.Component) componentMap.get(MetadataType.HEADER);
        if (headerComp == null)
        {
            throw new IOException("Cannot read SSTable if cannot deserialize stats header info");
        }

        this.header = headerComp.toHeader(metadata);
        this.helper = new DeserializationHelper(metadata, MessagingService.VERSION_30, DeserializationHelper.Flag.FROM_REMOTE);

        // open SSTableStreamReader so opened in parallel inside thread pool
        // and buffered + ready to go when CompactionIterator starts reading
        reader.set(new SSTableStreamReader());
        stats.openedSSTable();
        this.openedNanos = System.nanoTime();
    }

    public boolean ignore()
    {
        return reader.get() == null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(metadata.keyspace, metadata.name, ssTable);
    }

    @Override
    public boolean equals(final Object o)
    {
        return o instanceof FourZeroSSTableReader &&
               metadata.keyspace.equals(((FourZeroSSTableReader) o).metadata.keyspace) &&
               metadata.name.equals(((FourZeroSSTableReader) o).metadata.name) &&
               ssTable.equals(((FourZeroSSTableReader) o).ssTable);
    }

    public boolean isRepaired()
    {
        return statsMetadata.repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE;
    }

    public DecoratedKey first()
    {
        return this.first;
    }

    public DecoratedKey last()
    {
        return this.last;
    }

    public long getMinTimestamp()
    {
        return statsMetadata.minTimestamp;
    }

    public long getMaxTimestamp()
    {
        return statsMetadata.maxTimestamp;
    }

    public StatsMetadata getSSTableMetadata()
    {
        return statsMetadata;
    }

    ISSTableScanner getScanner()
    {
        final ISSTableScanner result = reader.getAndSet(null);
        if (result == null)
        {
            throw new IllegalStateException("SSTableStreamReader cannot be re-used");
        }
        return result;
    }

    @Override
    @NotNull
    public BigInteger firstToken()
    {
        return firstToken;
    }

    @Override
    @NotNull
    public BigInteger lastToken()
    {
        return lastToken;
    }

    public class SSTableStreamReader implements ISSTableScanner
    {
        private final DataInputStream dis;
        private final DataInputPlus in;
        private DecoratedKey key;
        private DeletionTime partitionLevelDeletion;
        private SSTableSimpleIterator iterator;
        private Row staticRow;

        SSTableStreamReader() throws IOException
        {
            final RawInputStream dataStream;
            try (@Nullable final InputStream compressionInfoInputStream = ssTable.openCompressionStream())
            {
                final InputStream dataInputStream = ssTable.openDataStream();
                if (compressionInfoInputStream != null)
                {
                    dataStream = CompressedRawInputStream.fromInputStream(dataInputStream, compressionInfoInputStream, version.hasMaxCompressedLength(), stats);
                }
                else
                {
                    dataStream = new RawInputStream(new DataInputStream(dataInputStream), new byte[64 * 1024], stats);
                }
            }
            this.dis = new DataInputStream(dataStream);
            this.in = new DataInputPlus.DataInputStreamPlus(this.dis);
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        public boolean hasNext()
        {
            try
            {
                while (true)
                {
                    key = metadata.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
                    partitionLevelDeletion = DeletionTime.serializer.deserialize(in);
                    iterator = SSTableSimpleIterator.create(metadata, in, header, helper, partitionLevelDeletion);
                    staticRow = iterator.readStaticRow();
                    if (filters.isEmpty() || filters.stream().anyMatch(filter -> !filter.skipPartition(key.getKey(), FourZeroUtils.tokenToBigInteger(key.getToken()))))
                    {
                        // partition overlaps with filters
                        return true;
                    }
                    stats.skipedPartition(key.getKey(), FourZeroUtils.tokenToBigInteger(key.getToken()));
                    // skip partition efficiently without deserializing
                    final UnfilteredDeserializer deserializer = UnfilteredDeserializer.create(metadata, in, header, helper);
                    while (deserializer.hasNext())
                    {
                        deserializer.skipNext();
                    }
                }
            }
            catch (final EOFException e)
            {
                return false;
            }
            catch (final IOException e)
            {
                throw new SSTableStreamException(e);
            }
        }

        @Override
        public UnfilteredRowIterator next()
        {
            return new UnfilteredIterator();
        }

        @Override
        public void close()
        {
            LOGGER.debug("Closing SparkSSTableReader {}", ssTable);
            try
            {
                this.dis.close();
                if (openedNanos != null) {
                    stats.closedSSTable(System.nanoTime() - openedNanos);
                }
            }
            catch (final IOException e)
            {
                LOGGER.warn("IOException closing SSTable DataInputStream", e);
            }
        }

        @Override
        public long getLengthInBytes()
        {
            // this is mostly used to return Compaction info for Metrics or via JMX so we can ignore here
            return 0;
        }

        @Override
        public long getCompressedLengthInBytes()
        {
            return 0;
        }

        @Override
        public long getCurrentPosition()
        {
            // this is mostly used to return Compaction info for Metrics or via JMX so we can ignore here
            return 0;
        }

        @Override
        public long getBytesScanned()
        {
            return 0;
        }

        @Override
        public Set<SSTableReader> getBackingSSTables()
        {
            return Collections.emptySet();
        }

        private class UnfilteredIterator implements UnfilteredRowIterator
        {

            public RegularAndStaticColumns columns()
            {
                return metadata.regularAndStaticColumns();
            }

            @Override
            public TableMetadata metadata()
            {
                return metadata;
            }

            public boolean isReverseOrder()
            {
                return false;
            }

            public DecoratedKey partitionKey()
            {
                return key;
            }

            public DeletionTime partitionLevelDeletion()
            {
                return partitionLevelDeletion;
            }

            public Row staticRow()
            {
                return staticRow;
            }

            public EncodingStats stats()
            {
                return header.stats();
            }

            public boolean hasNext()
            {
                try
                {
                    return iterator.hasNext();
                }
                catch (final IOError e)
                {
                    // SSTableSimpleIterator::computeNext wraps IOException in IOError
                    // so we catch those, try to extract the IOException and re-wrap it in
                    // an SSTableStreamException, which we can then process in TableStreamScanner
                    if (e.getCause() instanceof IOException)
                    {
                        throw new SSTableStreamException((IOException) e.getCause());
                    }

                    // otherwise, just throw the IOError and deal with it further up the stack
                    throw e;
                }
            }

            public Unfiltered next()
            {
                // Note that in practice we know that IOException will be thrown by hasNext(), because that's
                // where the actual reading happens, so we don't bother catching IOError here (contrarily
                // to what we do in hasNext)
                return iterator.next();
            }

            public void close()
            {
            }
        }
    }
}
