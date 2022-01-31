package org.apache.cassandra.spark.reader.fourzero;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.apache.cassandra.spark.reader.common.RawInputStream;
import org.apache.cassandra.spark.reader.common.SSTableStreamException;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DecoratedKey;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DeletionTime;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.SerializationHeader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.UnfilteredDeserializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Row;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.SSTableSimpleIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.format.Version;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.net.MessagingService;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.DroppedColumn;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.service.ActiveRepairService;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.spark.sparksql.filters.CustomFilter;
import org.apache.cassandra.spark.sparksql.filters.PruneColumnFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.ByteBufUtils;
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

    private final TableMetadata metadata;
    @NotNull
    private final DataLayer.SSTable ssTable;
    private final StatsMetadata statsMetadata;
    @NotNull
    private final Version version;
    @NotNull
    private final DecoratedKey first, last;
    @NotNull
    private final BigInteger firstToken, lastToken;
    private final SerializationHeader header;
    private final DeserializationHelper helper;
    @NotNull
    private final AtomicReference<SSTableStreamReader> reader = new AtomicReference<>(null);
    @NotNull
    private final List<CustomFilter> filters;
    @NotNull
    private final Stats stats;
    @Nullable
    private Long startOffset = null;
    private Long openedNanos = null;
    @NotNull
    private final Function<StatsMetadata, Boolean> isRepaired;

    static class Builder
    {
        @NotNull
        final TableMetadata metadata;
        @NotNull
        final DataLayer.SSTable ssTable;
        @NotNull
        List<CustomFilter> filters = new ArrayList<>();
        @Nullable
        PruneColumnFilter columnFilter = null;
        boolean readIndexOffset = true;
        @NotNull
        Stats stats = Stats.DoNothingStats.INSTANCE;
        boolean useIncrementalRepair = true, isRepairPrimary = false;
        Function<StatsMetadata, Boolean> isRepaired = (stats) -> stats.repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE;

        Builder(@NotNull TableMetadata metadata,
                @NotNull final DataLayer.SSTable ssTable)
        {
            this.metadata = metadata;
            this.ssTable = ssTable;
        }

        public Builder withFilters(@NotNull final List<CustomFilter> filters)
        {
            this.filters = filters;
            return this;
        }

        public Builder withColumnFilter(@Nullable final PruneColumnFilter columnFilter)
        {
            this.columnFilter = columnFilter;
            return this;
        }

        public Builder withReadIndexOffset(final boolean readIndexOffset)
        {
            this.readIndexOffset = readIndexOffset;
            return this;
        }

        public Builder withStats(@NotNull final Stats stats)
        {
            this.stats = stats;
            return this;
        }

        public Builder useIncrementalRepair(final boolean useIncrementalRepair)
        {
            this.useIncrementalRepair = useIncrementalRepair;
            return this;
        }

        public Builder isRepairPrimary(final boolean isRepairPrimary)
        {
            this.isRepairPrimary = isRepairPrimary;
            return this;
        }

        public Builder withIsRepairedFunction(final Function<StatsMetadata, Boolean> isRepaired)
        {
            this.isRepaired = isRepaired;
            return this;
        }

        FourZeroSSTableReader build() throws IOException
        {
            return new FourZeroSSTableReader(metadata, ssTable, filters, columnFilter, readIndexOffset, stats, useIncrementalRepair, isRepairPrimary, isRepaired);
        }
    }

    public static Builder builder(@NotNull TableMetadata metadata,
                                  @NotNull final DataLayer.SSTable ssTable)
    {
        return new Builder(metadata, ssTable);
    }

    FourZeroSSTableReader(@NotNull TableMetadata metadata,
                          @NotNull final DataLayer.SSTable ssTable,
                          @NotNull List<CustomFilter> filters,
                          @Nullable PruneColumnFilter columnFilter,
                          final boolean readIndexOffset,
                          @NotNull final Stats stats,
                          final boolean useIncrementalRepair,
                          final boolean isRepairPrimary,
                          final Function<StatsMetadata, Boolean> isRepaired) throws IOException
    {
        final long startTimeNanos = System.nanoTime();
        long now;
        this.ssTable = ssTable;
        this.stats = stats;
        this.isRepaired = isRepaired;

        final File file = constructFilename(metadata.keyspace, metadata.name, ssTable.getDataFileName());
        final Descriptor descriptor = Descriptor.fromFilename(file);
        this.version = descriptor.version;

        SummaryDbUtils.Summary summary = null;
        Pair<DecoratedKey, DecoratedKey> keys = Pair.of(null, null);
        try
        {
            now = System.nanoTime();
            summary = SSTableCache.INSTANCE.keysFromSummary(metadata, ssTable);
            stats.readSummaryDb(ssTable, System.nanoTime() - now);
            keys = Pair.of(summary.first(), summary.last());
        }
        catch (final IOException e)
        {
            LOGGER.warn("Failed to read Summary.db file sstable='{}'", ssTable, e);
        }

        if (keys.getLeft() == null || keys.getRight() == null)
        {
            LOGGER.warn("Could not load first and last key from Summary.db file, so attempting Index.db fileName={}", ssTable.getDataFileName());
            now = System.nanoTime();
            keys = SSTableCache.INSTANCE.keysFromIndex(metadata, ssTable);
            stats.readIndexDb(ssTable, System.nanoTime() - now);
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
            stats.skippedSSTable(filters, firstToken, lastToken);
            LOGGER.info("Ignoring SSTableReader with firstToken={} lastToken={}, does not overlap with any filter", this.firstToken, this.lastToken);
            statsMetadata = null;
            header = null;
            helper = null;
            this.metadata = null;
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
                this.metadata = null;
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

        if (useIncrementalRepair && !isRepairPrimary && isRepaired())
        {
            stats.skippedRepairedSSTable(ssTable, statsMetadata.repairedAt);
            LOGGER.info("Ignoring repaired SSTable on non-primary repair replica sstable='{}' repairedAt={}", ssTable, statsMetadata.repairedAt);
            header = null;
            helper = null;
            this.metadata = null;
            return;
        }

        final Set<String> columnNames = metadata.columns().stream().map(f -> f.name.toString()).collect(Collectors.toSet());
        metadata.staticColumns().stream().map(f -> f.name.toString()).forEach(columnNames::add);

        final Map<ByteBuffer, DroppedColumn> droppedColumns = new HashMap<>();
        droppedColumns.putAll(buildDroppedColumns(metadata.keyspace, metadata.name, ssTable, headerComp.getRegularColumns(), columnNames, ColumnMetadata.Kind.REGULAR));
        droppedColumns.putAll(buildDroppedColumns(metadata.keyspace, metadata.name, ssTable, headerComp.getStaticColumns(), columnNames, ColumnMetadata.Kind.STATIC));
        if (!droppedColumns.isEmpty())
        {
            LOGGER.info("Rebuilding table metadata with dropped columns numDroppedColumns={} sstable='{}'", droppedColumns.size(), ssTable);
            metadata = metadata.unbuild().droppedColumns(droppedColumns).build();
        }

        this.header = headerComp.toHeader(metadata);
        this.helper = new DeserializationHelper(metadata, MessagingService.VERSION_30, DeserializationHelper.Flag.FROM_REMOTE, buildColumnFilter(metadata, columnFilter));
        this.metadata = metadata;

        if (readIndexOffset && summary != null)
        {
            final SummaryDbUtils.Summary finalSummary = summary;
            extractRange(filters).ifPresent(range -> readOffsets(finalSummary.summary(), range));
        }
        else
        {
            LOGGER.warn("Reading SSTable without looking up start/end offset, performance will potentially be degraded.");
        }

        // open SSTableStreamReader so opened in parallel inside thread pool
        // and buffered + ready to go when CompactionIterator starts reading
        reader.set(new SSTableStreamReader());
        stats.openedSSTable(ssTable, System.nanoTime() - startTimeNanos);
        this.openedNanos = System.nanoTime();
    }

    /**
     * Constructs full file path for a given combination of keyspace, table, and data file name,
     * while adjusting for data files with non-standard names prefixed with keyspace and table.
     *
     * @param keyspace Name of the keyspace
     * @param table    Name of the table
     * @param filename Name of the data file
     * @return A full file path, adjusted for non-standard file names
     */
    @VisibleForTesting
    @NotNull
    static File constructFilename(@NotNull final String keyspace,
                                  @NotNull final String table,
                                  @NotNull String filename)
    {
        final String[] components = filename.split("-");
        if (components.length == 6 &&
            components[0].equals(keyspace) &&
            components[1].equals(table))
        {
            filename = filename.substring(keyspace.length() + table.length() + 2);
        }

        return new File(String.format("./%s/%s", keyspace, table), filename);
    }

    private static Map<ByteBuffer, DroppedColumn> buildDroppedColumns(final String keyspace,
                                                                      final String table,
                                                                      final DataLayer.SSTable ssTable,
                                                                      final Map<ByteBuffer, AbstractType<?>> columns,
                                                                      final Set<String> columnNames,
                                                                      final ColumnMetadata.Kind kind)
    {
        final Map<ByteBuffer, DroppedColumn> droppedColumns = new HashMap<>();
        for (final Map.Entry<ByteBuffer, AbstractType<?>> entry : columns.entrySet())
        {
            final String colName = UTF8Type.instance.getString((entry.getKey()));
            if (!columnNames.contains(colName))
            {
                final AbstractType<?> type = entry.getValue();
                LOGGER.warn("Dropped column found colName={} sstable='{}'", colName, ssTable);
                final ColumnMetadata column = new ColumnMetadata(keyspace, table, ColumnIdentifier.getInterned(colName, true), type, ColumnMetadata.NO_POSITION, kind);
                long droppedTime = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()) - TimeUnit.MINUTES.toMicros(60);
                droppedColumns.put(entry.getKey(), new DroppedColumn(column, droppedTime));
            }
        }
        return droppedColumns;
    }

    /**
     * Extract the token range we care about from the Spark token range and the Partition key filters.
     *
     * @param filters list of filters
     * @return the token range we care about for this Spark worker.
     */
    public static Optional<Range<BigInteger>> extractRange(List<CustomFilter> filters)
    {
        // TODO: these 'CustomFilters' are too generic and we often end up doing something like this.
        // In reality we only have a single SparkRangeFilter for the Spark worker's token range,
        // and sometimes multiple PartitionKeyFilters if the user is using predicate push-downs.
        // TODO: a potential improvement when using PartitionKeyFilters might be to read the offset for all
        // the partition key filters (not just start & end), so we can efficiently skip over ignorable partition keys.
        final Optional<Range<BigInteger>> partitionKeyRange = filters.stream()
                                                                     .filter(CustomFilter::isSpecificRange)
                                                                     .map(CustomFilter::tokenRange)
                                                                     .reduce(CustomFilter::mergeRanges);
        if (partitionKeyRange.isPresent())
        {
            return partitionKeyRange;
        }
        return filters.stream() // when no partition key filters
                      .map(CustomFilter::tokenRange)
                      .reduce(CustomFilter::mergeRanges);
    }

    /**
     * Read Data.db offsets by binary searching Summary.db into Index.db, then reading offsets in Index.db.
     *
     * @param indexSummary Summary.db index summary
     * @param range        token range we care about for this Spark worker
     */
    private void readOffsets(final IndexSummary indexSummary,
                             final Range<BigInteger> range)
    {
        try
        {
            // if start is null we failed to find an overlapping token in the Index.db file,
            // this is unlikely as we already pre-filter the SSTable based on the start-end token range.
            // but in this situation we read the entire Data.db file to be safe, even if it hits performance
            this.startOffset = IndexDbUtils.findDataDbOffset(indexSummary, range, metadata.partitioner, ssTable, stats);
            if (this.startOffset == null)
            {
                LOGGER.error("Failed to find Data.db start offset, performance will be degraded sstable='{}'", ssTable);
            }
        }
        catch (IOException e)
        {
            LOGGER.warn("IOException finding SSTable offsets, cannot skip directly to start offset in Data.db. Performance will be degraded.", e);
        }
    }

    /**
     * Build a ColumnFilter if we need to prune any columns for more efficient deserialization of the SSTable.
     *
     * @param metadata     TableMetadata object
     * @param columnFilter prune column filter
     * @return ColumnFilter if and only if we can prune any columns when deserializing the SSTable, otherwise return null.
     */
    @Nullable
    private static ColumnFilter buildColumnFilter(TableMetadata metadata, @Nullable PruneColumnFilter columnFilter)
    {
        if (columnFilter == null)
        {
            return null;
        }
        final List<ColumnMetadata> include = metadata.columns()
                                                     .stream()
                                                     .filter(col -> columnFilter.includeColumn(col.name.toString()))
                                                     .collect(Collectors.toList());
        if (include.size() == metadata.columns().size())
        {
            return null; // no columns pruned
        }
        return ColumnFilter.allRegularColumnsBuilder(metadata)
                           .addAll(include)
                           .build();
    }

    public DataLayer.SSTable sstable()
    {
        return ssTable;
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
        return isRepaired.apply(this.statsMetadata);
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
        final RawInputStream dataStream;
        private DecoratedKey key;
        private DeletionTime partitionLevelDeletion;
        private SSTableSimpleIterator iterator;
        private Row staticRow;
        @Nullable
        private final BigInteger lastToken;
        private long lastTimeNanos = System.nanoTime();

        SSTableStreamReader() throws IOException
        {
            this.lastToken = filters.stream()
                                    .filter(f -> !f.isSpecificRange())
                                    .map(CustomFilter::tokenRange)
                                    .reduce(CustomFilter::mergeRanges)
                                    .map(Range::upperEndpoint)
                                    .orElse(null);
            try (@Nullable final InputStream compressionInfoInputStream = ssTable.openCompressionStream())
            {
                final DataInputStream dataInputStream = new DataInputStream(ssTable.openDataStream());

                if (compressionInfoInputStream != null)
                {
                    dataStream = CompressedRawInputStream.fromInputStream(ssTable, dataInputStream, compressionInfoInputStream, version.hasMaxCompressedLength(), stats);
                }
                else
                {
                    dataStream = new RawInputStream(dataInputStream, new byte[64 * 1024], stats);
                }
            }
            this.dis = new DataInputStream(dataStream);
            if (startOffset != null)
            {
                // skip to start offset, if known, of first in-range partition
                ByteBufUtils.skipFully(dis, startOffset);
                assert (this.dataStream.position() == startOffset);
                LOGGER.info("Using Data.db start offset to skip ahead startOffset={} sstable='{}'", startOffset, ssTable);
                stats.skippedDataDbStartOffset(startOffset);
            }
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
                    final BigInteger token = FourZeroUtils.tokenToBigInteger(key.getToken());
                    if (filters.isEmpty() || filters.stream().anyMatch(filter -> !filter.skipPartition(key.getKey(), token)))
                    {
                        // partition overlaps with filters
                        final long now = System.nanoTime();
                        stats.nextPartition(now - lastTimeNanos);
                        lastTimeNanos = now;
                        return true;
                    }
                    if (lastToken != null && startOffset != null && lastToken.compareTo(token) < 0)
                    {
                        // partition no longer overlaps SparkTokenRange so we've finished reading this SSTable
                        stats.skippedDataDbEndOffset(dataStream.position() - startOffset);
                        return false;
                    }
                    stats.skippedPartition(key.getKey(), FourZeroUtils.tokenToBigInteger(key.getToken()));
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
                if (openedNanos != null)
                {
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
