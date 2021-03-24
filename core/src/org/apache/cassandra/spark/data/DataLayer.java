package org.apache.cassandra.spark.data;

import java.io.InputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.EmptyScanner;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.sparksql.CustomFilter;
import org.apache.cassandra.spark.sparksql.NoMatchFoundException;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.StructType;
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
@SuppressWarnings({ "unused", "WeakerAccess" })
public abstract class DataLayer implements Serializable
{

    public static final long serialVersionUID = 42L;
    private static final Set<?> SUPPORTED_FILTER_TYPES = new HashSet<>(Arrays.asList(EqualTo.class, In.class));

    public enum FileType
    {
        DATA("Data.db"),
        INDEX("Index.db"),
        FILTER("Filter.db"),
        STATISTICS("Statistics.db"),
        SUMMARY("Summary.db"),
        COMPRESSION_INFO("CompressionInfo.db"),
        TOC("TOC.txt"),
        DIGEST("Digest.sha1"),
        CRC("CRC.db"),
        CRC32("Digest.crc32");

        private final String fileSuffix;

        FileType(final String fileSuffix)
        {
            this.fileSuffix = fileSuffix;
        }

        private static final Map<String, FileType> FILE_TYPE_HASH_MAP = new HashMap<>();

        static
        {
            for (final DataLayer.FileType fileType : FileType.values())
            {
                FILE_TYPE_HASH_MAP.put(fileType.getFileSuffix(), fileType);
            }
        }

        public static DataLayer.FileType fromExtension(final String extension)
        {
            Preconditions.checkArgument(FILE_TYPE_HASH_MAP.containsKey(extension), "Unknown sstable file type: " + extension);
            return FILE_TYPE_HASH_MAP.get(extension);
        }

        @Nullable
        public static Path resolveComponentFile(final FileType fileType, final Path dataFilePath)
        {
            final Path filePath = fileType == FileType.DATA ? dataFilePath : dataFilePath.resolveSibling(dataFilePath.getFileName().toString().replace(FileType.DATA.getFileSuffix(), fileType.getFileSuffix()));
            return Files.exists(filePath) ? filePath : null;
        }

        public String getFileSuffix()
        {
            return fileSuffix;
        }
    }

    public DataLayer()
    {
    }

    /**
     * Map Cassandra CQL table schema to SparkSQL StructType
     *
     * @return StructType representation of CQL table
     */
    public StructType structType()
    {
        final CassandraBridge bridge = bridge();
        StructType structType = new StructType();
        for (final CqlField field : cqlSchema().fields())
        {
            structType = structType.add(field.name(), bridge.toSparkSQLType(field.type(), bigNumberConfig(field)));
        }
        return structType;
    }

    /**
     * DataLayer can override this method to return the BigInteger/BigDecimal precision/scale values for a given column
     *
     * @return a BigNumberConfig object that specifies the desired precision/scale for BigDecimal and BigInteger.
     */
    public CassandraBridge.BigNumberConfig bigNumberConfig(final CqlField field)
    {
        return CassandraBridge.BigNumberConfig.DEFAULT;
    }

    /**
     * @return Cassandra version (3.0, 4.0 etc)
     */
    public abstract CassandraBridge.CassandraVersion version();

    /**
     * @return version specific CassandraBridge wrapping shaded packages
     */
    public CassandraBridge bridge()
    {
        return CassandraBridge.get(version());
    }

    public abstract int partitionCount();

    public abstract CqlSchema cqlSchema();

    public abstract boolean isInPartition(final BigInteger token, final ByteBuffer key);

    public List<CustomFilter> filtersInRange(final List<CustomFilter> filters) throws NoMatchFoundException
    {
        return filters;
    }

    /**
     * @return set of SSTables
     */
    public abstract SSTablesSupplier sstables(final List<CustomFilter> filters);

    public abstract Partitioner partitioner();

    /**
     * @return CompactionScanner for iterating over one or more SSTables, compacting data and purging tombstones
     */
    public IStreamScanner openCompactionScanner(final List<CustomFilter> filters)
    {
        List<CustomFilter> filtersInRange;
        try
        {
            filtersInRange = filtersInRange(filters);
        }
        catch (NoMatchFoundException e)
        {
            return EmptyScanner.INSTANCE;
        }
        return bridge().getCompactionScanner(cqlSchema(), partitioner(), sstables(filtersInRange), filtersInRange, stats());
    }

    /**
     * @param filters array of push down filters that
     * @return an array of push filters that are *not* supported by this data layer
     */
    public Filter[] unsupportedPushDownFilters(final Filter[] filters)
    {
        // push down filters other than EqualTo & In not supported yet
        return Arrays.stream(filters).filter(filter -> !SUPPORTED_FILTER_TYPES.contains(filter.getClass())).toArray(Filter[]::new);
    }

    /**
     * Override to plug in your own Stats instrumentation for recording internal events.
     *
     * @return Stats implementation to record internal events
     */
    public Stats stats() {
        return Stats.DoNothingStats.INSTANCE;
    }

    /**
     * Abstract class representing a single SSTable.
     * Implementations must override hashCode and equals methods
     */
    @SuppressWarnings("unused")
    public abstract class SSTable implements Serializable
    {

        public static final long serialVersionUID = 42L;

        public SSTable()
        {

        }

        @Nullable
        protected abstract InputStream openInputStream(final FileType fileType);

        @Nullable
        public InputStream openCompressionStream()
        {
            return openInputStream(FileType.COMPRESSION_INFO);
        }

        @Nullable
        public InputStream openStatsStream()
        {
            return openInputStream(FileType.STATISTICS);
        }

        @Nullable
        public InputStream openSummaryStream()
        {
            return openInputStream(FileType.SUMMARY);
        }

        @Nullable
        public InputStream openPrimaryIndexStream()
        {
            return openInputStream(FileType.INDEX);
        }

        @Nullable
        public InputStream openFilterStream()
        {
            return openInputStream(FileType.FILTER);
        }

        @NotNull
        public InputStream openDataStream()
        {
            return Objects.requireNonNull(openInputStream(FileType.DATA), "Data.db SSTable file component must exist");
        }

        public abstract boolean isMissing(final FileType fileType);

        public void verify() throws IncompleteSSTableException
        {
            // need Data.db file
            if (isMissing(FileType.DATA))
            {
                throw new IncompleteSSTableException(FileType.DATA);
            }
            // need Statistics.db file to open SerializationHeader
            if (isMissing(FileType.STATISTICS))
            {
                throw new IncompleteSSTableException(FileType.STATISTICS);
            }
            // need Summary.db or Index.db to read first/last partition key
            if (isMissing(FileType.SUMMARY) && isMissing(FileType.INDEX))
            {
                throw new IncompleteSSTableException(FileType.SUMMARY, FileType.INDEX);
            }
        }

        public abstract String getDataFileName();
    }
}
