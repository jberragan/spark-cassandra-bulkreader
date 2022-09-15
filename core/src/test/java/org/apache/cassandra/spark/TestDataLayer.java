package org.apache.cassandra.spark;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.NotImplementedException;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.TableIdLookup;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.sparksql.filters.CdcOffset;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
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
public class TestDataLayer extends DataLayer
{
    public static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("test-file-io-%d").setDaemon(true).build());

    @NotNull
    final CassandraBridge bridge;
    @NotNull
    final Collection<Path> dataDbFiles;
    @Nullable
    final CqlTable schema;
    final String jobId;

    public TestDataLayer(@NotNull final CassandraBridge bridge, @NotNull final Collection<Path> dataDbFiles)
    {
        this(bridge, dataDbFiles, null);
    }

    public TestDataLayer(@NotNull final CassandraBridge bridge, @NotNull final Collection<Path> dataDbFiles, @Nullable final CqlTable schema)
    {
        this.bridge = bridge;
        this.dataDbFiles = dataDbFiles;
        this.schema = schema;
        this.jobId = UUID.randomUUID().toString();
    }

    @Override
    public CassandraBridge.CassandraVersion version()
    {
        return bridge.getVersion();
    }

    @Override
    public int partitionCount()
    {
        return 0;
    }

    @Override
    public CqlTable cqlTable()
    {
        return schema;
    }

    public Set<CqlTable> cdcTables()
    {
        if (schema != null)
        {
            return Set.of(schema);
        }
        return Set.of();
    }

    @Override
    public boolean isInPartition(final BigInteger token, final ByteBuffer key)
    {
        return true;
    }

    public CommitLogProvider commitLogs()
    {
        throw new NotImplementedException("Test CommitLogProvider not implemented yet");
    }

    public TableIdLookup tableIdLookup()
    {
        throw new NotImplementedException("Test TableIdLookup not implemented yet");
    }

    protected ExecutorService executorService()
    {
        return EXECUTOR;
    }

    @Override
    public SSTablesSupplier sstables(@Nullable final SparkRangeFilter sparkRangeFilter,
                                     @NotNull final List<PartitionKeyFilter> partitionKeyFilters)
    {
        return LocalDataLayer.basicSupplier(listSSTables());
    }

    public Stream<SSTable> listSSTables()
    {
        return dataDbFiles.stream().map(TestSSTable::new);
    }

    @Override
    public Partitioner partitioner()
    {
        return Partitioner.Murmur3Partitioner;
    }

    public String jobId()
    {
        return jobId;
    }

    public CommitLog toLog(CassandraInstance instance, CdcOffset.SerializableCommitLog commitLog)
    {
        throw new NotImplementedException("Test toLog method not implemented yet");
    }

    class TestSSTable extends DataLayer.SSTable
    {
        private final Path dataDbFile;

        TestSSTable(final Path dataDbFile)
        {
            this.dataDbFile = dataDbFile;
        }

        @Nullable
        @Override
        protected InputStream openInputStream(final FileType fileType)
        {
            final Path filePath = FileType.resolveComponentFile(fileType, this.dataDbFile);
            try
            {
                return filePath != null ? new BufferedInputStream(new FileInputStream(filePath.toFile())) : null;
            }
            catch (final FileNotFoundException e)
            {
                return null;
            }
        }

        public boolean isMissing(FileType fileType)
        {
            return FileType.resolveComponentFile(fileType, this.dataDbFile) == null;
        }

        @Override
        public String getDataFileName()
        {
            return dataDbFile.getFileName().toString();
        }

        public int hashCode()
        {
            return dataDbFile.hashCode();
        }

        public boolean equals(Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (obj == this)
            {
                return true;
            }
            if (obj.getClass() != getClass())
            {
                return false;
            }

            return dataDbFile.equals(((TestSSTable) obj).dataDbFile);
        }
    }
}
