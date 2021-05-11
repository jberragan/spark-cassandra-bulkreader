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
import java.util.stream.Stream;

import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.sparksql.filters.CustomFilter;
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

    @NotNull
    final CassandraBridge bridge;
    @NotNull
    final Collection<Path> dataDbFiles;
    @Nullable
    final CqlSchema schema;

    public TestDataLayer(@NotNull final CassandraBridge bridge, @NotNull final Collection<Path> dataDbFiles)
    {
        this(bridge, dataDbFiles, null);
    }

    public TestDataLayer(@NotNull final CassandraBridge bridge, @NotNull final Collection<Path> dataDbFiles, @Nullable final CqlSchema schema)
    {
        this.bridge = bridge;
        this.dataDbFiles = dataDbFiles;
        this.schema = schema;
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
    public CqlSchema cqlSchema()
    {
        return schema;
    }

    @Override
    public boolean isInPartition(final BigInteger token, final ByteBuffer key)
    {
        return true;
    }

    @Override
    public SSTablesSupplier sstables(final List<CustomFilter> filters)
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
    }
}
