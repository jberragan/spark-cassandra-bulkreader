package org.apache.cassandra.spark.data;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.apache.cassandra.spark.sparksql.CustomFilter;
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

/**
 * Basic DataLayer implementation to read SSTables from local file system.  Mostly used for testing.
 */
@SuppressWarnings({ "unused", "WeakerAccess" })
public class LocalDataLayer extends DataLayer implements Serializable
{
    public static final long serialVersionUID = 42L;

    private final Partitioner partitioner;
    private final CassandraBridge.CassandraVersion version;
    private final String[] paths;
    private final CqlSchema cqlSchema;

    @VisibleForTesting
    public LocalDataLayer(@NotNull final CassandraBridge.CassandraVersion version,
                          @NotNull final String keyspace,
                          @NotNull final String createStmt,
                          final String... paths)
    {
        this(version, Partitioner.Murmur3Partitioner, keyspace, createStmt, Collections.emptySet(), paths);
    }

    public LocalDataLayer(@NotNull final CassandraBridge.CassandraVersion version,
                          @NotNull final Partitioner partitioner,
                          @NotNull final String keyspace,
                          @NotNull final String createStmt,
                          @NotNull final Set<String> udts,
                          final String... paths)
    {
        super();
        this.version = version;
        this.partitioner = partitioner;
        this.cqlSchema = bridge().buildSchema(keyspace, createStmt, new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, ImmutableMap.of("replication_factor", 1)), partitioner, udts);
        this.paths = paths;
    }

    private LocalDataLayer(@NotNull final CassandraBridge.CassandraVersion version,
                           @NotNull final Partitioner partitioner,
                           @NotNull final String[] paths,
                           @NotNull final CqlSchema cqlSchema)
    {
        this.version = version;
        this.partitioner = partitioner;
        this.paths = paths;
        this.cqlSchema = cqlSchema;
    }

    @Override
    public CassandraBridge.CassandraVersion version()
    {
        return version;
    }

    @Override
    public int partitionCount()
    {
        return 1;
    }

    @Override
    public CqlSchema cqlSchema()
    {
        return this.cqlSchema;
    }

    @Override
    public boolean isInPartition(final BigInteger token, final ByteBuffer key)
    {
        return true;
    }

    public static BasicSupplier basicSupplier(@NotNull final Stream<DataLayer.SSTable> ssTables)
    {
        return new BasicSupplier(ssTables.collect(Collectors.toSet()));
    }

    private static class BasicSupplier extends SSTablesSupplier
    {
        private final Set<DataLayer.SSTable> ssTables;

        BasicSupplier(@NotNull final Set<DataLayer.SSTable> ssTables)
        {
            this.ssTables = ssTables;
        }

        @Override
        public <T extends SparkSSTableReader> Set<T> openAll(final ReaderOpener<T> readerOpener)
        {
            return ssTables.stream().map(ssTable -> {
                try
                {
                    return readerOpener.openReader(ssTable);
                }
                catch (final IOException e)
                {
                    throw new RuntimeException(e);
                }
            }).filter(reader -> !reader.ignore()).collect(Collectors.toSet());
        }
    }

    @Override
    public SSTablesSupplier sstables(final List<CustomFilter> filters)
    {
        return LocalDataLayer.basicSupplier(listSSTables());
    }

    private Stream<SSTable> listSSTables()
    {
        return Arrays.stream(paths)
                     .map(path -> Paths.get(path))
                     .map(LocalDataLayer::list)
                     .flatMap(Function.identity())
                     .filter(path -> path.getFileName().toString().endsWith("-" + FileType.DATA.getFileSuffix()))
                     .map(Path::toString)
                     .map(DiskSSTable::new);
    }

    @Override
    public Partitioner partitioner()
    {
        return this.partitioner;
    }

    private static Stream<Path> list(final Path path)
    {
        try
        {
            return Files.list(path);
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public class DiskSSTable extends SSTable
    {

        private final String dataFilePath;

        DiskSSTable(final String dataFilePath)
        {
            this.dataFilePath = dataFilePath;
        }

        @Override
        protected InputStream openInputStream(final FileType fileType)
        {
            final Path dataFilePath = Paths.get(this.dataFilePath);
            final Path filePath = FileType.resolveComponentFile(fileType, dataFilePath);
            try
            {
                return filePath != null ? new BufferedInputStream(new FileInputStream(filePath.toFile())) : null;
            }
            catch (final FileNotFoundException e)
            {
                return null;
            }
        }

        @Override
        public boolean isMissing(final FileType fileType)
        {
            final Path dataFilePath = Paths.get(this.dataFilePath);
            return FileType.resolveComponentFile(fileType, dataFilePath) == null;
        }

        @Override
        public String getDataFileName()
        {
            return Paths.get(this.dataFilePath).getFileName().toString();
        }

        @Override
        public int hashCode()
        {
            return dataFilePath.hashCode();
        }

        @Override
        public boolean equals(final Object o)
        {
            return o instanceof DiskSSTable && dataFilePath.equals(((DiskSSTable) o).dataFilePath);
        }
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(7, 17)
               .append(cqlSchema)
               .append(paths)
               .append(version)
               .toHashCode();
    }

    @Override
    public boolean equals(final Object obj)
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

        final LocalDataLayer rhs = (LocalDataLayer) obj;
        return new EqualsBuilder()
               .append(cqlSchema, rhs.cqlSchema)
               .append(paths, rhs.paths)
               .append(version, rhs.version)
               .isEquals();
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<LocalDataLayer>
    {
        @Override
        public void write(final Kryo kryo, final Output out, final LocalDataLayer obj)
        {
            kryo.writeObject(out, obj.version);
            kryo.writeObject(out, obj.partitioner);
            kryo.writeObject(out, obj.paths);
            kryo.writeObject(out, obj.cqlSchema);
        }

        @Override
        public LocalDataLayer read(final Kryo kryo, final Input in, final Class<LocalDataLayer> type)
        {
            return new LocalDataLayer(
            kryo.readObject(in, CassandraBridge.CassandraVersion.class),
            kryo.readObject(in, Partitioner.class),
            kryo.readObject(in, String[].class),
            kryo.readObject(in, CqlSchema.class)
            );
        }
    }
}
