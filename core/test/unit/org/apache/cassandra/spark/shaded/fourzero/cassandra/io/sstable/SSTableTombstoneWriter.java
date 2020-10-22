package org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.shaded.fourzero.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.QueryOptions;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.UDHelper;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.statements.Bound;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.statements.schema.CreateTypeStatement;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Clustering;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.ClusteringBound;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.ClusteringComparator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Slice;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Slices;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.SystemKeyspace;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.IPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Functions;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Schema;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.SchemaConstants;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Tables;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Types;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Views;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.service.ClientState;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.ProtocolVersion;
import org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.TypeCodec;

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
 * Re-write of CQLSSTableWriter for writing tombstones to an SSTable for testing
 */
public class SSTableTombstoneWriter implements Closeable
{
    private static final ByteBuffer UNSET_VALUE = ByteBufferUtil.UNSET_BYTE_BUFFER;

    static
    {
        DatabaseDescriptor.clientInitialization(false);
        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
        {
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        }
    }

    private final AbstractSSTableSimpleWriter writer;
    private final DeleteStatement delete;
    private final List<ColumnSpecification> boundNames;
    private final List<TypeCodec> typeCodecs;
    private final ClusteringComparator comparator;

    private SSTableTombstoneWriter(final AbstractSSTableSimpleWriter writer,
                                   final DeleteStatement delete,
                                   final List<ColumnSpecification> boundNames,
                                   final ClusteringComparator comparator)
    {
        this.writer = writer;
        this.delete = delete;
        this.boundNames = boundNames;
        this.typeCodecs = boundNames.stream().map(bn -> UDHelper.codecFor(UDHelper.driverType(bn.type)))
                                    .collect(Collectors.toList());
        this.comparator = comparator;
    }

    /**
     * Returns a new builder for a SSTableTombstoneWriter.
     *
     * @return the new builder.
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Adds a new row to the writer.
     * <p>
     * This is a shortcut for {@code addRow(Arrays.asList(values))}.
     *
     * @param values the row values (corresponding to the bind variables of the
     *               deletion statement used when creating by this writer).
     */
    public void addRow(final Object... values) throws InvalidRequestException, IOException
    {
        addRow(Arrays.asList(values));
    }

    /**
     * Adds a new row to the writer.
     * <p>
     * Each provided value type should correspond to the types of the CQL column
     * the value is for. The correspondance between java type and CQL type is the
     * same one than the one documented at
     * www.datastax.com/drivers/java/2.0/apidocs/com/datastax/driver/core/DataType.Name.html#asJavaClass().
     * <p>
     * If you prefer providing the values directly as binary, use
     *
     * @param values the row values (corresponding to the bind variables of the
     *               deletion statement used when creating by this writer).
     */
    private void addRow(final List<Object> values) throws InvalidRequestException, IOException
    {
        final int size = Math.min(values.size(), boundNames.size());
        final List<ByteBuffer> rawValues = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
        {
            final Object value = values.get(i);
            rawValues.add(serialize(value, typeCodecs.get(i)));
        }

        rawAddRow(rawValues);
    }

    /**
     * Adds a new row to the writer given already serialized values.
     * <p>
     * This is a shortcut for {@code rawAddRow(Arrays.asList(values))}.
     *
     * @param values the row values (corresponding to the bind variables of the
     *               deletion statement used when creating by this writer) as binary.
     */
    private void rawAddRow(final List<ByteBuffer> values) throws InvalidRequestException, IOException
    {
        if (values.size() != boundNames.size())
        {
            throw new InvalidRequestException(String.format("Invalid number of arguments, expecting %d values but got %d", boundNames.size(), values.size()));
        }

        final QueryOptions options = QueryOptions.forInternalCalls(null, values);
        final List<ByteBuffer> keys = delete.buildPartitionKeyNames(options);

        final long now = System.currentTimeMillis();
        // Note that we asks indexes to not validate values (the last 'false' arg below) because that triggers a 'Keyspace.open'
        // and that forces a lot of initialization that we don't want.
        final UpdateParameters params = new UpdateParameters(delete.metadata,
                                                             delete.updatedColumns(),
                                                             options,
                                                             delete.getTimestamp(TimeUnit.MILLISECONDS.toMicros(now), options),
                                                             (int) TimeUnit.MILLISECONDS.toSeconds(now),
                                                             delete.getTimeToLive(options),
                                                             Collections.emptyMap());

        if (delete.hasSlices())
        {
            // write out range tombstones
            final SortedSet<ClusteringBound> startBounds = delete.getRestrictions().getClusteringColumnsBounds(Bound.START, options);
            final SortedSet<ClusteringBound> endBounds = delete.getRestrictions().getClusteringColumnsBounds(Bound.END, options);
            final Slices slices = toSlices(startBounds, endBounds);

            try
            {
                for (final ByteBuffer key : keys)
                {
                    for (final Slice slice : slices)
                    {
                        delete.addUpdateForKey(writer.getUpdateFor(key), slice, params);
                    }
                }
                return;
            }
            catch (final SSTableSimpleUnsortedWriter.SyncException e)
            {
                // If we use a BufferedWriter and had a problem writing to disk, the IOException has been
                // wrapped in a SyncException (see BufferedWriter below). We want to extract that IOE.
                throw (IOException) e.getCause();
            }
        }

        final SortedSet<Clustering> clusterings = delete.createClustering(options);
        try
        {
            for (final ByteBuffer key : keys)
            {
                for (final Clustering clustering : clusterings)
                {
                    delete.addUpdateForKey(writer.getUpdateFor(key), clustering, params);
                }
            }
        }
        catch (final SSTableSimpleUnsortedWriter.SyncException e)
        {
            // If we use a BufferedWriter and had a problem writing to disk, the IOException has been
            // wrapped in a SyncException (see BufferedWriter below). We want to extract that IOE.
            throw (IOException) e.getCause();
        }
    }

    private Slices toSlices(final SortedSet<ClusteringBound> startBounds, final SortedSet<ClusteringBound> endBounds)
    {
        assert startBounds.size() == endBounds.size();

        final Slices.Builder builder = new Slices.Builder(comparator);

        final Iterator<ClusteringBound> starts = startBounds.iterator();
        final Iterator<ClusteringBound> ends = endBounds.iterator();

        while (starts.hasNext())
        {
            final Slice slice = Slice.make(starts.next(), ends.next());
            if (!slice.isEmpty(comparator))
            {
                builder.add(slice);
            }
        }

        return builder.build();
    }

    /**
     * Close this writer.
     * <p>
     * This method should be called, otherwise the produced sstables are not
     * guaranteed to be complete (and won't be in practice).
     */
    public void close() throws IOException
    {
        writer.close();
    }

    @SuppressWarnings("unchecked")
    private ByteBuffer serialize(final Object value, final TypeCodec codec)
    {
        if (value == null || value == UNSET_VALUE)
        {
            return (ByteBuffer) value;
        }

        return codec.serialize(value, ProtocolVersion.NEWEST_SUPPORTED);
    }

    /**
     * A Builder for a SSTableTombstoneWriter object.
     */
    public static class Builder
    {
        private File directory;

        SSTableFormat.Type formatType = null;

        private CreateTableStatement.Raw schemaStatement;
        private final List<CreateTypeStatement.Raw> typeStatements;
        private ModificationStatement.Parsed deleteStatement;
        private IPartitioner partitioner;

        private boolean sorted = false;
        private long bufferSizeInMB = 128;

        Builder()
        {
            this.typeStatements = new ArrayList<>();
        }

        /**
         * The directory where to write the sstables (mandatory option).
         * <p>
         * This is a mandatory option.
         *
         * @param directory the directory to use, which should exists and be writable.
         * @return this builder.
         * @throws IllegalArgumentException if {@code directory} doesn't exist or is not writable.
         */
        public Builder inDirectory(final File directory)
        {
            if (!directory.exists())
            {
                throw new IllegalArgumentException(directory + " doesn't exists");
            }
            if (!directory.canWrite())
            {
                throw new IllegalArgumentException(directory + " exists but is not writable");
            }

            this.directory = directory;
            return this;
        }

        /**
         * The schema (CREATE TABLE statement) for the table for which sstable are to be created.
         * <p>
         * Please note that the provided CREATE TABLE statement <b>must</b> use a fully-qualified
         * table name, one that include the keyspace name.
         * <p>
         * This is a mandatory option.
         *
         * @param schema the schema of the table for which sstables are to be created.
         * @return this builder.
         * @throws IllegalArgumentException if {@code schema} is not a valid CREATE TABLE statement
         *                                  or does not have a fully-qualified table name.
         */
        public Builder forTable(final String schema)
        {
            this.schemaStatement = QueryProcessor.parseStatement(schema, CreateTableStatement.Raw.class, "CREATE TABLE");
            return this;
        }

        /**
         * The partitioner to use.
         * <p>
         * By default, {@code Murmur3Partitioner} will be used. If this is not the partitioner used
         * by the cluster for which the SSTables are created, you need to use this method to
         * provide the correct partitioner.
         *
         * @param partitioner the partitioner to use.
         * @return this builder.
         */
        public Builder withPartitioner(final IPartitioner partitioner)
        {
            this.partitioner = partitioner;
            return this;
        }

        /**
         * The DELETE statement defining the values to remove for a given CQL row.
         * <p>
         * Please note that the provided DELETE statement <b>must</b> use a fully-qualified
         * table name, one that include the keyspace name. Moreover, said statement must use
         * bind variables since these variables will be bound to values by the resulting writer.
         * <p>
         * This is a mandatory option.
         *
         * @param delete a delete statement that defines the order
         *               of column values to use.
         * @return this builder.
         * @throws IllegalArgumentException if {@code deleteStatement} is not a valid deletion
         *                                  statement, does not have a fully-qualified table name or have no bind variables.
         */
        public Builder using(final String delete)
        {
            this.deleteStatement = QueryProcessor.parseStatement(delete, ModificationStatement.Parsed.class, "DELETE");
            return this;
        }

        /**
         * The size of the buffer to use.
         * <p>
         * This defines how much data will be buffered before being written as
         * a new SSTable. This correspond roughly to the data size that will have the created
         * sstable.
         * <p>
         * The default is 128MB, which should be reasonable for a 1GB heap. If you experience
         * OOM while using the writer, you should lower this value.
         *
         * @param size the size to use in MB.
         * @return this builder.
         */
        public Builder withBufferSizeInMB(final int size)
        {
            this.bufferSizeInMB = size;
            return this;
        }

        public SSTableTombstoneWriter build()
        {
            if (directory == null)
            {
                throw new IllegalStateException("No ouptut directory specified, you should provide a directory with inDirectory()");
            }
            if (schemaStatement == null)
            {
                throw new IllegalStateException("Missing schema, you should provide the schema for the SSTable to create with forTable()");
            }
            if (deleteStatement == null)
            {
                throw new IllegalStateException("No delete statement specified, you should provide a delete statement through using()");
            }

            synchronized (SSTableTombstoneWriter.class)
            {
                if (Schema.instance.getKeyspaceMetadata(SchemaConstants.SCHEMA_KEYSPACE_NAME) == null)
                {
                    Schema.instance.load(SchemaKeyspace.metadata());
                }
                if (Schema.instance.getKeyspaceMetadata(SchemaConstants.SYSTEM_KEYSPACE_NAME) == null)
                {
                    Schema.instance.load(SystemKeyspace.metadata());
                }

                final String keyspaceName = schemaStatement.keyspace();

                if (Schema.instance.getKeyspaceMetadata(keyspaceName) == null)
                {
                    Schema.instance.load(KeyspaceMetadata.create(keyspaceName,
                                                                 KeyspaceParams.simple(1),
                                                                 Tables.none(),
                                                                 Views.none(),
                                                                 Types.none(),
                                                                 Functions.none()));
                }

                final KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspaceName);

                TableMetadata tableMetadata = ksm.tables.getNullable(schemaStatement.table());
                if (tableMetadata == null)
                {
                    final Types types = createTypes(keyspaceName);
                    tableMetadata = createTable(types);
                    Schema.instance.load(ksm.withSwapped(ksm.tables.with(tableMetadata)).withSwapped(types));
                }

                final DeleteStatement preparedDelete = prepareDelete();

                final TableMetadataRef ref = TableMetadataRef.forOfflineTools(tableMetadata);
                final AbstractSSTableSimpleWriter writer = sorted
                                                           ? new SSTableSimpleWriter(directory, ref, preparedDelete.updatedColumns())
                                                           : new SSTableSimpleUnsortedWriter(directory, ref, preparedDelete.updatedColumns(), bufferSizeInMB);

                if (formatType != null)
                {
                    writer.setSSTableFormatType(formatType);
                }

                return new SSTableTombstoneWriter(writer, preparedDelete, preparedDelete.getBindVariables(), tableMetadata.comparator);
            }
        }

        private Types createTypes(final String keyspace)
        {
            final Types.RawBuilder builder = Types.rawBuilder(keyspace);
            for (final CreateTypeStatement.Raw st : typeStatements)
            {
                st.addToRawBuilder(builder);
            }
            return builder.build();
        }

        /**
         * Creates the table according to schema statement
         *
         * @param types types this table should be created with
         */
        private TableMetadata createTable(final Types types)
        {
            final ClientState state = ClientState.forInternalCalls();
            final CreateTableStatement statement = schemaStatement.prepare(state);
            statement.validate(ClientState.forInternalCalls());

            final TableMetadata.Builder builder = statement.builder(types);
            if (partitioner != null)
            {
                builder.partitioner(partitioner);
            }

            return builder.build();
        }

        /**
         * Prepares delete statement for writing data to SSTable
         *
         * @return prepared Delete statement and it's bound names
         */
        private DeleteStatement prepareDelete()
        {
            final ClientState state = ClientState.forInternalCalls();
            final DeleteStatement delete = (DeleteStatement) deleteStatement.prepare(state);
            delete.validate(state);

            if (delete.hasConditions())
            {
                throw new IllegalArgumentException("Conditional statements are not supported");
            }
            if (delete.isCounter())
            {
                throw new IllegalArgumentException("Counter update statements are not supported");
            }
            if (delete.getBindVariables().isEmpty())
            {
                throw new IllegalArgumentException("Provided delete statement has no bind variables");
            }

            return delete;
        }
    }
}
