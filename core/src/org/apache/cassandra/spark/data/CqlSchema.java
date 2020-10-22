package org.apache.cassandra.spark.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
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
@SuppressWarnings({ "WeakerAccess", "unused" })
public class CqlSchema implements Serializable
{

    public static final long serialVersionUID = 42L;

    private final ReplicationFactor replicationFactor;
    private final String keyspace, table, createStmt;
    private final List<CqlField> fields;
    private final Set<CqlUdt> udts;

    private final Map<String, CqlField> fieldsMap;
    private final List<CqlField> partitionKeys, clusteringKeys, staticColumns, valueColumns;

    public CqlSchema(@NotNull final String keyspace,
                     @NotNull final String table,
                     @NotNull final String createStmt,
                     @NotNull final ReplicationFactor replicationFactor,
                     @NotNull final List<CqlField> fields)
    {
        this(keyspace, table, createStmt, replicationFactor, fields, Collections.emptySet());
    }

    public CqlSchema(@NotNull final String keyspace,
                     @NotNull final String table,
                     @NotNull final String createStmt,
                     @NotNull final ReplicationFactor replicationFactor,
                     @NotNull final List<CqlField> fields,
                     @NotNull final Set<CqlUdt> udts)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.createStmt = createStmt;
        this.replicationFactor = replicationFactor;
        this.fields = Collections.unmodifiableList(fields.stream().sorted().collect(Collectors.toList()));
        this.fieldsMap = Collections.unmodifiableMap(this.fields.stream().collect(Collectors.toMap(CqlField::name, Function.identity())));
        this.partitionKeys = Collections.unmodifiableList(this.fields.stream().filter(CqlField::isPartitionKey).sorted().collect(Collectors.toList()));
        this.clusteringKeys = Collections.unmodifiableList(this.fields.stream().filter(CqlField::isClusteringColumn).sorted().collect(Collectors.toList()));
        this.staticColumns = Collections.unmodifiableList(this.fields.stream().filter(CqlField::isStaticColumn).sorted().collect(Collectors.toList()));
        this.valueColumns = Collections.unmodifiableList(this.fields.stream().filter(CqlField::isValueColumn).sorted().collect(Collectors.toList()));
        this.udts = Collections.unmodifiableSet(udts);
    }

    public ReplicationFactor replicationFactor()
    {
        return replicationFactor;
    }

    public List<CqlField> partitionKeys()
    {
        return partitionKeys;
    }

    public int numPartitionKeys()
    {
        return partitionKeys.size();
    }

    public List<CqlField> clusteringKeys()
    {
        return clusteringKeys;
    }

    public int numClusteringKeys()
    {
        return clusteringKeys.size();
    }

    public int numCellColumns()
    {
        return numPartitionKeys() + numClusteringKeys() + numStaticColumns() + (numValueColumns() > 0 ? 1 : 0);
    }

    public List<CqlField> valueColumns()
    {
        return valueColumns;
    }

    public int numValueColumns()
    {
        return this.valueColumns.size();
    }

    public List<CqlField> staticColumns()
    {
        return staticColumns;
    }

    public int numStaticColumns()
    {
        return staticColumns.size();
    }

    public int numFields()
    {
        return this.fields.size();
    }

    public List<CqlField> fields()
    {
        return this.fields;
    }

    public Set<CqlUdt> udts() {
        return this.udts;
    }

    public Set<String> udtCreateStmts() {
        return this.udts.stream().map(f -> f.createStmt(this.keyspace)).collect(Collectors.toSet());
    }

    public CqlField getField(final String name)
    {
        return this.fieldsMap.get(name);
    }

    public String keyspace()
    {
        return keyspace;
    }

    public String table()
    {
        return table;
    }

    public String createStmt()
    {
        return createStmt;
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(73, 79)
               .append(keyspace)
               .append(table)
               .append(createStmt)
               .append(fields)
               .append(udts)
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

        final CqlSchema rhs = (CqlSchema) obj;
        return new EqualsBuilder()
               .append(keyspace, rhs.keyspace)
               .append(table, rhs.table)
               .append(createStmt, rhs.createStmt)
               .append(fields, rhs.fields)
               .append(udts, rhs.udts)
               .isEquals();
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CqlSchema>
    {
        @Override
        public CqlSchema read(final Kryo kryo, final Input input, final Class type)
        {
            final String keyspace = input.readString();
            final String table = input.readString();
            final String createStmt = input.readString();
            final ReplicationFactor rf = kryo.readObject(input, ReplicationFactor.class);
            final int numFields = input.readInt();
            final List<CqlField> fields = new ArrayList<>(numFields);
            for (int i = 0; i < numFields; i++)
            {
                fields.add(kryo.readObject(input, CqlField.class));
            }
            final int numUdts = input.readInt();
            final Set<CqlUdt> udts = new HashSet<>(numUdts);
            for (int i = 0; i < numUdts; i++)
            {
                udts.add(kryo.readObject(input, CqlUdt.class));
            }
            return new CqlSchema(keyspace, table, createStmt, rf, fields, udts);
        }

        @Override
        public void write(final Kryo kryo, final Output output, final CqlSchema schema)
        {
            output.writeString(schema.keyspace());
            output.writeString(schema.table());
            output.writeString(schema.createStmt());
            kryo.writeObject(output, schema.replicationFactor());
            final List<CqlField> fields = schema.fields();
            output.writeInt(fields.size());
            for (final CqlField field : fields)
            {
                kryo.writeObject(output, field);
            }
            final Set<CqlUdt> udts = schema.udts();
            output.writeInt(udts.size());
            for (final CqlUdt udt : udts)
            {
                kryo.writeObject(output, udt);
            }
        }
    }
}
