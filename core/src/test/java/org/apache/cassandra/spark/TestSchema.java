package org.apache.cassandra.spark;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.spark.cdc.SparkCdcEvent;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SparkCqlField;
import org.apache.cassandra.spark.data.SparkCqlTable;
import org.apache.cassandra.spark.data.fourzero.types.Blob;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.CassandraBridge.RangeTombstoneData;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.reader.fourzero.FourZeroSchemaBuilder;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
 * Helper class to create and test various schemas
 */
public class TestSchema
{
    @NotNull
    public final String keyspace, table, createStmt, insertStmt, updateStmt, deleteStmt;
    final List<SparkCqlField> partitionKeys, clusteringKeys, allFields;
    final Set<CqlField.CqlUdt> udts;
    private final Map<String, Integer> fieldPos;
    @Nullable
    private CassandraVersion version = null;
    private final int minCollectionSize;
    private final Integer blobSize;
    private final boolean withCdc;

    public static Builder builder()
    {
        return new Builder();
    }

    public static TestSchema basic(CassandraBridge bridge)
    {
        return basicBuilder(bridge).build();
    }

    public static Builder basicBuilder(CassandraBridge bridge)
    {
        return TestSchema.builder().withPartitionKey("a", bridge.aInt())
                         .withClusteringKey("b", bridge.aInt())
                         .withColumn("c", bridge.aInt());
    }

    @SuppressWarnings("SameParameterValue")
    public static class Builder
    {
        private String keyspace = null, table = null;
        private final List<SparkCqlField> partitionKeys = new ArrayList<>(), clusteringKeys = new ArrayList<>(), columns = new ArrayList<>();
        private final List<CqlField.SortOrder> sortOrders = new ArrayList<>();
        private List<String> insertFields = null, deleteFields;
        private int minCollectionSize = SparkTestUtils.MIN_COLLECTION_SIZE;
        private Integer blobSize = null;
        private boolean withCompression = true;
        private boolean withCdc = false;

        public Builder withKeyspace(final String keyspace)
        {
            this.keyspace = keyspace;
            return this;
        }

        public Builder withTable(final String table)
        {
            this.table = table;
            return this;
        }

        public Builder withPartitionKey(final String name, final SparkCqlField.SparkCqlType type)
        {
            partitionKeys.add(new SparkCqlField(true, false, false, name, type, 0));
            return this;
        }

        public Builder withClusteringKey(final String name, final SparkCqlField.SparkCqlType type)
        {
            clusteringKeys.add(new SparkCqlField(false, true, false, name, type, 0));
            return this;
        }

        public Builder withStaticColumn(final String name, final SparkCqlField.SparkCqlType type)
        {
            columns.add(new SparkCqlField(false, false, true, name, type, 0));
            return this;
        }

        public Builder withColumn(final String name, final SparkCqlField.SparkCqlType type)
        {
            columns.add(new SparkCqlField(false, false, false, name, type, 0));
            return this;
        }

        public Builder withSortOrder(final CqlField.SortOrder sortOrder)
        {
            sortOrders.add(sortOrder);
            return this;
        }

        Builder withInsertFields(final String... fields)
        {
            this.insertFields = Arrays.asList(fields);
            return this;
        }

        public Builder withDeleteFields(final String... fields)
        {
            this.deleteFields = Arrays.asList(fields);
            return this;
        }

        public Builder withMinCollectionSize(final int minCollectionSize)
        {
            this.minCollectionSize = minCollectionSize;
            return this;
        }

        public Builder withCompression(boolean withCompression)
        {
            this.withCompression = withCompression;
            return this;
        }

        // override blob size
        public Builder withBlobSize(final int blobSize)
        {
            this.blobSize = blobSize;
            return this;
        }

        public Builder withCdc(boolean withCdc)
        {
            this.withCdc = withCdc;
            return this;
        }

        public TestSchema build()
        {
            if (partitionKeys.isEmpty())
            {
                throw new IllegalArgumentException("Need at least one partition key");
            }
            return new TestSchema(
            keyspace != null ? keyspace : "keyspace_" + UUID.randomUUID().toString().replaceAll("-", ""),
            table != null ? table : "table_" + UUID.randomUUID().toString().replaceAll("-", ""),
            IntStream.range(0, partitionKeys.size()).mapToObj(i -> partitionKeys.get(i).cloneWithPos(i)).sorted().collect(Collectors.toList()),
            IntStream.range(0, clusteringKeys.size()).mapToObj(i -> clusteringKeys.get(i).cloneWithPos(i + partitionKeys.size())).sorted().collect(Collectors.toList()),
            IntStream.range(0, columns.size()).mapToObj(i -> columns.get(i).cloneWithPos(i + partitionKeys.size() + clusteringKeys.size())).sorted(Comparator.comparing(CqlField::name)).collect(Collectors.toList()),
            sortOrders, insertFields, deleteFields, minCollectionSize, blobSize, withCompression, withCdc
            );
        }
    }

    private TestSchema(@NotNull final String keyspace, @NotNull final String table,
                       final List<SparkCqlField> partitionKeys, final List<SparkCqlField> clusteringKeys, final List<SparkCqlField> columns,
                       final List<CqlField.SortOrder> sortOrders, @Nullable final List<String> insertOverrides, @Nullable final List<String> deleteFields, final int minCollectionSize,
                       @Nullable final Integer blobSize, boolean withCompression, boolean withCdc)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.partitionKeys = partitionKeys;
        this.clusteringKeys = clusteringKeys;
        this.allFields = new ArrayList<>(partitionKeys.size() + clusteringKeys.size() + columns.size());
        this.minCollectionSize = minCollectionSize;
        allFields.addAll(partitionKeys);
        allFields.addAll(clusteringKeys);
        allFields.addAll(columns);
        Collections.sort(allFields);
        this.fieldPos = allFields.stream().collect(Collectors.toMap(CqlField::name, CqlField::pos));
        this.blobSize = blobSize;
        this.withCdc = withCdc;

        // build create table statement
        final StringBuilder createStmtBuilder = new StringBuilder().append("CREATE TABLE ").append(keyspace).append(".").append(table).append(" (");
        for (final CqlField field : Stream.of(partitionKeys, clusteringKeys, columns).flatMap(Collection::stream).sorted().collect(Collectors.toList()))
        {
            createStmtBuilder.append(field.name())
                             .append(" ").append(field.cqlTypeName())
                             .append(field.isStaticColumn() ? " static" : "")
                             .append(", ");
        }

        createStmtBuilder.append("PRIMARY KEY((");
        createStmtBuilder.append(partitionKeys.stream().map(CqlField::name).collect(Collectors.joining(", "))).append(")");
        if (!clusteringKeys.isEmpty())
        {
            createStmtBuilder.append(", ");
            createStmtBuilder.append(clusteringKeys.stream().map(CqlField::name).collect(Collectors.joining(", "))).append("))");
        }
        else
        {
            createStmtBuilder.append("))");
        }
        String propertySep = " WITH "; // for the first property, "WITH" is used; for the rest, "AND" is used.
        if (!sortOrders.isEmpty())
        {
            createStmtBuilder.append(propertySep);
            createStmtBuilder.append("CLUSTERING ORDER BY (");
            for (int i = 0; i < sortOrders.size(); i++)
            {
                createStmtBuilder.append(clusteringKeys.get(i).name()).append(" ").append(sortOrders.get(i).toString());
                if (i < sortOrders.size() - 1)
                {
                    createStmtBuilder.append(", ");
                }
            }
            createStmtBuilder.append(")");
            propertySep = " AND ";
        }
        if (!withCompression)
        {
            createStmtBuilder.append(propertySep);
            createStmtBuilder.append("compression = {'enabled':'false'}");
            propertySep = " AND ";
        }
        createStmtBuilder.append(propertySep);
        createStmtBuilder.append("cdc = " + withCdc);
        createStmtBuilder.append(";");
        this.createStmt = createStmtBuilder.toString();

        // build insert statement
        final StringBuilder insertStmtBuilder = new StringBuilder().append("INSERT INTO ").append(keyspace).append(".").append(table).append(" (");
        if (insertOverrides != null)
        {
            insertStmtBuilder.append(String.join(", ", insertOverrides))
                             .append(") VALUES (")
                             .append(insertOverrides.stream().map(a -> "?").collect(Collectors.joining(", ")));
        }
        else
        {
            insertStmtBuilder.append(allFields.stream().sorted().map(CqlField::name).collect(Collectors.joining(", ")))
                             .append(") VALUES (")
                             .append(Stream.of(partitionKeys, clusteringKeys, columns).flatMap(Collection::stream).sorted().map(a -> "?").collect(Collectors.joining(", ")));
        }
        this.insertStmt = insertStmtBuilder.append(");").toString();

        // build update statement
        final StringBuilder updateStmtBuilder = new StringBuilder("UPDATE ").append(keyspace).append(".").append(table).append(" SET ");
        updateStmtBuilder.append(allFields.stream().sorted().filter(field -> !field.isPartitionKey() && !field.isClusteringColumn()).map(field -> field.name() + " = ?").collect(Collectors.joining(", ")));
        updateStmtBuilder.append(" WHERE ");
        updateStmtBuilder.append(allFields.stream().sorted().filter(field -> field.isPartitionKey() || field.isClusteringColumn()).map(field -> field.name() + " = ?").collect(Collectors.joining(" and ")));
        this.updateStmt = updateStmtBuilder.append(";").toString();

        // build delete statement
        final StringBuilder deleteStmtBuilder = new StringBuilder().append("DELETE FROM ").append(keyspace).append(".").append(table).append(" WHERE ");
        if (deleteFields != null)
        {
            deleteStmtBuilder.append(deleteFields.stream().map(override -> override + " ?").collect(Collectors.joining(" AND ")));
        }
        else
        {
            deleteStmtBuilder.append(allFields.stream().map(f -> f.name() + " = ?").collect(Collectors.joining(" AND ")));
        }
        this.deleteStmt = deleteStmtBuilder.append(";").toString();

        this.udts = allFields.stream().map(f -> f.type().udts()).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    public FourZeroSchemaBuilder schemaBuilder(final Partitioner partitioner)
    {
        return schemaBuilder(partitioner, new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, ImmutableMap.of("replication_factor", 1)));
    }

    public FourZeroSchemaBuilder schemaBuilder(final Partitioner partitioner,
                                               final ReplicationFactor rf)
    {
        return new FourZeroSchemaBuilder(this.createStmt, this.keyspace, rf, partitioner, Collections.emptySet(), null, withCdc);
    }

    public void setCassandraVersion(@NotNull final CassandraVersion version)
    {
        this.version = version;
    }

    public SparkCqlTable buildSchema()
    {
        return new SparkCqlTable(keyspace, table, createStmt, new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3)), SparkCqlField.cast(allFields), udts);
    }

    public static StructType cdcStructType()
    {
        return SparkCdcEvent.SCHEMA;
    }

    public TestRow[] randomRows(final int numRows)
    {
        return randomRows(numRows, 0);
    }

    @SuppressWarnings("SameParameterValue")
    private TestRow[] randomRows(final int numRows, final int numTombstones)
    {
        final TestSchema.TestRow[] testRows = new TestSchema.TestRow[numRows];
        for (int i = 0; i < testRows.length; i++)
        {
            testRows[i] = this.randomRow(i < numTombstones);
        }
        return testRows;
    }

    public TestRow randomRow()
    {
        return randomRow(false);
    }

    public TestRow randomPartitionDelete()
    {
        return randomRow(field -> !field.isPartitionKey());
    }

    private TestRow randomRow(final boolean tombstone)
    {
        return randomRow(field -> tombstone && field.isValueColumn());
    }

    private TestRow randomRow(final Predicate<CqlField> nullifiedFields)
    {
        final Object[] values = new Object[allFields.size()];
        for (final CqlField field : allFields)
        {
            if (nullifiedFields.test(field))
            {
                values[field.pos()] = null;
            }
            else
            {
                if (field.type() instanceof Blob && blobSize != null)
                {
                    values[field.pos()] = RandomUtils.randomByteBuffer(blobSize);
                }
                else
                {
                    values[field.pos()] = field.type().randomValue(minCollectionSize);
                }
            }
        }
        return new TestRow(values);
    }

    public TestRow toTestRow(final InternalRow row)
    {
        if (row instanceof GenericInternalRow)
        {
            final Object[] values = new Object[allFields.size()];
            for (final SparkCqlField field : allFields)
            {
                values[field.pos()] = field.type().sparkSqlRowValue((GenericInternalRow) row, field.pos());
            }
            return new TestRow(values);
        }
        throw new IllegalStateException("Can only convert GenericInternalRow");
    }

    public TestRow toTestRow(final Row row, Set<String> requiredColumns)
    {
        final Object[] values = new Object[requiredColumns == null ? allFields.size() : requiredColumns.size()];
        int skipped = 0;
        for (final SparkCqlField field : allFields)
        {
            if (requiredColumns != null && !requiredColumns.contains(field.name()))
            {
                skipped++;
                continue;
            }
            final int pos = field.pos() - skipped;
            if (row.get(pos) == null)
            {
                values[pos] = null;
            }
            else
            {
                values[pos] = field.type().sparkSqlRowValue(row, pos);
            }
        }

        return new TestRow(values);
    }

    @SuppressWarnings("SameParameterValue")
    public class TestRow implements CassandraBridge.IRow
    {
        private final Object[] values;
        private boolean isTombstoned;
        private boolean isInsert;
        private List<RangeTombstoneData> rangeTombstones;
        private int ttl;

        private TestRow(final Object[] values)
        {
            this(values, false, true);
        }

        private TestRow(final Object[] values, final boolean isTombstoned, boolean isInsert)
        {
            this.values = values;
            this.isTombstoned = isTombstoned;
            this.isInsert = isInsert;
        }

        public void setRangeTombstones(List<RangeTombstoneData> rangeTombstones)
        {
            this.rangeTombstones = rangeTombstones;
        }

        @Override
        public List<RangeTombstoneData> rangeTombstones()
        {
            return rangeTombstones;
        }

        @Override
        public boolean isDeleted()
        {
            return isTombstoned;
        }

        public void delete()
        {
            isTombstoned = true;
        }

        @Override
        public boolean isInsert()
        {
            return isInsert;
        }

        public void setTTL(int ttl)
        {
            this.ttl = ttl;
        }

        @Override
        public int ttl()
        {
            return ttl;
        }

        public void fromUpdate()
        {
            isInsert = false;
        }

        public void fromInsert()
        {
            isInsert = true;
        }

        public TestRow copy(final String field, final Object value)
        {
            return copy(getFieldPos(field), value);
        }

        public TestRow copy(final int pos, final Object value)
        {
            final Object[] newValues = new Object[values.length];
            System.arraycopy(values, 0, newValues, 0, values.length);
            newValues[pos] = value;
            return new TestRow(newValues);
        }

        /**
         * If a prune column filter is applied, convert expected TestRow to only include required columns
         * so we can compare with row returned by Spark.
         *
         * @param columns required columns, or null if no column selection criteria
         * @return a TestRow containing only the required columns
         */
        public TestRow withColumns(@Nullable Set<String> columns)
        {
            if (columns == null)
            {
                return this;
            }
            final Object[] result = new Object[columns.size()];
            int skipped = 0;
            for (CqlField field : allFields)
            {
                if (!columns.contains(field.name()))
                {
                    skipped++;
                    continue;
                }
                result[field.pos() - skipped] = values[field.pos()];
            }
            return new TestRow(result);
        }

        public TestRow nullifyUnsetColumn()
        {
            final Object[] newValues = new Object[values.length];
            System.arraycopy(values, 0, newValues, 0, values.length);
            for (int i = 0; i < newValues.length; i++)
            {
                if (newValues[i] == CassandraBridge.UNSET_MARKER)
                {
                    newValues[i] = null;
                }
            }
            return new TestRow(newValues);
        }

        public Object[] rawValues(int start, int end)
        {
            assert end <= values.length && start <= end
            : String.format("start: %s, end: %s", version, start, end);
            final Object[] result = new Object[end - start];
            System.arraycopy(values, start, result, 0, end - start);
            return result;
        }

        Object[] allValues()
        {
            return values(0, values.length);
        }

        // start inclusive, end exclusive
        public Object[] values(int start, int end)
        {
            //NOTE: CassandraBridge must be set before calling this class so we can convert 4.0 Date type to LocalDate to be used in CQLSSTableWriter
            assert (version != null) && end <= values.length && start <= end
            : String.format("version: %s, start: %s, end: %s", version, start, end);
            final Object[] result = new Object[end - start];
            for (int srcIdx = start, dstIdx = 0; srcIdx < end; srcIdx++, dstIdx++)
            {
                result[dstIdx] = convertForCqlWriter(getType(srcIdx), values[srcIdx]);
            }
            return result;
        }

        private Object convertForCqlWriter(final SparkCqlField.SparkCqlType type, final Object value)
        {
            return type.convertForCqlWriter(value, version);
        }

        public SparkCqlField.SparkCqlType getType(final int pos)
        {
            if (pos >= 0 && pos < allFields.size())
            {
                return allFields.get(pos).type();
            }
            throw new IllegalStateException("Unknown field at pos: " + pos);
        }

        boolean isNull(String field)
        {
            return get(field) == null;
        }

        String getString(final String field)
        {
            return (String) get(field);
        }

        UUID getUUID(final String field)
        {
            return (UUID) get(field);
        }

        public Long getLong(final String field)
        {
            return (Long) get(field);
        }

        Integer getInteger(final String field)
        {
            return (Integer) get(field);
        }

        Object get(final String field)
        {
            return get(getFieldPos(field));
        }

        private int getFieldPos(final String field)
        {
            return Objects.requireNonNull(fieldPos.get(field), "Unknown field: " + field);
        }

        @Override
        public Object get(final int pos)
        {
            return values[pos];
        }

        public String getKey()
        {
            final StringBuilder str = new StringBuilder();
            int total = partitionKeys.size() + clusteringKeys.size();
            for (int i = 0; i < total; i++)
            {
                final SparkCqlField.SparkCqlType type = i >= partitionKeys.size() ? clusteringKeys.get(i - partitionKeys.size()).type() : partitionKeys.get(i).type();
                str.append(toString(type, get(i)));
                if (i + 1 != total)
                {
                    str.append(':');
                }
            }
            return str.toString();
        }

        private String toString(final SparkCqlField.SparkCqlType type, final Object key)
        {
            if (key instanceof BigDecimal)
            {
                return ((BigDecimal) key).setScale(8, RoundingMode.CEILING).toPlainString();
            }
            else if (key instanceof Timestamp)
            {
                return new Date(((Timestamp) key).getTime()).toString();
            }
            else if (key instanceof Object[])
            {
                return String.format("[%s]", Arrays.stream((Object[]) key).map(o -> toString(type, o)).collect(Collectors.joining(", ")));
            }
            else if (key instanceof Map)
            {
                final SparkCqlField.SparkCqlType innerType = getFrozenInnerType(type);
                if (innerType instanceof CqlField.CqlMap)
                {
                    final SparkCqlField.SparkMap mapType = (SparkCqlField.SparkMap) innerType;
                    return ((Map<?, ?>) key).entrySet().stream()
                                            .sorted((Comparator<Map.Entry<?, ?>>) (o1, o2) -> mapType.keyType().compare(o1.getKey(), o2.getKey()))
                                            .map(Map.Entry::getValue)
                                            .collect(Collectors.toList()).toString();
                }
                return ((Map<?, ?>) key).entrySet().stream().collect(Collectors.toMap(e -> toString(innerType, e.getKey()), e -> toString(innerType, e.getValue()))).toString();
            }
            else if (key instanceof Collection)
            {
                final SparkCqlField.SparkCqlType innerType = ((SparkCqlField.SparkCollection) getFrozenInnerType(type)).sparkType();
                return ((Collection<?>) key).stream().sorted(innerType).map(o -> toString(innerType, o)).collect(Collectors.toList()).toString();
            }
            return key == null ? "null" : key.toString();
        }

        private SparkCqlField.SparkCqlType getFrozenInnerType(final SparkCqlField.SparkCqlType type)
        {
            if (type instanceof SparkCqlField.SparkFrozen)
            {
                return getFrozenInnerType(((SparkCqlField.SparkFrozen) type).inner());
            }
            return type;
        }

        @Override
        public String toString()
        {
            final String str = IntStream.range(0, values.length).mapToObj(i -> toString(allFields.get(i).type(), values[i])).collect(Collectors.joining(", "));
            return String.format("[%s]", str);
        }

        public int hashCode()
        {
            return Objects.hash(values);
        }

        public boolean equals(final Object o)
        {
            return o instanceof TestRow && SparkTestUtils.equals(values, ((TestRow) o).values);
        }
    }
}
