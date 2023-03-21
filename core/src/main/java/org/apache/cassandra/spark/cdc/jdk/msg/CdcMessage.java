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

package org.apache.cassandra.spark.cdc.jdk.msg;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.cdc.AbstractCdcEvent;
import org.apache.cassandra.spark.cdc.jdk.JdkCdcEvent;
import org.apache.cassandra.spark.cdc.jdk.JdkRangeTombstone;
import org.apache.cassandra.spark.cdc.jdk.JdkValueMetadata;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.fourzero.complex.CqlCollection;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.utils.ArrayUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.utils.ArrayUtils.orElse;

/**
 * Converts JdkCdcEvent into more user-consumable format, deserializing ByteBuffers into Java types.
 */
@SuppressWarnings("unused")
public class CdcMessage
{
    public static final CassandraBridge BRIDGE = CassandraBridge.get(CassandraBridge.CassandraVersion.FOURZERO);

    private final String keyspace, table;
    private final List<Column> partitionKeys;
    private final List<Column> clusteringKeys;
    private final List<Column> staticColumns;
    private final List<Column> valueColumns;
    private final long maxTimestampMicros;
    private final AbstractCdcEvent.Kind operationType;
    private final Map<String, Column> columns;
    private final List<RangeTombstone> rangeTombstoneList;
    @Nullable
    private final AbstractCdcEvent.TimeToLive ttl;
    @Nullable
    private final Map<String, List<Object>> complexCellDeletion;

    public CdcMessage(JdkCdcEvent event)
    {
        this(event.keyspace,
             event.table,
             toColumns(event.getPartitionKeys()),
             toColumns(event.getClusteringKeys()),
             toColumns(event.getStaticColumns()),
             toColumns(event.getValueColumns()),
             event.getTimestamp(TimeUnit.MICROSECONDS),
             event.getKind(),
             orElse(event.getRangeTombstoneList(), Collections.emptyList()).stream().map(JdkRangeTombstone::toRow).collect(Collectors.toList()),
             complexCellDeletion(event.getTombstonedCellsInComplex(), typeProvider(event)),
             event.getTtl());
    }

    @SuppressWarnings("unchecked")
    private static Function<String, CqlField.CqlType> typeProvider(JdkCdcEvent event)
    {
        final List<JdkValueMetadata> cols = ArrayUtils.combine(event.getPartitionKeys(), event.getClusteringKeys(), event.getStaticColumns(), event.getValueColumns());
        final Map<String, CqlField.CqlType> typeMap = cols.stream()
                                                          .collect(Collectors.toMap(v -> v.columnName, v -> BRIDGE.parseType(v.columnType)));
        return typeMap::get;
    }

    public CdcMessage(String keyspace,
                      String table,
                      List<Column> partitionKeys,
                      List<Column> clusteringKeys,
                      List<Column> staticColumns,
                      List<Column> valueColumns,
                      long maxTimestampMicros,
                      AbstractCdcEvent.Kind operationType,
                      List<RangeTombstone> rangeTombstoneList,
                      @Nullable Map<String, List<Object>> complexCellDeletion,
                      @Nullable AbstractCdcEvent.TimeToLive ttl)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.partitionKeys = partitionKeys;
        this.clusteringKeys = clusteringKeys;
        this.staticColumns = staticColumns;
        this.valueColumns = valueColumns;
        this.maxTimestampMicros = maxTimestampMicros;
        this.operationType = operationType;
        this.rangeTombstoneList = rangeTombstoneList;
        this.columns = new HashMap<>(partitionKeys.size() + clusteringKeys.size() + staticColumns.size() + valueColumns.size());
        partitionKeys.forEach(col -> this.columns.put(col.name(), col));
        clusteringKeys.forEach(col -> this.columns.put(col.name(), col));
        staticColumns.forEach(col -> this.columns.put(col.name(), col));
        valueColumns.forEach(col -> this.columns.put(col.name(), col));
        this.complexCellDeletion = complexCellDeletion;
        this.ttl = ttl;
    }

    private static Map<String, List<Object>> complexCellDeletion(@Nullable Map<String, List<ByteBuffer>> tombstonedCellsInComplex,
                                                                 Function<String, CqlField.CqlType> typeProvider)
    {
        if (tombstonedCellsInComplex == null)
        {
            return null;
        }

        return tombstonedCellsInComplex
               .entrySet()
               .stream()
               .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                   final CqlCollection type = (CqlCollection) typeProvider.apply(entry.getKey());
                   return entry.getValue().stream().map(ByteBuffer::duplicate).map(buf -> type.type().deserializeToJava(buf)).collect(Collectors.toList());
               }));
    }

    private static List<Column> toColumns(List<JdkValueMetadata> values)
    {
        if (values == null)
        {
            return Collections.emptyList();
        }
        return values.stream()
                     .map(JdkValueMetadata::toRow)
                     .collect(Collectors.toList());
    }

    public String keyspace()
    {
        return keyspace;
    }

    public String table()
    {
        return table;
    }

    public List<Column> partitionKeys()
    {
        return partitionKeys;
    }

    public List<Column> clusteringKeys()
    {
        return clusteringKeys;
    }

    public List<Column> staticColumns()
    {
        return staticColumns;
    }

    public List<Column> valueColumns()
    {
        return valueColumns;
    }

    @Nullable
    public AbstractCdcEvent.TimeToLive ttl()
    {
        return ttl;
    }

    @Nullable
    public Map<String, List<Object>> getComplexCellDeletion()
    {
        return complexCellDeletion;
    }

    public List<RangeTombstone> rangeTombstones()
    {
        return rangeTombstoneList;
    }

    public long lastModifiedTimeMicros()
    {
        return maxTimestampMicros;
    }

    public AbstractCdcEvent.Kind operationType()
    {
        return operationType;
    }

    // convenience apis

    @SuppressWarnings("unchecked")
    public List<Column> primaryKeys()
    {
        return ArrayUtils.combine(partitionKeys, clusteringKeys);
    }

    @SuppressWarnings("unchecked")
    public List<Column> allColumns()
    {
        return ArrayUtils.combine(partitionKeys, clusteringKeys, staticColumns, valueColumns);
    }

    @Nullable
    public Column column(String name)
    {
        return columns.get(name);
    }

    public Instant lastModifiedTime()
    {
        return Instant.EPOCH.plus(maxTimestampMicros, ChronoUnit.MICROS);
    }

    @SuppressWarnings("unchecked")
    public String toString()
    {
        return "{" +
               "\"operation\": " + operationType + ", " +
               "\"lastModifiedTimestamp\": " + maxTimestampMicros + ", " +
               ArrayUtils.concatToStream(partitionKeys, clusteringKeys, staticColumns, valueColumns)
                         .map(Object::toString)
                         .collect(Collectors.joining(", ")) +
               "}";
    }
}
