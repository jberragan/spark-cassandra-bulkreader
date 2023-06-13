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

package org.apache.cassandra.spark.cdc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.cdc.fourzero.SparkRangeTombstoneBuilder;
import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.mutable.WrappedArray;

import static org.apache.cassandra.spark.config.SchemaFeatureSet.CELL_DELETION_IN_COMPLEX;
import static org.apache.cassandra.spark.config.SchemaFeatureSet.LAST_MODIFIED_TIMESTAMP;
import static org.apache.cassandra.spark.config.SchemaFeatureSet.OPERATION_TYPE;
import static org.apache.cassandra.spark.config.SchemaFeatureSet.RANGE_DELETION;
import static org.apache.cassandra.spark.config.SchemaFeatureSet.TTL;
import static org.apache.cassandra.spark.utils.TimeUtils.toMicros;

/**
 * Cassandra version ignorant abstraction of CdcEvent
 */
public class SparkCdcEvent extends AbstractCdcEvent<SparkValueWithMetadata, SparkRangeTombstone> implements SparkRowSource
{
    // Generic schema for CDC event. The schema is table ignorant.
    public static final StructType SCHEMA;

    static
    {
        // The keyspace and table should always present.
        // The partition keys should always be present.
        // The clustering keys, static columns, value columns could be absent, depending on the schema or the operation.
        // The sequence of the keys/columns is the same as their definition order in the schema.

        // Using StructField array is merely to create StructType in one shot, in order to avoid creating new StructType
        // instances on every call of "add". The sequence in the array does not matter.
        StructField[] columns = new StructField[6 + SchemaFeatureSet.ALL_CDC_FEATURES.size()];
        int i = 0;
        columns[i++] = DataTypes.createStructField(KEYSPACE, DataTypes.StringType, false);
        columns[i++] = DataTypes.createStructField(TABLE, DataTypes.StringType, false);
        columns[i++] = DataTypes.createStructField(PARTITION_KEYS, DataTypes.createArrayType(SparkValueWithMetadata.SCHEMA), false);
        columns[i++] = DataTypes.createStructField(CLUSTERING_KEYS, DataTypes.createArrayType(SparkValueWithMetadata.SCHEMA), true);
        columns[i++] = DataTypes.createStructField(STATIC_COLUMNS, DataTypes.createArrayType(SparkValueWithMetadata.SCHEMA), true);
        columns[i++] = DataTypes.createStructField(VALUE_COLUMNS, DataTypes.createArrayType(SparkValueWithMetadata.SCHEMA), true);
        for (SchemaFeature f : SchemaFeatureSet.ALL_CDC_FEATURES)
        {
            columns[i++] = f.field();
        }
        SCHEMA = DataTypes.createStructType(columns);
    }

    public SparkCdcEvent(Kind kind, UnfilteredRowIterator partition)
    {
        super(kind, partition);
    }

    protected SparkCdcEvent(Kind kind, String keyspace, String table)
    {
        super(kind, keyspace, table);
    }

    public SparkRangeTombstoneBuilder rangeTombstoneBuilder(TableMetadata metadata)
    {
        return new SparkRangeTombstoneBuilder(metadata);
    }

    /**
     * This is the opposite of {@link #toRow()}
     */
    private SparkCdcEvent(Kind kind, String keyspace, String table, org.apache.spark.sql.Row row)
    {
        super(kind, keyspace, table);
        // note that the converted CdcEvent does not have table metadata
        StructType schema = SCHEMA;
        partitionKeys = arrayToCqlFields(row.get(schema.fieldIndex(PARTITION_KEYS)), false);
        clusteringKeys = arrayToCqlFields(row.get(schema.fieldIndex(CLUSTERING_KEYS)), true);
        staticColumns = arrayToCqlFields(row.get(schema.fieldIndex(STATIC_COLUMNS)), true);
        valueColumns = arrayToCqlFields(row.get(schema.fieldIndex(VALUE_COLUMNS)), true);
        maxTimestampMicros = toMicros(row.getTimestamp(schema.fieldIndex(LAST_MODIFIED_TIMESTAMP.fieldName())));
        unmakeCellDeletionInComplex(row);
        unmakeRangeDeletion(row);
        unmakeTTL(row);
    }

    @Override
    public SparkValueWithMetadata makeValue(String name, String type, ByteBuffer value)
    {
        return SparkValueWithMetadata.of(name, type, value);
    }

    private MapData makeCellDeletionInComplex()
    {
        // no tombstones in complex found
        if (tombstonedCellsInComplex == null || tombstonedCellsInComplex.isEmpty())
        {
            return null;
        }

        List<Object> cols = new ArrayList<>(tombstonedCellsInComplex.size());
        List<Object> tombstones = new ArrayList<>(tombstonedCellsInComplex.size());
        tombstonedCellsInComplex.forEach((colName, tombstonedKeys) -> {
            cols.add(UTF8String.fromString(colName)); // string in ArrayData is backed by UTF8String
            Object[] keys = tombstonedKeys.stream().map(bb -> {
                assert bb.hasArray();
                return bb.array();
            }).toArray();
            tombstones.add(ArrayData.toArrayData(keys));
        });
        return ArrayBasedMapData.apply(cols.toArray(), tombstones.toArray());
    }

    /**
     * The opposite of {@link #makeCellDeletionInComplex()}
     */
    private void unmakeCellDeletionInComplex(org.apache.spark.sql.Row row)
    {
        int index = SCHEMA.fieldIndex(CELL_DELETION_IN_COMPLEX.fieldName());
        if (row.isNullAt(index))
        {
            tombstonedCellsInComplex = null;
            return;
        }

        Map<String, WrappedArray<byte[]>> unwrapped = row.getJavaMap(index);
        tombstonedCellsInComplex = new HashMap<>(unwrapped.size());
        unwrapped.forEach((k, v) -> {
            List<ByteBuffer> bbs = new ArrayList<>(v.size());
            for (int i = 0; i < v.size(); i++)
            {
                bbs.add(ByteBuffer.wrap(v.apply(i)));
            }
            tombstonedCellsInComplex.put(k, bbs);
        });
    }

    @Override
    public InternalRow toRow()
    {
        Preconditions.checkState(getKind() != null,
                                 "A valid CDC event should have non-null kind");
        Preconditions.checkState(maxTimestampMicros != Long.MIN_VALUE,
                                 "A valid CDC event should have non-empty timestamp");
        StructType schema = SCHEMA;
        Object[] data = new Object[schema.size()];
        data[schema.fieldIndex(KEYSPACE)] = UTF8String.fromString(keyspace);
        data[schema.fieldIndex(TABLE)] = UTF8String.fromString(table);
        data[schema.fieldIndex(PARTITION_KEYS)] = cqlFieldsToArray(partitionKeys);
        data[schema.fieldIndex(CLUSTERING_KEYS)] = cqlFieldsToArray(clusteringKeys);
        data[schema.fieldIndex(STATIC_COLUMNS)] = cqlFieldsToArray(staticColumns);
        data[schema.fieldIndex(VALUE_COLUMNS)] = cqlFieldsToArray(valueColumns);
        data[schema.fieldIndex(LAST_MODIFIED_TIMESTAMP.fieldName())] = maxTimestampMicros;
        data[schema.fieldIndex(OPERATION_TYPE.fieldName())] = UTF8String.fromString(kind.toString().toLowerCase());
        data[schema.fieldIndex(CELL_DELETION_IN_COMPLEX.fieldName())] = makeCellDeletionInComplex();
        data[schema.fieldIndex(RANGE_DELETION.fieldName())] = makeRangeDeletion();
        data[schema.fieldIndex(TTL.fieldName())] = makeTTL();
        return new GenericInternalRow(data);
    }

    private ArrayData makeRangeDeletion()
    {
        if (rangeTombstoneList == null || rangeTombstoneList.isEmpty())
        {
            return null;
        }

        // The range tombstones has been validated already. But it does not harm to check for another time.
        validateRangeTombstoneMarkers();
        return ArrayData.toArrayData(rangeTombstoneList.stream()
                                                       .map(SparkRangeTombstone::toRow)
                                                       .toArray());
    }

    /**
     * The opposite of {@link #makeRangeDeletion()}
     */
    private void unmakeRangeDeletion(org.apache.spark.sql.Row row)
    {
        int index = SCHEMA.fieldIndex(RANGE_DELETION.fieldName());
        if (row.isNullAt(index))
        {
            rangeTombstoneList = null;
            return;
        }

        WrappedArray<org.apache.spark.sql.Row> rangeTombstones = row.getAs(index);
        rangeTombstoneList = new ArrayList<>(rangeTombstones.size());
        for (int i = 0; i < rangeTombstones.size(); i++)
        {
            rangeTombstoneList.add(SparkRangeTombstone.EMPTY.fromRow(rangeTombstones.apply(i)));
        }
    }

    private ArrayData makeTTL()
    {
        if (timeToLive == null)
        {
            return null;
        }

        return ArrayData.toArrayData(new int[]{ timeToLive.ttlInSec, timeToLive.expirationTimeInSec });
    }

    /**
     * The opposite of {@link #makeTTL()}
     */
    private void unmakeTTL(org.apache.spark.sql.Row row)
    {
        int index = SCHEMA.fieldIndex(TTL.fieldName());
        if (row.isNullAt(index))
        {
            timeToLive = null;
            return;
        }

        WrappedArray<Integer> tuple = row.getAs(index);
        timeToLive = new TimeToLive(tuple.apply(0), tuple.apply(1));
    }

    // Convert the field values into array data of the spark row field (e.g. pk, ck, etc.) accordingly.
    protected static ArrayData cqlFieldsToArray(List<SparkValueWithMetadata> values)
    {
        if (values == null)
        {
            // Values not present. The clustering keys, static columns and value columns are nullable.
            return null;
        }

        Object[] valArray = values.stream()
                                  .map(SparkValueWithMetadata::toRow)
                                  .toArray();
        return ArrayData.toArrayData(valArray);
    }

    protected static List<SparkValueWithMetadata> arrayToCqlFields(Object array, boolean nullable)
    {
        // we are ok with 1) array == null when nullable, and 2) array != null when not nullable
        Preconditions.checkArgument(nullable || array != null,
                                    "The input array cannot be null");

        if (array == null) // array is nullable if reaching here
        {
            return null;
        }

        @SuppressWarnings("unchecked") // let it crash on type mismatch
        WrappedArray<Row> values = (WrappedArray<org.apache.spark.sql.Row>) array;
        List<SparkValueWithMetadata> result = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++)
        {
            result.add(SparkValueWithMetadata.EMPTY.fromRow(values.apply(i)));
        }
        return result;
    }

    public static class Builder extends EventBuilder<SparkValueWithMetadata, SparkRangeTombstone, SparkCdcEvent> implements SparkRowSink<SparkCdcEvent>
    {
        public static final Builder EMPTY = new Builder(null, null);

        public static Builder of(Kind kind, UnfilteredRowIterator partition)
        {
            return new Builder(kind, partition);
        }

        private Builder(Kind kind, UnfilteredRowIterator partition)
        {
            super(kind, partition);
        }

        public SparkCdcEvent buildEvent(Kind kind, UnfilteredRowIterator partition)
        {
            return new SparkCdcEvent(kind, partition);
        }

        @Override
        public SparkCdcEvent fromRow(org.apache.spark.sql.Row row)
        {
            Preconditions.checkState(isEmptyBuilder(), "Cannot build from row with a non-empty builder.");
            String keyspace = row.getString(SCHEMA.fieldIndex(KEYSPACE));
            String table = row.getString(SCHEMA.fieldIndex(TABLE));
            String opType = row.getString(SCHEMA.fieldIndex(OPERATION_TYPE.fieldName()));
            Kind kind = Kind.valueOf(opType.toUpperCase());
            return new SparkCdcEvent(kind, keyspace, table, row);
        }
    }
}
