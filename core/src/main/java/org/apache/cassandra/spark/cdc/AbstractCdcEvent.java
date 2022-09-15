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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.WrappedArray;

/**
 * Cassandra version ignorant abstraction of CdcEvent
 */
public abstract class AbstractCdcEvent implements SparkRowSource
{
    protected static final String KEYSPACE        = "Keyspace";
    protected static final String TABLE           = "Table";
    protected static final String PARTITION_KEYS  = "PartitionKeys";
    protected static final String CLUSTERING_KEYS = "ClusteringKeys";
    protected static final String STATIC_COLUMNS  = "StaticColumns";
    protected static final String VALUE_COLUMNS   = "ValueColumns";

    public static final int NO_TTL = 0;
    public static final int NO_EXPIRATION = Integer.MAX_VALUE;

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
        columns[i++] = DataTypes.createStructField(PARTITION_KEYS, DataTypes.createArrayType(ValueWithMetadata.SCHEMA), false);
        columns[i++] = DataTypes.createStructField(CLUSTERING_KEYS, DataTypes.createArrayType(ValueWithMetadata.SCHEMA), true);
        columns[i++] = DataTypes.createStructField(STATIC_COLUMNS, DataTypes.createArrayType(ValueWithMetadata.SCHEMA), true);
        columns[i++] = DataTypes.createStructField(VALUE_COLUMNS, DataTypes.createArrayType(ValueWithMetadata.SCHEMA), true);
        for (SchemaFeature f : SchemaFeatureSet.ALL_CDC_FEATURES)
        {
            columns[i++] = f.field();
        }
        SCHEMA = DataTypes.createStructType(columns);
    }

    public final String keyspace;
    public final String table;
    protected Kind kind;

    protected AbstractCdcEvent(Kind kind, String keyspace, String table)
    {
        this.kind = kind;
        this.keyspace = keyspace;
        this.table = table;
    }

    /**
     * @return the kind of the cdc event.
     */
    public Kind getKind()
    {
        return kind;
    }

    /**
     * @return the timestamp of the cdc event in {@link TimeUnit}
     */
    public abstract long getTimestamp(TimeUnit timeUnit);

    /**
     * @return the partition keys. The returned list must not be null and empty.
     */
    public abstract List<ValueWithMetadata> getPartitionKeys();

    /**
     * @return the clustering keys. The returned list could be null if the mutation carries no clustering keys.
     */
    public abstract List<ValueWithMetadata> getClusteringKeys();

    /**
     * @return the static columns. The returned list could be null if the mutation carries no static columns.
     */
    public abstract List<ValueWithMetadata> getStaticColumns();

    /**
     * @return the value columns. The returned list could be null if the mutation carries no value columns.
     */
    public abstract List<ValueWithMetadata> getValueColumns();

    /**
     * The map returned contains the list of deleted keys (i.e. cellpath in Cassandra's terminology) of each affected
     * complext column. A complex column could be an unfrozen map, set and udt in Cassandra.
     * @return the tombstoned cells in the complex data columns. The returned map could be null if the mutation does not
     *         delete elements from complex.
     */
    public abstract Map<String, List<ByteBuffer>> getTombstonedCellsInComplex();

    /**
     * @return the range tombstone list. The returned list could be null if the mutation is not a range deletin.
     */
    public abstract List<RangeTombstone> getRangeTombstoneList();

    /**
     * @return the time to live. The returned value could be null if the mutation carries no such value.
     */
    public abstract TimeToLive getTtl();

    public enum Kind
    {
        INSERT,
        UPDATE,
        DELETE,
        PARTITION_DELETE,
        ROW_DELETE,
        RANGE_DELETE,
        COMPLEX_ELEMENT_DELETE,
    }

    public static class TimeToLive
    {
        public final int ttlInSec;
        public final int expirationTimeInSec;

        public TimeToLive(int ttlInSec, int expirationTimeInSec)
        {
            this.ttlInSec = ttlInSec;
            this.expirationTimeInSec = expirationTimeInSec;
        }
    }

    // Convert the field values into array data of the spark row field (e.g. pk, ck, etc.) accordingly.
    protected static ArrayData cqlFieldsToArray(List<ValueWithMetadata> values)
    {
        if (values == null)
        {
            // Values not present. The clustering keys, static columns and value columns are nullable.
            return null;
        }

        Object[] valArray = values.stream()
                                  .map(ValueWithMetadata::toRow)
                                  .toArray();
        return ArrayData.toArrayData(valArray);
    }

    protected static List<ValueWithMetadata> arrayToCqlFields(Object array, boolean nullable)
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
        List<ValueWithMetadata> result = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++)
        {
            result.add(ValueWithMetadata.EMPTY.fromRow(values.apply(i)));
        }
        return result;
    }
}
