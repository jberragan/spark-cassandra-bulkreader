package org.apache.cassandra.spark.reader;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.spark.utils.ByteBufUtils;

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
 * Rid - Row Identifier - contains the partition key, clustering keys and column name that uniquely identifies a row and column of data in Cassandra
 */
public class Rid
{
    private ByteBuffer partitionKey, columnName, value;
    private long timestamp;
    private BigInteger token;
    @VisibleForTesting
    boolean isNewPartition = false;
    private boolean isRowDeletion = false;
    private boolean isPartitionDeletion = false;
    private boolean isUpdate = false;

    // optional field
    // It memorizes the tombstoned elements/cells in a complex data
    // Only used in CDC
    private List<ByteBuffer> tombstonedCellsInComplex = null;

    // optional field
    // It memorizes the range tombstone markers with in the same partition.
    // Only used in CDC
    private List<RangeTombstoneMarker> rangeTombstoneMarkers = null;
    private boolean shouldConsumeRangeTombstoneMarkers = false;


    // partition key value

    public void setPartitionKeyCopy(final ByteBuffer partitionKeyBytes, final BigInteger token)
    {
        this.partitionKey = partitionKeyBytes;
        this.token = token;
        this.columnName = null;
        this.value = null;
        this.isNewPartition = true;
        this.timestamp = 0L;
    }

    public boolean isNewPartition()
    {
        if (this.isNewPartition)
        {
            this.isNewPartition = false;
            return true;
        }
        return false;
    }

    public boolean isPartitionDeletion()
    {
        return isPartitionDeletion;
    }

    public void setPartitionDeletion(boolean isPartitionDeletion)
    {
        this.isPartitionDeletion = isPartitionDeletion;
    }

    public boolean isUpdate()
    {
        return isUpdate;
    }

    public void setIsUpdate(boolean isUpdate)
    {
        this.isUpdate = isUpdate;
    }

    public boolean isRowDeletion()
    {
        return isRowDeletion;
    }

    public void setRowDeletion(boolean isRowDeletion)
    {
        this.isRowDeletion = isRowDeletion;
    }

    public ByteBuffer getPartitionKey()
    {
        return this.partitionKey;
    }

    public BigInteger getToken()
    {
        return this.token;
    }

    // column name containing concatenated clustering keys and column name

    public void setColumnNameCopy(final ByteBuffer columnBytes)
    {
        this.columnName = columnBytes;
    }

    public ByteBuffer getColumnName()
    {
        return this.columnName;
    }

    // value of cell

    public ByteBuffer getValue()
    {
        return this.value;
    }

    public void setValueCopy(final ByteBuffer value)
    {
        this.value = value;
    }

    // timestamp

    public void setTimestamp(final long timestamp)
    {
        this.timestamp = timestamp;
    }

    public long getTimestamp()
    {
        return this.timestamp;
    }

    // cdc: handle element deletion in complex
    // adds the serialized cellpath to the tombstone
    public void addCellTombstoneInComplex(ByteBuffer key)
    {
        if (tombstonedCellsInComplex == null)
        {
            tombstonedCellsInComplex = new ArrayList<>();
        }
        tombstonedCellsInComplex.add(key);
    }

    public boolean hasCellTombstoneInComplex()
    {
        return tombstonedCellsInComplex != null && !tombstonedCellsInComplex.isEmpty();
    }

    public List<ByteBuffer> getCellTombstonesInComplex()
    {
        return tombstonedCellsInComplex;
    }

    public void resetCellTombstonesInComplex()
    {
        tombstonedCellsInComplex = null;
    }

    public void addRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        if (rangeTombstoneMarkers == null)
        {
            rangeTombstoneMarkers = new ArrayList<>();
        }

        // ensure the marker list is valid
        if (rangeTombstoneMarkers.isEmpty())
        {
            Preconditions.checkArgument(!marker.isBoundary() && marker.isOpen(false),
                                        "The first marker should be an open bound");
            rangeTombstoneMarkers.add(marker);
        }
        else
        {
            RangeTombstoneMarker lastMarker = rangeTombstoneMarkers.get(rangeTombstoneMarkers.size() - 1);
            Preconditions.checkArgument((lastMarker.isOpen(false) && marker.isClose(false))
                                        || (lastMarker.isClose(false) && marker.isOpen(false)),
                                        "Current marker should close or open a new range");
            rangeTombstoneMarkers.add(marker);
        }
    }

    public boolean hasRangeTombstoneMarkers()
    {
        return rangeTombstoneMarkers != null && !rangeTombstoneMarkers.isEmpty();
    }

    public void setShouldConsumeRangeTombstoneMarkers(boolean val)
    {
        shouldConsumeRangeTombstoneMarkers = val;
    }

    public boolean shouldConsumeRangeTombstoneMarkers()
    {
        return shouldConsumeRangeTombstoneMarkers;
    }

    public List<RangeTombstoneMarker> getRangeTombstoneMarkers()
    {
        return rangeTombstoneMarkers;
    }

    public void resetRangeTombstoneMarkers()
    {
        rangeTombstoneMarkers = null;
        shouldConsumeRangeTombstoneMarkers = false;
    }

    @Override
    public String toString()
    {
        return ByteBufUtils.toHexString(this.getPartitionKey()) +
               ":" +
               ByteBufUtils.toHexString(this.getColumnName()) +
               ":" +
               ByteBufUtils.toHexString(this.getValue());
    }
}
