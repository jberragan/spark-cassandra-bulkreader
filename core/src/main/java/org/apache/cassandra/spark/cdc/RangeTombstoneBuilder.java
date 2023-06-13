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

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.ClusteringBound;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.google.common.collect.ImmutableList;

/**
 * Keep track of the last range tombstone marker to build {@link RangeTombstone}
 * The caller should check whether {@link #canBuild()} after adding marker, and it should build whenever possible.
 */
public abstract class RangeTombstoneBuilder<ValueType extends ValueWithMetadata, TombstoneType extends RangeTombstone<ValueType>>
{
    private final TableMetadata tableMetadata;
    private RangeTombstoneMarker rangeTombstoneMarker;
    private TombstoneType rangeTombstone;
    private boolean expectOpen = true;

    public RangeTombstoneBuilder(TableMetadata tableMetadata)
    {
        this.tableMetadata = tableMetadata;
    }

    public void add(RangeTombstoneMarker marker)
    {
        if (expectOpen)
        {
            Preconditions.checkArgument(!marker.isBoundary() && marker.isOpen(false),
                                        "Expect onyly open bound");
            rangeTombstoneMarker = marker;
            expectOpen = false;
        }
        else
        {
            Preconditions.checkArgument(marker.isClose(false), "Expect close bound or boundary");
            RangeTombstoneMarker lastMarker = rangeTombstoneMarker;
            ClusteringBound<?> open = lastMarker.openBound(false);
            List<ValueType> start = buildClusteringKey(open);
            ClusteringBound<?> close = marker.closeBound(false);
            List<ValueType> end = buildClusteringKey(close);
            rangeTombstone = buildTombstone(start, open.isInclusive(), end, close.isInclusive());
            // When marker is a boundary, it opens a new range immediately
            // We expect close for the next, i.e. expectOpen == false, and carry the boundary forward
            // Otherwise, we expect open for the next.
            if (marker.isBoundary())
            {
                rangeTombstoneMarker = marker;
            }
            else
            {
                expectOpen = true;
                // reset to null as the last range tombstone marker has been fully consumed
                rangeTombstoneMarker = null;
            }
        }
    }

    public abstract TombstoneType buildTombstone(List<ValueType> start, boolean isStartInclusive, List<ValueType> end, boolean isEndInclusive);

    public boolean canBuild()
    {
        return rangeTombstone != null;
    }

    public TombstoneType build()
    {
        TombstoneType res = rangeTombstone;
        rangeTombstone = null;
        return res;
    }

    /**
     * @return true when there is range tombstone marker not consumed.
     */
    public boolean hasIncompleteRange()
    {
        return rangeTombstoneMarker != null;
    }

    private List<ValueType> buildClusteringKey(ClusteringPrefix<?> clustering)
    {
        ImmutableList<ColumnMetadata> ckMetadata = tableMetadata.clusteringColumns();
        List<ValueType> result = new ArrayList<>(clustering.size());
        // a valid range bound does not have non-null values following a null value.
        for (int i = 0; i < ckMetadata.size() && i < clustering.size(); i++)
        {
            result.add(buildValue(ckMetadata.get(i).name.toCQLString(),
                                  ckMetadata.get(i).type.asCQL3Type().toString(),
                                  clustering.bufferAt(i)));
        }
        return result;
    }

    public abstract ValueType buildValue(String name, String type, ByteBuffer buf);
}
