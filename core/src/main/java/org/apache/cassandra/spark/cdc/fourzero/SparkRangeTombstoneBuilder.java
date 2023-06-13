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

package org.apache.cassandra.spark.cdc.fourzero;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.spark.cdc.RangeTombstoneBuilder;
import org.apache.cassandra.spark.cdc.SparkRangeTombstone;
import org.apache.cassandra.spark.cdc.SparkValueWithMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;

/**
 * Keep track of the last range tombstone marker to build {@link SparkRangeTombstone}
 * The caller should check whether {@link #canBuild()} after adding marker, and it should build whenever possible.
 */
public class SparkRangeTombstoneBuilder extends RangeTombstoneBuilder<SparkValueWithMetadata, SparkRangeTombstone>
{
    public SparkRangeTombstoneBuilder(TableMetadata tableMetadata)
    {
        super(tableMetadata);
    }

    @Override
    public SparkRangeTombstone buildTombstone(List<SparkValueWithMetadata> start,
                                              boolean isStartInclusive,
                                              List<SparkValueWithMetadata> end,
                                              boolean isEndInclusive)
    {
        return SparkRangeTombstone.of(start, isStartInclusive, end, isEndInclusive);
    }

    @Override
    public SparkValueWithMetadata buildValue(String name, String type, ByteBuffer buf)
    {
        return SparkValueWithMetadata.of(name, type, buf);
    }
}