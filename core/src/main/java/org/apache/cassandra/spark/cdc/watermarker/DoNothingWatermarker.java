package org.apache.cassandra.spark.cdc.watermarker;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.CdcUpdate;
import org.jetbrains.annotations.Nullable;

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
 * Watermarker that does nothing.
 */
public class DoNothingWatermarker implements Watermarker
{
    public static final DoNothingWatermarker INSTANCE = new DoNothingWatermarker();

    public Watermarker instance(String jobId)
    {
        return this;
    }

    public void recordReplicaCount(CdcUpdate update, int numReplicas)
    {

    }

    public int replicaCount(CdcUpdate update)
    {
        return 0;
    }

    public void untrackReplicaCount(CdcUpdate update)
    {

    }

    public boolean seenBefore(CdcUpdate update)
    {
        return false;
    }

    public void updateHighWaterMark(CommitLog.Marker marker)
    {

    }

    @Nullable
    public CommitLog.Marker highWaterMark(CassandraInstance instance)
    {
        return null;
    }

    public void persist(@Nullable Long maxAgeMicros)
    {

    }

    public void clear()
    {

    }
}
