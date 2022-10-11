package org.apache.cassandra.spark.cdc.watermarker;

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

public interface Watermarker
{

    /**
     * @param jobId job id that uniquely tracks this Spark job.
     * @return a Watermarker for this Spark job.
     */
    Watermarker instance(String jobId);

    /**
     * Insufficient replica copies to publish update, so record number of replicas read so we can publish once we achieve Consistency Level.
     *
     * @param update the cdc update we need to track until we receive CL mutations.
     */
    void recordReplicaCount(CdcUpdate update, int numReplicas);

    /**
     * Return how many replicas we have previously read for this mutation.
     *
     * @param update the cdc update.
     * @return number of replicas previously received or 0 if never seen before.
     */
    int replicaCount(CdcUpdate update);

    /**
     * We received sufficient replica copies for a given update we can stop tracking the number of replicas for this update.
     *
     * @param update the cdc update.
     */
    void untrackReplicaCount(CdcUpdate update);

    /**
     * @param update the cdc update.
     * @return true if we have previously seen this update before.
     */
    boolean seenBefore(CdcUpdate update);

    /**
     * Persist watermark state to a persistent external store that can be resumed in the next Spark Streaming batch.
     */
    void persist(@Nullable final Long maxAgeMicros);

    /**
     * Clear watermark history
     */
    void clear();
}
