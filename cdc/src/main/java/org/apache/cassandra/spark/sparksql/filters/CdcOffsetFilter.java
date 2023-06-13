package org.apache.cassandra.spark.sparksql.filters;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.spark.cdc.Marker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
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

/**
 * Cdc Offset filter used to checkpoint Streaming queries and only read mutations within watermark window.
 */
public class CdcOffsetFilter implements Serializable
{
    private final Map<CassandraInstance, Marker> startMarkers;
    private final Map<CassandraInstance, List<SerializableCommitLog>> logs;
    private final long startTimestampMicros;
    private final long maxAgeMicros;

    public CdcOffsetFilter(@NotNull final Map<CassandraInstance, Marker> startMarkers,
                           @NotNull final Map<CassandraInstance, List<SerializableCommitLog>> logs,
                           @NotNull final Long startTimestampMicros,
                           @NotNull final Duration watermarkWindow)
    {
        this.startMarkers = startMarkers;
        this.logs = logs;
        this.startTimestampMicros = startTimestampMicros;
        this.maxAgeMicros = startTimestampMicros - (watermarkWindow.toNanos() / 1000);
    }

    /**
     * Return true if mutation timestamp overlaps with watermark window.
     *
     * @param timestampMicros mutation timestamp in micros
     * @return true if timestamp overlaps with range
     */
    public boolean overlaps(final long timestampMicros)
    {
        return timestampMicros >= maxAgeMicros;
    }

    public static CdcOffsetFilter of(@NotNull final Map<CassandraInstance, Marker> startMarkers,
                                     @NotNull final Map<CassandraInstance, List<SerializableCommitLog>> logs,
                                     @NotNull final Long startTimestampMicros,
                                     @NotNull final Duration watermarkWindow)
    {
        return new CdcOffsetFilter(startMarkers, logs, startTimestampMicros, watermarkWindow);
    }

    public long maxAgeMicros()
    {
        return maxAgeMicros;
    }

    public Marker startMarker(CassandraInstance instance)
    {
        return Optional.ofNullable(this.startMarkers.get(instance))
                       .orElseGet(() -> Marker.origin(instance));
    }

    public Map<CassandraInstance, List<SerializableCommitLog>> allLogs()
    {
        return logs;
    }

    public List<SerializableCommitLog> logs(CassandraInstance instance)
    {
        return Optional.ofNullable(logs.get(instance))
                       .orElseGet(Collections::emptyList);
    }

    public long getStartTimestampMicros()
    {
        return startTimestampMicros;
    }
}
