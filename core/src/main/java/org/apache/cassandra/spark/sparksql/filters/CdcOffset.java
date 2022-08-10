package org.apache.cassandra.spark.sparksql.filters;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.spark.sql.execution.streaming.Offset;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.KeyDeserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.jetbrains.annotations.NotNull;
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

public class CdcOffset extends Offset implements Serializable, Comparable<CdcOffset>
{
    public static final ObjectMapper MAPPER = new ObjectMapper();

    static
    {
        final SimpleModule module = new SimpleModule("", Version.unknownVersion());
        module.addKeyDeserializer(CassandraInstance.class, new KeyDeserializer()
        {
            public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException, JsonProcessingException
            {
                return MAPPER.readValue(key, CassandraInstance.class);
            }
        });
        MAPPER.registerModule(module);
    }

    public static class InstanceLogs implements Serializable
    {
        @Nullable
        private final CommitLog.Marker marker;
        private final List<SerializableCommitLog> logs;

        public InstanceLogs(List<CommitLog> logs)
        {
            this.marker = logs.stream()
                              .map(CommitLog::maxMarker)
                              .max(CommitLog.Marker::compareTo)
                              .orElse(null);
            this.logs = logs.stream()
                            .map(SerializableCommitLog::new)
                            .collect(Collectors.toList());
        }

        @JsonCreator
        public InstanceLogs(@JsonProperty("marker") @Nullable CommitLog.Marker marker,
                            @JsonProperty("logs") List<SerializableCommitLog> logs)
        {
            this.marker = marker;
            this.logs = logs;
        }

        @Nullable
        public CommitLog.Marker getMarker()
        {
            return marker;
        }

        public List<SerializableCommitLog> getLogs()
        {
            return logs;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (obj == this)
            {
                return true;
            }
            if (!(obj instanceof InstanceLogs))
            {
                return false;
            }

            final InstanceLogs rhs = (InstanceLogs) obj;
            return Objects.equals(marker, rhs.marker) &&
                   logs.equals(rhs.logs);
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder(101, 103)
                   .append(marker)
                   .append(logs)
                   .toHashCode();
        }
    }

    private final long timestampMicros;
    private final Map<CassandraInstance, InstanceLogs> instances;

    /**
     * Serializable container for CommitLog info so it can be serialized and sent to executors.
     */
    public static class SerializableCommitLog implements Serializable
    {
        private final String name, path;
        private final long maxOffset, len;

        public SerializableCommitLog(CommitLog log)
        {

            this(log.name(), log.path(), log.maxOffset(), log.len());
        }

        @JsonCreator
        public SerializableCommitLog(@JsonProperty("name") String name,
                                     @JsonProperty("path") String path,
                                     @JsonProperty("maxOffset") long maxOffset,
                                     @JsonProperty("len") long len)
        {
            this.name = name;
            this.path = path;
            this.maxOffset = maxOffset;
            this.len = len;
        }

        public String getName()
        {
            return name;
        }

        public String getPath()
        {
            return path;
        }

        @SuppressWarnings("unused")
        public long getMaxOffset()
        {
            return maxOffset;
        }

        public long getLen()
        {
            return len;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (obj == this)
            {
                return true;
            }
            if (obj.getClass() != getClass())
            {
                return false;
            }

            final SerializableCommitLog rhs = (SerializableCommitLog) obj;
            return name.equals(rhs.name) &&
                   path.equals(rhs.path) &&
                   maxOffset == rhs.maxOffset &&
                   len == rhs.len;
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder(101, 103)
                   .append(name)
                   .append(path)
                   .append(maxOffset)
                   .append(len)
                   .toHashCode();
        }
    }

    public CdcOffset(long timestampMicros)
    {
        this(timestampMicros, Collections.emptyMap());
    }

    public CdcOffset(long timestampMicros, Stream<CommitLog> logs)
    {
        this.timestampMicros = timestampMicros;
        this.instances = logs.collect(Collectors.groupingBy(
        CommitLog::instance,
        Collectors.collectingAndThen(Collectors.toList(), InstanceLogs::new)
        ));
    }

    @JsonCreator
    public CdcOffset(@JsonProperty("timestampMicros") long timestampMicros,
                     @JsonProperty("instanceLogs") Map<CassandraInstance, InstanceLogs> instances)
    {
        this.timestampMicros = timestampMicros;
        this.instances = instances;
    }

    public static CdcOffset fromJson(final String json)
    {
        try
        {
            return MAPPER.readValue(json, CdcOffset.class);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String json()
    {
        try
        {
            return MAPPER.writeValueAsString(this);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public long getTimestampMicros()
    {
        return timestampMicros;
    }

    public Map<CassandraInstance, InstanceLogs> getInstanceLogs()
    {
        return instances;
    }

    public CommitLog.Marker marker(CassandraInstance instance)
    {
        return Optional.ofNullable(instances.get(instance))
                       .map(InstanceLogs::getMarker)
                       .orElse(CommitLog.Marker.origin(instance));
    }

    public Map<CassandraInstance, List<SerializableCommitLog>> allLogs()
    {
        return instances.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getLogs()));
    }

    public List<SerializableCommitLog> getLogs(CassandraInstance instance)
    {
        return Optional.ofNullable(instances.get(instance))
                       .map(l -> l.logs)
                       .orElse(Collections.emptyList());
    }

    @Override
    public String toString()
    {
        return json();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof CdcOffset))
        {
            return false;
        }

        final CdcOffset rhs = (CdcOffset) obj;
        return timestampMicros == rhs.timestampMicros &&
               instances.equals(rhs.instances);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(101, 103)
               .append(timestampMicros)
               .append(instances)
               .toHashCode();
    }

    public int compareTo(@NotNull CdcOffset o)
    {
        return Long.compare(timestampMicros, o.timestampMicros);
    }
}
