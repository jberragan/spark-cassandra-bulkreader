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

package org.apache.cassandra.spark.sparksql.filters;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.Marker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.jetbrains.annotations.NotNull;

public class CdcOffset implements Serializable, Comparable<CdcOffset>
{
    public static final ObjectMapper MAPPER = new ObjectMapper();
    public static final Serializer SERIALIZER = new Serializer();

    static
    {
        final SimpleModule module = new SimpleModule();
        module.addKeyDeserializer(CassandraInstance.class, new KeyDeserializer()
        {
            public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException
            {
                return MAPPER.readValue(key, CassandraInstance.class);
            }
        });
        MAPPER.registerModule(module);
    }

    private final long timestampMicros;
    private final Map<CassandraInstance, InstanceLogs> instances;

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

    public Map<CassandraInstance, Marker> startMarkers()
    {
        return instances.entrySet().stream()
                        .filter(e -> Objects.nonNull(e.getValue().getMarker()))
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getMarker()));
    }

    public Marker marker(CassandraInstance instance)
    {
        return Optional.ofNullable(instances.get(instance))
                       .map(InstanceLogs::getMarker)
                       .orElse(Marker.origin(instance));
    }

    public Map<CassandraInstance, List<SerializableCommitLog>> allLogs()
    {
        return instances.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getLogs()));
    }

    public List<SerializableCommitLog> getLogs(CassandraInstance instance)
    {
        return Optional.ofNullable(instances.get(instance))
                       .map(InstanceLogs::getLogs)
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

    // kryo

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CdcOffset>
    {
        public void write(Kryo kryo, Output out, CdcOffset obj)
        {
            out.writeLong(obj.timestampMicros);
            out.writeShort(obj.instances.size());
            for (final Map.Entry<CassandraInstance, InstanceLogs> entry : obj.instances.entrySet())
            {
                kryo.writeObject(out, entry.getKey(), CassandraInstance.SERIALIZER);
                kryo.writeObject(out, entry.getValue(), InstanceLogs.SERIALIZER);
            }
        }

        public CdcOffset read(Kryo kryo, Input in, Class<CdcOffset> type)
        {
            final long timestampMicros = in.readLong();
            final int len = in.readShort();

            final Map<CassandraInstance, InstanceLogs> instances = new HashMap<>(len);
            for (int i = 0; i < len; i++)
            {
                final CassandraInstance inst = kryo.readObject(in, CassandraInstance.class, CassandraInstance.SERIALIZER);
                final InstanceLogs logs = kryo.readObject(in, InstanceLogs.class, InstanceLogs.SERIALIZER);
                instances.put(inst, logs);
            }

            return new CdcOffset(timestampMicros, instances);
        }
    }
}
