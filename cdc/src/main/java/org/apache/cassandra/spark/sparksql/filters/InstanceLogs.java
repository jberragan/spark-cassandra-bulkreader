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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.Marker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.jetbrains.annotations.Nullable;

public class InstanceLogs implements Serializable
{
    public static final Serializer SERIALIZER = new Serializer();

    @Nullable
    private final Marker marker;
    private final List<SerializableCommitLog> logs;

    public InstanceLogs(List<CommitLog> logs)
    {
        this.marker = logs.stream()
                          .map(CommitLog::maxMarker)
                          .max(Marker::compareTo)
                          .orElse(null);
        this.logs = logs.stream()
                        .map(SerializableCommitLog::new)
                        .collect(Collectors.toList());
    }

    @JsonCreator
    public InstanceLogs(@JsonProperty("marker") @Nullable Marker marker,
                        @JsonProperty("logs") List<SerializableCommitLog> logs)
    {
        this.marker = marker;
        this.logs = logs;
    }

    @Nullable
    public Marker getMarker()
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

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<InstanceLogs>
    {
        @Override
        public void write(Kryo kryo, Output out, InstanceLogs o)
        {
            kryo.writeObject(out, o.marker, Marker.SERIALIZER);
            writeLogs(kryo, out, o);
        }

        public static void writeLogs(Kryo kryo, Output out, InstanceLogs o)
        {
            out.writeShort(o.logs.size());
            for (final SerializableCommitLog log : o.getLogs())
            {
                kryo.writeObject(out, log, SerializableCommitLog.SERIALIZER);
            }
        }

        @Override
        public InstanceLogs read(Kryo kryo, Input in, Class<InstanceLogs> type)
        {
            final Marker marker = kryo.readObject(in, Marker.class, Marker.SERIALIZER);
            return new InstanceLogs(marker, readLogs(kryo, in));
        }

        public static List<SerializableCommitLog> readLogs(Kryo kryo, Input in)
        {
            final int num = in.readShort();
            final List<SerializableCommitLog> logs = new ArrayList<>(num);
            for (int i = 0; i < num; i++)
            {
                logs.add(kryo.readObject(in, SerializableCommitLog.class, SerializableCommitLog.SERIALIZER));
            }
            return logs;
        }
    }

    // slightly more compact serializer that excludes CassandraInstance
    public static class CompactSerializer extends com.esotericsoftware.kryo.Serializer<InstanceLogs>
    {
        private final CassandraInstance instance;

        public CompactSerializer(final CassandraInstance instance)
        {
            this.instance = instance;
        }

        @Override
        public void write(Kryo kryo, Output out, InstanceLogs o)
        {
            if (o.marker == null)
            {
                out.writeBoolean(false);
            }
            else
            {
                out.writeBoolean(true);
                out.writeLong(o.marker.segmentId());
                out.writeInt(o.marker.position());
            }

            Serializer.writeLogs(kryo, out, o);
        }

        @Override
        public InstanceLogs read(Kryo kryo, Input in, Class<InstanceLogs> type)
        {
            Marker marker = null;
            if (in.readBoolean())
            {
                marker = new Marker(instance, in.readLong(), in.readInt());
            }

            return new InstanceLogs(marker, Serializer.readLogs(kryo, in));
        }
    }
}
