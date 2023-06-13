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

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;

/**
 * Serializable container for CommitLog info so it can be serialized and sent to executors.
 */
public class SerializableCommitLog implements Serializable
{
    public static final Serializer SERIALIZER = new Serializer();

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

    public String toString()
    {
        try
        {
            return CassandraInstance.MAPPER.writeValueAsString(this);
        }
        catch (JsonProcessingException e)
        {
            throw new RuntimeException(e);
        }
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

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<SerializableCommitLog>
    {
        public void write(Kryo kryo, Output out, SerializableCommitLog o)
        {
            out.writeString(o.name);
            out.writeString(o.path);
            out.writeLong(o.maxOffset);
            out.writeLong(o.len);
        }

        public SerializableCommitLog read(Kryo kryo, Input in, Class<SerializableCommitLog> type)
        {
            return new SerializableCommitLog(in.readString(), in.readString(), in.readLong(), in.readLong());
        }
    }
}
