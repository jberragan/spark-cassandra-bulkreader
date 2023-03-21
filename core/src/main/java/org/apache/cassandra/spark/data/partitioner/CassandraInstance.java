package org.apache.cassandra.spark.data.partitioner;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.sparksql.filters.CdcOffset;

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

@SuppressWarnings("WeakerAccess")
public class CassandraInstance implements Serializable
{
    public static final Serializer SERIALIZER = new Serializer();
    private final String token, node, dc;

    @JsonCreator
    public CassandraInstance(@JsonProperty("token") final String token,
                             @JsonProperty("node") final String node,
                             @JsonProperty("dc") final String dc)
    {
        this.token = token;
        this.node = node;
        this.dc = dc.toUpperCase();
    }

    @JsonGetter("token")
    public String token()
    {
        return this.token;
    }

    @JsonGetter("node")
    public String nodeName()
    {
        return this.node;
    }

    @JsonGetter("dc")
    public String dataCenter()
    {
        return this.dc;
    }

    public CommitLog.Marker zeroMarker()
    {
        return markerAt(0, 0);
    }

    public CommitLog.Marker markerAt(long section, int position)
    {
        return new CommitLog.Marker(this, section, position);
    }

    @Override
    public boolean equals(final Object obj)
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

        final CassandraInstance rhs = (CassandraInstance) obj;
        return new EqualsBuilder()
               .append(token, rhs.token)
               .append(node, rhs.node)
               .append(dc, rhs.dc)
               .isEquals();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(29, 31)
               .append(token)
               .append(node)
               .append(dc)
               .build();
    }

    public String toString()
    {
        try
        {
            return CdcOffset.MAPPER.writeValueAsString(this);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CassandraInstance>
    {
        @Override
        public CassandraInstance read(final Kryo kryo, final Input in, final Class type)
        {
            return new CassandraInstance(in.readString(), in.readString(), in.readString());
        }

        @Override
        public void write(final Kryo kryo, final Output out, final CassandraInstance instance)
        {
            out.writeString(instance.token());
            out.writeString(instance.nodeName());
            out.writeString(instance.dataCenter());
        }
    }
}
