package org.apache.cassandra.spark.sparksql.filters;

import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.sql.execution.streaming.Offset;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import org.apache.commons.lang.builder.HashCodeBuilder;

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

public class CdcOffset extends Offset implements Serializable, Comparable<CdcOffset>
{
    public static final ObjectMapper MAPPER = new ObjectMapper();

    private final long timestampMicros;

    @JsonCreator
    public CdcOffset(@JsonProperty("timestamp") long timestampMicros)
    {
        this.timestampMicros = timestampMicros;
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

    public long getTimestampMicros()
    {
        return timestampMicros;
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
        if (obj.getClass() != getClass())
        {
            return false;
        }

        final CdcOffset rhs = (CdcOffset) obj;
        return timestampMicros == rhs.timestampMicros;
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(101, 103)
               .append(timestampMicros)
               .toHashCode();
    }

    public int compareTo(@NotNull CdcOffset o)
    {
        return Long.compare(timestampMicros, o.timestampMicros);
    }
}
