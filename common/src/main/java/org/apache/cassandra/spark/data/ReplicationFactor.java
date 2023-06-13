package org.apache.cassandra.spark.data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
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
public class ReplicationFactor implements Serializable
{

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationFactor.class);
    public static final ReplicationFactor.Serializer SERIALIZER = new ReplicationFactor.Serializer();

    public enum ReplicationStrategy
    {
        LocalStrategy(0), SimpleStrategy(1), NetworkTopologyStrategy(2);
        public final int value;

        ReplicationStrategy(final int value)
        {
            this.value = value;
        }

        public static ReplicationStrategy valueOf(final int value)
        {
            switch (value)
            {
                case 0:
                    return LocalStrategy;
                case 1:
                    return SimpleStrategy;
                case 2:
                    return NetworkTopologyStrategy;
            }
            throw new IllegalStateException("Unknown ReplicationStrategy: " + value);
        }

        public static ReplicationStrategy getEnum(final String value)
        {
            for (final ReplicationStrategy v : values())
            {
                if (value.equalsIgnoreCase(v.name()) || value.endsWith("." + v.name()))
                {
                    return v;
                }
            }
            throw new IllegalArgumentException();
        }
    }

    @NotNull
    private final ReplicationStrategy replicationStrategy;
    @NotNull
    private final Map<String, Integer> options;

    public ReplicationFactor(@NotNull final Map<String, String> options)
    {
        this.replicationStrategy = ReplicationFactor.ReplicationStrategy.getEnum(options.get("class"));
        this.options = new LinkedHashMap<>(options.size());
        for (final Map.Entry<String, String> entry : options.entrySet())
        {
            if ("class".equals(entry.getKey()))
            {
                continue;
            }

            try
            {
                final String key = this.replicationStrategy == ReplicationStrategy.NetworkTopologyStrategy ? entry.getKey().toUpperCase() : entry.getKey();
                this.options.put(key, Integer.parseInt(entry.getValue()));
            }
            catch (final NumberFormatException e)
            {
                LOGGER.warn("Could not parse replication option: {} = {}", entry.getKey(), entry.getValue());
            }
        }
    }

    public ReplicationFactor(@NotNull final ReplicationStrategy replicationStrategy, @NotNull final Map<String, Integer> options)
    {
        this.replicationStrategy = replicationStrategy;
        this.options = new LinkedHashMap<>(options.size());

        if (!replicationStrategy.equals(ReplicationStrategy.LocalStrategy) && options.isEmpty())
        {
            throw new RuntimeException(String.format("Could not find replication info in schema map: %s.", options));
        }

        for (final Map.Entry<String, Integer> entry : options.entrySet())
        {
            if ("class".equals(entry.getKey()))
            {
                continue;
            }
            final String key = this.replicationStrategy == ReplicationStrategy.NetworkTopologyStrategy ? entry.getKey().toUpperCase() : entry.getKey();
            this.options.put(key, entry.getValue());
        }
    }

    public Integer getTotalReplicationFactor()
    {
        int total = 0;
        for (final Integer value : options.values())
        {
            total += value;
        }
        return total;
    }

    @NotNull
    public Map<String, Integer> getOptions()
    {
        return options;
    }

    @NotNull
    public ReplicationStrategy getReplicationStrategy()
    {
        return replicationStrategy;
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

        final ReplicationFactor rhs = (ReplicationFactor) obj;
        return new EqualsBuilder()
               .append(replicationStrategy, rhs.replicationStrategy)
               .append(options, rhs.options)
               .isEquals();
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(47, 53)
               .append(replicationStrategy)
               .append(options)
               .toHashCode();
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<ReplicationFactor>
    {
        @Override
        public void write(final Kryo kryo, final Output out, final ReplicationFactor rf)
        {
            out.writeByte(rf.replicationStrategy.value);
            out.writeByte(rf.options.size());
            for (final Map.Entry<String, Integer> entry : rf.options.entrySet())
            {
                out.writeString(entry.getKey());
                out.writeByte(entry.getValue());
            }
        }

        @Override
        public ReplicationFactor read(final Kryo kryo, final Input in, final Class<ReplicationFactor> type)
        {
            final ReplicationStrategy strategy = ReplicationStrategy.valueOf(in.readByte());
            final int numOptions = in.readByte();
            final Map<String, Integer> options = new HashMap<>(numOptions);
            for (int i = 0; i < numOptions; i++)
            {
                options.put(in.readString(), (int) in.readByte());
            }
            return new ReplicationFactor(strategy, options);
        }
    }
}

