package org.apache.cassandra.spark;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import org.apache.cassandra.spark.cdc.Marker;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
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
 * Helper class to register classes for Kryo serialization
 */
public class BaseKryoRegister
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseKryoRegister.class);
    private static final ConcurrentHashMap<Class<?>, com.esotericsoftware.kryo.Serializer<?>> SERIALIZERS = new ConcurrentHashMap<>(4);

    public static <T> void addSerializer(@NotNull final Class<T> type,
                                         @NotNull final com.esotericsoftware.kryo.Serializer<T> serializer)
    {
        LOGGER.info("Registering custom Kryo serializer type={}", type.getName());
        SERIALIZERS.put(type, serializer);
    }

    public void registerClasses(final Kryo kryo)
    {
        LOGGER.info("Initializing KryoRegister");
        for (final Map.Entry<Class<?>, com.esotericsoftware.kryo.Serializer<?>> entry : SERIALIZERS.entrySet())
        {
            kryo.register(entry.getKey(), entry.getValue());
        }
        kryo.register(CassandraInstance.class, CassandraInstance.SERIALIZER);
        kryo.register(ReplicationFactor.class, ReplicationFactor.SERIALIZER);
        kryo.register(CassandraRing.class, CassandraRing.SERIALIZER);
        kryo.register(TokenPartitioner.class, TokenPartitioner.SERIALIZER);
        kryo.register(Marker.class, Marker.SERIALIZER);
    }
}
