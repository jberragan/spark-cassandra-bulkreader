package org.apache.cassandra.spark;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.fourzero.complex.CqlUdt;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoRegistrator;
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
public class KryoRegister implements KryoRegistrator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KryoRegister.class);
    private static final ConcurrentHashMap<Class, com.esotericsoftware.kryo.Serializer> SERIALIZERS = new ConcurrentHashMap<>(4);

    public static <T> void addSerializer(@NotNull final Class<T> type,
                                         @NotNull final com.esotericsoftware.kryo.Serializer serializer)
    {
        LOGGER.info("Registering custom Kryo serializer type={}", type.getName());
        SERIALIZERS.put(type, serializer);
    }

    @Override
    public void registerClasses(final Kryo kryo)
    {
        LOGGER.info("Initializing KryoRegister");
        for (final Map.Entry<Class, com.esotericsoftware.kryo.Serializer> entry : SERIALIZERS.entrySet())
        {
            kryo.register(entry.getKey(), entry.getValue());
        }
        kryo.register(CqlField.class, new CqlField.Serializer());
        kryo.register(CqlSchema.class, new CqlSchema.Serializer());
        kryo.register(CqlUdt.class, new CqlUdt.Serializer());
        kryo.register(LocalDataLayer.class, new LocalDataLayer.Serializer());
        kryo.register(CassandraInstance.class, new CassandraInstance.Serializer());
        kryo.register(ReplicationFactor.class, new ReplicationFactor.Serializer());
        kryo.register(CassandraRing.class, new CassandraRing.Serializer());
        kryo.register(TokenPartitioner.class, new TokenPartitioner.Serializer());
    }

    public static void setup(final SparkConf conf)
    {
        // use KryoSerializer
        LOGGER.info("Setting up Kryo");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        // add KryoRegister to SparkConf serialization if not already there
        final Set<String> currentRegistrators = Arrays.stream(conf.get("spark.kryo.registrator", "").split(",")).filter(StringUtils::isEmpty).collect(Collectors.toSet());
        final String className = KryoRegister.class.getName();
        currentRegistrators.add(className);
        LOGGER.info("Setting kryo registrators: " + String.join(",", currentRegistrators));
        conf.set("spark.kryo.registrator", String.join(",", currentRegistrators));
        conf.registerKryoClasses(new Class<?>[]{ KryoRegister.class });
    }
}
