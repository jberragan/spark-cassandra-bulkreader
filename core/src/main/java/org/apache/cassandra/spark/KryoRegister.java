package org.apache.cassandra.spark;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.SparkCqlField;
import org.apache.cassandra.spark.data.SparkCqlTable;
import org.apache.cassandra.spark.data.fourzero.complex.CqlUdt;
import org.apache.cassandra.spark.sparksql.filters.SerializableCommitLog;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoRegistrator;

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
public class KryoRegister extends BaseKryoRegister implements KryoRegistrator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KryoRegister.class);

    @Override
    public void registerClasses(final Kryo kryo)
    {
        super.registerClasses(kryo);
        kryo.register(CqlField.class, CqlField.SERIALIZER);
        kryo.register(CqlTable.class, CqlTable.SERIALIZER);
        kryo.register(CqlUdt.class, CqlUdt.SERIALIZER);
        new CdcKryoRegister().registerClasses(kryo);
        kryo.register(LocalDataLayer.class, LocalDataLayer.SERIALIZER);
        kryo.register(SerializableCommitLog.class, SerializableCommitLog.SERIALIZER);
        kryo.register(SparkCqlField.class, CqlField.SERIALIZER);
        kryo.register(SparkCqlTable.class, CqlTable.SERIALIZER);
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
