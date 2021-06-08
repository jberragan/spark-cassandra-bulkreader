package org.apache.cassandra.spark.sparksql;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

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

@SuppressWarnings("unused")
public class LocalDataSource extends CassandraDataSource
{
    @Override
    public String shortName()
    {
        return "localsstabledatasource";
    }

    @Override
    public DataLayer getDataLayer(final DataSourceOptions options)
    {
        return new LocalDataLayer(
        CassandraBridge.CassandraVersion.valueOf(options.get("version").orElse(CassandraBridge.CassandraVersion.THREEZERO.toString())),
        Partitioner.valueOf(options.get("partitioner").orElse(Partitioner.Murmur3Partitioner.name())),
        options.get("keyspace").orElseThrow(() -> new RuntimeException("No keyspace specified")),
        options.get("createStmt").orElseThrow(() -> new RuntimeException("No createStmt specified")),
        options.getBoolean("addLastModifiedTimestampColumn", false),
        options.get("udts").map(s -> s.split("\n")).map(s -> Arrays.stream(s).filter(StringUtils::isNotEmpty).collect(Collectors.toSet())).orElse(Collections.emptySet()),
        options.getBoolean("useSSTableInputStream", false),
        options.get("dirs").map(m -> m.split(",")).orElseThrow(() -> new RuntimeException("No paths specified"))
        );
    }
}
