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

package org.apache.cassandra.spark.data;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.jetbrains.annotations.NotNull;

public class SparkCqlTable extends CqlTable
{
    private final List<SparkCqlField> partitionKeys, clusteringKeys;

    public SparkCqlTable(@NotNull final String keyspace,
                         @NotNull final String table,
                         @NotNull final String createStmt,
                         @NotNull final ReplicationFactor replicationFactor,
                         @NotNull final List<CqlField> fields)
    {
        this(keyspace, table, createStmt, replicationFactor, fields, Collections.emptySet());
    }

    public SparkCqlTable(@NotNull final String keyspace,
                         @NotNull final String table,
                         @NotNull final String createStmt,
                         @NotNull final ReplicationFactor replicationFactor,
                         @NotNull final List<CqlField> fields,
                         @NotNull final Set<CqlField.CqlUdt> udts)
    {
        super(keyspace, table, createStmt, replicationFactor, fields, udts);
        Preconditions.checkArgument(fields.stream().filter(f -> f instanceof SparkCqlField).count() == fields.size(), "Fields must be decorated as SparkCqlFields");
        this.partitionKeys = SparkCqlField.castToSpark(super.partitionKeys());
        this.clusteringKeys = SparkCqlField.castToSpark(super.clusteringKeys());
    }

    public List<SparkCqlField> sparkFields()
    {
        return SparkCqlField.castToSpark(super.fields());
    }

    public SparkCqlField getSparkField(String columnName)
    {
        return (SparkCqlField) super.getField(columnName);
    }

    public List<SparkCqlField> sparkClusteringKeys()
    {
        return clusteringKeys;
    }

    public List<SparkCqlField> sparkPartitionKeys()
    {
        return partitionKeys;
    }
}
