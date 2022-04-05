package org.apache.cassandra.spark.cdc;

import java.util.NoSuchElementException;
import java.util.UUID;

import org.jetbrains.annotations.Nullable;

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

public interface TableIdLookup
{
    /**
     * For CDC, the TableId is serialized in the CommitLog, but we generate a new schema instance in the Spark JVM with a random tableId.
     * We need to convert the tableId used in Spark to match the tableId used in the C* cluster from the CommitLog to table id we use in Spark.
     *
     * @param keyspace keyspace name
     * @param table    table name
     * @return tableId used in the Cassandra cluster as UUID, this tableId should match the tableId serialized in CommitLog.
     *         The return value can be null if the lookup is a no-op.
     * @throws NoSuchElementException if the tableId cannot be found.
     */
    @Nullable
    UUID lookup(String keyspace, String table) throws NoSuchElementException;
}
