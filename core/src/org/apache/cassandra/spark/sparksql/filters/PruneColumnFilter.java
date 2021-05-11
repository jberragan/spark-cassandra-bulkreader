package org.apache.cassandra.spark.sparksql.filters;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Set;

import com.google.common.collect.Range;

import org.apache.cassandra.spark.reader.SparkSSTableReader;
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
 * Prune column push-down filter to skip reading columns that are not needed.
 */
public class PruneColumnFilter implements CustomFilter
{
    private final Set<String> requiredColumns;

    public PruneColumnFilter(@NotNull final Set<String> requiredColumns)
    {
        this.requiredColumns = requiredColumns;
    }

    public Set<String> requiredColumns() {
        return requiredColumns;
    }

    public int size() {
        return requiredColumns.size();
    }

    @Override
    public boolean includeColumn(String columnName)
    {
        return requiredColumns.contains(columnName);
    }

    @Override
    public boolean canFilterByColumn() {
        return true;
    }

    @Override
    public boolean overlaps(Range<BigInteger> tokenRange)
    {
        return false;
    }

    @Override
    public boolean skipPartition(ByteBuffer key, BigInteger token)
    {
        return false;
    }

    @Override
    public boolean canFilterByKey()
    {
        return false;
    }

    @Override
    public boolean filter(ByteBuffer key)
    {
        return true;
    }

    @Override
    public boolean filter(SparkSSTableReader reader)
    {
        return true;
    }
}
