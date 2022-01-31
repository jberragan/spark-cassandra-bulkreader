package org.apache.cassandra.spark.reader;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.spark.utils.ByteBufUtils;

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
 * Rid - Row Identifier - contains the partition key, clustering keys and column name that uniquely identifies a row and column of data in Cassandra
 */
public class Rid
{
    private ByteBuffer partitionKey, columnName, value;
    private long columnTimestamp;
    private BigInteger token;
    @VisibleForTesting
    boolean isNewPartition = false;

    // partition key value

    public void setPartitionKeyCopy(final ByteBuffer partitionKeyBytes, final BigInteger token)
    {
        this.partitionKey = partitionKeyBytes;
        this.token = token;
        this.columnName = null;
        this.value = null;
        this.isNewPartition = true;
        this.columnTimestamp = 0L;
    }

    public boolean isNewPartition()
    {
        if (this.isNewPartition)
        {
            this.isNewPartition = false;
            return true;
        }
        return false;
    }

    public ByteBuffer getPartitionKey()
    {
        return this.partitionKey;
    }

    public BigInteger getToken() {
        return this.token;
    }

    // column name containing concatenated clustering keys and column name

    public void setColumnNameCopy(final ByteBuffer columnBytes)
    {
        this.columnName = columnBytes;
    }

    public ByteBuffer getColumnName()
    {
        return this.columnName;
    }

    // value of cell

    public ByteBuffer getValue()
    {
        return this.value;
    }

    public void setValueCopy(final ByteBuffer value)
    {
        this.value = value;
    }

    // timestamp

    public void setColumnTimestamp(final long timestamp)
    {
        this.columnTimestamp = timestamp;
    }

    public long getColumnTimestamp()
    {
        return this.columnTimestamp;
    }

    @Override
    public String toString()
    {
        return ByteBufUtils.toHexString(this.getPartitionKey()) +
               ":" +
               ByteBufUtils.toHexString(this.getColumnName()) +
               ":" +
               ByteBufUtils.toHexString(this.getValue());
    }
}
