package org.apache.cassandra.spark.cdc;

import java.nio.ByteBuffer;

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

public abstract class ValueWithMetadata
{
    public final String columnName;
    public final String columnType;
    private final ByteBuffer value;

    public ValueWithMetadata(String columnName, String columnType, ByteBuffer value)
    {
        this.columnName = columnName;
        this.columnType = columnType;
        this.value = value;
    }

    /**
     * @return the value as byte array
     */
    public byte[] getBytes()
    {
        // if bb is null, we should return null; Null means deletion
        if (value == null)
        {
            return null;
        }
        return ByteBufUtils.getArray(value);
    }

    /**
     * @return the duplicated {@link ByteBuffer} of the value
     */
    public ByteBuffer getValue()
    {
        if (value == null)
        {
            return null;
        }
        return value.duplicate();
    }
}