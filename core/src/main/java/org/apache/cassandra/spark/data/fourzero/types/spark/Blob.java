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

package org.apache.cassandra.spark.data.fourzero.types.spark;

import java.nio.ByteBuffer;

import org.apache.cassandra.spark.data.SparkCqlField;
import org.apache.cassandra.spark.utils.ByteBufUtils;
import org.jetbrains.annotations.NotNull;

public class Blob extends org.apache.cassandra.spark.data.fourzero.types.Blob implements SparkCqlField.BinaryTraits
{
    public static final Blob INSTANCE = new Blob();

    @Override
    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return ByteBufUtils.getArray((ByteBuffer) o); // byte[]
    }

    @Override
    public Object toTestRowType(Object value)
    {
        return ByteBuffer.wrap((byte[]) value);
    }
}
