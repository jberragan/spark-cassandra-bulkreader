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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.cassandra.spark.data.SparkCqlField;
import org.jetbrains.annotations.NotNull;

public class Inet extends org.apache.cassandra.spark.data.fourzero.types.Inet implements SparkCqlField.BinaryTraits
{
    public static Inet INSTANCE = new Inet();

    @Override
    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return ((InetAddress) o).getAddress(); // byte[]
    }

    @Override
    public Object toTestRowType(Object value)
    {
        try
        {
            return InetAddress.getByAddress((byte[]) value);
        }
        catch (final UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }
}
