package org.apache.cassandra.spark.data.partitioner;

import java.math.BigInteger;

import com.google.common.collect.Range;

import org.jetbrains.annotations.NotNull;
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


public class NotEnoughReplicasException extends RuntimeException
{
    NotEnoughReplicasException(final String msg)
    {
        super(msg);
    }

    public NotEnoughReplicasException(@NotNull final ConsistencyLevel consistencyLevel,
                                      @NotNull final Range<BigInteger> range,
                                      final int minRequired,
                                      final int numInstances,
                                      @Nullable final String dc)
    {
        super(String.format("Insufficient replicas found to achieve consistency level %s for token range %s - %s, required %d but only %d found, dc=%s", consistencyLevel.name(), range.lowerEndpoint(), range.upperEndpoint(), minRequired, numInstances, dc));
    }

    static boolean isNotEnoughReplicasException(@Nullable final Throwable throwable)
    {
        Throwable t = throwable;
        while (t != null)
        {
            if (t instanceof NotEnoughReplicasException)
            {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
