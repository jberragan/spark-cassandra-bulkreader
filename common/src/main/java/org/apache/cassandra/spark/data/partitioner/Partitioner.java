package org.apache.cassandra.spark.data.partitioner;

import java.math.BigInteger;

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
public enum Partitioner
{
    RandomPartitioner(BigInteger.ZERO, BigInteger.valueOf(2).pow(127).subtract(BigInteger.ONE)),
    Murmur3Partitioner(BigInteger.valueOf(2).pow(63).negate(),
                       BigInteger.valueOf(2).pow(63).subtract(BigInteger.ONE));

    private final BigInteger minToken, maxToken;

    Partitioner(final BigInteger minToken, final BigInteger maxToken)
    {
        this.minToken = minToken;
        this.maxToken = maxToken;
    }

    public BigInteger minToken()
    {
        return minToken;
    }

    public BigInteger maxToken()
    {
        return maxToken;
    }

    @Override
    public String toString()
    {
        return "org.apache.cassandra.dht." + super.toString();
    }
}
