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

package org.apache.cassandra.spark.reader.fourzero;

import java.math.BigInteger;

import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.dht.Token;

public class BaseFourZeroUtils
{
    public static BigInteger tokenToBigInteger(final Token token)
    {
        if (token instanceof Murmur3Partitioner.LongToken)
        {
            return BigInteger.valueOf((long) token.getTokenValue());
        }
        if (token instanceof RandomPartitioner.BigIntegerToken)
        {
            return ((RandomPartitioner.BigIntegerToken) token).getTokenValue();
        }

        throw new UnsupportedOperationException("Unexpected token type: " + token.getClass().getName());
    }

    public static long tokenToLong(final Token token)
    {
        if (token instanceof Murmur3Partitioner.LongToken)
        {
            return (long) token.getTokenValue();
        }
        if (token instanceof RandomPartitioner.BigIntegerToken)
        {
            return ((RandomPartitioner.BigIntegerToken) token).getTokenValue().longValue();
        }

        throw new UnsupportedOperationException("Unexpected token type: " + token.getClass().getName());
    }
}
