package org.apache.cassandra.spark.reader;

import java.math.BigInteger;

import com.google.common.collect.Range;

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
public interface SparkSSTableReader
{
    BigInteger firstToken();

    BigInteger lastToken();

    default Range<BigInteger> range()
    {
        return Range.closed(firstToken(), lastToken());
    }

    /**
     * @return true if this sstable should not be read as part of this Spark partition
     */
    boolean ignore();

    /**
     * @param reader sstable reader
     * @param range  token range
     * @return true if SSTable reader overlaps with a given token range
     */
    public static boolean overlaps(final SparkSSTableReader reader, final Range<BigInteger> range)
    {
        return range.isConnected(reader.range());
    }
}
