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

package org.apache.cassandra.spark.stats;

import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.utils.streaming.SSTableSource;

public interface IStats
{
    IStats DO_NOTHING = new IStats()
    {
    };

    default void inputStreamEnd(SSTableSource<? extends SSTable> ssTable, long runTimeNanos, long totalNanosBlocked)
    {

    }

    default void inputStreamEndBuffer(SSTableSource<? extends SSTable> ssTable)
    {

    }

    default void inputStreamTimeBlocked(SSTableSource<? extends SSTable> ssTable, long nanos)
    {

    }

    default void inputStreamByteRead(SSTableSource<? extends SSTable> ssTable, int len, int queueSize, int percentComplete)
    {

    }

    default void inputStreamFailure(SSTableSource<? extends SSTable> ssTable, Throwable t)
    {

    }

    default void inputStreamBytesWritten(SSTableSource<? extends SSTable> ssTable, int len)
    {

    }

    default void inputStreamBytesSkipped(SSTableSource<? extends SSTable> ssTable, long bufferedSkipped, long rangeSkipped)
    {

    }
}
