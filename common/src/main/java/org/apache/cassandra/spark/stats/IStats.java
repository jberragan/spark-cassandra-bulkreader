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

import org.apache.cassandra.spark.utils.streaming.Source;
import org.apache.cassandra.spark.utils.streaming.CassandraFile;

public interface IStats<FileType extends CassandraFile>
{
    IStats<?> DO_NOTHING = new IStats<CassandraFile>()
    {
    };

    default void inputStreamEnd(Source<FileType> source, long runTimeNanos, long totalNanosBlocked)
    {

    }

    default void inputStreamEndBuffer(Source<FileType> ssTable)
    {

    }

    default void inputStreamTimeBlocked(Source<FileType> source, long nanos)
    {

    }

    default void inputStreamByteRead(Source<FileType> source, int len, int queueSize, int percentComplete)
    {

    }

    default void inputStreamFailure(Source<FileType> source, Throwable t)
    {

    }

    default void inputStreamBytesWritten(Source<FileType> ssTable, int len)
    {

    }

    default void inputStreamBytesSkipped(Source<FileType> source, long bufferedSkipped, long rangeSkipped)
    {

    }
}
