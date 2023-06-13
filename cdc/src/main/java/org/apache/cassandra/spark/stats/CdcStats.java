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

public class CdcStats implements IStats
{
    public static class DoNothingCdcStats extends CdcStats
    {
        public static final DoNothingCdcStats INSTANCE = new DoNothingCdcStats();
    }

    // SSTableInputStream

    public void inputStreamEnd(SSTableSource<? extends SSTable> ssTable, long runTimeNanos, long totalNanosBlocked)
    {

    }

    public void inputStreamEndBuffer(SSTableSource<? extends SSTable> ssTable)
    {

    }

    public void inputStreamTimeBlocked(SSTableSource<? extends SSTable> ssTable, long nanos)
    {

    }

    public void inputStreamByteRead(SSTableSource<? extends SSTable> ssTable, int len, int queueSize, int percentComplete)
    {

    }

    public void inputStreamFailure(SSTableSource<? extends SSTable> ssTable, Throwable t)
    {

    }

    public void inputStreamBytesWritten(SSTableSource<? extends SSTable> ssTable, int len)
    {

    }

    public void inputStreamBytesSkipped(SSTableSource<? extends SSTable> ssTable, long bufferedSkipped, long rangeSkipped)
    {

    }

    // CDC Stats

    /**
     * Difference between the time change was created and time the same was read by a spark worker
     *
     * @param latency time difference, in milli secs
     */
    public void changeReceived(String keyspace, String table, long latency)
    {
    }

    /**
     * Difference between the time change was created and time the same produced as a spark row
     *
     * @param latency time difference, in milli secs
     */
    public void changeProduced(String keyspace, String table, long latency)
    {
    }

    /**
     * Report the change is published within a single batch.
     *
     * @param keyspace
     * @param table
     */
    public void changePublished(String keyspace, String table)
    {
    }

    /**
     * Report the late change on publishing. A late change is one spans across multiple batches before publishing
     *
     * @param keyspace
     * @param table
     */
    public void lateChangePublished(String keyspace, String table)
    {
    }

    /**
     * Report the late change on dropping. Cdc worker eventually dropps the unpublished change if it is too old.
     *
     * @param maxTimestampMicros timestamp in microseconds.
     */
    public void lateChangeDropped(String keyspace, String table, long maxTimestampMicros)
    {
    }

    /**
     * Report the change that is not tracked and ignored
     *
     * @param incrCount delta value to add to the count
     */
    public void untrackedChangesIgnored(String keyspace, String table, long incrCount)
    {
    }

    /**
     * Report the change that is out of the token range
     *
     * @param incrCount delta value to add to the count
     */
    public void outOfTokenRangeChangesIgnored(String keyspace, String table, long incrCount)
    {
    }

    /**
     * Report the update that has insufficient replicas to deduplicate
     *
     * @param keyspace
     * @param table
     */
    public void insufficientReplicas(String keyspace, String table)
    {
    }

    /**
     * Number of successfully read mutations
     *
     * @param incrCount delta value to add to the count
     */
    public void mutationsReadCount(long incrCount)
    {
    }

    /**
     * Deserialized size of a successfully read mutation
     *
     * @param nBytes mutation size in bytes
     */
    public void mutationsReadBytes(long nBytes)
    {
    }

    /**
     * Called when received a mutation with unknown table
     *
     * @param incrCount delta value to add to the count
     */
    public void mutationsIgnoredUnknownTableCount(long incrCount)
    {
    }

    /**
     * Called when deserialization of a mutation fails
     *
     * @param incrCount delta value to add to the count
     */
    public void mutationsDeserializeFailedCount(long incrCount)
    {
    }

    /**
     * Called when a mutation's checksum calculation fails or doesn't match with expected checksum
     *
     * @param incrCount delta value to add to the count
     */
    public void mutationsChecksumMismatchCount(long incrCount)
    {
    }

    /**
     * Time taken to read a commit log file
     *
     * @param timeTaken time taken, in nano secs
     */
    public void commitLogReadTime(long timeTaken)
    {
    }

    /**
     * Number of mutations read by a micro batch
     *
     * @param count mutations count
     */
    public void mutationsReadPerBatch(long count)
    {
    }

    /**
     * Time taken by a micro batch, i.e, to read commit log files of a batch
     *
     * @param timeTaken time taken, in nano secs
     */
    public void mutationsBatchReadTime(long timeTaken)
    {
    }

    /**
     * Time taken to aggregate and filter mutations.
     *
     * @param timeTakenNanos time taken in nanoseconds
     */
    public void mutationsFilterTime(long timeTakenNanos)
    {
    }

    /**
     * Number of unexpected commit log EOF occurrences
     *
     * @param incrCount delta value to add to the count
     */
    public void commitLogSegmentUnexpectedEndErrorCount(long incrCount)
    {
    }

    /**
     * Number of invalid mutation size occurrences
     *
     * @param incrCount delta value to add to the count
     */
    public void commitLogInvalidSizeMutationCount(long incrCount)
    {
    }

    /**
     * Number of IO exceptions seen while reading commit log header
     *
     * @param incrCount delta value to add to the count
     */
    public void commitLogHeaderReadFailureCount(long incrCount)
    {
    }

    /**
     * Time taken to read a commit log's header
     *
     * @param timeTaken time taken, in nano secs
     */
    public void commitLogHeaderReadTime(long timeTaken)
    {
    }

    /**
     * Time taken to read a commit log's segment/section
     *
     * @param timeTaken time taken, in nano secs
     */
    public void commitLogSegmentReadTime(long timeTaken)
    {
    }

    /**
     * Number of commit logs skipped
     *
     * @param incrCount delta value to add to the count
     */
    public void skippedCommitLogsCount(long incrCount)
    {
    }

    /**
     * Number of bytes skipped/seeked when reading the commit log
     *
     * @param nBytes number of bytes
     */
    public void commitLogBytesSkippedOnRead(long nBytes)
    {
    }

    /**
     * Number of commit log bytes fetched
     *
     * @param nBytes number of bytes
     */
    public void commitLogBytesFetched(long nBytes)
    {
    }

    /**
     * Number of sub batches in a micro batch
     *
     * @param incrCount delta value to add to the count
     */
    public void subBatchesPerMicroBatchCount(long incrCount)
    {
    }

    /**
     * Number of mutations read by a sub batch of a micro batch
     *
     * @param count mutations count
     */
    public void mutationsReadPerSubMicroBatch(long count)
    {
    }
}
