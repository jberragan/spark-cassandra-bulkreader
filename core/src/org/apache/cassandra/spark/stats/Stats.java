package org.apache.cassandra.spark.stats;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.CustomFilter;
import org.apache.cassandra.spark.utils.streaming.SSTableSource;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;

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

public abstract class Stats
{

    public static class DoNothingStats extends Stats
    {
        public static final DoNothingStats INSTANCE = new DoNothingStats();
    }

    // spark row iterator

    /**
     * On open SparkRowIterator
     */
    public void openedSparkRowIterator()
    {

    }

    /**
     * On iterate to next row
     */
    public void nextRow()
    {

    }

    /**
     * Open closed SparkRowIterator
     *
     * @param timeOpenNanos time SparkRowIterator was open in nanos
     */
    public void closedSparkRowIterator(final long timeOpenNanos)
    {

    }

    // spark cell iterator

    /**
     * On opened SparkCellIterator
     */
    public void openedSparkCellIterator()
    {

    }

    /**
     * On iterate to next cell
     */
    public void nextCell()
    {

    }

    /**
     * SSTableReader skipped partition in SparkCellIterator e.g. because out-of-range
     *
     * @param key   partition key
     * @param token partition key token
     */
    public void skippedPartitionInIterator(ByteBuffer key, BigInteger token)
    {

    }

    /**
     * On closed SparkCellIterator
     *
     * @param timeOpenNanos time SparkCellIterator was open in nanos
     */
    public void closedSparkCellIterator(final long timeOpenNanos)
    {

    }

    // CompactionScanner

    /**
     * On opened CompactionScanner
     *
     * @param timeToOpenNanos time to open the CompactionScanner in nanos
     */
    public void openedCompactionScanner(final long timeToOpenNanos)
    {

    }

    // SSTable Data.db InputStream

    /**
     * On open an input stream on a Data.db file
     */
    public void openedDataInputStream()
    {

    }

    /**
     * On skip bytes from an input stream on a Data.db file,
     * mostly from SSTableReader skipping out of range partition.
     */
    public void skippedBytes(long len)
    {

    }

    /**
     * On read bytes from an input stream on a Data.db file
     */
    public void readBytes(int len)
    {

    }

    /**
     * On decompress bytes from an input stream on a compressed Data.db file
     *
     * @param compressedLen   compressed length in bytes
     * @param decompressedLen compressed length in bytes
     */
    public void decompressedBytes(int compressedLen, int decompressedLen)
    {

    }

    /**
     * On close an input stream on a Data.db file
     */
    public void closedDataInputStream()
    {

    }

    // partition push-down filters

    /**
     * Partition key push-down filter skipped SSTable because Filter.db did not contain partition
     */
    public void missingInBloomFilter()
    {

    }

    /**
     * Partition key push-down filter skipped SSTable because Index.db did not contain partition
     */
    public void missingInIndex()
    {

    }

    // SSTable filters

    /**
     * SSTableReader skipped SSTable e.g. because not overlaps with Spark worker token range
     *
     * @param filters    list of filters used to filter SSTable
     * @param firstToken sstable first token
     * @param lastToken  sstable last token
     */
    public void skippedSSTable(List<CustomFilter> filters, BigInteger firstToken, BigInteger lastToken)
    {

    }

    /**
     * SSTableReader skipped partition e.g. because out-of-range
     *
     * @param key   partition key
     * @param token partition key token
     */
    public void skippedPartition(ByteBuffer key, BigInteger token)
    {

    }

    /**
     * SSTableReader opened an SSTable
     */
    public void openedSSTable()
    {

    }

    /**
     * SSTableReader closed an SSTable
     *
     * @param timeOpenNanos time in nanoseconds SSTable was open
     */
    public void closedSSTable(long timeOpenNanos)
    {

    }

    // sstable input stream

    /**
     * When {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream} queue is full, usually indicating
     * job is CPU-bound and blocked on the CompactionIterator.
     *
     * @param ssTable the sstable source for this input stream
     */
    public void inputStreamQueueFull(SSTableSource<? extends DataLayer.SSTable> ssTable)
    {

    }

    /**
     * Failure occurred in the {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream}.
     *
     * @param ssTable the sstable source for this input stream
     * @param t       throwable
     */
    public void inputStreamFailure(SSTableSource<? extends DataLayer.SSTable> ssTable, Throwable t)
    {

    }

    /**
     * Time the {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream} spent blocking on queue waiting for bytes.
     * High time spent blocking indicates the job is network-bound, or blocked on the {@link org.apache.cassandra.spark.utils.streaming.SSTableSource} to
     * supply the bytes.
     *
     * @param ssTable the sstable source for this input stream
     * @param nanos   time in nanoseconds.
     */
    public void inputStreamTimeBlocked(SSTableSource<? extends DataLayer.SSTable> ssTable, long nanos)
    {

    }

    /**
     * Bytes written to {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream} by the {@link org.apache.cassandra.spark.utils.streaming.SSTableSource}.
     *
     * @param ssTable the sstable source for this input stream
     * @param len     number of bytes written
     */
    public void inputStreamBytesWritten(SSTableSource<? extends DataLayer.SSTable> ssTable, int len)
    {

    }

    /**
     * Bytes read from {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream}.
     *
     * @param ssTable         the sstable source for this input stream
     * @param len             number of bytes read
     * @param queueSize       current queue size
     * @param percentComplete % completion
     */
    public void inputStreamByteRead(SSTableSource<? extends DataLayer.SSTable> ssTable, int len, int queueSize, int percentComplete)
    {

    }

    /**
     * {@link org.apache.cassandra.spark.utils.streaming.SSTableSource} has finished writing to {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream}
     * after reaching expected file length.
     *
     * @param ssTable the sstable source for this input stream
     */
    public void inputStreamEndBuffer(SSTableSource<? extends DataLayer.SSTable> ssTable)
    {

    }

    /**
     * {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream} finished and closed.
     *
     * @param ssTable             the sstable source for this input stream
     * @param runTimeNanos        total time open in nanoseconds.
     * @param totalNanosBlocked   total time blocked on queue waiting for bytes in nanoseconds
     * @param totalBytes          total bytes read by the InputStream, should equal expected file length
     * @param totalQueueFullCount total count of the number of times the queue was full, and the {@link org.apache.cassandra.spark.utils.streaming.SSTableInputStream} needed to wait for the queue to drain before requesting more bytes.
     */
    public void inputStreamEnd(SSTableSource<? extends DataLayer.SSTable> ssTable, long runTimeNanos, long totalNanosBlocked, long totalBytes, long totalQueueFullCount)
    {

    }

    /**
     * Called when the InputStream skips bytes.
     *
     * @param ssTable              the sstable source for this input stream
     * @param bufferedSkipped the number of bytes already buffered in memory skipped
     * @param rangeSkipped    the number of bytes skipped by efficiently incrementing the start range for the next request
     */
    public void inputStreamBytesSkipped(SSTableSource<? extends DataLayer.SSTable> ssTable, long bufferedSkipped, long rangeSkipped)
    {

    }
}
