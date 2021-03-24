package org.apache.cassandra.spark.stats;

import org.apache.cassandra.spark.sparksql.CustomFilter;

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
public abstract class Stats {

    public static class DoNothingStats extends Stats {
        public static final DoNothingStats INSTANCE = new DoNothingStats();
    }

    // spark row iterator

    /**
     * On open SparkRowIterator
     */
    public void openedSparkRowIterator() {

    }

    /**
     * On iterate to next row
     */
    public void nextRow() {

    }

    /**
     * Open closed SparkRowIterator
     *
     * @param timeOpenNanos time SparkRowIterator was open in nanos
     */
    public void closedSparkRowIterator(final long timeOpenNanos) {

    }

    // spark cell iterator

    /**
     * On opened SparkCellIterator
     */
    public void openedSparkCellIterator() {

    }

    /**
     * On iterate to next cell
     */
    public void nextCell() {

    }

    /**
     * SSTableReader skipped partition in SparkCellIterator e.g. because out-of-range
     *
     * @param key   partition key
     * @param token partition key token
     */
    public void skippedPartitionInIterator(ByteBuffer key, BigInteger token) {

    }

    /**
     * On closed SparkCellIterator
     *
     * @param timeOpenNanos time SparkCellIterator was open in nanos
     */
    public void closedSparkCellIterator(final long timeOpenNanos) {

    }

    // CompactionScanner

    /**
     * On opened CompactionScanner
     *
     * @param timeToOpenNanos time to open the CompactionScanner in nanos
     */
    public void openedCompactionScanner(final long timeToOpenNanos) {

    }

    // SSTable Data.db InputStream

    /**
     * On open an input stream on a Data.db file
     */
    public void openedDataInputStream() {

    }

    /**
     * On skip bytes from an input stream on a Data.db file,
     * mostly from SSTableReader skipping out of range partition.
     */
    public void skippedBytes(long len) {

    }

    /**
     * On read bytes from an input stream on a Data.db file
     */
    public void readBytes(int len) {

    }

    /**
     * On decompress bytes from an input stream on a compressed Data.db file
     *
     * @param compressedLen   compressed length in bytes
     * @param decompressedLen compressed length in bytes
     */
    public void decompressedBytes(int compressedLen, int decompressedLen) {

    }

    /**
     * On close an input stream on a Data.db file
     */
    public void closedDataInputStream() {

    }

    // partition push-down filters

    /**
     * Partition key push-down filter skipped SSTable because Filter.db did not contain partition
     */
    public void missingInBloomFilter() {

    }

    /**
     * Partition key push-down filter skipped SSTable because Index.db did not contain partition
     */
    public void missingInIndex() {

    }

    // SSTable filters

    /**
     * SSTableReader skipped SSTable e.g. because not overlaps with Spark worker token range
     *
     * @param filters    list of filters used to filter SSTable
     * @param firstToken sstable first token
     * @param lastToken  sstable last token
     */
    public void skipedSSTable(List<CustomFilter> filters, BigInteger firstToken, BigInteger lastToken) {

    }

    /**
     * SSTableReader skipped partition e.g. because out-of-range
     *
     * @param key   partition key
     * @param token partition key token
     */
    public void skipedPartition(ByteBuffer key, BigInteger token) {

    }

    /**
     * SSTableReader opened an SSTable
     */
    public void openedSSTable() {

    }

    /**
     * SSTableReader closed an SSTable
     *
     * @param timeOpenNanos time in nanoseconds SSTable was open
     */
    public void closedSSTable(long timeOpenNanos) {

    }

}
