package org.apache.cassandra.spark.data.partitioner;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import com.google.common.collect.Range;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.Test;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.IncompleteSSTableException;
import org.apache.cassandra.spark.data.PartitionedDataLayer;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.apache.cassandra.spark.reader.common.SSTableStreamException;
import org.jetbrains.annotations.Nullable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

public class SingleReplicaTests
{
    public static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("replicas-tests-%d").setDaemon(true).build());

    @Test
    public void testOpenSSTables() throws ExecutionException, InterruptedException, IOException
    {
        runTest(false); // missing no files
    }

    @Test
    public void testMissingNonEssentialFiles() throws ExecutionException, InterruptedException, IOException
    {
        runTest(false, DataLayer.FileType.FILTER); // missing non-essential SSTable file component
    }

    @Test
    public void testMissingOnlySummaryFile() throws ExecutionException, InterruptedException, IOException
    {
        // Summary.db can be missing if we can use Index.db
        runTest(false, DataLayer.FileType.SUMMARY);
    }

    @Test
    public void testMissingOnlyIndexFile() throws ExecutionException, InterruptedException, IOException
    {
        // Index.db can be missing if we can use Summary.db
        runTest(false, DataLayer.FileType.INDEX);
    }

    @Test(expected = IOException.class)
    public void testMissingDataFile() throws ExecutionException, InterruptedException, IOException
    {
        runTest(true, DataLayer.FileType.DATA);
    }

    @Test(expected = IOException.class)
    public void testMissingStatisticsFile() throws ExecutionException, InterruptedException, IOException
    {
        runTest(true, DataLayer.FileType.STATISTICS);
    }

    @Test(expected = IOException.class)
    public void testMissingSummaryPrimaryIndex() throws ExecutionException, InterruptedException, IOException
    {
        runTest(true, DataLayer.FileType.SUMMARY, DataLayer.FileType.INDEX);
    }

    @Test(expected = IOException.class)
    public void testFailOpenReader() throws ExecutionException, InterruptedException, IOException
    {
        runTest(true, ssTable -> {
            throw new IOException("Couldn't open Summary.db file");
        }, Range.closed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(8710962479251732707L)));
    }

    @Test
    public void testFilterOverlap() throws ExecutionException, InterruptedException, IOException
    {
        // should not filter out SSTables overlapping with token range
        runTest(false, ssTable -> new Reader(ssTable, BigInteger.valueOf(50), BigInteger.valueOf(150L)), Range.closed(BigInteger.valueOf(0L), BigInteger.valueOf(100L)));
    }

    @Test
    public void testFilterInnerlap() throws ExecutionException, InterruptedException, IOException
    {
        // should not filter out SSTables overlapping with token range
        runTest(false, ssTable -> new Reader(ssTable, BigInteger.valueOf(25), BigInteger.valueOf(75L)), Range.closed(BigInteger.valueOf(0L), BigInteger.valueOf(100L)));
    }

    @Test
    public void testFilterBoundary() throws ExecutionException, InterruptedException, IOException
    {
        // should not filter out SSTables overlapping with token range
        runTest(false, ssTable -> new Reader(ssTable, BigInteger.valueOf(100L), BigInteger.valueOf(102L)), Range.closed(BigInteger.valueOf(0L), BigInteger.valueOf(100L)));
    }

    private static void runTest(final boolean shouldThrowIOException, final DataLayer.FileType... missingFileTypes) throws ExecutionException, InterruptedException, IOException
    {
        runTest(shouldThrowIOException, Reader::new, Range.closed(BigInteger.valueOf(-9223372036854775808L), BigInteger.valueOf(8710962479251732707L)), missingFileTypes);
    }

    private static void runTest(final boolean shouldThrowIOException,
                                final SSTablesSupplier.ReaderOpener<Reader> readerOpener,
                                final Range<BigInteger> range,
                                final DataLayer.FileType... missingFileTypes) throws InterruptedException, IOException, ExecutionException
    {
        final PartitionedDataLayer dataLayer = mock(PartitionedDataLayer.class);
        final CassandraInstance instance = new CassandraInstance("-9223372036854775808", "local1-i1", "DC1");

        final DataLayer.SSTable ssTable1 = mockSSTable();
        final DataLayer.SSTable ssTable2 = mockSSTable();
        final DataLayer.SSTable ssTable3 = mockSSTable();
        for (final DataLayer.FileType fileType : missingFileTypes)
        {
            when(ssTable3.isMissing(eq(fileType))).thenReturn(true); // verify() should throw IncompleteSSTableException when missing Statistic.db file
        }

        final Stream<DataLayer.SSTable> sstables = Stream.of(ssTable1, ssTable2, ssTable3);
        when(dataLayer.listInstance(eq(0), eq(range), eq(instance))).thenReturn(CompletableFuture.completedFuture(sstables));

        final SingleReplica replica = new SingleReplica(instance, dataLayer, range, 0, EXECUTOR);
        final Set<Reader> readers;
        try
        {
            readers = replica.openReplicaAsync(readerOpener).get();
        }
        catch (final ExecutionException e)
        {
            // extract IOException and rethrow if wrapped in SSTableStreamException
            final IOException io = SSTableStreamException.getIOException(e);
            if (io != null)
            {
                throw io;
            }
            throw e;
        }
        if (shouldThrowIOException)
        {
            fail("Should throw IOException because an SSTable is corrupt");
        }
        assertEquals(3, readers.size());
    }

    static DataLayer.SSTable mockSSTable() throws IncompleteSSTableException
    {
        final DataLayer.SSTable ssTable = mock(DataLayer.SSTable.class);
        when(ssTable.isMissing(any(DataLayer.FileType.class))).thenReturn(false);
        doCallRealMethod().when(ssTable).verify();
        return ssTable;
    }

    public static class Reader implements SparkSSTableReader
    {

        final BigInteger firstToken, lastToken;
        final DataLayer.SSTable ssTable;

        Reader(final DataLayer.SSTable ssTable)
        {
            this(ssTable, BigInteger.valueOf(0L), BigInteger.valueOf(1L));
        }

        Reader(final DataLayer.SSTable ssTable, @Nullable final BigInteger firstToken, @Nullable final BigInteger lastToken)
        {
            this.ssTable = ssTable;
            this.firstToken = firstToken;
            this.lastToken = lastToken;
        }

        @Override
        public BigInteger firstToken()
        {
            return this.firstToken;
        }

        @Override
        public BigInteger lastToken()
        {
            return this.lastToken;
        }

        public boolean ignore()
        {
            return false;
        }
    }
}
