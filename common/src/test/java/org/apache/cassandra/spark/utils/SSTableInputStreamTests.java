package org.apache.cassandra.spark.utils;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang.mutable.MutableInt;
import org.junit.Test;

import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.stats.IStats;
import org.apache.cassandra.spark.utils.streaming.BufferingInputStream;
import org.apache.cassandra.spark.utils.streaming.Source;
import org.apache.cassandra.spark.utils.streaming.CassandraFile;
import org.apache.cassandra.spark.utils.streaming.StreamBuffer;
import org.apache.cassandra.spark.utils.streaming.StreamConsumer;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.utils.streaming.BufferingInputStream.timeoutLeftNanos;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

/**
 * Test the {@link BufferingInputStream} by mocking the {@link Source}.
 */
public class SSTableInputStreamTests
{
    public static final Random RAND = new Random();

    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);
    static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setNameFormat("sstable-tests-%d").setDaemon(true).build());
    static final int DEFAULT_CHUNK_SIZE = 8192;
    @SuppressWarnings("unchecked")
    static final IStats<SSTable> STATS = (IStats<SSTable>) IStats.DO_NOTHING;

    // mocked tests

    @Test
    public void testMockedClient() throws IOException
    {
        runMockedTest(1, 1, DEFAULT_CHUNK_SIZE);
        runMockedTest(1, 5, DEFAULT_CHUNK_SIZE * 5);
        runMockedTest(10, 10, Source.DEFAULT_MAX_BUFFER_SIZE);
        runMockedTest(20, 1024, 33554400L);
        runMockedTest(10, 10, DEFAULT_CHUNK_SIZE * 10);
    }

    private interface SSTableRequest
    {
        void request(long start, long end, StreamConsumer consumer);
    }

    private static Source<SSTable> buildSource(long size, Long maxBufferSize, Long requestChunkSize, SSTableRequest request, Duration duration)
    {
        return new Source<SSTable>()
        {
            public void request(long start, long end, StreamConsumer consumer)
            {
                request.request(start, end, consumer);
            }

            public SSTable file()
            {
                return null;
            }

            public CassandraFile.FileType fileType()
            {
                return null;
            }

            public long size()
            {
                return size;
            }

            public long maxBufferSize()
            {
                return maxBufferSize != null ? maxBufferSize : Source.DEFAULT_MAX_BUFFER_SIZE;
            }

            public long chunkBufferSize()
            {
                return requestChunkSize != null ? requestChunkSize : Source.DEFAULT_CHUNK_BUFFER_SIZE;
            }

            public Duration timeout()
            {
                return duration;
            }
        };
    }

    // test SSTableInputStream using mocked SSTableSource
    private void runMockedTest(final int numRequests, final int chunksPerRequest, final long maxBufferSize) throws IOException
    {
        final long requestChunkSize = (long) DEFAULT_CHUNK_SIZE * chunksPerRequest;
        final long fileSize = requestChunkSize * (long) numRequests;
        final AtomicInteger requestCount = new AtomicInteger(0);
        final Source<SSTable> mockedClient = buildSource(fileSize, maxBufferSize, requestChunkSize, (start, end, consumer) -> {
            requestCount.incrementAndGet();
            writeBuffers(consumer, randomBuffers(chunksPerRequest));
        }, null);
        final BufferingInputStream<SSTable> is = new BufferingInputStream<SSTable>(mockedClient, STATS);
        readStreamFully(is);
        assertEquals(numRequests, requestCount.get());
        assertEquals(0L, is.bytesBuffered());
        assertEquals(fileSize, is.bytesWritten());
        assertEquals(fileSize, is.bytesRead());
    }

    @Test(expected = IOException.class)
    public void testFailure() throws IOException
    {
        final int chunksPerRequest = 10;
        final int numRequests = 10;
        final long len = Source.DEFAULT_CHUNK_BUFFER_SIZE * chunksPerRequest * numRequests;
        final AtomicInteger count = new AtomicInteger(0);
        final Source<SSTable> source = buildSource(len, Source.DEFAULT_MAX_BUFFER_SIZE, Source.DEFAULT_CHUNK_BUFFER_SIZE, (start, end, consumer) -> {
            if (count.incrementAndGet() > (numRequests / 2))
            {
                // half way through throw random exception
                EXECUTOR.submit(() -> consumer.onError(new RuntimeException("Something bad happened...")));
            }
            else
            {
                writeBuffers(consumer, randomBuffers(chunksPerRequest));
            }
        }, null);
        readStreamFully(new BufferingInputStream<SSTable>(source, STATS));
        fail("Should have failed with IOException");
    }

    @Test
    public void testTimeout()
    {
        final long now = System.nanoTime();
        assertEquals(Duration.ofMillis(100).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(1000), now, now - Duration.ofMillis(900).toNanos()));
        assertEquals(Duration.ofMillis(-500).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(1000), now, now - Duration.ofMillis(1500).toNanos()));
        assertEquals(Duration.ofMillis(995).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(1000), now, now - Duration.ofMillis(5).toNanos()));
        assertEquals(Duration.ofMillis(1000).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(1000), now, now - Duration.ofMillis(0).toNanos()));
        assertEquals(Duration.ofMillis(1000).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(1000), now, now + Duration.ofMillis(500).toNanos()));
        assertEquals(Duration.ofMillis(35000).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(60000), now, now - Duration.ofMillis(25000).toNanos()));
        assertEquals(Duration.ofMillis(-5000).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(60000), now, now - Duration.ofMillis(65000).toNanos()));
        assertEquals(Duration.ofMillis(0).toNanos(),
                     timeoutLeftNanos(Duration.ofMillis(60000), now, now - Duration.ofMillis(60000).toNanos()));
    }

    @SuppressWarnings("UnstableApiUsage")
    @Test
    public void testTimeoutShouldAccountForActivityTime()
    {
        final int chunksPerRequest = 10;
        final int numRequests = 10;
        final long len = Source.DEFAULT_CHUNK_BUFFER_SIZE * chunksPerRequest * numRequests;
        final AtomicInteger count = new AtomicInteger(0);
        final Duration timeout = Duration.ofMillis(1000);
        final long startTime = System.nanoTime();
        final long sleepTimeInMillis = 100L;
        final Source<SSTable> source = buildSource(len, Source.DEFAULT_MAX_BUFFER_SIZE, Source.DEFAULT_CHUNK_BUFFER_SIZE, (start, end, consumer) -> {
            // Only respond once so future requests will time out
            if (count.incrementAndGet() == 1)
            {
                EXECUTOR.submit(() -> {
                    Uninterruptibles.sleepUninterruptibly(sleepTimeInMillis, TimeUnit.MILLISECONDS);
                    writeBuffers(consumer, randomBuffers(chunksPerRequest));
                });
            }
        }, timeout);
        try
        {
            readStreamFully(new BufferingInputStream<>(source, STATS));
            fail("Should not reach here, should throw TimeoutException");
        }
        catch (IOException io)
        {
            assertTrue(io.getCause() instanceof TimeoutException);
        }
        Duration duration = Duration.ofNanos(System.nanoTime() - startTime);
        final Duration maxAcceptable = timeout.plus(Duration.ofMillis(sleepTimeInMillis));
        assertTrue("Timeout didn't account for activity time. Took " + duration.toMillis() + "ms should have taken at most " + maxAcceptable.toMillis() + "ms",
                   duration.minus(maxAcceptable).toMillis() < 100);
    }

    @Test
    public void testSkipOnInit() throws IOException
    {
        final int size = 20971520;
        final int chunkSize = 1024;
        final int numChunks = 16;
        final MutableInt bytesRead = new MutableInt(0);
        final MutableInt count = new MutableInt(0);
        final Source<SSTable> source = new Source<SSTable>()
        {
            public void request(long start, long end, StreamConsumer consumer)
            {
                assertNotEquals(0, start);
                final int len = (int) (end - start + 1);
                consumer.onRead(randomBuffer(len));
                bytesRead.add(len);
                count.increment();
                consumer.onEnd();
            }

            public long chunkBufferSize()
            {
                return chunkSize;
            }

            public SSTable file()
            {
                return null;
            }

            public CassandraFile.FileType fileType()
            {
                return CassandraFile.FileType.INDEX;
            }

            public long size()
            {
                return size;
            }

            @Nullable
            public Duration timeout()
            {
                return Duration.ofSeconds(5);
            }
        };

        final int bytesToRead = chunkSize * numChunks;
        final long skipAhead = size - bytesToRead + 1;
        try (final BufferingInputStream<SSTable> stream = new BufferingInputStream<>(source, STATS))
        {
            // skip ahead so we only read the final chunks
            ByteBufUtils.skipFully(stream, skipAhead);
            readStreamFully(stream);
        }
        // verify we only read final chunks and not the start of the file
        assertEquals(bytesToRead, bytesRead.intValue());
        assertEquals(numChunks, count.intValue());
    }

    @Test
    public void testSkipToEnd() throws IOException
    {
        final Source<SSTable> source = new Source<SSTable>()
        {
            public void request(long start, long end, StreamConsumer consumer)
            {
                consumer.onRead(randomBuffer((int) (end - start + 1)));
                consumer.onEnd();
            }

            public SSTable file()
            {
                return null;
            }

            public CassandraFile.FileType fileType()
            {
                return CassandraFile.FileType.INDEX;
            }

            public long size()
            {
                return 20971520;
            }

            @Nullable
            public Duration timeout()
            {
                return Duration.ofSeconds(5);
            }
        };

        try (final BufferingInputStream<SSTable> stream = new BufferingInputStream<>(source, STATS))
        {
            ByteBufUtils.skipFully(stream, 20971520);
            readStreamFully(stream);
        }
    }

    // utils

    private static ImmutableList<StreamBuffer> randomBuffers(final int num)
    {
        return ImmutableList.copyOf(IntStream.range(0, num)
                                             .mapToObj(i -> randomBuffer())
                                             .collect(Collectors.toList()));
    }

    static byte[] randBytes(final int size)
    {
        final byte[] b = new byte[size];
        RAND.nextBytes(b);
        return b;
    }

    private static StreamBuffer randomBuffer()
    {
        return randomBuffer(DEFAULT_CHUNK_SIZE);
    }

    private static StreamBuffer randomBuffer(int size)
    {
        return StreamBuffer.wrap(randBytes(size));
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private static void readStreamFully(final BufferingInputStream<SSTable> inputStream) throws IOException
    {
        try (final BufferingInputStream<SSTable> in = inputStream)
        {
            while (in.read() != -1)
            {
            }
        }
    }

    private static void writeBuffers(final StreamConsumer consumer, final ImmutableList<StreamBuffer> bufs)
    {
        if (bufs.isEmpty())
        {
            // no more buffers so finished
            consumer.onEnd();
            return;
        }

        SCHEDULER.schedule(() -> {
            EXECUTOR.submit(() -> {
                // write next buffer to StreamConsumer
                consumer.onRead(bufs.get(0));
                writeBuffers(consumer, bufs.subList(1, bufs.size()));
            });
        }, RAND.nextInt(50), TimeUnit.MICROSECONDS); // inject random latency
    }
}
