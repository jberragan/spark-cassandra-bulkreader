package org.apache.cassandra.spark.utils.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.stats.Stats;
import org.jetbrains.annotations.NotNull;

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
 * The InputStream into the CompactionIterator needs to be a blocking {@link java.io.InputStream},
 * but we don't want to block on network calls, or buffer too much data in memory otherwise we will hit OOMs for large Data.db files.
 * <p>
 * This helper class uses the {@link SSTableSource} implementation provided to asynchronously read
 * the SSTable bytes on-demand, managing flow control if not ready for more bytes and buffering enough without reading entirely into memory.
 * <p>
 * The generic {@link SSTableSource} allows users to pass in their own implementations to read from any source.
 * <p>
 * This enables the Bulk Reader library to scale to read many SSTables without OOMing, and controls the flow by
 * buffering more bytes on-demand as the data is drained.
 * <p>
 * This class expects the consumer thread to be single-threaded, and the producer thread to be single-threaded OR serialized to ensure ordering of events.
 */
@SuppressWarnings({ "WeakerAccess", "unused" })
public class SSTableInputStream<SSTable extends DataLayer.SSTable> extends InputStream implements StreamConsumer
{
    public static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("is-timeout").setDaemon(true).build());
    private static final StreamBuffer.ByteArrayWrapper END_BUFFER = StreamBuffer.wrap(new byte[0]);
    private static final StreamBuffer.ByteArrayWrapper END_BUFFER_WITH_ERROR = StreamBuffer.wrap(new byte[0]);

    private enum StreamState
    {Init, Reading, NextBuffer, End, Closed}

    private final BlockingQueue<StreamBuffer> queue;
    private final SSTableSource<SSTable> source;
    private final Stats stats;
    private final CompletableFuture<Void> startFuture = new CompletableFuture<>();
    private final long startTimeNanos;

    // variables accessed by both producer and consumer thread so must be volatile or atomic
    private volatile Throwable throwable = null;
    private volatile long lastActivityNanos = System.nanoTime();
    private final AtomicLong bytesWritten = new AtomicLong(0L), bytesRead = new AtomicLong(0L), rangeStart = new AtomicLong(0L);
    private final AtomicInteger queueFullCount = new AtomicInteger(0);
    private final AtomicBoolean paused = new AtomicBoolean(false);

    // variables only used by the InputStream consumer thread so do not need to be volatile
    private StreamState state = StreamState.Init;
    private StreamBuffer currentBuffer = null;
    private int pos, len;
    private long timeBlockedNanos = 0L;

    /**
     * @param source SSTableSource to async provide the bytes after {@link SSTableSource#request(long, long, StreamConsumer)} is called.
     * @param stats  {@link Stats} implementation for recording instrumentation.
     */
    public SSTableInputStream(SSTableSource<SSTable> source, Stats stats)
    {
        this.source = source;
        this.queue = new LinkedBlockingQueue<>();
        this.startTimeNanos = System.nanoTime();
        this.stats = stats;
        start();
    }

    private void start()
    {
        requestMore();
        scheduleTimeout();
        state = StreamState.NextBuffer;
    }

    public long startTimeNanos()
    {
        return startTimeNanos;
    }

    public long timeBlockedNanos()
    {
        return timeBlockedNanos;
    }

    public long rangeStart()
    {
        return rangeStart.get();
    }

    public long bytesRead()
    {
        return bytesRead.get();
    }

    public long bytesWritten()
    {
        return bytesWritten.get();
    }

    /**
     * @return future that completes when the first request for bytes has completed and buffered.
     */
    public CompletableFuture<Void> startFuture()
    {
        return startFuture;
    }

    public long bytesBuffered()
    {
        return bytesWritten() - bytesRead();
    }

    public boolean isFinished()
    {
        return rangeStart() >= source.size();
    }

    private boolean isClosed()
    {
        return state == StreamState.Closed;
    }

    /**
     * Resume requesting more bytes if paused and queue is no longer full.
     */
    private void resumeIfQueueDrained()
    {
        while (tryUnpause())
        {
            // successfully unpaused, so resume requesting more bytes
            requestMore();
        }
    }

    /**
     * Queue is full so pause requesting more bytes until the queue is drained.
     * This is only ever be called by the producer thread so doesn't need CAS operation.
     */
    private void pause()
    {
        paused.set(true);
    }

    /**
     * Try to unpause if queue is now drained, can be called by producer and consumer thread
     * so CAS operation ensures only 1 thread unpauses.
     *
     * @return true if:
     * 1. the stream is currently paused (so not requesting more bytes),
     * 2. and the queue has since been drained,
     * 3. and this request wins the CAS to unpause so can continue requesting more bytes.
     */
    private boolean tryUnpause()
    {
        return paused.get() && // stream is currently paused
               !isQueueFull() && // the queue is drained/no longer full
               paused.compareAndSet(true, false); // this request wins the CAS to unpause
    }

    /**
     * Request more bytes using {@link SSTableSource#request(long, long, StreamConsumer)} for the next range.
     */
    private void requestMore()
    {
        if (isClosed())
        {
            return;
        }

        final long chunkSize = source.chunkBufferSize();
        final long start = rangeStart();
        final long end = Math.min(source.size(), start + chunkSize);
        if (end >= start)
        {
            rangeStart.addAndGet(chunkSize);
            source.request(start, end, this);
        }
        else
        {
            throw new IllegalStateException(String.format("Tried to request invalid range start=%d end=%d", start, end));
        }
    }

    /**
     * The number of bytes buffered is greater than or equal to {@link SSTableSource#maxBufferSize()}
     * so wait for queue to drain before requesting more.
     *
     * @return true if queue is full
     */
    public boolean isQueueFull()
    {
        if (bytesBuffered() >= source.maxBufferSize() || queue.remainingCapacity() == 0)
        {
            stats.inputStreamQueueFull(source);
            queueFullCount.incrementAndGet();
            return true;
        }
        return false;
    }

    // timeout

    /**
     * @param timeout           duration timeout
     * @param nowNanos          current time now in nanoseconds
     * @param lastActivityNanos last activity time in nanoseconds
     * @return the timeout remaining in nanoseconds, or less than or equal to 0 if timeout already expired
     */
    public static long timeoutLeftNanos(Duration timeout, long nowNanos, long lastActivityNanos)
    {
        return Math.min(timeout.toNanos(), timeout.toNanos() - (nowNanos - lastActivityNanos));
    }

    private void scheduleTimeout()
    {
        final Duration timeout = source.timeout();
        if (timeout == null)
        {
            // timeout disabled
            return;
        }

        final long timeoutNanos = timeoutLeftNanos(timeout, System.nanoTime(), lastActivityNanos);
        if (timeoutNanos <= 0)
        {
            timeoutError(timeout);
            return;
        }

        SSTableInputStream.SCHEDULER.schedule(() -> {
            if (isClosed() || isFinished())
            {
                return;
            }

            if (state == StreamState.Init || System.nanoTime() - lastActivityNanos <= timeoutNanos)
            {
                // still making progress so schedule another timeout
                scheduleTimeout();
            }
            else
            {
                timeoutError(timeout);
            }
        }, timeoutNanos, TimeUnit.NANOSECONDS);
    }

    private void timeoutError(Duration timeout)
    {
        onError(new TimeoutException(String.format("No activity on SSTableInputStream for %d seconds", timeout.getSeconds())));
    }

    public void touchTimeout()
    {
        lastActivityNanos = System.nanoTime();
    }

    /**
     * {@link StreamConsumer} method implementations.
     */

    @Override
    public void onRead(StreamBuffer buffer)
    {
        final int len = buffer.readableBytes();
        if (len <= 0)
        {
            return;
        }
        queue.add(buffer);
        bytesWritten.addAndGet(len);
        stats.inputStreamBytesWritten(source, len);
        touchTimeout();
    }

    @Override
    public void onEnd()
    {
        if (isFinished())
        {
            // reached the end
            queue.add(END_BUFFER);
            stats.inputStreamEndBuffer(source);
        }
        else
        {
            if (isQueueFull())
            {
                pause();
                resumeIfQueueDrained(); // handle possible race where queue drained since calling isQueueFull
            }
            else
            {
                // fire off next request
                requestMore();
            }
        }
        if (!startFuture.isDone())
        {
            startFuture.complete(null);
        }
        touchTimeout();
    }

    @Override
    public void onError(@NotNull final Throwable t)
    {
        final Throwable cause = t.getCause() != null ? t.getCause() : t;
        throwable = cause;
        queue.add(END_BUFFER_WITH_ERROR);
        stats.inputStreamFailure(source, t);
        startFuture.completeExceptionally(cause);
    }

    /**
     * {@link java.io.InputStream} method implementations.
     */

    @Override
    public int available()
    {
        return Math.toIntExact(bytesBuffered() - pos);
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    // copied from JDK11 jdk.internal.util.Preconditions.checkFromIndexSize()
    private static <X extends RuntimeException> void checkFromIndexSize(int fromIndex, int size, int length)
    {
        if ((length | fromIndex | size) < 0 || size > length - fromIndex)
        {
            throw new IndexOutOfBoundsException(String.format("Index out of bounds fromIndex=%d, size=%d, length=%d", fromIndex, size, length));
        }
    }

    @Override
    public int read(byte[] b, int off, int length) throws IOException
    {
        SSTableInputStream.checkFromIndexSize(off, length, b.length);
        if (length == 0)
        {
            return 0;
        }

        if (checkState() < 0)
        {
            return -1;
        }

        final int readLen = Math.min(len - pos, length);
        currentBuffer.getBytes(pos, b, off, readLen);
        pos += readLen;
        maybeReleaseBuffer();
        return readLen;
    }

    @Override
    public int read() throws IOException
    {
        if (checkState() < 0)
        {
            return -1;
        }
        // convert to unsigned byte
        final int b = currentBuffer.getByte(pos++) & 0xFF;
        maybeReleaseBuffer();
        return b;
    }

    @Override
    public void close()
    {
        if (state == StreamState.Closed)
        {
            return;
        }
        else if (state != StreamState.End)
        {
            end();
        }
        state = StreamState.Closed;
        releaseBuffer();
        queue.clear();
    }

    @Override
    public void reset() throws IOException
    {
        throw new IOException("reset not supported");
    }

    // internal methods for {@link java.io.InputStream}

    /**
     * If pos >= len, we have finished with this {@link SSTableInputStream#currentBuffer} so release and
     * move to the State {@link StreamState#NextBuffer} so next buffer is popped from the {@link LinkedBlockingQueue}
     * when {@link InputStream#read()} or {@link InputStream#read(byte[], int, int)} is next called.
     */
    private void maybeReleaseBuffer()
    {
        if (pos < len)
        {
            // still bytes remaining in the currentBuffer so keep reading
            return;
        }

        bytesRead.addAndGet(pos);
        releaseBuffer();
        state = StreamState.NextBuffer;
        touchTimeout();
        resumeIfQueueDrained();
        stats.inputStreamByteRead(source, pos, queue.size(), (int) (pos * 100.0 / (double) source.size()));
    }

    /**
     * Release current buffer.
     */
    private void releaseBuffer()
    {
        if (currentBuffer != null)
        {
            currentBuffer.release();
            currentBuffer = null;
        }
    }

    /**
     * Pop next buffer from the queue, block on {@link LinkedBlockingQueue} until bytes are available.
     *
     * @throws IOException exception on error
     */
    private void nextBuffer() throws IOException
    {
        final long startNanos = System.nanoTime();
        try
        {
            // block on queue until next buffer available
            currentBuffer = queue.take();
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        final long nanosBlocked = System.nanoTime() - startNanos;
        timeBlockedNanos += nanosBlocked; // measure time spent blocking for monitoring
        stats.inputStreamTimeBlocked(source, nanosBlocked);

        len = currentBuffer.readableBytes();
        state = StreamState.Reading;
        pos = 0;
        if (currentBuffer == null)
        {
            throw new IOException("Obtained a null buffer from the queue");
        }
        else if (currentBuffer == END_BUFFER_WITH_ERROR)
        {
            throw new IOException(throwable);
        }
    }

    /**
     * When reading from the InputStream first check the state, if stream is already closed or we need to
     * pop off the next buffer from the {@link LinkedBlockingQueue}
     *
     * @return -1 if we have reached the end of the InputStream or 0 if still open.
     * @throws IOException throw IOException if stream is already closed.
     */
    private int checkState() throws IOException
    {
        switch (state)
        {
            case Closed:
                throw new IOException("Stream is closed");
            case End:
                return -1;
            case Init:
                start();
            case NextBuffer:
                nextBuffer();
                if (currentBuffer == END_BUFFER)
                {
                    end();
                    return -1;
                }
        }
        return 0;
    }

    /**
     * Reached the end of the InputStream and all bytes have been read.
     */
    private void end()
    {
        state = StreamState.End;
        stats.inputStreamEnd(source, System.nanoTime() - startTimeNanos, timeBlockedNanos, bytesRead(), queueFullCount.get());
    }
}
