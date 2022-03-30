package org.apache.cassandra.spark.utils.streaming;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.ThrowableUtils;
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
    private static final StreamBuffer.ByteArrayWrapper END_MARKER = StreamBuffer.wrap(new byte[0]);
    private static final StreamBuffer.ByteArrayWrapper FINISHED_MARKER = StreamBuffer.wrap(new byte[0]);
    private static final StreamBuffer.ByteArrayWrapper ERROR_MARKER = StreamBuffer.wrap(new byte[0]);

    private enum StreamState
    {Init, Reading, NextBuffer, End, Closed}

    private final BlockingQueue<StreamBuffer> queue;
    private final SSTableSource<SSTable> source;
    private final Stats stats;
    private final long startTimeNanos;

    // variables accessed by both producer, consumer & timeout thread so must be volatile or atomic
    private volatile Throwable throwable = null;
    private volatile long lastActivityNanos = System.nanoTime();
    private volatile boolean activeRequest = false;
    private volatile boolean closed = false;
    private final AtomicLong bytesWritten = new AtomicLong(0L);

    // variables only used by the InputStream consumer thread so do not need to be volatile or atomic
    private long rangeStart = 0L, bytesRead = 0L, timeBlockedNanos = 0L;
    private boolean skipping = false;
    private StreamState state = StreamState.Init;
    private StreamBuffer currentBuffer = null;
    private int pos, len;

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
    }

    public long startTimeNanos()
    {
        return startTimeNanos;
    }

    public long timeBlockedNanos()
    {
        return timeBlockedNanos;
    }

    public long bytesWritten()
    {
        return bytesWritten.get();
    }

    public long bytesRead()
    {
        return bytesRead;
    }

    public long bytesBuffered()
    {
        return bytesWritten() - bytesRead();
    }

    public boolean isFinished()
    {
        return bytesWritten() >= source.size();
    }

    private boolean isClosed()
    {
        return state == StreamState.Closed;
    }

    /**
     * Can request more bytes if:
     * 1. a request not already in-flight
     * 2. not in the middle of skip method call
     * 3. the queue buffer is not full i.e. bytes in memory not greater than or equal to maxBufferSize
     * 4. the InputStream is not closed
     *
     * @return true if can request more bytes
     */
    private boolean canRequestMore()
    {
        return !(activeRequest || skipping || isBufferFull() || isClosed());
    }

    /**
     * Maybe request more bytes if possible.
     */
    private void maybeRequestMore()
    {
        if (canRequestMore())
        {
            requestMore();
        }
    }

    /**
     * Request more bytes using {@link SSTableSource#request(long, long, StreamConsumer)} for the next range.
     */
    private void requestMore()
    {
        if (rangeStart >= source.size())
        {
            return; // finished
        }

        final long chunkSize = rangeStart == 0 ? source.headerChunkSize() : source.chunkBufferSize();
        final long rangeEnd = Math.min(source.size(), rangeStart + chunkSize);
        if (rangeEnd >= rangeStart)
        {
            activeRequest = true;
            source.request(rangeStart, rangeEnd, this);
            rangeStart += chunkSize + 1; // increment range start pointer for next request
        }
        else
        {
            throw new IllegalStateException(String.format("Tried to request invalid range start=%d end=%d", rangeStart, rangeEnd));
        }
    }

    /**
     * The number of bytes buffered is greater than or equal to {@link SSTableSource#maxBufferSize()}
     * so wait for queue to drain before requesting more.
     *
     * @return true if queue is full
     */
    public boolean isBufferFull()
    {
        return bytesBuffered() >= source.maxBufferSize();
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
            if (closed || isFinished())
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
        if (len <= 0 || closed)
        {
            return;
        }
        bytesWritten.addAndGet(len);
        queue.add(buffer);
        stats.inputStreamBytesWritten(source, len);
        touchTimeout();
    }

    @Override
    public void onEnd()
    {
        activeRequest = false;
        if (isFinished())
        {
            queue.add(FINISHED_MARKER);
        }
        else
        {
            queue.add(END_MARKER);
        }
        touchTimeout();
    }

    @Override
    public void onError(@NotNull final Throwable t)
    {
        throwable = ThrowableUtils.rootCause(t);
        activeRequest = false;
        queue.add(ERROR_MARKER);
        stats.inputStreamFailure(source, t);
    }

    /**
     * {@link java.io.InputStream} method implementations.
     */

    @Override
    public int available()
    {
        return Math.toIntExact(bytesBuffered());
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    /**
     * If the schema contains large blobs that can be filtered, then we can more efficiently
     * skip bytes by incrementing the startRange and avoid wastefully streaming bytes across the network.
     *
     * @param n number of bytes
     * @return number of bytes actually skipped
     * @throws IOException IOException
     */
    @Override
    public long skip(long n) throws IOException
    {
        if (n <= 0)
        {
            return 0;
        }
        else if (n <= bytesBuffered())
        {
            final long actual = super.skip(n);
            stats.inputStreamBytesSkipped(source, actual, 0);
            return actual;
        }

        this.skipping = true;
        long remaining = n;
        do
        {
            // drain any buffered bytes and block until active request
            // completes and the queue is empty
            remaining -= super.skip(remaining);
            if (remaining <= 0)
            {
                break;
            }
        } while (activeRequest || !queue.isEmpty());

        // increment range start pointer to efficiently skip
        // without reading bytes across the network unnecessarily
        if (remaining > 0)
        {
            rangeStart += remaining;
            bytesWritten.addAndGet(remaining);
            bytesRead += remaining;
        }

        // remove skip marker & resume requesting bytes
        this.skipping = false;
        maybeRequestMore();
        stats.inputStreamBytesSkipped(source, n - remaining, remaining);
        return n;
    }

    /**
     * Allows directly reading into ByteBuffer without intermediate copy.
     *
     * @param buf the ByteBuffer
     * @throws EOFException if attempts to read beyond the end of the file
     * @throws IOException  io exception for failure during i/o
     */
    public void read(ByteBuffer buf) throws IOException
    {
        int length = buf.remaining();
        while (length > 0)
        {
            if (checkState() < 0)
            {
                throw new EOFException();
            }

            final int readLen = Math.min(len - pos, length);
            if (readLen > 0)
            {
                currentBuffer.getBytes(pos, buf, readLen);
                pos += readLen;
                bytesRead += readLen;
            }
            maybeReleaseBuffer();
            length = buf.remaining();
        }
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
        if (readLen > 0)
        {
            currentBuffer.getBytes(pos, b, off, readLen);
            pos += readLen;
            bytesRead += readLen;
        }
        maybeReleaseBuffer();
        return readLen;
    }

    @Override
    public int read() throws IOException
    {
        do
        {
            if (checkState() < 0)
            {
                return -1;
            }

            if (currentBuffer.readableBytes() == 0)
            {
                // current buffer might be empty, normally if it is a marker buffer e.g. END_MARKER
                maybeReleaseBuffer();
            }
        } while (currentBuffer == null);

        // convert to unsigned byte
        final int b = currentBuffer.getByte(pos++) & 0xFF;
        bytesRead++;
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
        closed = true;
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
        maybeRequestMore();
        if (pos < len)
        {
            // still bytes remaining in the currentBuffer so keep reading
            return;
        }

        releaseBuffer();
        state = StreamState.NextBuffer;
        touchTimeout();
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
                // first request: start requesting bytes & schedule timeout
                requestMore();
                scheduleTimeout();
                state = StreamState.NextBuffer;
            case NextBuffer:
                nextBuffer();
                if (currentBuffer == END_MARKER)
                {
                    return handleEndMarker();
                }
                else if (currentBuffer == FINISHED_MARKER)
                {
                    return handleFinishedMarker();
                }
                else if (currentBuffer == ERROR_MARKER)
                {
                    throw new IOException(throwable);
                }
        }
        return 0;
    }

    /**
     * Handle finished marker returned in the queue, indicating all bytes from source have been reqeusted
     * and input stream can close.
     *
     * @return always return -1 as stream is closed.
     */
    private int handleFinishedMarker()
    {
        releaseBuffer();
        end();
        stats.inputStreamEndBuffer(source);
        return -1;
    }

    /**
     * Handle end marker returned in the queue, indicating previous request has finished
     *
     * @return -1 if we have reached the end of the InputStream or 0 if still open.
     * @throws IOException throw IOException if stream is already closed.
     */
    private int handleEndMarker() throws IOException
    {
        if (skipping)
        {
            return -1;
        }
        releaseBuffer();
        maybeRequestMore();
        state = StreamState.NextBuffer;
        return checkState();
    }

    /**
     * Reached the end of the InputStream and all bytes have been read.
     */
    private void end()
    {
        state = StreamState.End;
        stats.inputStreamEnd(source, System.nanoTime() - startTimeNanos, timeBlockedNanos);
    }
}