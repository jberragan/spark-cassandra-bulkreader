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

package org.apache.cassandra.spark.cdc.jdk;

import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.cdc.ICommitLogMarkers;
import org.apache.cassandra.spark.cdc.watermarker.InMemoryWatermarker;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class CdcConsumer extends JdkCdcIterator implements Consumer<JdkCdcEvent>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcConsumer.class);

    @Nullable
    private volatile CdcConsumer prev = null;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicReference<CompletableFuture<Void>> active = new AtomicReference<>(null);

    public CdcConsumer(@NotNull final String jobId,
                       final int partitionId)
    {
        super(jobId, partitionId);
    }

    public CdcConsumer(@NotNull final String jobId,
                       final int partitionId,
                       final long epoch,
                       @Nullable Range<BigInteger> range,
                       @NotNull final ICommitLogMarkers markers,
                       @NotNull final InMemoryWatermarker.SerializationWrapper serializationWrapper)
    {
        super(jobId, partitionId, epoch, range, markers, serializationWrapper);
    }

    public CdcConsumer(@NotNull final String jobId,
                       final int partitionId,
                       final long epoch,
                       @Nullable RangeFilter rangeFilter,
                       @NotNull final ICommitLogMarkers markers,
                       @NotNull final InMemoryWatermarker watermarker)
    {
        super(jobId, partitionId, epoch, rangeFilter, markers, watermarker);
    }

    public void start()
    {
        if (!isRunning.get() && isRunning.compareAndSet(false, true))
        {
            LOGGER.info("Starting CDC Consumer jobId={} partitionId={}", jobId, partitionId);
            scheduleRun(0);
        }
    }

    @Override
    protected void onFinish()
    {
        if (this.builder == null)
        {
            return;
        }
        super.close(); // close without stopping scheduler
        persist();
    }

    @Override
    protected boolean isFinished()
    {
        // cdc consumer instance is finished once scanner is exhausted
        return this.scanner == null || super.isFinished();
    }

    public void stop()
    {
        stop(true);
    }

    public void stop(boolean blocking)
    {
        if (isRunning.get() && isRunning.compareAndSet(true, false))
        {
            LOGGER.info("Stopping CDC Consumer jobId={} partitionId={}", jobId, partitionId);
            final CompletableFuture<Void> activeFuture = active.get();
            if (activeFuture != null && active.compareAndSet(activeFuture, null) && blocking)
            {
                // block until active future completes
                activeFuture.join();
            }
            LOGGER.info("Stopped CDC Consumer jobId={} partitionId={}", jobId, partitionId);
        }
    }

    protected void scheduleNextRun()
    {
        final CompletableFuture<Void> future = active.get();
        if (future != null)
        {
            active.compareAndSet(future, null);
        }

        final JdkCdcIterator prevIt = this.prev;
        scheduleRun(prevIt == null ? sleepMillis() : prevIt.sleepMillis());
    }

    protected void scheduleRun(long delayMillis)
    {
        if (!isRunning.get())
        {
            return;
        }

        active.getAndUpdate((curr) -> {
            if (curr == null)
            {
                return delayMillis <= 0 ? executor().submit(this::runSafe) : executor().schedule(this::runSafe, delayMillis);
            }
            return curr;
        });
    }

    protected void runSafe()
    {
        try
        {
            run();
            scheduleNextRun();
        }
        catch (Throwable t)
        {
            if (handleError(t))
            {
                scheduleNextRun();
            }
            else
            {
                stop();
            }
        }
    }

    @SuppressWarnings("unchecked")
    <Type extends JdkCdcIterator> Type nextEpoch()
    {
        return (Type) super.nextEpoch().toConsumer();
    }

    @Override
    public CdcConsumer toConsumer()
    {
        return this;
    }

    /**
     * @param t throwable
     * @return true if Cdc consumer can continue, or false if it should stop.
     */
    protected boolean handleError(Throwable t)
    {
        LOGGER.error("Unexpected error in CdcConsumer", t);
        return true;
    }

    protected void run()
    {
        // clone next iterator from previous state if it exists
        final CdcConsumer prevIt = this.prev;
        try (final CdcConsumer next = prevIt == null ? this.nextEpoch() : prevIt.nextEpoch())
        {
            while (next.next())
            {
                next.advanceToNextColumn();
                this.accept(next.data());
            }
            Preconditions.checkArgument(next.scanner == null && next.builder == null, "Iterator should have been closed after micro-batch completes");
            this.prev = next;
        }
    }

    @Override
    public void close()
    {
        this.stop();
        super.close();
    }
}
