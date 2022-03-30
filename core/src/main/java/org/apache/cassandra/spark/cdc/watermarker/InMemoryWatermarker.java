package org.apache.cassandra.spark.cdc.watermarker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.CdcUpdate;
import org.apache.spark.TaskContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
 * In-memory watermark implementation that caches position to start reading from each instance.
 * WARNING: this implementation is for local testing only and should not be used in a Spark cluster.
 * The task allocation in Spark cannot guarantee a partition will be assigned to the same executor.
 */
@ThreadSafe
public class InMemoryWatermarker implements Watermarker
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryWatermarker.class);

    public static final InMemoryWatermarker INSTANCE = new InMemoryWatermarker();
    @VisibleForTesting
    public static String TEST_THREAD_NAME = null; // allow unit tests to bypass TaskContext check as no easy way to set ThreadLocal TaskContext

    // store watermarker per Spark job
    protected final Map<String, JobWatermarker> jobs = new ConcurrentHashMap<>();

    public Watermarker instance(String jobId)
    {
        return jobs.computeIfAbsent(jobId, this::newInstance).get();
    }

    public void recordReplicaCount(CdcUpdate update, int numReplicas)
    {
        throw new IllegalAccessError();
    }

    public int replicaCount(CdcUpdate update)
    {
        throw new IllegalAccessError();
    }

    // allow sub-classes to override with own implementation
    public JobWatermarker newInstance(String jobId)
    {
        return new JobWatermarker(jobId);
    }

    public void untrackReplicaCount(CdcUpdate update)
    {
        throw new IllegalAccessError();
    }

    public boolean seenBefore(CdcUpdate update)
    {
        throw new IllegalAccessError();
    }

    public void updateHighWaterMark(CommitLog.Marker marker)
    {
        throw new IllegalAccessError();
    }

    @Nullable
    public CommitLog.Marker highWaterMark(CassandraInstance instance)
    {
        throw new IllegalAccessError();
    }

    public void persist(@Nullable final Long maxAgeMicros)
    {
        throw new IllegalAccessError();
    }

    public void clear()
    {
        jobs.values().forEach(JobWatermarker::clear);
        jobs.clear();
    }

    /**
     * Stores per Spark partition watermarker for a given Spark job.
     */
    @ThreadSafe
    public static class JobWatermarker implements Watermarker
    {
        protected final String jobId;

        protected final Map<Integer, PartitionWatermarker> watermarkers = new ConcurrentHashMap<>();

        public JobWatermarker(String jobId)
        {
            this.jobId = jobId;
        }

        public String jobId()
        {
            return jobId;
        }

        public Watermarker instance(String jobId)
        {
            Preconditions.checkArgument(this.jobId.equals(jobId));
            return get();
        }

        public void recordReplicaCount(CdcUpdate update, int numReplicas)
        {
            get().recordReplicaCount(update, numReplicas);
        }

        public int replicaCount(CdcUpdate update)
        {
            return get().replicaCount(update);
        }

        public void untrackReplicaCount(CdcUpdate update)
        {
            get().untrackReplicaCount(update);
        }

        public boolean seenBefore(CdcUpdate update)
        {
            return get().seenBefore(update);
        }

        public void updateHighWaterMark(CommitLog.Marker marker)
        {
            get().updateHighWaterMark(marker);
        }

        public CommitLog.Marker highWaterMark(CassandraInstance instance)
        {
            return get().highWaterMark(instance);
        }

        public void persist(@Nullable final Long maxAgeMicros)
        {
            get().persist(maxAgeMicros);
        }

        public void clear()
        {
            watermarkers.values().forEach(Watermarker::clear);
            watermarkers.clear();
        }

        public PartitionWatermarker get()
        {
            if (!Thread.currentThread().getName().equals(TEST_THREAD_NAME))
            {
                Preconditions.checkNotNull(TaskContext.get(), "This method must be called by a Spark executor thread");
            }
            return watermarkers.computeIfAbsent(TaskContext.getPartitionId(), this::newInstance);
        }

        // allow sub-classes to override with own implementation
        public PartitionWatermarker newInstance(int partitionId)
        {
            return new PartitionWatermarker(partitionId);
        }
    }

    /**
     * Tracks highwater mark per instance and number of replicas previously received for updates that did not achieve the consistency level.
     */
    public static class PartitionWatermarker implements Watermarker
    {
        // tracks replica count for mutations with insufficient replica copies
        protected final Map<CdcUpdate, Integer> replicaCount = new ConcurrentHashMap<>(1024);
        // high watermark tracks how far we have read in the CommitLogs per CassandraInstance
        protected final Map<CassandraInstance, CommitLog.Marker> highWatermarks = new ConcurrentHashMap<>();

        final int partitionId;

        public PartitionWatermarker(int partitionId)
        {
            this.partitionId = partitionId;
        }

        public int partitionId()
        {
            return partitionId;
        }

        public Watermarker instance(String jobId)
        {
            return this;
        }

        public void recordReplicaCount(CdcUpdate update, int numReplicas)
        {
            replicaCount.put(update, numReplicas);
        }

        public int replicaCount(CdcUpdate update)
        {
            return replicaCount.getOrDefault(update, 0);
        }

        public void untrackReplicaCount(CdcUpdate update)
        {
            replicaCount.remove(update);
        }

        public boolean seenBefore(CdcUpdate update)
        {
            return replicaCount.containsKey(update);
        }

        public void updateHighWaterMark(CommitLog.Marker marker)
        {
            // this method will be called by executor thread when reading through CommitLog
            // so use AtomicReference to ensure thread-safe and visible to other threads
            if (marker == this.highWatermarks.merge(marker.instance(), marker,
                                                    (oldValue, newValue) -> newValue.compareTo(oldValue) > 0 ? newValue : oldValue))
            {
                LOGGER.debug("Updated highwater mark instance={} marker='{}' partitionId={}", marker.instance().nodeName(), marker, partitionId());
            }
        }

        public CommitLog.Marker highWaterMark(CassandraInstance instance)
        {
            return highWatermarks.get(instance);
        }

        public void persist(@Nullable final Long maxAgeMicros)
        {
            replicaCount.keySet().removeIf(u -> isExpired(u, maxAgeMicros));
        }

        public void clear()
        {
            replicaCount.clear();
            highWatermarks.clear();
        }

        public boolean isExpired(@NotNull final CdcUpdate update,
                                 @Nullable final Long maxAgeMicros)
        {
            return maxAgeMicros != null && update.maxTimestampMicros() < maxAgeMicros;
        }
    }
}
