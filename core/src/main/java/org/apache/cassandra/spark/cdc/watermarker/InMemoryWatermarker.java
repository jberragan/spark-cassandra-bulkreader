package org.apache.cassandra.spark.cdc.watermarker;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.commons.lang.NotImplementedException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.PartitionUpdateWrapper;
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
    public static final InMemoryWatermarker INSTANCE = new InMemoryWatermarker(new TaskContextProvider()
    {
        public boolean hasTaskContext()
        {
            return TaskContext.get() != null;
        }

        public int partitionId()
        {
            return TaskContext.getPartitionId();
        }
    });
    @VisibleForTesting
    public static String TEST_THREAD_NAME = null; // allow unit tests to bypass TaskContext check as no easy way to set ThreadLocal TaskContext
    private final TaskContextProvider taskContextProvider;

    public interface TaskContextProvider
    {
        boolean hasTaskContext();

        int partitionId();
    }

    public InMemoryWatermarker(TaskContextProvider taskContextProvider)
    {
        this.taskContextProvider = taskContextProvider;
    }

    // store watermarker per Spark job
    protected final Map<String, JobWatermarker> jobs = new ConcurrentHashMap<>();

    public Watermarker instance(String jobId)
    {
        return jobs.computeIfAbsent(jobId, this::newInstance).get();
    }

    public void recordReplicaCount(PartitionUpdateWrapper update, int numReplicas)
    {
        throw new IllegalAccessError();
    }

    public int replicaCount(PartitionUpdateWrapper update)
    {
        throw new IllegalAccessError();
    }

    // allow sub-classes to override with own implementation
    public JobWatermarker newInstance(String jobId)
    {
        return new JobWatermarker(jobId, taskContextProvider);
    }

    public void untrackReplicaCount(PartitionUpdateWrapper update)
    {
        throw new IllegalAccessError();
    }

    public boolean seenBefore(PartitionUpdateWrapper update)
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

    public void apply(SerializationWrapper wrapper)
    {
        throw new NotImplementedException();
    }

    public SerializationWrapper serializationWrapper()
    {
        throw new NotImplementedException();
    }

    /**
     * Stores per Spark partition watermarker for a given Spark job.
     */
    @ThreadSafe
    public static class JobWatermarker implements Watermarker
    {
        protected final String jobId;
        protected final TaskContextProvider taskContextProvider;

        protected final Map<Integer, PartitionWatermarker> watermarkers = new ConcurrentHashMap<>();

        public JobWatermarker(String jobId, TaskContextProvider taskContextProvider)
        {
            this.jobId = jobId;
            this.taskContextProvider = taskContextProvider;
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

        public void recordReplicaCount(PartitionUpdateWrapper update, int numReplicas)
        {
            get().recordReplicaCount(update, numReplicas);
        }

        public int replicaCount(PartitionUpdateWrapper update)
        {
            return get().replicaCount(update);
        }

        public void untrackReplicaCount(PartitionUpdateWrapper update)
        {
            get().untrackReplicaCount(update);
        }

        public boolean seenBefore(PartitionUpdateWrapper update)
        {
            return get().seenBefore(update);
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
                Preconditions.checkArgument(taskContextProvider.hasTaskContext(), "This method must be called by a Spark executor thread");
            }
            return watermarkers.computeIfAbsent(taskContextProvider.partitionId(), this::newInstance);
        }

        // allow sub-classes to override with own implementation
        public PartitionWatermarker newInstance(int partitionId)
        {
            return new PartitionWatermarker(partitionId);
        }

        public void apply(SerializationWrapper wrapper)
        {
            get().apply(wrapper);
        }

        public SerializationWrapper serializationWrapper()
        {
            return get().serializationWrapper();
        }
    }

    /**
     * Tracks highwater mark per instance and number of replicas previously received for updates that did not achieve the consistency level.
     */
    public static class PartitionWatermarker implements Watermarker
    {
        // tracks replica count for mutations with insufficient replica copies
        protected final Map<PartitionUpdateWrapper, Integer> replicaCount = new ConcurrentHashMap<>(1024);
        // high watermark tracks how far we have read in the CommitLogs per CassandraInstance

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

        public void recordReplicaCount(PartitionUpdateWrapper update, int numReplicas)
        {
            replicaCount.put(update, numReplicas);
        }

        public int replicaCount(PartitionUpdateWrapper update)
        {
            return replicaCount.getOrDefault(update, 0);
        }

        public void untrackReplicaCount(PartitionUpdateWrapper update)
        {
            replicaCount.remove(update);
        }

        public boolean seenBefore(PartitionUpdateWrapper update)
        {
            return replicaCount.containsKey(update);
        }

        public void persist(@Nullable final Long maxAgeMicros)
        {
            replicaCount.keySet().removeIf(u -> isExpired(u, maxAgeMicros));
        }

        public void clear()
        {
            replicaCount.clear();
        }

        public boolean isExpired(@NotNull final PartitionUpdateWrapper update,
                                 @Nullable final Long maxAgeMicros)
        {
            return maxAgeMicros != null && update.maxTimestampMicros() < maxAgeMicros;
        }

        public void apply(SerializationWrapper wrapper)
        {
            this.replicaCount.putAll(wrapper.replicaCount);
        }

        public SerializationWrapper serializationWrapper()
        {
            return new SerializationWrapper(replicaCount);
        }
    }

    public static class SerializationWrapper implements Serializable
    {
        private final Map<PartitionUpdateWrapper, Integer> replicaCount;

        public SerializationWrapper()
        {
            this(Collections.emptyMap());
        }

        public SerializationWrapper(Map<PartitionUpdateWrapper, Integer> replicaCount)
        {
            this.replicaCount = replicaCount;
        }

        public static class Serializer extends com.esotericsoftware.kryo.Serializer<SerializationWrapper>
        {
            public static final InMemoryWatermarker.SerializationWrapper.Serializer INSTANCE = new InMemoryWatermarker.SerializationWrapper.Serializer();

            private final PartitionUpdateWrapper.Serializer updateSerializer = new PartitionUpdateWrapper.Serializer();

            public SerializationWrapper read(Kryo kryo, Input in, Class type)
            {
                // read replica counts
                final int numUpdates = in.readShort();
                final Map<PartitionUpdateWrapper, Integer> replicaCounts = new HashMap<>(numUpdates);
                for (int i = 0; i < numUpdates; i++)
                {
                    replicaCounts.put(kryo.readObject(in, PartitionUpdateWrapper.class, updateSerializer), (int) in.readByte());
                }

                return new SerializationWrapper(replicaCounts);
            }

            public void write(Kryo kryo, Output out, SerializationWrapper o)
            {
                // write replica counts for late mutations
                out.writeShort(o.replicaCount.size());
                for (final Map.Entry<PartitionUpdateWrapper, Integer> entry : o.replicaCount.entrySet())
                {
                    PartitionUpdateWrapper update = entry.getKey();
                    kryo.writeObject(out, update, updateSerializer);
                    out.writeByte(entry.getValue());
                }
            }
        }
    }
}
