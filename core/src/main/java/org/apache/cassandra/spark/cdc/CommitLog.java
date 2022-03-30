package org.apache.cassandra.spark.cdc;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.cassandra.spark.utils.streaming.SSTableSource;
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

public interface CommitLog extends AutoCloseable
{
    /**
     * @return filename of the CommitLog
     */
    String name();

    /**
     * @return path to the CommitLog
     */
    String path();

    /**
     * @return the max offset that can be read in the CommitLog. This may be less than or equal to {@link CommitLog#len()}.
     * The reader should not read passed this point when the commit log is incomplete.
     */
    long maxOffset();

    /**
     * @return length of the CommitLog in bytes
     */
    long len();

    /**
     * @return an SSTableSource for asynchronously reading the CommitLog bytes.
     */
    SSTableSource<? extends DataLayer.SSTable> source();

    /**
     * @return the CassandraInstance this CommitLog resides on.
     */
    CassandraInstance instance();

    default CommitLog.Marker zeroMarker()
    {
        return markerAt(0, 0);
    }

    default CommitLog.Marker markerAt(long section, int offset)
    {
        return new CommitLog.Marker(instance(), section, offset);
    }

    /**
     * Override to provide custom stats implementation.
     *
     * @return stats instance for publishing stats
     */
    default Stats stats()
    {
        return Stats.DoNothingStats.INSTANCE;
    }

    class Marker implements Comparable<Marker>
    {
        final CassandraInstance instance;
        final long segmentId;
        final int position;

        public Marker(final CassandraInstance instance,
                      final long segmentId,
                      final int position)
        {
            this.instance = instance;
            this.segmentId = segmentId;
            this.position = position;
        }

        /**
         * Marks the start position of the section.
         *
         * @return position in CommitLog of the section.
         */
        public long segmentId()
        {
            return segmentId;
        }

        public CassandraInstance instance()
        {
            return instance;
        }

        /**
         * The offset into the section where the mutation starts.
         *
         * @return mutation offset within the section
         */
        public int position()
        {
            return position;
        }

        @Override
        public int compareTo(@NotNull Marker o)
        {
            Preconditions.checkArgument(instance.equals(o.instance), "CommitLog Markers should be on the same instance");
            final int c = Long.compare(segmentId, o.segmentId());
            if (c == 0)
            {
                return Integer.compare(position, o.position());
            }
            return c;
        }

        public String toString()
        {
            return String.format("{" +
                                 "\"segmentId\": %d, " +
                                 "\"position\": %d" +
                                 " }", segmentId, position);
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder(73, 79)
                   .append(instance)
                   .append(segmentId)
                   .append(position)
                   .toHashCode();
        }

        @Override
        public boolean equals(final Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (obj == this)
            {
                return true;
            }
            if (obj.getClass() != getClass())
            {
                return false;
            }

            final Marker rhs = (Marker) obj;
            return new EqualsBuilder()
                   .append(instance, rhs.instance)
                   .append(segmentId, rhs.segmentId)
                   .append(position, rhs.position)
                   .isEquals();
        }
    }
}
