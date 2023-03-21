package org.apache.cassandra.spark.cdc;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
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
    Logger LOGGER = LoggerFactory.getLogger(CommitLog.class);

    // match both legacy and new version of commitlogs Ex: CommitLog-12345.log and CommitLog-6-12345.log.
    Pattern COMMIT_LOG_FILE_PATTERN = Pattern.compile("CommitLog(-(\\d+))?-(\\d+).log");

    static Optional<Pair<Integer, Long>> extractVersionAndSegmentId(@NotNull final CommitLog log)
    {
        return extractVersionAndSegmentId(log.name());
    }

    static Optional<Pair<Integer, Long>> extractVersionAndSegmentId(@NotNull final String filename)
    {
        final Matcher matcher = CommitLog.COMMIT_LOG_FILE_PATTERN.matcher(filename);
        if (matcher.matches())
        {
            try
            {
                final int version = matcher.group(2) == null ? 6 : Integer.parseInt(matcher.group(2));
                if (version != 6 && version != 7)
                {
                    throw new IllegalStateException("Unknown commitlog version " + version);
                }
                // logic taken from org.apache.cassandra.db.commitlog.CommitLogDescriptor.getMessagingVersion()
                return Optional.of(Pair.of(version == 6 ? 10 : 12, Long.parseLong(matcher.group(3))));
            }
            catch (NumberFormatException e)
            {
                LOGGER.error("Could not parse commit log segmentId name={}", filename, e);
                return Optional.empty();
            }
        }
        LOGGER.error("Could not parse commit log filename name={}", filename);
        return Optional.empty(); // cannot extract segment id
    }

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

    default long segmentId()
    {
        return Objects.requireNonNull(extractVersionAndSegmentId(this).map(Pair::getRight).orElseThrow(() -> new RuntimeException("Could not extract segmentId from CommitLog")), "Could not extract segmentId from CommitLog");
    }

    default CommitLog.Marker zeroMarker()
    {
        return markerAt(segmentId(), 0);
    }

    default CommitLog.Marker maxMarker()
    {
        return markerAt(segmentId(), (int) maxOffset());
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

    class Marker implements Comparable<Marker>, Serializable
    {
        public static final Serializer SERIALIZER = new Serializer();

        public static CommitLog.Marker origin(CassandraInstance instance)
        {
            return new CommitLog.Marker(instance, 0, 0);
        }

        final CassandraInstance instance;
        final long segmentId;
        final int position;

        @JsonCreator
        public Marker(@JsonProperty("instance") final CassandraInstance instance,
                      @JsonProperty("segmentId") final long segmentId,
                      @JsonProperty("position") final int position)
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
        @JsonGetter("segmentId")
        public long segmentId()
        {
            return segmentId;
        }

        @JsonGetter("instance")
        public CassandraInstance instance()
        {
            return instance;
        }

        /**
         * The offset into the section where the mutation starts.
         *
         * @return mutation offset within the section
         */
        @JsonGetter("position")
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

        public static class Serializer extends com.esotericsoftware.kryo.Serializer<Marker>
        {
            public void write(Kryo kryo, Output out, Marker o)
            {
                kryo.writeObject(out, o.instance, CassandraInstance.SERIALIZER);
                out.writeLong(o.segmentId);
                out.writeInt(o.position);
            }

            public Marker read(Kryo kryo, Input in, Class<Marker> type)
            {
                return new Marker(kryo.readObject(in, CassandraInstance.class, CassandraInstance.SERIALIZER), in.readLong(), in.readInt());
            }
        }
    }
}
