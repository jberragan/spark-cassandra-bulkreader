package org.apache.cassandra.spark.cdc;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.shaded.fourzero.yaml.snakeyaml.error.Mark;
import org.apache.cassandra.spark.sparksql.filters.RangeFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface ICommitLogMarkers extends Serializable
{
    PerInstance EMPTY = new PerInstance(ImmutableMap.of());
    Serializer SERIALIZER = new Serializer();

    @NotNull
    default Marker startMarker(CommitLog log)
    {
        return startMarker(log.instance());
    }

    /**
     * @param instance CassandraInstance
     * @return minimum CommitLog Marker for a CassandraInstance to start reading from.
     */
    @NotNull
    Marker startMarker(CassandraInstance instance);

    /**
     * @param position current position in the CommitLog
     * @param token    Cassandra token of current mutation.
     * @return true if we can ignore this token if we have already read passed this position.
     */
    boolean canIgnore(Marker position, BigInteger token);

    static PerInstance of(@Nullable final Marker marker)
    {
        if (marker == null)
        {
            return EMPTY;
        }
        return of(ImmutableMap.of(marker.instance(), marker));
    }

    static PerInstance of(Map<CassandraInstance, Marker> markers)
    {
        return new PerInstance(markers);
    }

    static PerInstance of(ICommitLogMarkers markers1,
                          ICommitLogMarkers markers2)
    {
        return new PerInstance(markers1.values(), markers2.values());
    }

    boolean isEmpty();

    Collection<Marker> values();

    static PerRangeBuilder perRangeBuilder()
    {
        return new PerRangeBuilder();
    }

    class PerRangeBuilder
    {
        private final Map<CassandraInstance, Map<Range<BigInteger>, Marker>> markers;

        public PerRangeBuilder()
        {
            this.markers = new HashMap<>();
        }

        public PerRangeBuilder add(@NotNull final RangeFilter range,
                                   @NotNull final Marker marker)
        {
            return add(range.tokenRange(), marker);
        }

        public PerRangeBuilder add(@NotNull final Range<BigInteger> range,
                                   @NotNull final Marker marker)
        {
            markers.computeIfAbsent(marker.instance(), (inst) -> new HashMap<>()).put(range, marker);
            return this;
        }

        public PerRange build()
        {
            return new PerRange(this.markers);
        }
    }

    /**
     * Stores CommitLog markers per CassandraInstance, taking the minimum marker when there are duplicates.
     */
    class PerInstance implements ICommitLogMarkers
    {
        final Map<CassandraInstance, Marker> markers;

        public PerInstance(Collection<Marker> markers1, Collection<Marker> markers2)
        {
            this(
            Stream.concat(markers1.stream(), markers2.stream())
                  .collect(Collectors.toMap(Marker::instance, Function.identity(), Marker::min))
            );
        }

        public PerInstance(Map<CassandraInstance, Marker> markers)
        {
            this.markers = ImmutableMap.copyOf(markers);
        }

        @NotNull
        public Marker startMarker(CassandraInstance instance)
        {
            return Optional.ofNullable(markers.get(instance))
                           .orElseGet(instance::zeroMarker);
        }

        public boolean canIgnore(Marker position, BigInteger token)
        {
            return false;
        }

        public int size()
        {
            return markers.size();
        }

        public boolean isEmpty()
        {
            return markers.isEmpty();
        }

        public Collection<Marker> values()
        {
            return markers.values();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(markers);
        }

        @Override
        public boolean equals(Object obj)
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

            final PerInstance rhs = (PerInstance) obj;
            return markers.equals(rhs.markers);
        }
    }

    class Serializer extends com.esotericsoftware.kryo.Serializer<ICommitLogMarkers>
    {
        @Override
        public ICommitLogMarkers read(final Kryo kryo, final Input in, final Class type)
        {
            final int size = in.readShort();
            final Map<CassandraInstance, Marker> markers = new HashMap<>(size);
            for (int i = 0; i < size; i++)
            {
                final Marker marker = kryo.readObject(in, Marker.class, Marker.SERIALIZER);
                markers.put(marker.instance(), marker);
            }
            return new PerInstance(markers);
        }

        @Override
        public void write(final Kryo kryo, final Output out, final ICommitLogMarkers markers)
        {
            final Collection<Marker> allMarkers = markers.values();
            out.writeShort(allMarkers.size());
            for (Marker marker : allMarkers)
            {
                kryo.writeObject(out, marker, Marker.SERIALIZER);
            }
        }
    }

    /**
     * Stores CommitLog markers per CassandraInstance, storing per token range
     */
    class PerRange implements ICommitLogMarkers
    {
        private final Map<CassandraInstance, Map<Range<BigInteger>, Marker>> markers;

        public PerRange(Map<CassandraInstance, Map<Range<BigInteger>, Marker>> markers)
        {
            this.markers = ImmutableMap.copyOf(markers);
        }

        @NotNull
        public Marker startMarker(CassandraInstance instance)
        {
            return markers.getOrDefault(instance, ImmutableMap.of())
                          .values()
                          .stream()
                          .min(Marker::compareTo)
                          .orElseGet(instance::zeroMarker);
        }

        public boolean canIgnore(Marker position, BigInteger token)
        {
            final Map<Range<BigInteger>, Marker> instMarkers = markers.get(position.instance());
            if (instMarkers == null || instMarkers.isEmpty())
            {
                return false;
            }

            // if position is before any previously consumed range (that overlaps with token) then we can ignore as already published
            return instMarkers.entrySet()
                              .stream()
                              .filter(entry -> entry.getKey().contains(token))
                              .map(Map.Entry::getValue)
                              .anyMatch(position::isBefore);
        }

        public int size()
        {
            return markers.size();
        }

        public boolean isEmpty()
        {
            return markers.isEmpty();
        }

        public Collection<Marker> values()
        {
            return markers.keySet().stream()
                          .map(this::startMarker)
                          .collect(Collectors.toList());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(markers);
        }

        @Override
        public boolean equals(Object obj)
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

            final PerRange rhs = (PerRange) obj;
            return markers.equals(rhs.markers);
        }
    }
}
