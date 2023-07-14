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

import java.io.IOException;
import java.math.BigInteger;

import com.google.common.collect.Range;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.CdcKryoRegister;
import org.apache.cassandra.spark.cdc.ICommitLogMarkers;
import org.apache.cassandra.spark.cdc.watermarker.InMemoryWatermarker;
import org.apache.cassandra.spark.reader.fourzero.CompressionUtil;
import org.apache.cassandra.spark.utils.KryoUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class CdcState
{
    public final long epoch;
    @Nullable
    public final Range<BigInteger> range;
    @NotNull
    public final ICommitLogMarkers markers;
    @NotNull
    public final InMemoryWatermarker.SerializationWrapper serializationWrapper;

    protected CdcState(final long epoch,
                       @Nullable final Range<BigInteger> range,
                       @NotNull final ICommitLogMarkers markers,
                       @NotNull final InMemoryWatermarker.SerializationWrapper serializationWrapper)
    {
        this.epoch = epoch;
        this.range = range;
        this.markers = markers;
        this.serializationWrapper = serializationWrapper;
    }

    public abstract <StateType extends CdcState> StateType newInstance(final StateType other,
                                                                       final long epoch,
                                                                       @Nullable final Range<BigInteger> range,
                                                                       final ICommitLogMarkers markers,
                                                                       final InMemoryWatermarker.SerializationWrapper wrapper);

    @SuppressWarnings("unchecked")
    public <StateType extends CdcState> StateType merge(final Range<BigInteger> range,
                                                        final StateType other)
    {
        return (StateType) CdcState.merge(range, this, other);
    }

    /**
     * Merge two previous cdc states, filtered for the new token range.
     *
     * @param range  the new token range, if null then merge without filtering by token range.
     * @param state1 cdc state.
     * @param state2 cdc state.
     * @return a new cdc state object that merges the markers and watermarker state for the new token range.
     */
    public static <StateType extends CdcState> StateType merge(@Nullable final Range<BigInteger> range,
                                                               final StateType state1,
                                                               final StateType state2)
    {
        final ICommitLogMarkers mergedMarkers = mergeMarkers(state1, state2);
        final InMemoryWatermarker.SerializationWrapper mergedWrapper = state1.serializationWrapper
                                                                       .merge(state2.serializationWrapper)
                                                                       .filter(range);
        return state1.newInstance(state2, Math.max(state1.epoch, state2.epoch), range, mergedMarkers, mergedWrapper);
    }

    /**
     * @param state1 cdc state.
     * @param state2 cdc state.
     * @return a ICommitLogMarkers.PerRange merged object that tracks the min. position per token range.
     */
    public static ICommitLogMarkers mergeMarkers(@NotNull final CdcState state1, @NotNull final CdcState state2)
    {
        return mergeMarkers(state1.markers, state1.range, state2.markers, state2.range);
    }

    public static ICommitLogMarkers mergeMarkers(@NotNull final ICommitLogMarkers markers1,
                                                 @Nullable final Range<BigInteger> range1,
                                                 @NotNull final ICommitLogMarkers markers2,
                                                 @Nullable final Range<BigInteger> range2)
    {
        if (range1 == null || range2 == null)
        {
            return ICommitLogMarkers.of(markers1, markers2);
        }

        final ICommitLogMarkers.PerRangeBuilder builder = ICommitLogMarkers.perRangeBuilder();
        markers1.values().forEach(marker -> builder.add(range1, marker));
        markers2.values().forEach(marker -> builder.add(range2, marker));
        return builder.build();
    }

    // Serialization Helpers

    // Kryo

    public static abstract class Serializer<StateType extends CdcState> extends com.esotericsoftware.kryo.Serializer<StateType>
    {
        public abstract StateType newInstance(Kryo kryo,
                                              Input in,
                                              Class<StateType> type,
                                              long epoch,
                                              @Nullable Range<BigInteger> range,
                                              ICommitLogMarkers markers,
                                              InMemoryWatermarker.SerializationWrapper serializationWrapper);

        public void writeAdditionalFields(final Kryo kryo, final Output out, final StateType state)
        {

        }

        @Override
        public void write(final Kryo kryo, final Output out, final StateType state)
        {
            out.writeLong(state.epoch);

            KryoUtils.writeRange(out, state.range);
            kryo.writeObject(out, state.markers, ICommitLogMarkers.SERIALIZER);

            kryo.writeObject(out, state.serializationWrapper, InMemoryWatermarker.SerializationWrapper.Serializer.INSTANCE);
            writeAdditionalFields(kryo, out, state);
        }

        @Override
        public StateType read(Kryo kryo, Input in, Class<StateType> type)
        {
            final long epoch = in.readLong();
            final Range<BigInteger> range = KryoUtils.readRange(in);
            final ICommitLogMarkers markers = kryo.readObject(in, ICommitLogMarkers.class, ICommitLogMarkers.SERIALIZER);
            final InMemoryWatermarker.SerializationWrapper serializationWrapper = kryo.readObject(in, InMemoryWatermarker.SerializationWrapper.class, InMemoryWatermarker.SerializationWrapper.Serializer.INSTANCE);

            return newInstance(kryo, in, type, epoch, range, markers, serializationWrapper);
        }
    }

    public static <Type extends CdcState> Type deserialize(byte[] compressed,
                                                           Class<Type> tClass,
                                                           Serializer<Type> serializer)
    {
        try
        {
            return KryoUtils.deserialize(CdcKryoRegister.kryo(), CompressionUtil.INSTANCE.uncompress(compressed), tClass, serializer);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
