package org.apache.cassandra.spark.reader.fourzero;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.spark.stats.Stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

public class CdcScannerTests
{
    @Test
    public void testPublishedClAll()
    {
        final Watermarker watermarker = watermarker(false);
        final long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        final PartitionUpdateWrapper update1 = CdcScannerTests.cdcUpdate(now);
        final PartitionUpdateWrapper update2 = CdcScannerTests.cdcUpdate(now);
        final PartitionUpdateWrapper update3 = CdcScannerTests.cdcUpdate(now);
        final List<PartitionUpdateWrapper> updates = Arrays.asList(update1, update2, update3);
        test(updates, watermarker, (ks) -> 3, true);
        for (final PartitionUpdateWrapper update : updates)
        {
            verify(watermarker, never()).recordReplicaCount(eq(update), anyInt());
        }
    }

    @Test
    public void testPublishedClQuorum()
    {
        final Watermarker watermarker = watermarker(false);
        final long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        final PartitionUpdateWrapper update1 = CdcScannerTests.cdcUpdate(now);
        final PartitionUpdateWrapper update2 = CdcScannerTests.cdcUpdate(now);
        final List<PartitionUpdateWrapper> updates = Arrays.asList(update1, update2);
        test(updates, watermarker, (ks) -> 2, true);
        for (final PartitionUpdateWrapper update : updates)
        {
            verify(watermarker, never()).recordReplicaCount(eq(update), anyInt());
        }
    }

    @Test
    public void testInsufficientReplicas()
    {
        final Watermarker watermarker = watermarker(false);
        final long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        final PartitionUpdateWrapper update1 = CdcScannerTests.cdcUpdate(now);
        final List<PartitionUpdateWrapper> updates = Collections.singletonList(update1);
        test(updates, watermarker, (ks) -> 2, false);
        for (final PartitionUpdateWrapper update : updates)
        {
            verify(watermarker).recordReplicaCount(eq(update), eq(1));
        }
    }

    @Test
    public void testInsufficientReplicasLate()
    {
        final Watermarker watermarker = watermarker(false);
        final long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        final PartitionUpdateWrapper update1 = CdcScannerTests.cdcUpdate(now);
        final List<PartitionUpdateWrapper> updates = Collections.singletonList(update1);
        test(updates, watermarker, (ks) -> 2, false);
        for (final PartitionUpdateWrapper update : updates)
        {
            verify(watermarker).recordReplicaCount(eq(update), eq(1));
        }
    }

    @Test
    public void testLateMutation()
    {
        final Watermarker watermarker = watermarker(true);
        final long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        final PartitionUpdateWrapper update1 = CdcScannerTests.cdcUpdate(now);
        final PartitionUpdateWrapper update2 = CdcScannerTests.cdcUpdate(now);
        final List<PartitionUpdateWrapper> updates = Arrays.asList(update1, update2);
        test(updates, watermarker, (ks) -> 2, true);
        verify(watermarker).untrackReplicaCount(eq(update1));
    }

    private void test(List<PartitionUpdateWrapper> updates,
                      Watermarker watermarker,
                      Function<String, Integer> minimumReplicasFunc,
                      boolean shouldPublish)
    {
        assertEquals(shouldPublish, CdcScannerBuilder.filter(updates, minimumReplicasFunc, watermarker, Stats.DoNothingStats.INSTANCE));
    }

    private Watermarker watermarker(boolean isLate)
    {
        final Watermarker watermarker = mock(Watermarker.class);
        when(watermarker.seenBefore(any(PartitionUpdateWrapper.class))).thenReturn(isLate);
        return watermarker;
    }

    @Test
    public void testCommitLogFilename()
    {
        testCommitLogRegex("CommitLog-6-12345.log", 10, 12345L);
        testCommitLogRegex("CommitLog-12345.log", 10, 12345L);
        testCommitLogRegex("CommitLog-7-1646094405659.log", 12, 1646094405659L);
        testCommitLogRegex("CommitLog-1646094405659.log", 10, 1646094405659L);
        testCommitLogRegex("CommitLog-6-abcd.log", null, null);
        testCommitLogRegex("CommitLog-abcd.log", null, null);
        testCommitLogRegex("CommitLog.log", null, null);
        testCommitLogRegex("abcd", null, null);
    }

    @Test(expected = IllegalStateException.class)
    public void testInvalidVersion()
    {
        testCommitLogRegex("CommitLog-242-1646094405659.log", null, null);
    }

    private static void testCommitLogRegex(String filename, Integer expectedVersion, Long expectedSegmentId)
    {
        final Optional<Pair<Integer, Long>> pair = CommitLog.extractVersionAndSegmentId(filename);
        if (!pair.isPresent())
        {
            assertNull(expectedVersion);
            assertNull(expectedSegmentId);
            return;
        }
        assertEquals(expectedVersion, pair.get().getLeft());
        assertEquals(expectedSegmentId, pair.get().getRight());
    }

    public static PartitionUpdateWrapper cdcUpdate(long timestamp)
    {
        final PartitionUpdateWrapper update = mock(PartitionUpdateWrapper.class);
        when(update.maxTimestampMicros()).thenReturn(timestamp * 1000L); // in micros
        when(update.partitionUpdate()).thenReturn(PartitionUpdate.emptyUpdate(null, null));
        return update;
    }
}
