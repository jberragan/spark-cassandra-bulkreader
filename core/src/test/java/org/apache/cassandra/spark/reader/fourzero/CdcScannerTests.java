package org.apache.cassandra.spark.reader.fourzero;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.cdc.watermarker.WatermarkerTests;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.CdcUpdate;
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
        final CdcUpdate update1 = WatermarkerTests.cdcUpdate(now);
        final CdcUpdate update2 = WatermarkerTests.cdcUpdate(now);
        final CdcUpdate update3 = WatermarkerTests.cdcUpdate(now);
        final List<CdcUpdate> updates = Arrays.asList(update1, update2, update3);
        test(updates, watermarker, 3, true);
        for (final CdcUpdate update : updates)
        {
            verify(watermarker, never()).recordReplicaCount(eq(update), anyInt());
        }
    }

    @Test
    public void testPublishedClQuorum()
    {
        final Watermarker watermarker = watermarker(false);
        final long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        final CdcUpdate update1 = WatermarkerTests.cdcUpdate(now);
        final CdcUpdate update2 = WatermarkerTests.cdcUpdate(now);
        final List<CdcUpdate> updates = Arrays.asList(update1, update2);
        test(updates, watermarker, 2, true);
        for (final CdcUpdate update : updates)
        {
            verify(watermarker, never()).recordReplicaCount(eq(update), anyInt());
        }
    }

    @Test
    public void testInsufficientReplicas()
    {
        final Watermarker watermarker = watermarker(false);
        final long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        final CdcUpdate update1 = WatermarkerTests.cdcUpdate(now);
        final List<CdcUpdate> updates = Collections.singletonList(update1);
        test(updates, watermarker, 2, false);
        for (final CdcUpdate update : updates)
        {
            verify(watermarker).recordReplicaCount(eq(update), eq(1));
        }
    }

    @Test
    public void testInsufficientReplicasLate()
    {
        final Watermarker watermarker = watermarker(false);
        final long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        final CdcUpdate update1 = WatermarkerTests.cdcUpdate(now);
        final List<CdcUpdate> updates = Collections.singletonList(update1);
        test(updates, watermarker, 2, false);
        for (final CdcUpdate update : updates)
        {
            verify(watermarker).recordReplicaCount(eq(update), eq(1));
        }
    }

    @Test
    public void testLateMutation()
    {
        final Watermarker watermarker = watermarker(true);
        final long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        final CdcUpdate update1 = WatermarkerTests.cdcUpdate(now);
        final CdcUpdate update2 = WatermarkerTests.cdcUpdate(now);
        final List<CdcUpdate> updates = Arrays.asList(update1, update2);
        test(updates, watermarker, 2, true);
        verify(watermarker).untrackReplicaCount(eq(update1));
    }

    private void test(List<CdcUpdate> updates,
                      Watermarker watermarker,
                      int minimumReplicasPerMutation,
                      boolean shouldPublish)
    {
        assertEquals(shouldPublish, CdcScannerBuilder.filter(updates, minimumReplicasPerMutation, watermarker, Stats.DoNothingStats.INSTANCE));
    }

    private Watermarker watermarker(boolean isLate)
    {
        final Watermarker watermarker = mock(Watermarker.class);
        when(watermarker.seenBefore(any(CdcUpdate.class))).thenReturn(isLate);
        return watermarker;
    }

    @Test
    public void testCommitLogFilename()
    {
        assertEquals(12345L, Objects.requireNonNull(CdcScannerBuilder.extractSegmentId("CommitLog-4-12345.log")).longValue());
        assertEquals(12345L, Objects.requireNonNull(CdcScannerBuilder.extractSegmentId("CommitLog-12345.log")).longValue());
        assertEquals(1646094405659L, Objects.requireNonNull(CdcScannerBuilder.extractSegmentId("CommitLog-7-1646094405659.log")).longValue());
        assertEquals(1646094405659L, Objects.requireNonNull(CdcScannerBuilder.extractSegmentId("CommitLog-1646094405659.log")).longValue());
        assertEquals(1646094405659L, Objects.requireNonNull(CdcScannerBuilder.extractSegmentId("CommitLog-242-1646094405659.log")).longValue());
        assertNull(CdcScannerBuilder.extractSegmentId("CommitLog-123-abcd.log"));
        assertNull(CdcScannerBuilder.extractSegmentId("CommitLog-abcd.log"));
        assertNull(CdcScannerBuilder.extractSegmentId("CommitLog.log"));
        assertNull(CdcScannerBuilder.extractSegmentId("abcd"));
    }
}