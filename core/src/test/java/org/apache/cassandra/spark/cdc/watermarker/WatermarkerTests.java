package org.apache.cassandra.spark.cdc.watermarker;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.PartitionUpdateWrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
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

public class WatermarkerTests
{
    private static final String JOB_ID = "101";

    @BeforeClass
    public static void setup()
    {
        InMemoryWatermarker.TEST_THREAD_NAME = Thread.currentThread().getName();
    }

    @AfterClass
    public static void tearDown()
    {
        InMemoryWatermarker.TEST_THREAD_NAME = null;
    }

    @Test
    public void testLateMutation()
    {
        final Watermarker watermarker = InMemoryWatermarker.INSTANCE.instance(JOB_ID);
        watermarker.clear();

        final CassandraInstance in1 = new CassandraInstance("0L", "inst1", "DC1");

        // verify late mutations track earliest marker
        final long now = System.currentTimeMillis();
        final PartitionUpdateWrapper mutation1 = cdcUpdate(now);
        watermarker.recordReplicaCount(mutation1, 2);
        final PartitionUpdateWrapper mutation2 = cdcUpdate(now);
        watermarker.recordReplicaCount(mutation2, 2);
        final PartitionUpdateWrapper mutation3 = cdcUpdate(now);
        watermarker.recordReplicaCount(mutation3, 2);
        final PartitionUpdateWrapper mutation4 = cdcUpdate(now);
        watermarker.recordReplicaCount(mutation4, 2);

        assertTrue(watermarker.seenBefore(mutation1));
        assertTrue(watermarker.seenBefore(mutation2));
        assertTrue(watermarker.seenBefore(mutation3));
        assertTrue(watermarker.seenBefore(mutation4));
        assertEquals(2, watermarker.replicaCount(mutation1));
        assertEquals(2, watermarker.replicaCount(mutation2));
        assertEquals(2, watermarker.replicaCount(mutation3));
        assertEquals(2, watermarker.replicaCount(mutation4));

        // clear mutations and verify watermark tracks last offset in order
        watermarker.untrackReplicaCount(mutation2);
        watermarker.untrackReplicaCount(mutation3);
        watermarker.untrackReplicaCount(mutation4);
        watermarker.untrackReplicaCount(mutation1);

        assertEquals(0, watermarker.replicaCount(mutation1));
        assertEquals(0, watermarker.replicaCount(mutation2));
        assertEquals(0, watermarker.replicaCount(mutation3));
        assertEquals(0, watermarker.replicaCount(mutation4));
    }

    @Test
    public void testPublishedMutation()
    {
        final Watermarker watermarker = InMemoryWatermarker.INSTANCE.instance(JOB_ID);
        watermarker.clear();
        final CassandraInstance in1 = new CassandraInstance("0L", "inst1", "DC1");
        final long now = System.currentTimeMillis();
        CommitLog.Marker end = new CommitLog.Marker(in1, 5L, 600);

        final PartitionUpdateWrapper lateMutation1 = cdcUpdate(now);
        watermarker.recordReplicaCount(lateMutation1, 2);
        final PartitionUpdateWrapper lateMutation2 = cdcUpdate(now);
        watermarker.recordReplicaCount(lateMutation2, 2);
        final PartitionUpdateWrapper lateMutation3 = cdcUpdate(now);
        watermarker.recordReplicaCount(lateMutation3, 2);

        watermarker.untrackReplicaCount(lateMutation1);
        watermarker.untrackReplicaCount(lateMutation2);
        watermarker.untrackReplicaCount(lateMutation3);
    }

    public static PartitionUpdateWrapper cdcUpdate(long timestamp)
    {
        final PartitionUpdateWrapper update = mock(PartitionUpdateWrapper.class);
        when(update.maxTimestampMicros()).thenReturn(timestamp * 1000L); // in micros
        return update;
    }
}
