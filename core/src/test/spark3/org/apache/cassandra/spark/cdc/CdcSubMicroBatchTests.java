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

package org.apache.cassandra.spark.cdc;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.reader.CassandraBridge;

import static org.apache.cassandra.spark.cdc.CdcTester.testWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;

public class CdcSubMicroBatchTests extends VersionRunner
{
    @ClassRule
    public static TemporaryFolder DIR = new TemporaryFolder();

    @BeforeClass
    public static void setup()
    {
        CdcTester.setup(DIR, 1);
    }

    @AfterClass
    public static void tearDown()
    {
        CdcTester.tearDown();
    }

    public CdcSubMicroBatchTests(CassandraBridge.CassandraVersion version)
    {
        super(version);
    }

    public static final TestStats STATS = new TestStats();

    @Test
    public void moreThanOneSubMicroBatch()
    {
        qt().withExamples(1).forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((t1, t2, t3) -> {
                STATS.reset();
                testWith(bridge, DIR, TestSchema.builder()
                                                .withPartitionKey("pk", bridge.uuid())
                                                .withClusteringKey("ck1", t1)
                                                .withClusteringKey("ck2", t2)
                                                .withClusteringKey("ck3", t3)
                                                .withColumn("c1", bridge.bigint())
                                                .withColumn("c2", bridge.text()))
                .withStatsClass(CdcSubMicroBatchTests.class.getName() + ".STATS")
                .withNumRows(50000)
                .withExpectedNumRows(50000)
                .withStatsClass(CdcSubMicroBatchTests.class.getName() + ".STATS")
                .withCdcEventChecker((testRows, events) -> {
                    assertEquals(testRows.size(), events.size());
                    assertTrue(STATS.getCounterValue(TestStats.TEST_CDC_SUB_BATCHES_PER_MICRO_BATCH_COUNT) > 1);
                    assertEquals(testRows.size(), STATS.getStats(TestStats.TEST_CDC_MUTATIONS_READ_PER_SUB_MICRO_BATCH)
                                                       .stream()
                                                       .mapToLong(Long::longValue)
                                                       .sum());
                })
                .run();
            });
    }
}
