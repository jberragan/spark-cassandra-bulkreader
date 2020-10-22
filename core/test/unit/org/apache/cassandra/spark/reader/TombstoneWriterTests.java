package org.apache.cassandra.spark.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.DataLayer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;

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
 * Test we can write out partition, row and range tombstones to SSTables using the SSTableTombstoneWriter
 */
public class TombstoneWriterTests
{
    private static final int NUM_ROWS = 50;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testPartitionTombstone()
    {
        final TestSchema schema = TestSchema.basicBuilder().withDeleteFields("a =").build();
        qt().forAll(TestUtils.tombstoneVersions())
            .checkAssert((version) -> TestUtils.runTest(version, (partitioner, dir, ignore) -> {
                // write tombstone sstable
                TestUtils.writeTombstoneSSTable(partitioner, version, dir, schema.createStmt, schema.deleteStmt, (writer) -> {
                    for (int i = 0; i < NUM_ROWS; i++)
                    {
                        writer.write(i);
                    }
                });

                // convert sstable to json
                final Path dataDbFile = TestUtils.getFirstFileType(dir, DataLayer.FileType.DATA);
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                TestUtils.sstableToJson(version, dataDbFile, out);
                final JsonNode node;
                try
                {
                    node = MAPPER.readTree(out.toByteArray());
                }
                catch (final IOException e)
                {
                    throw new RuntimeException(e);
                }

                // verify sstable contains partition tombstones
                assertEquals(NUM_ROWS, node.size());
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    final JsonNode partition = node.get(i).get("partition");
                    final int key = partition.get("key").get(0).asInt();
                    assertTrue(key >= 0 && key < NUM_ROWS);
                    assertTrue(node.get(i).has("rows"));
                    assertTrue(partition.has("deletion_info"));
                    assertTrue(partition.get("deletion_info").has("marked_deleted"));
                    assertTrue(partition.get("deletion_info").has("local_delete_time"));
                }
            }));
    }

    @Test
    public void testRowTombstone()
    {
        final TestSchema schema = TestSchema.basicBuilder().withDeleteFields("a =", "b =").build();
        qt().forAll(TestUtils.tombstoneVersions())
            .checkAssert((version) -> TestUtils.runTest(version, (partitioner, dir, ignore) -> {
                // write tombstone sstable
                TestUtils.writeTombstoneSSTable(partitioner, version, dir, schema.createStmt, schema.deleteStmt, (writer) -> {
                    for (int i = 0; i < NUM_ROWS; i++)
                    {
                        writer.write(i, i);
                    }
                });

                // convert sstable to json
                final Path dataDbFile = TestUtils.getFirstFileType(dir, DataLayer.FileType.DATA);
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                TestUtils.sstableToJson(version, dataDbFile, out);
                final JsonNode node;
                try
                {
                    node = MAPPER.readTree(out.toByteArray());
                }
                catch (final IOException e)
                {
                    throw new RuntimeException(e);
                }

                // verify sstable contains row tombstones
                assertEquals(NUM_ROWS, node.size());
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    final JsonNode partition = node.get(i).get("partition");
                    final int key = partition.get("key").get(0).asInt();
                    assertTrue(key >= 0 && key < NUM_ROWS);
                    assertFalse(partition.has("deletion_info"));

                    assertTrue(node.get(i).has("rows"));
                    final JsonNode row = node.get(i).get("rows").get(0);
                    assertEquals("row", row.get("type").asText());
                    assertEquals(key, row.get("clustering").get(0).asInt());
                    assertTrue(row.has("deletion_info"));
                    assertTrue(row.get("deletion_info").has("marked_deleted"));
                    assertTrue(row.get("deletion_info").has("local_delete_time"));
                }
            }));
    }

    @Test
    public void testRangeTombstone()
    {
        final TestSchema schema = TestSchema.basicBuilder().withDeleteFields("a =", "b >=", "b <").build();
        qt().forAll(TestUtils.tombstoneVersions())
            .checkAssert((version) -> TestUtils.runTest(version, (partitioner, dir, ignore) -> {
                // write tombstone sstable
                TestUtils.writeTombstoneSSTable(partitioner, version, dir, schema.createStmt, schema.deleteStmt, (writer) -> {
                    for (int i = 0; i < NUM_ROWS; i++)
                    {
                        writer.write(i, 50, 999);
                    }
                });

                // convert sstable to json
                final Path dataDbFile = TestUtils.getFirstFileType(dir, DataLayer.FileType.DATA);
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                TestUtils.sstableToJson(version, dataDbFile, out);
                final JsonNode node;
                try
                {
                    node = MAPPER.readTree(out.toByteArray());
                }
                catch (final IOException e)
                {
                    throw new RuntimeException(e);
                }

                // verify sstable contains range tombstones
                assertEquals(NUM_ROWS, node.size());
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    final JsonNode partition = node.get(i).get("partition");
                    final int key = partition.get("key").get(0).asInt();
                    assertTrue(key >= 0 && key < NUM_ROWS);
                    assertFalse(partition.has("deletion_info"));

                    assertTrue(node.get(i).has("rows"));
                    assertEquals(2, node.get(i).get("rows").size());

                    final JsonNode row1 = node.get(i).get("rows").get(0);
                    assertEquals("range_tombstone_bound", row1.get("type").asText());
                    final JsonNode start = row1.get("start");
                    assertEquals("inclusive", start.get("type").asText());
                    assertEquals(50, start.get("clustering").get(0).asInt());
                    assertTrue(start.has("deletion_info"));
                    assertTrue(start.get("deletion_info").has("marked_deleted"));
                    assertTrue(start.get("deletion_info").has("local_delete_time"));

                    final JsonNode row2 = node.get(i).get("rows").get(1);
                    assertEquals("range_tombstone_bound", row2.get("type").asText());
                    final JsonNode end = row2.get("end");
                    assertEquals("exclusive", end.get("type").asText());
                    assertEquals(999, end.get("clustering").get(0).asInt());
                    assertTrue(end.has("deletion_info"));
                    assertTrue(end.get("deletion_info").has("marked_deleted"));
                    assertTrue(end.get("deletion_info").has("local_delete_time"));
                }
            }));
    }
}
