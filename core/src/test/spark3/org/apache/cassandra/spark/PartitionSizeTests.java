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

package org.apache.cassandra.spark;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.spark.SparkTestUtils.NUM_COLS;
import static org.apache.cassandra.spark.SparkTestUtils.NUM_ROWS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PartitionSizeTests extends VersionRunner
{
    public PartitionSizeTests(CassandraVersion version)
    {
        super(version);
    }

    @Test
    public void testReadingPartitionSize()
    {
        SparkTestUtils.runTest(version, (partitioner, dir, bridge) -> {
            final TestSchema schema = TestSchema.builder()
                                                .withPartitionKey("a", bridge.text())
                                                .withClusteringKey("b", bridge.aInt())
                                                .withColumn("c", bridge.aInt())
                                                .withColumn("d", bridge.text()).build();

            Map<String, Integer> sizes = new HashMap<>(NUM_ROWS);
            SparkTestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> {
                for (int i = 0; i < NUM_ROWS; i++)
                {
                    final String key = UUID.randomUUID().toString();
                    int size = 0;
                    for (int j = 0; j < NUM_COLS; j++)
                    {
                        final String str = TestUtils.randomLowEntropyString();
                        writer.write(key, j, i + j, str);
                        size += 4 + 4 + str.getBytes(StandardCharsets.UTF_8).length;
                    }
                    sizes.put(key, size);
                }
            });

            final Dataset<Row> ds = SparkTestUtils.openLocalPartitionSizeSource(partitioner, dir, schema.keyspace, schema.createStmt, version, Collections.emptySet(), null);
            final List<Row> rows = ds.collectAsList();
            assertEquals(NUM_ROWS, rows.size());
            for (final Row row : rows)
            {
                final String key = row.getString(0);
                final long uncompressed = row.getLong(1);
                final long compressed = row.getLong(2);
                assertTrue(sizes.containsKey(key));
                final long len = sizes.get(key);
                assertTrue(len < uncompressed);
                assertTrue(Math.abs(uncompressed - len) < 500); // uncompressed size should be ~len size but with a fixed overhead
                assertTrue(compressed < uncompressed);
                assertTrue(compressed / (float) uncompressed < 0.1);
            }
        });
    }
}
