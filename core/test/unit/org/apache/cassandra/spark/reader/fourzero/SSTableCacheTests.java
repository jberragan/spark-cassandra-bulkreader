package org.apache.cassandra.spark.reader.fourzero;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import org.apache.cassandra.spark.TestDataLayer;
import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.DecoratedKey;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.BloomFilter;

import static org.apache.cassandra.spark.TestUtils.getFileType;
import static org.apache.cassandra.spark.TestUtils.runTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
public class SSTableCacheTests
{
    @Test
    public void testCache()
    {
        runTest((partitioner, dir, bridge) -> {
            // write an SSTable
            final TestSchema schema = TestSchema.basic(bridge);
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> IntStream.range(0, 10).forEach(i -> writer.write(i, 0, i)));
            TestUtils.writeSSTable(bridge, dir, partitioner, schema, (writer) -> IntStream.range(20, 100).forEach(i -> writer.write(i, 1, i)));
            final List<Path> dataFiles = getFileType(dir, DataLayer.FileType.DATA).collect(Collectors.toList());
            final Path dataFile0 = dataFiles.get(0);
            final Path dataFile1 = dataFiles.get(1);
            final TestDataLayer dataLayer = new TestDataLayer(bridge, dataFiles);
            final List<DataLayer.SSTable> sstables = dataLayer.listSSTables().collect(Collectors.toList());
            final TableMetadata metaData = new FourZeroSchemaBuilder(schema.createStmt, schema.keyspace, new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy, ImmutableMap.of("replication_factor", 1)), partitioner).tableMetaData();
            final DataLayer.SSTable ssTable0 = sstables.get(0);
            assertFalse(SSTableCache.INSTANCE.containsSummary(ssTable0));
            assertFalse(SSTableCache.INSTANCE.containsIndex(ssTable0));
            assertFalse(SSTableCache.INSTANCE.containsStats(ssTable0));

            final SummaryDbUtils.Summary key1 = SSTableCache.INSTANCE.keysFromSummary(metaData, ssTable0);
            assertNotNull(key1);
            assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable0));
            assertFalse(SSTableCache.INSTANCE.containsIndex(ssTable0));
            assertFalse(SSTableCache.INSTANCE.containsStats(ssTable0));
            assertFalse(SSTableCache.INSTANCE.containsFilter(ssTable0));

            final Pair<DecoratedKey, DecoratedKey> key2 = SSTableCache.INSTANCE.keysFromIndex(metaData, ssTable0);
            assertEquals(key1.first(), key2.getLeft());
            assertEquals(key1.last(), key2.getRight());
            assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable0));
            assertTrue(SSTableCache.INSTANCE.containsIndex(ssTable0));
            assertFalse(SSTableCache.INSTANCE.containsStats(ssTable0));
            assertFalse(SSTableCache.INSTANCE.containsFilter(ssTable0));

            final Descriptor descriptor0 = Descriptor.fromFilename(new File(String.format("./%s/%s", schema.keyspace, schema.table), dataFile0.getFileName().toString()));
            final Map<MetadataType, MetadataComponent> componentMap = SSTableCache.INSTANCE.componentMapFromStats(ssTable0, descriptor0);
            assertNotNull(componentMap);
            assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable0));
            assertTrue(SSTableCache.INSTANCE.containsIndex(ssTable0));
            assertTrue(SSTableCache.INSTANCE.containsStats(ssTable0));
            assertFalse(SSTableCache.INSTANCE.containsFilter(ssTable0));
            assertEquals(componentMap, SSTableCache.INSTANCE.componentMapFromStats(ssTable0, descriptor0));

            final BloomFilter filter = SSTableCache.INSTANCE.bloomFilter(ssTable0, descriptor0);
            assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable0));
            assertTrue(SSTableCache.INSTANCE.containsIndex(ssTable0));
            assertTrue(SSTableCache.INSTANCE.containsStats(ssTable0));
            assertTrue(SSTableCache.INSTANCE.containsFilter(ssTable0));
            assertTrue(filter.isPresent(key1.first()));
            assertTrue(filter.isPresent(key1.last()));

            final DataLayer.SSTable ssTable1 = sstables.get(1);
            final Descriptor descriptor1 = Descriptor.fromFilename(new File(String.format("./%s/%s", schema.keyspace, schema.table), dataFile1.getFileName().toString()));
            assertFalse(SSTableCache.INSTANCE.containsSummary(ssTable1));
            assertFalse(SSTableCache.INSTANCE.containsIndex(ssTable1));
            assertFalse(SSTableCache.INSTANCE.containsStats(ssTable1));
            assertFalse(SSTableCache.INSTANCE.containsFilter(ssTable1));
            final SummaryDbUtils.Summary key3 = SSTableCache.INSTANCE.keysFromSummary(metaData, ssTable1);
            assertNotEquals(key1.first(), key3.first());
            assertNotEquals(key1.last(), key3.last());
            final Pair<DecoratedKey, DecoratedKey> key4 = SSTableCache.INSTANCE.keysFromIndex(metaData, ssTable1);
            assertNotEquals(key1.first(), key4.getLeft());
            assertNotEquals(key1.last(), key4.getRight());
            assertEquals(SSTableCache.INSTANCE.keysFromSummary(metaData, ssTable1).first(), SSTableCache.INSTANCE.keysFromIndex(metaData, ssTable1).getLeft());
            assertEquals(SSTableCache.INSTANCE.keysFromSummary(metaData, ssTable1).last(), SSTableCache.INSTANCE.keysFromIndex(metaData, ssTable1).getRight());
            assertNotEquals(componentMap, SSTableCache.INSTANCE.componentMapFromStats(ssTable1, descriptor1));
            final Pair<DecoratedKey, DecoratedKey> key5 = SSTableCache.INSTANCE.keysFromIndex(metaData, ssTable1);
            assertTrue(SSTableCache.INSTANCE.bloomFilter(ssTable1, descriptor1).isPresent(key5.getLeft()));
            assertTrue(SSTableCache.INSTANCE.containsSummary(ssTable1));
            assertTrue(SSTableCache.INSTANCE.containsIndex(ssTable1));
            assertTrue(SSTableCache.INSTANCE.containsStats(ssTable1));
            assertTrue(SSTableCache.INSTANCE.containsFilter(ssTable1));
        });
    }
}
