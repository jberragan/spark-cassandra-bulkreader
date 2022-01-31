package org.apache.cassandra.spark.data;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.fourzero.SchemaBuilderTests;

import static org.apache.cassandra.spark.reader.CassandraBridge.CassandraVersion.THREEZERO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
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
public class LocalDataLayerTests
{
    @Test
    public void testLocalDataLayer() throws IOException
    {
        final Path d1 = Files.createTempDirectory("d1"), d2 = Files.createTempDirectory("d2"), d3 = Files.createTempDirectory("d3"), d4 = Files.createTempDirectory("d4");
        final LocalDataLayer dataLayer = new LocalDataLayer(THREEZERO, "backup_test", SchemaBuilderTests.SCHEMA_TXT, Stream.of(d1, d2, d3, d4).map(d -> d.toAbsolutePath().toString()).toArray(String[]::new));
        assertEquals(THREEZERO, dataLayer.version());
        assertEquals(1, dataLayer.partitionCount());
        assertNotNull(dataLayer.cqlSchema());
        assertTrue(dataLayer.isInPartition(BigInteger.ZERO, ByteBuffer.wrap(new byte[0])));
        assertEquals(Partitioner.Murmur3Partitioner, dataLayer.partitioner());
        final SSTablesSupplier ssTables = dataLayer.sstables(new ArrayList<>());
        assertNotNull(ssTables);
        assertTrue(ssTables.openAll((sstable, isRepairPrimary) -> null).isEmpty());
    }

    @Test
    public void testEquality()
    {
        final LocalDataLayer d1 = new LocalDataLayer(THREEZERO, "backup_test", SchemaBuilderTests.SCHEMA_TXT, "/cassandra/d1/data/backup_test/sbr_test/snapshot/snapshotName/", "/cassandra/d2/data/backup_test/sbr_test/snapshot/snapshotName/", "/cassandra/d3/data/backup_test/sbr_test/snapshot/snapshotName/", "/cassandra/d4/data/backup_test/sbr_test/snapshot/snapshotName/");
        final LocalDataLayer d2 = new LocalDataLayer(THREEZERO, "backup_test", SchemaBuilderTests.SCHEMA_TXT, "/cassandra/d1/data/backup_test/sbr_test/snapshot/snapshotName/", "/cassandra/d2/data/backup_test/sbr_test/snapshot/snapshotName/", "/cassandra/d3/data/backup_test/sbr_test/snapshot/snapshotName/", "/cassandra/d4/data/backup_test/sbr_test/snapshot/snapshotName/");
        assertNotSame(d1, d2);
        assertEquals(d1, d1);
        assertEquals(d2, d2);
        assertNotEquals(null, d2);
        assertNotEquals(new ArrayList<>(), d1);
        assertEquals(d1, d2);
        assertEquals(d1.hashCode(), d2.hashCode());
    }
}
