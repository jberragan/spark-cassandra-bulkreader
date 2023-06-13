package org.apache.cassandra.spark.sparksql;

import java.io.IOException;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.sparksql.filters.CdcOffset;
import org.apache.cassandra.spark.utils.streaming.SSTableSource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

public class CdcOffsetTests
{
    @Test
    public void testJsonSerialization() throws IOException
    {
        final CdcOffset offset = new CdcOffset(500L);
        final String json = CdcOffset.MAPPER.writeValueAsString(offset);
        assertNotNull(json);
        final CdcOffset deserialized = CdcOffset.fromJson(json);
        assertEquals(offset, deserialized);
    }

    @Test
    public void testJsonWithMarkers() throws IOException
    {
        final CassandraInstance inst1 = new CassandraInstance("0", "local-i1", "DC1");
        final CassandraInstance inst2 = new CassandraInstance("1", "local-i1", "DC1");
        final CassandraInstance inst3 = new CassandraInstance("2", "local-i1", "DC1");
        final CdcOffset offset = new CdcOffset(500L, Stream.of(
        new TestCommitLog(inst1, "CommitLog-6-12345.log", 500),
        new TestCommitLog(inst1, "CommitLog-6-12346.log", 20000),
        new TestCommitLog(inst1, "CommitLog-6-12347.log", 500000),
        new TestCommitLog(inst2, "CommitLog-6-12348.log", 500),
        new TestCommitLog(inst2, "CommitLog-6-12349.log", 20000),
        new TestCommitLog(inst2, "CommitLog-6-12350.log", 500000),
        new TestCommitLog(inst3, "CommitLog-6-12351.log", 500),
        new TestCommitLog(inst3, "CommitLog-6-12352.log", 20000),
        new TestCommitLog(inst3, "CommitLog-6-12353.log", 500000)
        ));
        final String json = CdcOffset.MAPPER.writeValueAsString(offset);
        assertNotNull(json);
        final CdcOffset deserialized = CdcOffset.fromJson(json);
        assertEquals(offset, deserialized);
    }

    private static class TestCommitLog implements CommitLog
    {
        private final CassandraInstance instance;
        private final String name;
        private final int maxOffset;

        public TestCommitLog(CassandraInstance instance, String name, int maxOffset)
        {
            this.instance = instance;
            this.name = name;
            this.maxOffset = maxOffset;
        }

        public String name()
        {
            return name;
        }

        public String path()
        {
            return "/cassandra/d1/cdc_raw/" + name;
        }

        public long maxOffset()
        {
            return maxOffset;
        }

        public long len()
        {
            return 67108864L;
        }

        public SSTableSource<? extends SSTable> source()
        {
            return null;
        }

        public CassandraInstance instance()
        {
            return instance;
        }

        public void close() throws Exception
        {

        }
    }
}
