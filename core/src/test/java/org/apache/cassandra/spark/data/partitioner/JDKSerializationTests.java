package org.apache.cassandra.spark.data.partitioner;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import com.google.common.collect.Range;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.cdc.CommitLog;
import org.apache.cassandra.spark.cdc.CommitLogProvider;
import org.apache.cassandra.spark.cdc.TableIdLookup;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.PartitionedDataLayer;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.sparksql.filters.SerializableCommitLog;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.TestUtils.deserialize;
import static org.apache.cassandra.spark.TestUtils.serialize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
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
public class JDKSerializationTests extends VersionRunner
{

    public JDKSerializationTests(CassandraVersion version)
    {
        super(version);
    }

    @Test
    public void testPartitionedDataLayer()
    {
        final CassandraRing ring = TestUtils.createRing(Partitioner.Murmur3Partitioner, 1024);
        final TestSchema schema = TestSchema.basic(bridge);
        final CqlTable cqlTable = new CqlTable(schema.keyspace, schema.table, schema.createStmt, ring.replicationFactor(), Collections.emptyList());
        final PartitionedDataLayer partitionedDataLayer = new TestPartitionedDataLayer(4, 16, null, ring, cqlTable);
        final byte[] ar = serialize(partitionedDataLayer);
        final TestPartitionedDataLayer deserialized = deserialize(ar, TestPartitionedDataLayer.class);
        assertNotNull(deserialized);
        assertNotNull(deserialized.ring());
        assertNotNull(deserialized.partitioner());
        assertNotNull(deserialized.tokenPartitioner());
        assertEquals(Partitioner.Murmur3Partitioner, deserialized.partitioner());
    }

    @Test
    public void testCqlFieldSet()
    {
        final CqlField.CqlSet setType = bridge.set(bridge.text());
        final CqlField field = new CqlField(true, false, false, RandomStringUtils.randomAlphanumeric(5, 20), setType, 10);
        final byte[] ar = serialize(field);
        final CqlField deserialized = deserialize(ar, CqlField.class);
        assertEquals(field, deserialized);
        assertEquals(field.name(), deserialized.name());
        assertEquals(field.type(), deserialized.type());
        assertEquals(field.pos(), deserialized.pos());
        assertEquals(field.isPartitionKey(), deserialized.isPartitionKey());
        assertEquals(field.isClusteringColumn(), deserialized.isClusteringColumn());
    }

    @Test
    public void testCqlUdt()
    {
        final CqlField.CqlUdt udt1 = bridge
                                     .udt("udt_keyspace", "udt_table")
                                     .withField("c", bridge.text())
                                     .withField("b", bridge.timestamp())
                                     .withField("a", bridge.bigint())
                                     .build();
        final CqlField.CqlUdt udt2 = bridge
                                     .udt("udt_keyspace", "udt_table")
                                     .withField("a", bridge.bigint())
                                     .withField("b", bridge.timestamp())
                                     .withField("c", bridge.text())
                                     .build();
        assertNotEquals(udt2, udt1);
        final byte[] b = serialize(udt1);
        final CqlField.CqlUdt deserialized = deserialize(b, CqlField.CqlUdt.class);
        assertEquals(udt1, deserialized);
        assertNotEquals(udt2, deserialized);
        for (int i = 0; i < deserialized.fields().size(); i++)
        {
            assertEquals(udt1.field(i), deserialized.field(i));
        }
    }

    public static class TestPartitionedDataLayer extends PartitionedDataLayer
    {
        private final CassandraRing ring;
        private final CqlTable cqlTable;
        private final TokenPartitioner tokenPartitioner;
        private final String jobId;

        public TestPartitionedDataLayer(final int defaultParallelism, final int numCores, @Nullable final String dc,
                                        final CassandraRing ring, final CqlTable cqlTable)
        {
            super(ConsistencyLevel.LOCAL_QUORUM, dc);
            this.ring = ring;
            this.cqlTable = cqlTable;
            this.tokenPartitioner = new TokenPartitioner(ring, defaultParallelism, numCores);
            this.jobId = UUID.randomUUID().toString();
        }

        public CompletableFuture<Stream<SSTable>> listInstance(int partitionId, @NotNull Range<BigInteger> range, @NotNull CassandraInstance instance)
        {
            return CompletableFuture.completedFuture(Stream.of());
        }

        public CassandraRing ring()
        {
            return ring;
        }

        public TokenPartitioner tokenPartitioner()
        {
            return tokenPartitioner;
        }

        public CompletableFuture<List<CommitLog>> listCommitLogs(CassandraInstance instance)
        {
            throw new NotImplementedException("Test listCommitLogs not implemented yet");
        }

        protected ExecutorService executorService()
        {
            return SingleReplicaTests.EXECUTOR;
        }

        public String jobId()
        {
            return jobId;
        }

        @Override
        public CommitLog toLog(final int partitionId, CassandraInstance instance, SerializableCommitLog commitLog)
        {
            throw new NotImplementedException("Test toLog method not implemented yet");
        }

        public CassandraVersion version()
        {
            return CassandraVersion.FOURZERO;
        }

        public CqlTable cqlTable()
        {
            return cqlTable;
        }

        public Set<CqlTable> cdcTables()
        {
            return new HashSet<>(Collections.singletonList(cqlTable));
        }

        public CommitLogProvider commitLogs()
        {
            throw new NotImplementedException("Test CommitLogProvider not implemented yet");
        }

        public ReplicationFactor rf(String keyspace)
        {
            return ring.replicationFactor();
        }

        public TableIdLookup tableIdLookup()
        {
            throw new NotImplementedException("Test TableIdLookup not implemented yet");
        }
    }
}
