package org.apache.cassandra.spark.sparksql;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.data.VersionRunner;
import org.apache.cassandra.spark.stats.Stats;

import org.junit.Test;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.reader.Rid;
import org.apache.cassandra.spark.utils.ColumnTypes;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
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

public class SparkRowIteratorTests extends VersionRunner
{
    private static final int NUM_ROWS = 50;

    public SparkRowIteratorTests(CassandraBridge.CassandraVersion version)
    {
        super(version);
    }

    @Test
    public void testBasicKeyValue()
    {
        // i.e. "create table keyspace.table (a %s, b %s, primary key(a));"
        qt()
        .forAll(TestUtils.versions(), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
        .checkAssert((version, t1, t2) -> runTest(version, TestSchema.builder().withPartitionKey("a", t1).withColumn("b", t2).build()));
    }

    @Test
    public void testMultiPartitionKeys()
    {
        qt()
        .forAll(TestUtils.versions(), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
        .checkAssert((version, t1, t2, t3) -> runTest(version, TestSchema.builder().withPartitionKey("a", t1).withPartitionKey("b", t2).withPartitionKey("c", t3).withColumn("d", bridge.bigint()).build()));
    }

    @Test
    public void testBasicClusteringKeyThreeZero()
    {
        qt()
        .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.sortOrder())
        .checkAssert((t1, t2, t3, so) -> runTest(CassandraBridge.CassandraVersion.THREEZERO, TestSchema.builder().withPartitionKey("a", t1).withClusteringKey("b", t2).withColumn("c", t3).withSortOrder(so).build()));
    }

    @Test
    public void testBasicClusteringKeyFourZero()
    {
        qt()
        .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.sortOrder())
        .checkAssert((t1, t2, t3, so) -> runTest(CassandraBridge.CassandraVersion.FOURZERO, TestSchema.builder().withPartitionKey("a", t1).withClusteringKey("b", t2).withColumn("c", t3).withSortOrder(so).build()));
    }

    @Test
    public void testMultiClusteringKeyThreeZero()
    {
        qt()
        .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.sortOrder(), TestUtils.sortOrder())
        .checkAssert((t1, t2, so1, so2) -> runTest(CassandraBridge.CassandraVersion.THREEZERO, TestSchema.builder().withPartitionKey("a", bridge.bigint()).withClusteringKey("b", t1).withClusteringKey("c", t2).withColumn("d", bridge.bigint()).withSortOrder(so1).withSortOrder(so2).build()));
    }

    @Test
    public void testMultiClusteringKeyFourZero()
    {
        qt()
        .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge), TestUtils.sortOrder(), TestUtils.sortOrder())
        .checkAssert((t1, t2, so1, so2) -> runTest(CassandraBridge.CassandraVersion.FOURZERO, TestSchema.builder().withPartitionKey("a", bridge.bigint()).withClusteringKey("b", t1).withClusteringKey("c", t2).withColumn("d", bridge.bigint()).withSortOrder(so1).withSortOrder(so2).build()));
    }

    @Test
    public void testUdt()
    {
        qt()
        .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
        .checkAssert((t1, t2) -> runTest(CassandraBridge.CassandraVersion.FOURZERO, TestSchema.builder().withPartitionKey("a", bridge.bigint()).withClusteringKey("b", bridge.text()).withColumn("c", bridge.udt("keyspace", "testudt").withField("x", t1).withField("y", bridge.ascii()).withField("z", t2).build()).build()));
    }

    @Test
    public void testTuple()
    {
        qt()
        .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
        .checkAssert((t1, t2) -> runTest(CassandraBridge.CassandraVersion.FOURZERO, TestSchema.builder().withPartitionKey("a", bridge.bigint()).withClusteringKey("b", bridge.text()).withColumn("c", bridge.tuple(bridge.aInt(), t1, bridge.ascii(), t2, bridge.date())).build()));
    }

    private static void runTest(final CassandraBridge.CassandraVersion version, final TestSchema schema)
    {
        runTest(version, schema, schema.randomRows(NUM_ROWS));
    }

    private static void runTest(final CassandraBridge.CassandraVersion version,
                                final TestSchema schema,
                                final TestSchema.TestRow[] testRows)
    {
        try
        {
            schema.setCassandraVersion(version);
            testRowIterator(version, schema, testRows);
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void testRowIterator(final CassandraBridge.CassandraVersion version,
                                        final TestSchema schema,
                                        final TestSchema.TestRow[] testRows) throws IOException
    {
        final CassandraBridge bridge = CassandraBridge.get(version);
        final CqlSchema cqlSchema = schema.buildSchema();
        final int numRows = testRows.length;
        final int numColumns = cqlSchema.fields().size() - cqlSchema.numPartitionKeys() - cqlSchema.numClusteringKeys();
        final List<CqlField> columns = cqlSchema.fields().stream().filter(f -> !f.isPartitionKey()).filter(f -> !f.isClusteringColumn()).sorted().collect(Collectors.toList());
        final Rid rid = new Rid();
        final AtomicInteger rowPos = new AtomicInteger();
        final AtomicInteger colPos = new AtomicInteger();

        // mock data layer
        final DataLayer dataLayer = mock(DataLayer.class);
        when(dataLayer.cqlSchema()).thenReturn(cqlSchema);
        when(dataLayer.version()).thenReturn(version);
        when(dataLayer.isInPartition(any(BigInteger.class), any(ByteBuffer.class))).thenReturn(true);
        when(dataLayer.bridge()).thenCallRealMethod();
        when(dataLayer.stats()).thenReturn(Stats.DoNothingStats.INSTANCE);
        when(dataLayer.requestedFeatures()).thenCallRealMethod();

        // mock scanner
        final IStreamScanner scanner = mock(IStreamScanner.class);
<<<<<<< HEAD
        when(scanner.rid()).thenReturn(rid);
=======
//        when(scanner.hasNext()).thenAnswer(invocation -> rowPos.get() < numRows);
        when(scanner.data()).thenReturn(rid);
>>>>>>> d84d14c (Support multi-table for CDC (#223))
        doAnswer(invocation -> {
            final int col = colPos.getAndIncrement();
            if (rowPos.get() >= numRows)
            {
                return false;
            }
            final TestSchema.TestRow testRow = testRows[rowPos.get()];
            // write next partition key
            if (col == 0)
            {
                if (cqlSchema.numPartitionKeys() == 1)
                {
                    final CqlField partitionKey = cqlSchema.partitionKeys().get(0);
                    rid.setPartitionKeyCopy(partitionKey.serialize(testRow.get(partitionKey.pos())), BigInteger.ONE);
                }
                else
                {
                    assert cqlSchema.numPartitionKeys() > 1;
                    final ByteBuffer[] partitionBuffers = new ByteBuffer[cqlSchema.numPartitionKeys()];
                    int pos = 0;
                    for (final CqlField partitionKey : cqlSchema.partitionKeys())
                    {
                        partitionBuffers[pos] = partitionKey.serialize(testRow.get(partitionKey.pos()));
                        pos++;
                    }
                    rid.setPartitionKeyCopy(ColumnTypes.build(false, partitionBuffers), BigInteger.ONE);
                }
            }

            // write next clustering keys & column name
            final CqlField column = columns.get(col);
            final ByteBuffer[] colBuffers = new ByteBuffer[cqlSchema.numClusteringKeys() + 1];
            int pos = 0;
            for (final CqlField clusteringColumn : cqlSchema.clusteringKeys())
            {
                colBuffers[pos] = clusteringColumn.serialize(testRow.get(clusteringColumn.pos()));
                pos++;
            }
            colBuffers[pos] = bridge.ascii().serialize(column.name());
            rid.setColumnNameCopy(ColumnTypes.build(false, colBuffers));

            // write value, timestamp and tombstone
            rid.setValueCopy(column.serialize(testRow.get(column.pos())));

            // move to next row
            if (colPos.get() == numColumns)
            {
                if (rowPos.getAndIncrement() >= numRows)
                {
                    throw new IllegalStateException("Went too far...");
                }
                // reset column position
                colPos.set(0);
            }

            return true;
        }).when(scanner).next();

        when(dataLayer.openCompactionScanner(anyList(), any())).thenReturn(scanner);

        // use SparkRowIterator and verify values match expected
        final SparkRowIterator it = new SparkRowIterator(dataLayer);
        int rowCount = 0;
        while (it.next())
        {
            while (rowCount < testRows.length && testRows[rowCount].isDeleted())
            // skip tombstones
            {
                rowCount++;
            }
            if (rowCount >= testRows.length)
            {
                break;
            }

            final TestSchema.TestRow row = testRows[rowCount];
            assertEquals(row, schema.toTestRow(it.get()));
            rowCount++;
        }
        assertEquals(numRows, rowCount);
        it.close();
    }
}
