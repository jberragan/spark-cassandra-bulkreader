package org.apache.cassandra.spark.cdc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.cdc.fourzero.LocalCdcEventWriter;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.fourzero.FourZero;
import org.apache.cassandra.spark.sparksql.LocalDataSource;
import org.apache.cassandra.spark.utils.ThrowableUtils;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.jetbrains.annotations.Nullable;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

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
 * Helper for writing CommitLogs using the TestSchema and reading back with Spark Streaming to verify matches the expected.
 */
public class CdcTester
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcTester.class);
    public static final int DEFAULT_NUM_ROWS = 1000;

    public static FourZeroCommitLog LOG; //FIXME: use generic commit log
    private final String statsClass;

    public static void setup(TemporaryFolder testFolder)
    {
        FourZero.setup();
        FourZero.setCommitLogPath(testFolder.getRoot().toPath());
        FourZero.setCDC(testFolder.getRoot().toPath());
        LOG = new FourZeroCommitLog(testFolder.getRoot());
    }

    public static void tearDown()
    {
        if (LOG == null)
        {
            return;
        }

        try
        {
            LOG.stop();
        }
        finally
        {
            LOG.clear();
        }
    }

    public void reset()
    {
        LOGGER.info("Resetting CDC test environment testId={} schema='{}' thread={}", testId, cqlSchema.fields(), Thread.currentThread().getName());
        TestUtils.clearDirectory(outputDir, path -> LOGGER.info("Clearing test output path={}", path.toString()));
        CdcTester.tearDown();
        LOG.start();
    }

    final CassandraBridge bridge;
    @Nullable
    final Set<String> requiredColumns;
    final UUID testId;
    final Path testDir, outputDir, checkpointDir;
    final TestSchema schema;
    final CqlSchema cqlSchema;
    final int numRows;
    final int expectedNumRows;
    final List<CdcWriter> writers;
    int count = 0;
    final String dataSourceFQCN;
    final boolean addLastModificationTime;
    BiConsumer<Map<String, TestSchema.TestRow>, List<AbstractCdcEvent>> eventsChecker;


    CdcTester(CassandraBridge bridge,
              TestSchema schema,
              Path testDir,
              List<CdcWriter> writers,
              int numRows,
              int expectedNumRows,
              String dataSourceFQCN,
              boolean addLastModificationTime,
              BiConsumer<Map<String, TestSchema.TestRow>, List<AbstractCdcEvent>> eventsChecker,
              @Nullable final String statsClass)
    {
        this.bridge = bridge;
        this.testId = UUID.randomUUID();
        this.testDir = testDir;
        this.writers = writers;
        this.outputDir = testDir.resolve(testId + "_out");
        this.checkpointDir = testDir.resolve(testId + "_checkpoint");
        this.requiredColumns = null;
        this.numRows = numRows;
        this.expectedNumRows = expectedNumRows;
        this.dataSourceFQCN = dataSourceFQCN;
        this.addLastModificationTime = addLastModificationTime;
        this.eventsChecker = eventsChecker;
        this.statsClass = statsClass;
        try
        {
            Files.createDirectory(outputDir);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        this.schema = schema;
        this.cqlSchema = schema.buildSchema();
    }

    public interface CdcWriter
    {
        void write(CdcTester tester, Map<String, TestSchema.TestRow> rows, BiConsumer<TestSchema.TestRow, Long> writer);
    }

    public static class Builder
    {
        CassandraBridge bridge;
        TestSchema.Builder schemaBuilder;
        Path testDir;
        int numRows = CdcTester.DEFAULT_NUM_ROWS;
        int expecetedNumRows = numRows;
        List<CdcWriter> writers = new ArrayList<>();
        String dataSourceFQCN = LocalDataSource.class.getName();
        boolean addLastModificationTime = false;
        BiConsumer<Map<String, TestSchema.TestRow>, List<AbstractCdcEvent>> eventChecker;
        private String statsClass = null;

        Builder(CassandraBridge bridge, TestSchema.Builder schemaBuilder, Path testDir)
        {
            this.bridge = bridge;
            this.schemaBuilder = schemaBuilder.withCdc(true);
            this.testDir = testDir;

            // add default writer
            this.writers.add((tester, rows, writer) -> {
                final long timestampMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                IntStream.range(0, tester.numRows)
                         .forEach(i -> writer.accept(Tester.newUniqueRow(tester.schema, rows), timestampMicros));
            });
        }

        Builder clearWriters()
        {
            this.writers.clear();
            return this;
        }

        Builder withWriter(CdcWriter writer)
        {
            this.writers.add(writer);
            return this;
        }

        Builder withNumRows(int numRows)
        {
            this.numRows = numRows;
            return this;
        }

        Builder withExpectedNumRows(int expectedNumRows)
        {
            this.expecetedNumRows = expectedNumRows;
            return this;
        }

        Builder withDataSource(String dataSourceFQCN)
        {
            this.dataSourceFQCN = dataSourceFQCN;
            return this;
        }

        Builder withAddLastModificationTime(boolean addLastModificationTime)
        {
            this.addLastModificationTime = addLastModificationTime;
            return this;
        }

        Builder withCdcEventChecker(BiConsumer<Map<String, TestSchema.TestRow>, List<AbstractCdcEvent>> checker)
        {
            this.eventChecker = checker;
            return this;
        }

        Builder withStatsClass(String statsClass)
        {
            this.statsClass = statsClass;
            return this;
        }

        void run()
        {
            new CdcTester(bridge, schemaBuilder.build(), testDir, writers, numRows, expecetedNumRows,
                          dataSourceFQCN, addLastModificationTime, eventChecker, statsClass).run();
        }
    }

    void logRow(TestSchema.TestRow row, long timestamp)
    {
        bridge.log(cqlSchema, LOG, row, timestamp);
        count++;
    }

    void run()
    {
        final Map<String, TestSchema.TestRow> rows = new LinkedHashMap<>(numRows);
        CassandraBridge.CassandraVersion version = CassandraBridge.CassandraVersion.FOURZERO;
        LocalCdcEventWriter cdcEventWriter = new LocalCdcEventWriter();
        LocalCdcEventWriter.events.clear();
        List<AbstractCdcEvent> cdcEvents = LocalCdcEventWriter.events;

        try
        {
            LOGGER.info("Running CDC test testId={} schema='{}' thread={}", testId, cqlSchema.fields(), Thread.currentThread().getName());
            schema.schemaBuilder(Partitioner.Murmur3Partitioner);
            schema.setCassandraVersion(version);

            // write some mutations to CDC CommitLog
            for (CdcWriter writer : writers)
            {
                writer.write(this, rows, (row, timestamp) -> {
                    rows.put(row.getKey(), row);
                    this.logRow(row, timestamp);
                });
            }
            LOG.sync();
            LOGGER.info("Logged mutations={} testId={}", count, testId);


            // run streaming query and output to outputDir
            final StreamingQuery query = TestUtils.openStreaming(schema.keyspace, schema.createStmt,
                                                                 version,
                                                                 Partitioner.Murmur3Partitioner,
                                                                 testDir.resolve("cdc"),
                                                                 outputDir,
                                                                 checkpointDir,
                                                                 dataSourceFQCN,
                                                                 statsClass,
                                                                 cdcEventWriter);
            // wait for query to write output parquet files before reading to verify test output matches expected
            int prevNumRows = 0;
            long start = System.currentTimeMillis();
            while (cdcEvents.size() < expectedNumRows)
            {
                prevNumRows = cdcEvents.size();
                long elapsedSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start);
                if (elapsedSecs > 20)
                {
                    // timeout eventually if no progress
                    LOGGER.warn("Expected {} rows only {} found after {} seconds testId={} ",
                                expectedNumRows, prevNumRows, elapsedSecs, testId);
                    break;
                }
                Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            }

            query.stop();
            query.awaitTermination();
        }
        catch (StreamingQueryException e)
        {
            if (!e.getCause().getMessage().equals("Job aborted."))
            {
                fail("SparkStreaming job failed with exception: " + e.getMessage());
            }
        }
        catch (TimeoutException e)
        {
            fail("Streaming query timed out: " + e.getMessage());
        }
        catch (final Throwable t)
        {
            LOGGER.error("Unexpected error in CdcTester", ThrowableUtils.rootCause(t));
        }
        finally
        {
            try
            {
                // read streaming output from outputDir and verify the rows match expected
                LOGGER.info("Finished CDC test, verifying output testId={} schema='{}' thread={} actualRows={}",
                            testId, cqlSchema.fields(), Thread.currentThread().getName(), cdcEvents.size());

                if (eventsChecker != null)
                {
                    assertNotNull(cdcEvents);
                    eventsChecker.accept(rows, cdcEvents);
                }
            }
            finally
            {
                reset();
            }
        }
    }

    public static Builder testWith(CassandraBridge bridge, TemporaryFolder testDir, TestSchema.Builder schemaBuilder)
    {
        return testWith(bridge, testDir.getRoot().toPath(), schemaBuilder);
    }

    public static Builder testWith(CassandraBridge bridge, Path testDir, TestSchema.Builder schemaBuilder)
    {
        return new Builder(bridge, schemaBuilder, testDir);
    }

    // tl;dr; text and varchar cql types are the same internally in Cassandra
    // TEXT is UTF8 encoded string, as same as varchar. Both are represented as UTF8Type internally.
    private static final Set<String> sameType = Set.of("text", "varchar");
    public static void assertCqlTypeEquals(String expectedType, String testType)
    {
        if (!expectedType.equals(testType))
        {
            if (!sameType.contains(testType) || !sameType.contains(expectedType))
            {
                fail(String.format("Expected type: %s; test type: %s", expectedType, testType));
            }
        }
    }
}
