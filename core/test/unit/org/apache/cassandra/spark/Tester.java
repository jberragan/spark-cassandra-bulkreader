package org.apache.cassandra.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.mutable.MutableLong;

import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.quicktheories.core.Gen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

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

class Tester
{
    static final int DEFAULT_NUM_ROWS = 200;

    @NotNull
    private final List<CassandraBridge.CassandraVersion> versions;
    @NotNull
    private final TestSchema schema;
    private final int numRandomRows, expectedRowCount;
    @NotNull
    private final List<Consumer<TestSchema.TestRow>> writeListeners, readListeners;
    @NotNull
    private final List<Writer> writers;
    @NotNull
    private final List<Consumer<Dataset<Row>>> checks;
    @NotNull
    private final List<Integer> numSSTables;
    @Nullable
    private final Runnable reset;
    @Nullable
    private final String filterExpression;
    @Nullable
    private final String[] columns;
    @NotNull
    private final Set<String> sumFields;
    private final boolean shouldCheckNumSSTables;
    private final boolean addLastModifiedTimestamp;
    private final int delayBetweenSSTablesInSecs;

    private Tester(@NotNull final List<CassandraBridge.CassandraVersion> versions, @NotNull final TestSchema schema, @NotNull final List<Integer> numSSTables, @NotNull final List<Consumer<TestSchema.TestRow>> writeListeners,
                   @NotNull final List<Consumer<TestSchema.TestRow>> readListeners, @NotNull final List<Writer> writers, @NotNull final List<Consumer<Dataset<Row>>> checks, @NotNull final Set<String> sumFields,
                   @Nullable final Runnable reset, @Nullable final String filterExpression, final int numRandomRows, final int expectedRowCount, final boolean shouldCheckNumSSTables,
                   @Nullable final String[] columns, final boolean addLastModifiedTimestamp, final int delayBetweenSSTablesInSecs)
    {
        this.versions = versions;
        this.schema = schema;
        this.numSSTables = numSSTables;
        this.writeListeners = writeListeners;
        this.readListeners = readListeners;
        this.writers = writers;
        this.checks = checks;
        this.sumFields = sumFields;
        this.reset = reset;
        this.filterExpression = filterExpression;
        this.numRandomRows = numRandomRows;
        this.expectedRowCount = expectedRowCount;
        this.shouldCheckNumSSTables = shouldCheckNumSSTables;
        this.columns = columns;
        this.addLastModifiedTimestamp = addLastModifiedTimestamp;
        this.delayBetweenSSTablesInSecs = delayBetweenSSTablesInSecs;
    }

    static Builder builder(@NotNull final TestSchema schema)
    {
        return new Builder(schema);
    }

    static class Writer
    {
        final Consumer<CassandraBridge.IWriter> consumer;
        final boolean isTombstoneWriter;

        Writer(final Consumer<CassandraBridge.IWriter> consumer)
        {
            this(consumer, false);
        }

        Writer(final Consumer<CassandraBridge.IWriter> consumer, final boolean isTombstoneWriter)
        {
            this.consumer = consumer;
            this.isTombstoneWriter = isTombstoneWriter;
        }
    }

    public static class Builder
    {
        @NotNull
        final List<CassandraBridge.CassandraVersion> versions = TestUtils.testableVersions();
        @NotNull
        final TestSchema schema;
        int numRandomRows = DEFAULT_NUM_ROWS, expectedRowCount = -1;
        @NotNull
        final List<Consumer<TestSchema.TestRow>> writeListeners = new ArrayList<>(), readListeners = new ArrayList<>();
        @NotNull
        final List<Writer> writers = new ArrayList<>();
        @NotNull
        final List<Consumer<Dataset<Row>>> checks = new ArrayList<>();
        @Nullable
        Runnable reset = null;
        @NotNull
        private List<Integer> numSSTables = new ArrayList<>(Arrays.asList(1, 2, 5));
        @NotNull
        private Set<String> sumFields = new HashSet<>();
        @Nullable
        private String filterExpression;
        @Nullable
        private String[] columns = null;
        private boolean shouldCheckNumSSTables = true;
        private boolean addLastModifiedTimestamp = false;
        private int delayBetweenSSTablesInSecs = 0;

        private Builder(@NotNull final TestSchema schema)
        {
            this.schema = schema;
        }

        // runs a test for every Cassandra version given
        Builder withVersions(@NotNull final Collection<CassandraBridge.CassandraVersion> versions)
        {
            this.versions.clear();
            this.versions.addAll(versions);
            return this;
        }

        // runs a test for every number of SSTables given
        Builder withNumRandomSSTables(final Integer... numSSTables)
        {
            this.numSSTables = ImmutableList.copyOf(Arrays.asList(numSSTables));
            return this;
        }

        Builder withSumField(final String... fields)
        {
            this.sumFields = ImmutableSet.copyOf(Arrays.asList(fields));
            return this;
        }

        Builder withNumRandomRows(final int numRow)
        {
            this.numRandomRows = numRow;
            return this;
        }

        Builder dontWriteRandomData()
        {
            this.numSSTables = Collections.singletonList(0);
            this.numRandomRows = 0;
            return this;
        }

        Builder withWriteListener(@Nullable final Consumer<TestSchema.TestRow> writeListener)
        {
            if (writeListener != null)
            {
                this.writeListeners.add(writeListener);
            }
            return this;
        }

        Builder withReadListener(@Nullable final Consumer<TestSchema.TestRow> readListener)
        {
            if (readListener != null)
            {
                this.readListeners.add(readListener);
            }
            return this;
        }

        Builder withSSTableWriter(@Nullable final Consumer<CassandraBridge.IWriter> consumer)
        {
            if (consumer != null)
            {
                this.writers.add(new Writer(consumer));
            }
            return this;
        }

        Builder withTombstoneWriter(@Nullable final Consumer<CassandraBridge.IWriter> consumer)
        {
            if (consumer != null)
            {
                this.writers.add(new Writer(consumer, true));
            }
            return this;
        }

        Builder withCheck(@Nullable final Consumer<Dataset<Row>> check)
        {
            if (check != null)
            {
                this.checks.add(check);
            }
            return this;
        }

        Builder withExpectedRowCountPerSSTable(final int expectedRowCount)
        {
            this.expectedRowCount = expectedRowCount;
            return this;
        }

        Builder withReset(final Runnable reset)
        {
            this.reset = reset;
            return this;
        }

        Builder withFilter(@NotNull String filterExpression)
        {
            this.filterExpression = filterExpression;
            return this;
        }

        Builder withColumns(@NotNull String... columns)
        {
            this.columns = columns;
            return this;
        }

        Builder dontCheckNumSSTables()
        {
            this.shouldCheckNumSSTables = false;
            return this;
        }

        Builder withLastModifiedTimestampColumn()
        {
            this.addLastModifiedTimestamp = true;
            return this;
        }

        Builder withDelayBetweenSSTablesInSecs(int delay)
        {
            this.delayBetweenSSTablesInSecs = delay;
            return this;
        }

        void run()
        {
            new Tester(versions, schema, numSSTables, writeListeners, readListeners, writers, checks, sumFields, reset, filterExpression, numRandomRows, expectedRowCount, shouldCheckNumSSTables, columns, addLastModifiedTimestamp, delayBetweenSSTablesInSecs).run();
        }
    }

    private void run()
    {
        qt().forAll(versions(), numSSTables()).checkAssert(this::run);
    }

    private Gen<CassandraBridge.CassandraVersion> versions()
    {
        return arbitrary().pick(versions);
    }

    private Gen<Integer> numSSTables()
    {
        return arbitrary().pick(numSSTables);
    }

    private void run(final CassandraBridge.CassandraVersion version, final int numSSTables)
    {
        TestUtils.runTest(version, (partitioner, dir, bridge) -> {
            schema.setCassandraVersion(version);

            // write SSTables with random data
            final Map<String, MutableLong> sum = this.sumFields.stream().collect(Collectors.toMap(Function.identity(), ignore -> new MutableLong()));
            final Map<String, TestSchema.TestRow> rows = new HashMap<>(numRandomRows);
            IntStream.range(0, numSSTables)
                     .forEach(i ->
                              TestUtils.writeSSTable(bridge, dir, partitioner, schema,
                                                  (writer) ->
                                                  IntStream.range(0, numRandomRows).forEach(j -> {
                                                      TestSchema.TestRow testRow;
                                                      do
                                                      {
                                                          testRow = schema.randomRow();
                                                      }
                                                      while (rows.containsKey(testRow.getKey())); // don't write duplicate rows

                                                      for (final Consumer<TestSchema.TestRow> writeListener : this.writeListeners)
                                                      {
                                                          writeListener.accept(testRow);
                                                      }

                                                      for (final String sumField : sumFields)
                                                      {
                                                          sum.get(sumField).add((Number) testRow.get(sumField));
                                                      }
                                                      rows.put(testRow.getKey(), testRow);

                                                      writer.write(testRow.allValues());
                                                  })));
            int sstableCount = numSSTables;

            // write any custom SSTables e.g. overwriting existing data or tombstones
            for (final Writer writer : writers)
            {
                if (sstableCount != 0)
                {
                    try
                    {
                        TimeUnit.SECONDS.sleep(delayBetweenSSTablesInSecs);
                    }
                    catch (InterruptedException e)
                    {
                        throw new RuntimeException(e.getMessage());
                    }
                }
                if (writer.isTombstoneWriter)
                {
                    TestUtils.writeTombstoneSSTable(partitioner, bridge.getVersion(), dir, schema.createStmt, schema.deleteStmt, writer.consumer);
                }
                else
                {
                    TestUtils.writeSSTable(bridge, dir, partitioner, schema, writer);
                }
                sstableCount++;
            }

            if (shouldCheckNumSSTables)
            {
                assertEquals("Number of SSTables written does not match expected", sstableCount, TestUtils.countSSTables(dir));
            }

            final Dataset<Row> ds = TestUtils.openLocalDataset(partitioner, dir, schema.keyspace, schema.createStmt, version, schema.udts, addLastModifiedTimestamp, filterExpression, columns);
            int rowCount = 0;
            final Set<String> requiredColumns = columns == null ? null : new HashSet<>(Arrays.asList(columns));
            for (final Row row : ds.collectAsList())
            {
                if (requiredColumns != null)
                {
                    Set<String> actualColumns = new HashSet<>(Arrays.asList(row.schema().fieldNames()));
                    assertEquals("Actual Columns and Required Columns should be the same", actualColumns, requiredColumns);
                }

                final TestSchema.TestRow actualRow = schema.toTestRow(row, requiredColumns);
                if (numRandomRows > 0)
                {
                    // if we wrote random data, verify values exist
                    final String key = actualRow.getKey();
                    assertTrue("Unexpected row read in Spark", rows.containsKey(key));
                    assertEquals("Row read in Spark does not match expected", rows.get(key).withColumns(requiredColumns), actualRow);
                }

                for (final Consumer<TestSchema.TestRow> readListener : this.readListeners)
                {
                    readListener.accept(actualRow);
                }
                rowCount++;
            }
            if (expectedRowCount >= 0)
            {
                assertEquals("Number of rows read does not match expected", expectedRowCount * sstableCount, rowCount);
            }

            // verify numerical fields sum to expected value
            for (final String sumField : sumFields)
            {
                assertEquals("Field '" + sumField + "' does not sum to expected amount", sum.get(sumField).getValue().longValue(), ds.groupBy().sum(sumField).first().getLong(0));
            }

            // run SparkSQL checks
            for (final Consumer<Dataset<Row>> check : checks)
            {
                check.accept(ds);
            }

            if (reset != null)
            {
                reset.run();
            }
        });
    }
}
