package org.apache.cassandra.spark.reader;

import java.io.Closeable;
import java.io.IOException;

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
 * A rid is just a pair of ids that uniquely identifies the row and the column of a data entity.
 * Reading a Cassandra sstable pivots the data in a way that projects all columns against the rows
 * they belong to:
 * <p>
 * Cassandra:
 * <p>
 * r1 | c1, c2, c3
 * r2 | c4
 * r3 | c5, c6, c7, c8
 * <p>
 * pivoted:
 * <p>
 * r1 | c1
 * r1 | c2
 * r1 | c3
 * r2 | c4
 * r3 | c5
 * r3 | c6
 * r3 | c7
 * r3 | c8
 * <p>
 * During a loading operation we will extract up to a few trillion items out of sstables, so it is of
 * high importance to reuse objects - the caller to the scanner creates a rid using the
 * callers implementation of those interfaces; the scanner then calls set**Copy() to provide the data
 * at which point the implementation should make a copy of the provided bytes.
 * <p>
 * Upon return from the next() call the current values of the scanner can be obtained by calling
 * the methods in Rid, getPartitionKey(), getColumnName(), getValue().
 */
@SuppressWarnings("unused")
public interface IStreamScanner extends Closeable
{
    Rid getRid();

    boolean hasNext() throws IOException;

    void next() throws IOException;
}
