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

package org.apache.cassandra.spark.sparksql;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.spark.cdc.AbstractCdcEvent;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.stats.Stats;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.jetbrains.annotations.NotNull;

public class CdcRowIterator implements PartitionReader<InternalRow>
{
    private final Stats stats;
    private final IStreamScanner<AbstractCdcEvent> cdcStreamScanner;
    private final long openTimeNanos;

    public CdcRowIterator(@NotNull DataLayer dataLayer,
                          @NotNull final CdcOffsetFilter cdcOffsetFilter)
    {
        this.stats = dataLayer.stats();
        this.cdcStreamScanner = dataLayer.openCdcScanner(cdcOffsetFilter);
        this.openTimeNanos = System.nanoTime();
        stats.openedSparkRowIterator();
    }

    @Override
    public boolean next() throws IOException
    {
        stats.nextRow();
        return cdcStreamScanner.next();
    }

    @Override
    public InternalRow get()
    {
        AbstractCdcEvent event = cdcStreamScanner.data();
        InternalRow row = event.toRow();
        stats.mutationProducedLatency(System.currentTimeMillis() - event.getTimestamp(TimeUnit.MILLISECONDS));
        return row;
    }

    @Override
    public void close() throws IOException
    {
        stats.closedSparkRowIterator(System.nanoTime() - openTimeNanos);
    }
}
