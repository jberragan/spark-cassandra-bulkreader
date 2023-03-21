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

package org.apache.cassandra.spark.cdc.jdk;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.spark.cdc.AbstractCdcEvent;
import org.apache.cassandra.spark.cdc.ICassandraSource;
import org.apache.cassandra.spark.cdc.RangeTombstoneBuilder;
import org.apache.cassandra.spark.cdc.RowSource;
import org.apache.cassandra.spark.cdc.jdk.msg.CdcMessage;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.TableMetadata;

public class JdkCdcEvent extends AbstractCdcEvent<JdkValueMetadata, JdkRangeTombstone> implements RowSource<CdcMessage>
{
    public JdkCdcEvent(Kind kind, UnfilteredRowIterator partition, ICassandraSource cassandraSource)
    {
        super(kind, partition, cassandraSource);
    }

    protected JdkCdcEvent(Kind kind, String keyspace, String table, ICassandraSource cassandraSource)
    {
        super(kind, keyspace, table, cassandraSource);
    }

    public JdkRangeTombstoneBuilder rangeTombstoneBuilder(TableMetadata metadata)
    {
        return new JdkRangeTombstoneBuilder(metadata);
    }

    public JdkValueMetadata makeValue(String name, String type, ByteBuffer value)
    {
        return new JdkValueMetadata(name, type, value);
    }

    public CdcMessage toRow()
    {
        return new CdcMessage(this);
    }

    public static class Builder extends EventBuilder<JdkValueMetadata, JdkRangeTombstone, JdkCdcEvent>
    {
        public static Builder of(Kind kind, UnfilteredRowIterator partition, ICassandraSource cassandraSource)
        {
            return new Builder(kind, partition, cassandraSource);
        }

        private Builder(Kind kind, UnfilteredRowIterator partition, ICassandraSource cassandraSource)
        {
            super(kind, partition, cassandraSource);
        }

        public JdkCdcEvent buildEvent(Kind kind, UnfilteredRowIterator partition, ICassandraSource cassandraSource)
        {
            return new JdkCdcEvent(kind, partition, cassandraSource);
        }
    }

    public static class JdkRangeTombstoneBuilder extends RangeTombstoneBuilder<JdkValueMetadata, JdkRangeTombstone>
    {
        public JdkRangeTombstoneBuilder(TableMetadata tableMetadata)
        {
            super(tableMetadata);
        }

        public JdkRangeTombstone buildTombstone(List<JdkValueMetadata> start, boolean isStartInclusive, List<JdkValueMetadata> end, boolean isEndInclusive)
        {
            return new JdkRangeTombstone(start, isStartInclusive, end, isEndInclusive);
        }

        public JdkValueMetadata buildValue(String name, String type, ByteBuffer buf)
        {
            return new JdkValueMetadata(name, type, buf);
        }
    }
}
