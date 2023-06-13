package org.apache.cassandra.spark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import org.apache.cassandra.spark.cdc.Marker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.sparksql.filters.CdcOffset;
import org.apache.cassandra.spark.sparksql.filters.InstanceLogs;
import org.apache.cassandra.spark.sparksql.filters.SerializableCommitLog;
import org.apache.cassandra.spark.utils.TimeUtils;

import static org.apache.cassandra.spark.utils.KryoUtils.deserialize;
import static org.apache.cassandra.spark.utils.KryoUtils.serializeToBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CdcKryoSerializationTests
{
    public static Kryo kryo()
    {
        return CdcKryoRegister.kryo();
    }

    @Test
    public void testCdcOffset()
    {
        final CassandraInstance inst1 = new CassandraInstance("0", "local1-i1", "DC1");
        final Marker marker1 = new Marker(inst1, 500L, 200);
        final CassandraInstance inst2 = new CassandraInstance("1", "local2-i1", "DC1");
        final Marker marker2 = new Marker(inst2, 1000L, 350);
        final CassandraInstance inst3 = new CassandraInstance("2", "local3-i1", "DC1");
        final Marker marker3 = new Marker(inst2, 1500L, 500);

        final CdcOffset offset = new CdcOffset(TimeUtils.nowMicros(),
                                               ImmutableMap.of(
                                               inst1, new InstanceLogs(marker1, ImmutableList.of(new SerializableCommitLog("CommitLog-6-12345.log", "/cassandra/d1", 500L, 10000L))),
                                               inst2, new InstanceLogs(marker2, ImmutableList.of(new SerializableCommitLog("CommitLog-6-98765.log", "/cassandra/d2", 1200L, 15000L))),
                                               inst3, new InstanceLogs(marker3, ImmutableList.of(new SerializableCommitLog("CommitLog-6-123987.log", "/cassandra/d3", 1500L, 20000L)))
                                               ));
        final byte[] ar = serializeToBytes(kryo(), offset);
        final CdcOffset deserialized = deserialize(kryo(), ar, CdcOffset.class);
        assertNotNull(deserialized);
        assertEquals(offset, deserialized);
    }
}
