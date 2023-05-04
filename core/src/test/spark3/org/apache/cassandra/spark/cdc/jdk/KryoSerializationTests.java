package org.apache.cassandra.spark.cdc.jdk;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.Test;

import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.cdc.Marker;
import org.apache.cassandra.spark.cdc.watermarker.InMemoryWatermarker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.sparksql.filters.CdcOffset;
import org.apache.cassandra.spark.sparksql.filters.InstanceLogs;
import org.apache.cassandra.spark.sparksql.filters.SerializableCommitLog;
import org.apache.cassandra.spark.utils.TimeUtils;

import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;

public class KryoSerializationTests
{
    @Test
    public void testJdkCdcIteratorTests()
    {
        qt().forAll(TestUtils.partitioners()).checkAssert(partitioner -> {
            final CassandraInstance inst1 = new CassandraInstance("0", "local1-i1", "DC1");
            final Marker marker1 = new Marker(inst1, 500L, 200);
            final CassandraInstance inst2 = new CassandraInstance("1", "local2-i1", "DC1");
            final Marker marker2 = new Marker(inst2, 1000L, 350);
            final CassandraInstance inst3 = new CassandraInstance("2", "local3-i1", "DC1");
            final Marker marker3 = new Marker(inst3, 1500L, 500);

            final String jobId = UUID.randomUUID().toString();
            final long epoch = 500L;
            final int partitionId = 0;
            final CdcOffset offset = new CdcOffset(TimeUtils.nowMicros(),
                                                   ImmutableMap.of(
                                                   inst1, new InstanceLogs(marker1, ImmutableList.of(new SerializableCommitLog("CommitLog-6-12345.log", "/cassandra/d1", 500L, 10000L))),
                                                   inst2, new InstanceLogs(marker2, ImmutableList.of(new SerializableCommitLog("CommitLog-6-98765.log", "/cassandra/d2", 1200L, 15000L))),
                                                   inst3, new InstanceLogs(marker3, ImmutableList.of(new SerializableCommitLog("CommitLog-6-123987.log", "/cassandra/d3", 1500L, 20000L)))
                                                   ));

            final InMemoryWatermarker.SerializationWrapper wrapper = new InMemoryWatermarker.SerializationWrapper();
            final ByteBuffer buf;
            final Path dir;
            try
            {
                dir = Files.createTempDirectory(UUID.randomUUID().toString());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

            final Range<BigInteger> range = Range.closed(partitioner.minToken(), partitioner.maxToken());
            try (final TestJdkCdcIterator it = new TestJdkCdcIterator(jobId, partitionId, epoch, range, offset.startMarkers(), wrapper, dir.toString()))
            {
                buf = it.serializeToBytes();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

            try (TestJdkCdcIterator it = JdkCdcIterator.deserialize(buf, TestJdkCdcIterator.class, TestJdkCdcIterator.testSerializer()))
            {
                assertEquals(jobId, it.jobId());
                assertEquals(partitionId, it.partitionId());
                assertEquals(epoch, it.epoch());
                assertEquals(offset.startMarkers(), it.startMarkers());
                assertEquals(range, it.rangeFilter == null ? null : it.rangeFilter.tokenRange());
            }
        });
    }
}
