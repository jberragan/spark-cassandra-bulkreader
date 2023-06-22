package org.apache.cassandra.spark.cdc.jdk;

import java.math.BigInteger;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.spark.cdc.Marker;
import org.apache.cassandra.spark.cdc.watermarker.InMemoryWatermarker;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.spark.utils.TimeUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MergeStateTests
{
    @ClassRule
    public static TemporaryFolder DIR = new TemporaryFolder();

    @Test
    public void testMergeIterators()
    {
        final String jobId = UUID.randomUUID().toString();
        final int epoch = 999;
        final Range<BigInteger> r1 = Range.closed(new BigInteger("-1537228672809129302"), new BigInteger("0"));
        final Range<BigInteger> r2 = Range.closed(new BigInteger("-3074457345618258603"), new BigInteger("-1537228672809129302"));
        final CassandraInstance inst1 = new CassandraInstance("0", "node-1", "DC1");
        final CassandraInstance inst2 = new CassandraInstance("-1537228672809129302", "node-2", "DC1");
        final CassandraInstance inst3 = new CassandraInstance("-3074457345618258603", "node-3", "DC1");
        final long now = TimeUtils.nowMicros();

        final InMemoryWatermarker.SerializationWrapper wrapper1 = new InMemoryWatermarker.SerializationWrapper(
        ImmutableMap.of(new PartitionUpdateWrapper(null, "ks", "tb", now, new byte[]{ 'a' }, 100, new BigInteger("-1537228672809129301")), 2,
               new PartitionUpdateWrapper(null, "ks", "tb", now, new byte[]{ 'b' }, 100, new BigInteger("-10000")), 1,
               new PartitionUpdateWrapper(null, "ks", "tb", now, new byte[]{ 'c' }, 100, new BigInteger("-500")), 1)
        );
        assertEquals(3, wrapper1.filter(r1).replicaCount.size());
        assertEquals(0, wrapper1.filter(r2).replicaCount.size());

        final TestJdkCdcIterator it1 = new TestJdkCdcIterator(jobId, 101, epoch, r1,
                                                              ImmutableMap.of(inst1, new Marker(inst1, 500L, 16384),
                                                                     inst2, new Marker(inst2, 700L, 0)),
                                                              wrapper1, DIR.getRoot().toString());

        final InMemoryWatermarker.SerializationWrapper wrapper2 = new InMemoryWatermarker.SerializationWrapper(
        ImmutableMap.of(new PartitionUpdateWrapper(null, "ks", "tb", now, new byte[]{ 'd' }, 100, new BigInteger("-1537228672809129400")), 2,
               new PartitionUpdateWrapper(null, "ks", "tb", now, new byte[]{ 'e' }, 100, new BigInteger("-1537228672809129500")), 1,
               new PartitionUpdateWrapper(null, "ks", "tb", now, new byte[]{ 'f' }, 100, new BigInteger("-1537228672809129600")), 1,
               new PartitionUpdateWrapper(null, "ks", "tb", now, new byte[]{ 'g' }, 100, new BigInteger("-3074457345618258604")), 1)  // out-of-range mutation to test
        );
        assertEquals(0, wrapper2.filter(r1).replicaCount.size());
        assertEquals(3, wrapper2.filter(r2).replicaCount.size());
        final TestJdkCdcIterator it2 = new TestJdkCdcIterator(jobId, 102, epoch + 500, r2,
                                                              ImmutableMap.of(inst2, new Marker(inst2, 600L, 32768),
                                                                              inst3, new Marker(inst3, 800L, 4096)),
                                                              wrapper2, DIR.getRoot().toString());

        // merge iterators to combine both token ranges
        final Range<BigInteger> mergedRange = Range.closed(r2.lowerEndpoint(), r1.upperEndpoint());
        final TestJdkCdcIterator merged = JdkCdcIterator.mergeIterators(103, mergedRange, it1, it2);

        assertEquals(jobId, merged.jobId);
        assertEquals(103, merged.partitionId);
        assertEquals(epoch + 500, merged.epoch);
        assertNotNull(merged.rangeFilter);
        assertEquals(mergedRange, merged.rangeFilter.tokenRange());

        // merged iterator should contain mutations for new wider token range
        assertEquals(6, merged.serializationWrapper().replicaCount.size());
        assertEquals(3, merged.serializationWrapper().filter(r1).replicaCount.size());
        assertEquals(3, merged.serializationWrapper().filter(r2).replicaCount.size());

        // merged iterator should contain min CommitLog marker across instances
        assertEquals(new Marker(inst1, 500L, 16384), merged.startMarkers.get(inst1));
        assertEquals(new Marker(inst2, 600L, 32768), merged.startMarkers.get(inst2));
        assertEquals(new Marker(inst3, 800L, 4096), merged.startMarkers.get(inst3));
    }

    @Test
    public void testMarkerMin() {
        final CassandraInstance inst = new CassandraInstance("0", "local0-i1", "DC1");
        final Marker m1 = new Marker(inst, 500L, 300);
        final Marker m2 = new Marker(inst, 500L, 500);
        final Marker m3 = new Marker(inst, 800L, 200);
        assertEquals(m1, Marker.min(m1, m2));
        assertEquals(m1, Marker.min(m2, m1));
        assertEquals(m1, Marker.min(m1, m3));
        assertEquals(m1, Marker.min(m3, m1));
        assertEquals(m2, Marker.min(m2, m3));
        assertEquals(m3, Marker.min(m3, m3));
    }
}
