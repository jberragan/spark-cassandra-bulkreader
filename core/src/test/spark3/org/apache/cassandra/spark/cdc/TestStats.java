package org.apache.cassandra.spark.cdc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.spark.stats.Stats;

public class TestStats extends Stats
{
    final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    final Map<String, List<Long>> stats = new ConcurrentHashMap<>();

    public static final String TEST_CDC_MUTATION_RECEIVED_LATENCY = "CDC: mutation received latency";
    public static final String TEST_CDC_MUTATIONS_READ_COUNT = "CDC: mutations read count";
    public static final String TEST_CDC_MUTATIONS_READ_BYTES = "CDC: mutations read bytes";
    public static final String TEST_CDC_MUTATIONS_IGNORED_UNKNOWN_TABLE_COUNT = "CDC: mutations with unknown table count";
    public static final String TEST_CDC_MUTATIONS_CHECKSUM_MISMATCH_COUNT = "CDC: mutations with checksum mismatch count";
    public static final String TEST_CDC_MUTATIONS_IGNORED_UNTRACKED_TABLE_COUNT = "CDC: mutations with untracked table id count";
    public static final String TEST_MUTATIONS_IGNORED_OUT_OF_TOKEN_RANGE_COUNT = "CDC: mutations with out of token range count";
    public static final String TEST_CDC_COMMIT_LOG_READ_TIME = "CDC: time taken to read commit log";
    public static final String TEST_CDC_MUTATIONS_READ_PER_BATCH = "CDC: mutations read per batch";
    public static final String TEST_CDC_TIME_TAKEN_TO_READ_BATCH = "CDC: time taken to read batch";
    public static final String TEST_MUTATIONS_DESERIALIZE_FAILED_COUNT = "CDC: mutations with deserialization error count";

    private void incrementCounter(String key, long incrCount) {
        counters.computeIfAbsent(key, val -> new AtomicLong(0)).addAndGet(incrCount);
    }

    private void addStat(String key, long value) {
        stats.computeIfAbsent(key, val -> Collections.synchronizedList(new ArrayList<>())).add(value);
    }

    public long getCounterValue(String key) {
        return counters.getOrDefault(key, new AtomicLong(0)).get();
    }

    public List<Long> getStats(String key) {
        return stats.getOrDefault(key, new ArrayList<>());
    }

    public void mutationsReadCount(long incrCount) {
        incrementCounter(TEST_CDC_MUTATIONS_READ_COUNT, incrCount);
    }

    public void mutationsReadBytes(long nBytes) {
        addStat(TEST_CDC_MUTATIONS_READ_BYTES, nBytes);
    }

    public void mutationsIgnoredUnknownTableCount(long incrCount) {
        incrementCounter(TEST_CDC_MUTATIONS_IGNORED_UNKNOWN_TABLE_COUNT, incrCount);
    }

    public void mutationsChecksumMismatchCount(long incrCount) {
        incrementCounter(TEST_CDC_MUTATIONS_CHECKSUM_MISMATCH_COUNT, incrCount);
    }

    public void mutationsIgnoredUntrackedTableCount(long incrCount) {
        incrementCounter(TEST_CDC_MUTATIONS_IGNORED_UNTRACKED_TABLE_COUNT, incrCount);
    }

    public void mutationsIgnoredOutOfTokenRangeCount(long incrCount) {
        incrementCounter(TEST_MUTATIONS_IGNORED_OUT_OF_TOKEN_RANGE_COUNT, incrCount);
    }

    public void mutationsDeserializeFailedCount(long incrCount) {
        incrementCounter(TEST_MUTATIONS_DESERIALIZE_FAILED_COUNT, incrCount);
    }

    public void commitLogReadTime(long timeTaken) {
        addStat(TEST_CDC_COMMIT_LOG_READ_TIME, timeTaken);
    }

    public void mutationsReadPerBatch(long count) {
        addStat(TEST_CDC_MUTATIONS_READ_PER_BATCH, count);
    }

    public void mutationsBatchReadTime(long timeTaken) {
        addStat(TEST_CDC_TIME_TAKEN_TO_READ_BATCH, timeTaken);
    }

    public void mutationReceivedLatency(long latency) {
        addStat(TEST_CDC_MUTATION_RECEIVED_LATENCY, latency);
    }

    public void reset() {
        counters.clear();
        stats.clear();
    }
}
