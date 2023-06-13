package org.apache.cassandra.spark.stats;

public interface ICdcStats
{
    // CDC Stats

    /**
     * Difference between the time change was created and time the same was read by a spark worker
     *
     * @param latency time difference, in milli secs
     */
    public default void changeReceived(String keyspace, String table, long latency)
    {
    }

    /**
     * Difference between the time change was created and time the same produced as a spark row
     *
     * @param latency time difference, in milli secs
     */
    public default void changeProduced(String keyspace, String table, long latency)
    {
    }

    /**
     * Report the change is published within a single batch.
     *
     * @param keyspace
     * @param table
     */
    public default void changePublished(String keyspace, String table)
    {
    }

    /**
     * Report the late change on publishing. A late change is one spans across multiple batches before publishing
     *
     * @param keyspace
     * @param table
     */
    public default void lateChangePublished(String keyspace, String table)
    {
    }

    /**
     * Report the late change on dropping. Cdc worker eventually dropps the unpublished change if it is too old.
     *
     * @param maxTimestampMicros timestamp in microseconds.
     */
    public default void lateChangeDropped(String keyspace, String table, long maxTimestampMicros)
    {
    }

    /**
     * Report the change that is not tracked and ignored
     *
     * @param incrCount delta value to add to the count
     */
    public default void untrackedChangesIgnored(String keyspace, String table, long incrCount)
    {
    }

    /**
     * Report the change that is out of the token range
     *
     * @param incrCount delta value to add to the count
     */
    public default void outOfTokenRangeChangesIgnored(String keyspace, String table, long incrCount)
    {
    }

    /**
     * Report the update that has insufficient replicas to deduplicate
     *
     * @param keyspace
     * @param table
     */
    public default void insufficientReplicas(String keyspace, String table)
    {
    }

    /**
     * Number of successfully read mutations
     *
     * @param incrCount delta value to add to the count
     */
    public default void mutationsReadCount(long incrCount)
    {
    }

    /**
     * Deserialized size of a successfully read mutation
     *
     * @param nBytes mutation size in bytes
     */
    public default void mutationsReadBytes(long nBytes)
    {
    }

    /**
     * Called when received a mutation with unknown table
     *
     * @param incrCount delta value to add to the count
     */
    public default void mutationsIgnoredUnknownTableCount(long incrCount)
    {
    }

    /**
     * Called when deserialization of a mutation fails
     *
     * @param incrCount delta value to add to the count
     */
    public default void mutationsDeserializeFailedCount(long incrCount)
    {
    }

    /**
     * Called when a mutation's checksum calculation fails or doesn't match with expected checksum
     *
     * @param incrCount delta value to add to the count
     */
    public default void mutationsChecksumMismatchCount(long incrCount)
    {
    }

    /**
     * Time taken to read a commit log file
     *
     * @param timeTaken time taken, in nano secs
     */
    public default void commitLogReadTime(long timeTaken)
    {
    }

    /**
     * Number of mutations read by a micro batch
     *
     * @param count mutations count
     */
    public default void mutationsReadPerBatch(long count)
    {
    }

    /**
     * Time taken by a micro batch, i.e, to read commit log files of a batch
     *
     * @param timeTaken time taken, in nano secs
     */
    public default void mutationsBatchReadTime(long timeTaken)
    {
    }

    /**
     * Time taken to aggregate and filter mutations.
     *
     * @param timeTakenNanos time taken in nanoseconds
     */
    public default void mutationsFilterTime(long timeTakenNanos)
    {
    }

    /**
     * Number of unexpected commit log EOF occurrences
     *
     * @param incrCount delta value to add to the count
     */
    public default void commitLogSegmentUnexpectedEndErrorCount(long incrCount)
    {
    }

    /**
     * Number of invalid mutation size occurrences
     *
     * @param incrCount delta value to add to the count
     */
    public default void commitLogInvalidSizeMutationCount(long incrCount)
    {
    }

    /**
     * Number of IO exceptions seen while reading commit log header
     *
     * @param incrCount delta value to add to the count
     */
    public default void commitLogHeaderReadFailureCount(long incrCount)
    {
    }

    /**
     * Time taken to read a commit log's header
     *
     * @param timeTaken time taken, in nano secs
     */
    public default void commitLogHeaderReadTime(long timeTaken)
    {
    }

    /**
     * Time taken to read a commit log's segment/section
     *
     * @param timeTaken time taken, in nano secs
     */
    public default void commitLogSegmentReadTime(long timeTaken)
    {
    }

    /**
     * Number of commit logs skipped
     *
     * @param incrCount delta value to add to the count
     */
    public default void skippedCommitLogsCount(long incrCount)
    {
    }

    /**
     * Number of bytes skipped/seeked when reading the commit log
     *
     * @param nBytes number of bytes
     */
    public default void commitLogBytesSkippedOnRead(long nBytes)
    {
    }

    /**
     * Number of commit log bytes fetched
     *
     * @param nBytes number of bytes
     */
    public default void commitLogBytesFetched(long nBytes)
    {
    }

    /**
     * Number of sub batches in a micro batch
     *
     * @param incrCount delta value to add to the count
     */
    public default void subBatchesPerMicroBatchCount(long incrCount)
    {
    }

    /**
     * Number of mutations read by a sub batch of a micro batch
     *
     * @param count mutations count
     */
    public default void mutationsReadPerSubMicroBatch(long count)
    {
    }
}
