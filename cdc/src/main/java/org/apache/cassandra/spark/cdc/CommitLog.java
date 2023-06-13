package org.apache.cassandra.spark.cdc;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.stats.CdcStats;
import org.apache.cassandra.spark.utils.streaming.Source;
import org.apache.cassandra.spark.utils.streaming.CassandraFile;
import org.jetbrains.annotations.NotNull;

public interface CommitLog extends AutoCloseable, CassandraFile
{
    Logger LOGGER = LoggerFactory.getLogger(CommitLog.class);

    // match both legacy and new version of commitlogs Ex: CommitLog-12345.log and CommitLog-6-12345.log.
    Pattern COMMIT_LOG_FILE_PATTERN = Pattern.compile("CommitLog(-(\\d+))?-(\\d+).log");

    static Optional<Pair<Integer, Long>> extractVersionAndSegmentId(@NotNull final CommitLog log)
    {
        return extractVersionAndSegmentId(log.name());
    }

    static Optional<Pair<Integer, Long>> extractVersionAndSegmentId(@NotNull final String filename)
    {
        final Matcher matcher = CommitLog.COMMIT_LOG_FILE_PATTERN.matcher(filename);
        if (matcher.matches())
        {
            try
            {
                final int version = matcher.group(2) == null ? 6 : Integer.parseInt(matcher.group(2));
                if (version != 6 && version != 7)
                {
                    throw new IllegalStateException("Unknown commitlog version " + version);
                }
                // logic taken from org.apache.cassandra.db.commitlog.CommitLogDescriptor.getMessagingVersion()
                return Optional.of(Pair.of(version == 6 ? 10 : 12, Long.parseLong(matcher.group(3))));
            }
            catch (NumberFormatException e)
            {
                LOGGER.error("Could not parse commit log segmentId name={}", filename, e);
                return Optional.empty();
            }
        }
        LOGGER.error("Could not parse commit log filename name={}", filename);
        return Optional.empty(); // cannot extract segment id
    }

    /**
     * @return filename of the CommitLog
     */
    String name();

    /**
     * @return path to the CommitLog
     */
    String path();

    /**
     * @return the max offset that can be read in the CommitLog. This may be less than or equal to {@link CommitLog#len()}.
     * The reader should not read passed this point when the commit log is incomplete.
     */
    long maxOffset();

    /**
     * @return length of the CommitLog in bytes
     */
    long len();

    /**
     * @return a Source for asynchronously reading the CommitLog bytes.
     */
    Source<CommitLog> source();

    /**
     * @return the CassandraInstance this CommitLog resides on.
     */
    CassandraInstance instance();

    default long segmentId()
    {
        return Objects.requireNonNull(extractVersionAndSegmentId(this).map(Pair::getRight).orElseThrow(RuntimeException::new), "Could not extract segmentId from CommitLog");
    }

    default Marker zeroMarker()
    {
        return markerAt(segmentId(), 0);
    }

    default Marker maxMarker()
    {
        return markerAt(segmentId(), (int) maxOffset());
    }

    default Marker markerAt(long section, int offset)
    {
        return new Marker(instance(), section, offset);
    }

    /**
     * Override to provide custom stats implementation.
     *
     * @return stats instance for publishing stats
     */
    default CdcStats stats()
    {
        return CdcStats.DoNothingCdcStats.INSTANCE;
    }
}
