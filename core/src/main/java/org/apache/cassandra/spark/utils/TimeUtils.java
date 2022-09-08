package org.apache.cassandra.spark.utils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class TimeUtils
{
    public static long secsToMicros(long seconds)
    {
        return TimeUnit.SECONDS.toMicros(seconds);
    }

    // It drops any precision higher than milliseconds from duration
    public static long toMicros(Duration duration)
    {
        return TimeUnit.MILLISECONDS.toMicros(duration.toMillis());
    }

    public static long toMicros(java.sql.Timestamp timestamp)
    {
        long millis = timestamp.getTime();
        int nanos = timestamp.getNanos(); // nanos of the fractional seconds component.
        nanos = nanos % 1000_000; // only keep the micros component.
        return TimeUnit.MILLISECONDS.toMicros(millis) + TimeUnit.NANOSECONDS.toMicros(nanos);
    }
}
