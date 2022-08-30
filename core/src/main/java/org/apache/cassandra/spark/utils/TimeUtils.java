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
    public static long durationToMicros(Duration duration)
    {
        return TimeUnit.MILLISECONDS.toMicros(duration.toMillis());
    }
}
