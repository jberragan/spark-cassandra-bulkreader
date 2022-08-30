package org.apache.cassandra.spark.utils;

import java.time.Duration;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimeUtilsTest
{
    @Test
    public void testDurationToMicros()
    {
        assertEquals(1000_000L, TimeUtils.durationToMicros(Duration.ofSeconds(1)));
        assertEquals(1234_000L, TimeUtils.durationToMicros(Duration.ofMillis(1234)));
    }
}
