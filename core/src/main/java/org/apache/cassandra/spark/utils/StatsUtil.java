package org.apache.cassandra.spark.utils;

import java.util.concurrent.Callable;
import java.util.function.LongConsumer;

public class StatsUtil
{

    /**
     * Convenient helper that captures the time taken to run the task and pass it to the {@link LongConsumer} to report.
     * It is suitable for the tasks that have simple scope. For the complicated ones that have distinct callsites for
     * starting and stopping the timer, the method does not fit.
     */
    public static <T> T reportTimeTaken(Callable<T> task, LongConsumer nanosTaken)
    {
        long current = System.nanoTime();
        try
        {
            T res = task.call();
            nanosTaken.accept(System.nanoTime() - current);
            return res;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
