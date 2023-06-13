package org.apache.cassandra.spark.cdc.watermarker;

import org.apache.spark.TaskContext;

public class SparkInMemoryWatermarker
{
    public static final InMemoryWatermarker INSTANCE = new InMemoryWatermarker(new InMemoryWatermarker.TaskContextProvider()
    {
        public boolean hasTaskContext()
        {
            return TaskContext.get() != null;
        }

        public int partitionId()
        {
            return TaskContext.getPartitionId();
        }
    });
}
