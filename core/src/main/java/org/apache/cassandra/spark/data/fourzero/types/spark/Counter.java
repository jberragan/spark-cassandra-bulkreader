package org.apache.cassandra.spark.data.fourzero.types.spark;

import org.apache.cassandra.spark.data.SparkCqlField;

public class Counter extends org.apache.cassandra.spark.data.fourzero.types.Counter implements SparkCqlField.NotImplementedTrait
{
    public static final Counter INSTANCE = new Counter();
}
