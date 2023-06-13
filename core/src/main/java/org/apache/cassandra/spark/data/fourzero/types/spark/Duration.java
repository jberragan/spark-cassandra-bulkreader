package org.apache.cassandra.spark.data.fourzero.types.spark;

import org.apache.cassandra.spark.data.SparkCqlField;

public class Duration extends org.apache.cassandra.spark.data.fourzero.types.Duration implements SparkCqlField.NotImplementedTrait
{
    public static final Duration INSTANCE = new Duration();
}
