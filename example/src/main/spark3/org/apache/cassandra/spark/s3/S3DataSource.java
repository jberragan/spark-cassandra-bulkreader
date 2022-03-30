package org.apache.cassandra.spark.s3;

import java.util.Map;
import java.util.function.Supplier;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.cassandra.spark.sparksql.CassandraTableProvider;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

// S3DataSource that implements Spark 3 CassandraTableProvider
public class S3DataSource extends CassandraTableProvider
{
    public DataLayer getDataLayer(CaseInsensitiveStringMap options)
    {
        return new S3DataLayer(
        ConsistencyLevel.valueOf(options.getOrDefault("consistencyLevel", ConsistencyLevel.LOCAL_QUORUM.toString())),
        getOrThrow(options, "clusterName"),
        getOrThrow(options, "keyspace"),
        getOrThrow(options, "table"),
        getOrThrow(options, "tableCreateStmt"),
        getOrThrow(options, "DC"),
        getOrThrow(options, "s3-region"),
        getOrThrow(options, "s3-bucket"),
        options.getInt("defaultParallelism", 1),
        options.getInt("numCores", 1)
        );
    }

    static String getOrThrow(Map<String, String> options, String key)
    {
        return getOrThrow(options, key, () -> new RuntimeException("No " + key + " specified"));
    }

    static String getOrThrow(Map<String, String> options, String key, Supplier<RuntimeException> throwable)
    {
        final String value = options.get(key);
        if (value == null)
        {
            throw throwable.get();
        }
        return value;
    }

    public String shortName()
    {
        return "s3-datasource";
    }
}
