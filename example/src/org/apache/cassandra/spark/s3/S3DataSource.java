package org.apache.cassandra.spark.s3;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.cassandra.spark.sparksql.CassandraDataSource;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

public class S3DataSource extends CassandraDataSource
{
    public DataLayer getDataLayer(DataSourceOptions options)
    {
        return new S3DataLayer(
        options.get("consistencyLevel").map(ConsistencyLevel::valueOf).orElse(ConsistencyLevel.LOCAL_QUORUM),
        options.get("clusterName").orElseThrow(() -> new RuntimeException("No cluster name specified")),
        options.get("keyspace").orElseThrow(() -> new RuntimeException("No keyspace specified")),
        options.get("table").orElseThrow(() -> new RuntimeException("No table specified")),
        options.get("tableCreateStmt").orElseThrow(() -> new RuntimeException("No tableCreateStmt specified")),
        options.get("DC").orElseThrow(() -> new RuntimeException("No DC specified")),
        options.get("s3-region").orElseThrow(() -> new RuntimeException("No S3 region specified")),
        options.get("s3-bucket").orElseThrow(() -> new RuntimeException("No S3 bucket specified")),
        options.getInt("defaultParallelism", 1),
        options.getInt("numCores", 1)
        );
    }

    public String shortName()
    {
        return "s3-datasource";
    }
}
