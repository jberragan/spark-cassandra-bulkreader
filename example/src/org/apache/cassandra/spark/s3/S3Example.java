package org.apache.cassandra.spark.s3;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * An S3 example DataLayer implementation of the PartitionedDataLayer that can read from an entire C* cluster and scale linearly with more Spark workers.
 *
 * NOTE: this is provided only as an example implementation and is not prod ready.
 * Most importantly for large-scale use cases the InputStream opened on the Data.db files should be streaming/chunked and gracefully handle backpressure to prevent buffering too much data in memory and causing OOMs.
 * See {@link org.apache.cassandra.spark.s3.S3Client#open(String, String, String, String, String, String, DataLayer.FileType)}
 *
 * Set the env variables ACCESS_KEY & SECRET_KEY to use BasicAWSCredentials.
 *
 * It expects the data to be stored in the format: s3://bucketName/clusterName/keyspace/table/dataCenter/instanceToken
 *  e.g. s3://cassandra-spark-bulkreader/myCluster/myKeyspace/myTable/DC1/-9223372036854775808
 *       s3://cassandra-spark-bulkreader/myCluster/myKeyspace/myTable/DC1/-3074457345618258603
 *       s3://cassandra-spark-bulkreader/myCluster/myKeyspace/myTable/DC1/3074457345618258602
 */
public class S3Example
{
    public static void main(final String[] args)
    {
        final SparkSession spark = SparkSession.builder()
                                               .config(new SparkConf().set("spark.master", "local"))
                                               .getOrCreate();

        final Dataset<Row> df = spark.read().format(S3DataSource.class.getName())
                                     .option("s3-region", "us-west-2")
                                     .option("s3-bucket", "cassandra-spark-bulkreader")
                                     .option("clusterName", "myCluster")
                                     .option("keyspace", "myKeyspace")
                                     .option("table", "myTable")
                                     .option("tableCreateStmt", "CREATE TABLE myKeyspace.myTable (a bigint PRIMARY KEY, b bigint, c map<bigint, bigint>)")
                                     .option("DC", "DC1")
                                     .load();

        for (Row row : df.collectAsList())
        {
            System.out.println("Row: " + row);
        }
    }
}
