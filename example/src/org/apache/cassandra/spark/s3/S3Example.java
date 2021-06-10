package org.apache.cassandra.spark.s3;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * An S3 example DataLayer implementation of the PartitionedDataLayer that can read from an entire C* cluster and scale linearly with more Spark workers.
 *
 * NOTE: this is provided only as an example implementation and has not been tested at scale.
 *
 * Set the env variables ACCESS_KEY & SECRET_KEY to use BasicAWSCredentials.
 *
 * It expects the data to be stored in the format: s3://bucketName/clusterName/keyspace/table/dataCenter/instanceToken
 *  e.g. s3://cassandra-spark-bulkreader/myCluster/myKeyspace/myTable/DC1/-9223372036854775808/md-1-big-Data.db
 *       s3://cassandra-spark-bulkreader/myCluster/myKeyspace/myTable/DC1/-9223372036854775808/md-1-big-CompressionInfo.db
 *       s3://cassandra-spark-bulkreader/myCluster/myKeyspace/myTable/DC1/-9223372036854775808/md-1-big-Summary.db
 *       3://cassandra-spark-bulkreader/myCluster/myKeyspace/myTable/DC1/-9223372036854775808/md-1-big-Statistics.db
 *       s3://cassandra-spark-bulkreader/myCluster/myKeyspace/myTable/DC1/-9223372036854775808/md-2-big-Data.db
 *       s3://cassandra-spark-bulkreader/myCluster/myKeyspace/myTable/DC1/-9223372036854775808/md-2-big-CompressionInfo.db
 *       s3://cassandra-spark-bulkreader/myCluster/myKeyspace/myTable/DC1/-3074457345618258603/...etc
 *       s3://cassandra-spark-bulkreader/myCluster/myKeyspace/myTable/DC1/3074457345618258602/...etc
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
        System.out.println("Number of rows: " + df.count());
    }
}
