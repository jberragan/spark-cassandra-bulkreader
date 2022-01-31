package org.apache.cassandra.spark;

import org.apache.cassandra.spark.sparksql.LocalDataSource;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SimpleExample
{
    public static void main(final String[] args)
    {
        final SparkSession spark = SparkSession.builder()
                .config(new SparkConf().set("spark.master", "local"))
                .getOrCreate();

        final Dataset<Row> df = spark.read().format(LocalDataSource.class.getName())
                .option("keyspace", "test")
                .option("createStmt", "CREATE TABLE IF NOT EXISTS test.basic_test (a bigint PRIMARY KEY, b bigint, c bigint);")
                .option("dirs", args[0])
                .load();

        for (Row row : df.collectAsList())
        {
            System.out.println("Row: " + row);
        }
    }
}
