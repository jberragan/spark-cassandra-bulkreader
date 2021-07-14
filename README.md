# Apache Cassandra-Spark Bulk Reader [Beta]

This project provides a library for reading raw Cassandra SSTables into SparkSQL along the principles of ’streaming compaction’.

By reading the raw SSTables directly, the Cassandra-Spark Bulk Reader enables efficient and fast massive-scale analytics queries without impacting the performance of a production Cassandra cluster. 

**This project is in Beta.**

Requirements
------------
  1. Java >= 1.8 (OpenJDK or Oracle), or Java 11
  2. Apache Cassandra 3.0+
  3. Apache Spark 2.4

Overview
--------------------------------------

The primary interface for reading SSTables is through the DataLayer abstraction. A simple example 'LocalDataLayer' implementation is provided for reading SSTables from a local file system.

The role of the DataLayer is to:
 - return a SchemaStruct, mapping the Cassandra CQL table schema to the SparkSQL schema.
 - a list of sstables available for reading.
 - a method to open an InputStream on any file component of an sstable (e.g. data, compression, summary etc).

The PartitionedDataLayer abstraction builds on the DataLayer interface for partitioning Spark workers across a Cassandra token ring - allowing the Spark job to scale linearly - and reading from sufficient Cassandra replicas to achieve a user-specified consistency level.

At the core, the Bulk Reader uses the Apache Cassandra [CompactionIterator](https://github.com/mariusae/cassandra/blob/master/src/java/org/apache/cassandra/io/CompactionIterator.java) to perform the streaming compaction. The SparkRowIterator and SparkCellIterator iterate through the CompactionIterator, deserialize the ByteBuffers, convert into the appropriate SparkSQL data type, and finally pivot each cell into a SparkSQL row.

Features
---------

The bulk reader supports all major Cassandra features:

* Cassandra 3.0 & 4.0.
* Murmur3 and Random Partitioner.
* Native CQL data types (ascii, bigint, blob, boolean, date, decimal, double, float, inet, int, smallint, text, time, timestamp, timeuuid, tinyint, uuid, varchar, varint).
* Tuples & collections (map, set, list).
* User-defined types.
* Frozen or nested data types (native types, collections, tuples or UDTs nested inside other data types).
* Any supported data-types for primary key fields.
* Respect Spark prune columns filter to efficiently exclude columns. 
* Optional last modified timestamp column to aggregate and append last modified timestamp per row.
* Efficient InputStream skip implementation (when using [SSTableInputStream](core/src/org/apache/cassandra/spark/utils/streaming/SSTableInputStream.java)) to efficiently skip out-of-range partitions or excluded columns without reading through the [DataLayer](core/src/org/apache/cassandra/spark/data/DataLayer.java). 

Gotchas/unsupported features:
* Counters.
* Duration data type.
* The PartitionedDataLayer has been tested with Cassandra clusters that use 1 token per Cassandra instance. Any PartitionedDataLayer implementation that needs virtual nodes can extend the CassandraInstance class to allow multiple tokens per instance.
* Due to how Spark sets the precision and scale per Decimal data type, loss of precision can occur when using the BigDecimal data type.
* EACH_QUORUM consistency level has not been implemented yet. 

Getting Started
------------

For a basic example to read from a local filesystem see: [SimpleExample](example/src/org.apache.cassandra.spark/SimpleExample.java).

By default the example expects the schema:

    CREATE TABLE IF NOT EXISTS test.basic_test (a bigint PRIMARY KEY, b bigint, c bigint);

To run:

     ./gradlew example:build && ./gradlew example:run  --args="/path/to/cassandra/d1/data/dir,/path/to/cassandra/d2/data/dir"
       ....
       Row: [2,2,2]
       Row: [3,3,3]
       Row: [4,4,4]
       Row: [0,0,0]
       Row: [1,1,1]

**Note**, the core module pulls in Apache Spark as *compileOnly*, so you either need to depend on Spark as *compile* in your project or pull in the Spark jars at runtime. 

To implement your own DataLayer, first take a look at the example local implementation: [LocalDataSource](core/src/org/apache/cassandra/spark/sparksql/LocalDataSource.java), [LocalDataLayer](core/src/org/apache/cassandra/spark/data/LocalDataLayer.java).

To implement a DataLayer that partitions the Spark workers and respects a given consistency level, extend the [PartitionedDataLayer](core/src/org/apache/cassandra/spark/data/PartitionedDataLayer.java).

Real S3 Example
------------

For an example implementation of the [PartitionedDataLayer](core/src/org/apache/cassandra/spark/data/PartitionedDataLayer.java), see [S3Example](example/src/org/apache/cassandra/spark/s3/S3Example.java), [S3DataLayer](example/src/org/apache/cassandra/spark/s3/S3DataLayer.java).

This example shows you how to read an entire C* cluster backed-up to cloud storage (e.g. for backups.), and how to build a [PartitionedDataLayer](core/src/org/apache/cassandra/spark/data/PartitionedDataLayer.java) that partitions the Spark workers and scales linearly across Spark workers.

    final Dataset<Row> df = spark.read().format(S3DataSource.class.getName())
                                     .option("s3-region", "us-west-2")
                                     .option("s3-bucket", "my-bucket")
                                     .option("clusterName", "myCluster")
                                     .option("keyspace", "myKeyspace")
                                     .option("table", "myTable")
                                     .option("tableCreateStmt", "CREATE TABLE myKeyspace.myTable (a bigint PRIMARY KEY, b bigint, c map<bigint, bigint>)")
                                     .option("DC", "DC1")
                                     .load();
        System.out.println("Number of rows: " + df.count());
  
Testing
---------

The project is robustly tested using a bespoke property-based testing system that uses [QuickTheories](https://github.com/quicktheories/QuickTheories) to enumerate many Cassandra CQL schemas, write random data using the Cassandra [CQLSSTableWriter](https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/io/sstable/CQLSSTableWriter.java), read the SSTables into SparkSQL and verify the resulting SparkSQL rows match the expected.  

For examples tests see org.apache.cassandra.spark.EndToEndTests.
