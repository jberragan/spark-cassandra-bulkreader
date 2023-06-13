package org.apache.cassandra.spark.cdc;

import java.nio.ByteBuffer;
import java.util.List;

public interface ICassandraSource
{
    /**
     * Read values from Cassandra, instead of using the values from commitlog
     *
     * @param keyspace          name of the keyspace
     * @param table             name of the Table
     * @param columnsToFetch    lis of columns to fetch
     * @param primaryKeyColumns primary key columns to locate the row
     * @return list of values read from cassandra. The size should be the same as columnsToFetch
     */
    ByteBuffer readFromCassandra(String keyspace, String table, List<String> columnsToFetch,
                                 List<ValueWithMetadata> primaryKeyColumns);
}
