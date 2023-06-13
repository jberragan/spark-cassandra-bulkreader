package org.apache.cassandra.spark.cdc;

public interface RowSource<RowType>
{
    RowType toRow();
}