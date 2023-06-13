package org.apache.cassandra.spark.cdc;

public interface RowSink<ReturnType, RowType>
{
    ReturnType fromRow(RowType row);
}