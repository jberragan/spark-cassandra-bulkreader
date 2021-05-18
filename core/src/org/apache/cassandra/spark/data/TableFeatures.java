package org.apache.cassandra.spark.data;

public interface TableFeatures
{
    boolean addLastModifiedTimestamp();

    String lastModifiedTimestampColumnName();

    class Default implements TableFeatures
    {
        public boolean addLastModifiedTimestamp()
        {
            return false;
        }

        public String lastModifiedTimestampColumnName()
        {
            return "last_modified_timestamp";
        }
    }
}
