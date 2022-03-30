package org.apache.cassandra.spark.data;

public interface TableFeatures
{
    boolean addLastModifiedTimestamp();

    String lastModifiedTimestampColumnName();

    boolean addUpdatedFieldsIndicator();

    String updatedFieldsIndicatorColumnName();

    boolean addUpdateFlag();

    String updateFlagColumnName();

    class Default implements TableFeatures
    {
        public static final String LAST_MODIFIED_TIMESTAMP_COLUMN_NAME = "last_modified_timestamp";
        public static final String UPDATED_FIELDS_INDICATOR_COLUMN_NAME = "updated_fields_indicator";
        public static final String UPDATE_FLAG_COLUMN_NAME = "is_update";

        public boolean addLastModifiedTimestamp()
        {
            return false;
        }

        public String lastModifiedTimestampColumnName()
        {
            return LAST_MODIFIED_TIMESTAMP_COLUMN_NAME;
        }

        @Override
        public boolean addUpdatedFieldsIndicator()
        {
            return false;
        }

        @Override
        public String updatedFieldsIndicatorColumnName()
        {
            return UPDATED_FIELDS_INDICATOR_COLUMN_NAME;
        }

        public boolean addUpdateFlag()
        {
            return false;
        }

        public String updateFlagColumnName()
        {
            return UPDATE_FLAG_COLUMN_NAME;
        }
    }
}
