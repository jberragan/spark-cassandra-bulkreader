package org.apache.cassandra.spark.data;

public interface TableFeatures
{
    boolean addLastModifiedTimestamp();

    String lastModifiedTimestampColumnName();

    boolean addUpdatedFieldsIndicator();

    String updatedFieldsIndicatorColumnName();

    boolean addUpdateFlag();

    String updateFlagColumnName();

    // TODO: refactor the features. Extract a feature interface, - name, - enabled, - spark struct type. And make it able to load from options
    boolean supportCellDeletionInComplex();

    String supportCellDeletionInComplexColumnName();

    class Default implements TableFeatures
    {
        public static final String LAST_MODIFIED_TIMESTAMP_COLUMN_NAME = "last_modified_timestamp";
        public static final String UPDATED_FIELDS_INDICATOR_COLUMN_NAME = "updated_fields_indicator";
        public static final String UPDATE_FLAG_COLUMN_NAME = "is_update";

        public static final String SUPPORT_CELL_DELETION_IN_COMPLEX_COLUMN_NAME = "cell_deletion_in_complex";

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

        @Override
        public boolean supportCellDeletionInComplex() {
            return false;
        }

        @Override
        public String supportCellDeletionInComplexColumnName() {
            return SUPPORT_CELL_DELETION_IN_COMPLEX_COLUMN_NAME;
        }
    }
}
