package org.apache.cassandra.spark.sparksql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

/**
 * Wrapper iterator around SparkCellIterator to normalize cells into Spark SQL rows
 */
public class SparkRowIterator implements InputPartitionReader<InternalRow>
{
    private final SparkCellIterator it;
    private final int numFields;
    private final boolean noValueColumns;
    private SparkCellIterator.Cell cell = null;

    @VisibleForTesting
    public SparkRowIterator(@NotNull final DataLayer dataLayer)
    {
        this(dataLayer, null, new ArrayList<>());
    }

    SparkRowIterator(@NotNull final DataLayer dataLayer, @Nullable final StructType requiredSchema, @NotNull final List<CustomFilter> filters)
    {
        this.it = new SparkCellIterator(dataLayer, requiredSchema, filters);
        this.numFields = dataLayer.cqlSchema().numFields();
        this.noValueColumns = dataLayer.cqlSchema().numValueColumns() == 0;
    }

    @Override
    public boolean next()
    {
        if (this.cell != null)
        {
            return true;
        }
        return this.it.hasNext();
    }

    @Override
    public InternalRow get()
    {
        final Object[] result = new Object[this.numFields];
        int count = 0;

        // pivot values to normalize each cell into single SparkSQL or 'CQL' type row
        do
        {
            if (this.cell == null)
            {
                this.cell = this.it.next();
            }

            assert this.cell.values.length > 0 && this.cell.values.length <= this.numFields;
            if (count == 0)
            {
                // on first iteration, copy all partition keys, clustering keys, static columns
                assert this.cell.isNewRow;

                // need to handle special case where schema is only partition or clustering keys - i.e. not value columns
                final int len = this.noValueColumns ? this.cell.values.length : this.cell.values.length - 1;
                System.arraycopy(this.cell.values, 0, result, 0, len);
                count += len;
            }
            else if (this.cell.isNewRow)
                // current row is incomplete so we have moved to new row before reaching end
                // break out to return current incomplete row and handle next row in next iteration
            {
                break;
            }

            if (!this.noValueColumns)
            {
                // copy the next value column
                result[this.cell.pos] = this.cell.values[this.cell.values.length - 1];
                count++;
            }
            this.cell = null;
        } while (count < result.length && this.next());

        return new GenericInternalRow(result);
    }

    @Override
    public void close() throws IOException
    {
        this.it.close();
    }
}
