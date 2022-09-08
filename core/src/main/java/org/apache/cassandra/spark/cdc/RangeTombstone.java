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

package org.apache.cassandra.spark.cdc;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

public class RangeTombstone implements SparkRowSource, SparkRowSink<RangeTombstone>
{
    protected static final String RANGE_START     = "Start";
    protected static final String RANGE_START_INCL= "StartInclusive";
    protected static final String RANGE_END       = "End";
    protected static final String RANGE_END_INCL  = "EndInclusive";
    private final List<ValueWithMetadata> startBound;
    public final boolean startInclusive;
    private final List<ValueWithMetadata> endBound;
    public final boolean endInclusive;

    public static final StructType SCHEMA;
    static
    {
        StructField[] rt = new StructField[4];
        int i = 0;
        // The array of binaries follows the same seq of the clustering key definition,
        // e.g. for primary key (pk, ck1, ck2), the array value could be [ck1] or [ck1, ck2], but never (ck2) w/o ck1
        rt[i++] = DataTypes.createStructField(RANGE_START, DataTypes.createArrayType(ValueWithMetadata.SCHEMA), false);
        rt[i++] = DataTypes.createStructField(RANGE_START_INCL, DataTypes.BooleanType, false);
        rt[i++] = DataTypes.createStructField(RANGE_END, DataTypes.createArrayType(ValueWithMetadata.SCHEMA), false);
        rt[i++] = DataTypes.createStructField(RANGE_END_INCL, DataTypes.BooleanType, false);
        SCHEMA = DataTypes.createStructType(rt);
    }

    public static final RangeTombstone EMPTY = new RangeTombstone();

    public static RangeTombstone of(@NotNull List<ValueWithMetadata> startBound, boolean startInclusive,
                                    @NotNull List<ValueWithMetadata> endBound, boolean endInclusive)
    {
        return new RangeTombstone(startBound, startInclusive, endBound, endInclusive);
    }

    private RangeTombstone()
    {
        this.startInclusive = false;
        this.startBound = null;
        this.endInclusive = false;
        this.endBound = null;
    }

    private RangeTombstone(@NotNull List<ValueWithMetadata> startBound, boolean startInclusive,
                           @NotNull List<ValueWithMetadata> endBound, boolean endInclusive)
    {
        this.startBound = new ArrayList<>(startBound);
        this.startInclusive = startInclusive;
        this.endBound = new ArrayList<>(endBound);
        this.endInclusive = endInclusive;
    }

    @Override
    public RangeTombstone fromRow(Row row)
    {
        boolean startIncl = row.getBoolean(SCHEMA.fieldIndex(RANGE_START_INCL));
        boolean endIncl = row.getBoolean(SCHEMA.fieldIndex(RANGE_END_INCL));
        List<ValueWithMetadata> start = AbstractCdcEvent.arrayToCqlFields(row.get(SCHEMA.fieldIndex(RANGE_START)), false);
        List<ValueWithMetadata> end = AbstractCdcEvent.arrayToCqlFields(row.get(SCHEMA.fieldIndex(RANGE_END)), false);
        // start and end are not nullable. Compiler is just being dumb.
        return new RangeTombstone(start, startIncl, end, endIncl);
    }

    @Override
    public InternalRow toRow()
    {
        Preconditions.checkState(startBound != null && endBound != null);
        Object[] rt = new Object[SCHEMA.size()];
        rt[SCHEMA.fieldIndex(RANGE_START)] = AbstractCdcEvent.cqlFieldsToArray(startBound);
        rt[SCHEMA.fieldIndex(RANGE_START_INCL)] = startInclusive;
        rt[SCHEMA.fieldIndex(RANGE_END)] = AbstractCdcEvent.cqlFieldsToArray(endBound);
        rt[SCHEMA.fieldIndex(RANGE_END_INCL)] = endInclusive;
        return new GenericInternalRow(rt);
    }

    public List<ValueWithMetadata> getStartBound()
    {
        if (startBound == null)
        {
            return null;
        }
        return new ArrayList<>(startBound);
    }

    public List<ValueWithMetadata> getEndBound()
    {
        if (endBound == null)
        {
            return null;
        }
        return new ArrayList<>(endBound);
    }
}
