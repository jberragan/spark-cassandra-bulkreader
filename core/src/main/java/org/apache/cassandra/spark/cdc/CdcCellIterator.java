package org.apache.cassandra.spark.cdc;

import java.util.List;

import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.reader.IStreamScanner;
import org.apache.cassandra.spark.sparksql.SparkCellIterator;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.cassandra.spark.sparksql.filters.CustomFilter;
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

public class CdcCellIterator extends SparkCellIterator
{
    public CdcCellIterator(@NotNull DataLayer dataLayer,
                           @Nullable StructType requiredSchema,
                           @NotNull List<CustomFilter> filters)
    {
        super(dataLayer, requiredSchema, filters);
    }

    @Override
    protected IStreamScanner openScanner(@NotNull final List<CustomFilter> filters)
    {
        final CdcOffsetFilter filter = (CdcOffsetFilter) filters.stream()
                                                                .filter(f -> f instanceof CdcOffsetFilter)
                                                                .findFirst().orElse(null);
        return this.dataLayer.openCdcScanner(filter);
    }
}