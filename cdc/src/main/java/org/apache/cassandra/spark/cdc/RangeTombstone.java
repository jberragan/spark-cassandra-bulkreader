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

import org.jetbrains.annotations.NotNull;

public abstract class RangeTombstone<ValueType extends ValueWithMetadata>
{
    protected static final String RANGE_START = "Start";
    protected static final String RANGE_START_INCL = "StartInclusive";
    protected static final String RANGE_END = "End";
    protected static final String RANGE_END_INCL = "EndInclusive";
    final List<ValueType> startBound;
    public final boolean startInclusive;
    final List<ValueType> endBound;
    public final boolean endInclusive;

    public RangeTombstone()
    {
        this.startInclusive = false;
        this.startBound = null;
        this.endInclusive = false;
        this.endBound = null;
    }

    public RangeTombstone(@NotNull List<ValueType> startBound, boolean startInclusive,
                          @NotNull List<ValueType> endBound, boolean endInclusive)
    {
        this.startBound = new ArrayList<>(startBound);
        this.startInclusive = startInclusive;
        this.endBound = new ArrayList<>(endBound);
        this.endInclusive = endInclusive;
    }

    public List<ValueType> getStartBound()
    {
        if (startBound == null)
        {
            return null;
        }
        return new ArrayList<>(startBound);
    }

    public List<ValueType> getEndBound()
    {
        if (endBound == null)
        {
            return null;
        }
        return new ArrayList<>(endBound);
    }
}
