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

package org.apache.cassandra.spark.cdc.jdk;

import java.util.List;

import org.apache.cassandra.spark.cdc.RangeTombstone;
import org.apache.cassandra.spark.cdc.RowSource;
import org.jetbrains.annotations.NotNull;

public class JdkRangeTombstone extends RangeTombstone<JdkValueMetadata> implements RowSource<org.apache.cassandra.spark.cdc.jdk.msg.RangeTombstone>
{
    public JdkRangeTombstone(@NotNull List<JdkValueMetadata> startBound, boolean startInclusive,
                             @NotNull List<JdkValueMetadata> endBound, boolean endInclusive)
    {
        super(startBound, startInclusive, endBound, endInclusive);
    }

    public org.apache.cassandra.spark.cdc.jdk.msg.RangeTombstone toRow()
    {
        return new org.apache.cassandra.spark.cdc.jdk.msg.RangeTombstone(this);
    }
}
