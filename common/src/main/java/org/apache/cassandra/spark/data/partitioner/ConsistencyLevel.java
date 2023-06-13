package org.apache.cassandra.spark.data.partitioner;

import org.apache.cassandra.spark.data.ReplicationFactor;
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

public enum ConsistencyLevel
{
    ANY(0),
    ONE(1),
    TWO(2),
    THREE(3),
    QUORUM(4),
    ALL(5),
    LOCAL_QUORUM(6, true),
    EACH_QUORUM(7),
    SERIAL(8),
    LOCAL_SERIAL(9),
    LOCAL_ONE(10, true);

    public final int code;
    public final boolean isDCLocal;

    ConsistencyLevel(final int code)
    {
        this(code, false);
    }

    ConsistencyLevel(final int code, final boolean isDCLocal)
    {
        this.code = code;
        this.isDCLocal = isDCLocal;
    }

    private int quorumFor(final ReplicationFactor rf)
    {
        return (rf.getTotalReplicationFactor() / 2) + 1;
    }

    private int localQuorumFor(@NotNull final ReplicationFactor rf, @Nullable final String dc)
    {
        return (rf.getReplicationStrategy() == ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy)
               ? getNetworkTopologyRf(rf, dc)
               : quorumFor(rf);
    }

    private int getNetworkTopologyRf(@NotNull final ReplicationFactor rf, @Nullable final String dc)
    {
        final int dcRf;
        // single DC and no DC specified so return only DC replication factor
        if (dc == null && rf.getOptions().size() == 1)
        {
            dcRf = rf.getOptions().values().iterator().next();
        }
        else
        {
            if (!rf.getOptions().containsKey(dc))
            {
                throw new IllegalArgumentException(String.format("DC %s not found in replication factor %s", dc, rf.getOptions().keySet()));
            }
            dcRf = rf.getOptions().get(dc);
        }
        return (dcRf / 2) + 1;
    }

    public int blockFor(@NotNull final ReplicationFactor rf, @Nullable final String dc)
    {
        switch (this)
        {
            case ONE:
            case LOCAL_ONE:
            case ANY:
                return 1;
            case TWO:
                return 2;
            case THREE:
                return 3;
            case QUORUM:
            case SERIAL:
                return quorumFor(rf);
            case ALL:
                return rf.getTotalReplicationFactor();
            case LOCAL_QUORUM:
            case LOCAL_SERIAL:
                return localQuorumFor(rf, dc);
            case EACH_QUORUM:
                if (rf.getReplicationStrategy() == ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy)
                {
                    int n = 0;
                    for (final String datacenter : rf.getOptions().keySet())
                    {
                        n += localQuorumFor(rf, datacenter);
                    }
                    return n;
                }
                else
                {
                    return quorumFor(rf);
                }
            default:
                throw new UnsupportedOperationException("Invalid consistency level: " + this);
        }
    }
}
