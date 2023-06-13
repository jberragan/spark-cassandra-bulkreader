package org.apache.cassandra.spark.data;

import java.util.ArrayList;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;

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
public class ReplicationFactorTests
{

    @Test
    public void testReplicationFactorNts()
    {
        final ReplicationFactor rf = new ReplicationFactor(ImmutableMap.of("class", "NetworkTopologyStrategy", "DC1", "3", "dc2", "5"));
        assertEquals(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, rf.getReplicationStrategy());
        assertEquals(Integer.valueOf(3), rf.getOptions().get("DC1"));
        assertEquals(Integer.valueOf(5), rf.getOptions().get("DC2"));
    }

    @Test
    public void testReplicationFactorNtsOss()
    {
        final ReplicationFactor rf = new ReplicationFactor(ImmutableMap.of("class", SSTable.OSS_PACKAGE_NAME + "locator.NetworkTopologyStrategy", "DC1", "9", "DC2", "2"));
        assertEquals(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, rf.getReplicationStrategy());
        assertEquals(new Integer(9), rf.getOptions().get("DC1"));
        assertEquals(new Integer(2), rf.getOptions().get("DC2"));
    }

    @Test
    public void testReplicationFactorNtsShaded()
    {
        final ReplicationFactor rf = new ReplicationFactor(ImmutableMap.of("class", SSTable.SHADED_PACKAGE_NAME + "locator.NetworkTopologyStrategy", "DC1", "3", "DC2", "3"));
        assertEquals(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, rf.getReplicationStrategy());
        assertEquals(new Integer(3), rf.getOptions().get("DC1"));
        assertEquals(new Integer(3), rf.getOptions().get("DC2"));
    }

    @Test
    public void testReplicationFactorSimple()
    {
        final ReplicationFactor rf = new ReplicationFactor(ImmutableMap.of("class", "SimpleStrategy", "replication_factor", "3"));
        assertEquals(ReplicationFactor.ReplicationStrategy.SimpleStrategy, rf.getReplicationStrategy());
        assertEquals(new Integer(3), rf.getOptions().get("replication_factor"));
    }

    @Test
    public void testReplicationFactorSimpleOss()
    {
        final ReplicationFactor rf = new ReplicationFactor(ImmutableMap.of("class", SSTable.OSS_PACKAGE_NAME + "locator.SimpleStrategy", "replication_factor", "5"));
        assertEquals(ReplicationFactor.ReplicationStrategy.SimpleStrategy, rf.getReplicationStrategy());
        assertEquals(new Integer(5), rf.getOptions().get("replication_factor"));
    }

    @Test
    public void testReplicationFactorSimpleShaded()
    {
        final ReplicationFactor rf = new ReplicationFactor(ImmutableMap.of("class", SSTable.SHADED_PACKAGE_NAME + "locator.SimpleStrategy", "replication_factor", "5"));
        assertEquals(ReplicationFactor.ReplicationStrategy.SimpleStrategy, rf.getReplicationStrategy());
        assertEquals(new Integer(5), rf.getOptions().get("replication_factor"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnexpectedRFClass()
    {
        new ReplicationFactor(ImmutableMap.of("class", SSTable.SHADED_PACKAGE_NAME + "locator.NotSimpleStrategy", "replication_factor", "5"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnknownRFClass()
    {
        new ReplicationFactor(ImmutableMap.of("class", "NoSuchStrategy", "replication_factor", "5"));
    }

    @Test
    public void testEquality()
    {
        final ReplicationFactor rf1 = new ReplicationFactor(ImmutableMap.of("class", SSTable.SHADED_PACKAGE_NAME + "locator.SimpleStrategy", "replication_factor", "5"));
        final ReplicationFactor rf2 = new ReplicationFactor(ImmutableMap.of("class", SSTable.SHADED_PACKAGE_NAME + "locator.SimpleStrategy", "replication_factor", "5"));
        assertNotSame(rf1, rf2);
        assertNotEquals(null, rf1);
        assertNotEquals(rf2, null);
        assertEquals(rf1, rf1);
        assertEquals(rf2, rf2);
        assertNotEquals(new ArrayList<>(), rf1);
        assertEquals(rf1, rf2);
        assertEquals(rf1.hashCode(), rf2.hashCode());
    }
}
