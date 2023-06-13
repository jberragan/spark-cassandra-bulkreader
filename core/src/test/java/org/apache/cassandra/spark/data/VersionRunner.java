package org.apache.cassandra.spark.data;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.CassandraVersion;

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

// run tests Parameterized for multiple versions of Cassandra
@RunWith(Parameterized.class)
public abstract class VersionRunner
{
    public final CassandraVersion version;
    public CassandraBridge bridge;

    @Parameterized.Parameters
    public static Collection<Object[]> versions()
    {
        return Arrays.asList(new Object[][]{
        { CassandraVersion.FOURZERO }
        });
    }

    public VersionRunner(CassandraVersion version)
    {
        this.version = version;
    }

    @Before
    public void initialize()
    {
        this.bridge = CassandraBridge.get(version);
    }
}
