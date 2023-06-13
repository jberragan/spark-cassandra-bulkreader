package org.apache.cassandra.spark.cdc;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.SparkTestUtils;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.fourzero.FourZero;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.Mutation;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.commitlog.CommitLog;

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

public class FourZeroCommitLog implements CassandraBridge.ICommitLog
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FourZeroCommitLog.class);
    private final File folder;

    public FourZeroCommitLog(File folder)
    {
        this.folder = folder;
        this.start();
    }

    @Override
    public synchronized void start()
    {
        LOGGER.info("Starting CommitLog.");
        DatabaseDescriptor.setCDCSpaceInMB(1024);
        CommitLog.instance.start();
    }

    @Override
    public synchronized void stop()
    {
        try
        {
            LOGGER.info("Shutting down CommitLog.");
            CommitLog.instance.shutdownBlocking();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void clear()
    {
        SparkTestUtils.clearDirectory(folder.toPath(), (path) -> LOGGER.info("Deleting CommitLog: " + path.toString()));
    }

    @Override
    public void add(CassandraBridge.IMutation mutation)
    {
        if (mutation instanceof FourZero.FourZeroMutation)
        {
            add(((FourZero.FourZeroMutation) mutation).mutation);
        }
    }

    public void add(Mutation mutation)
    {
        CommitLog.instance.add(mutation);
    }

    @Override
    public void sync()
    {
        try
        {
            CommitLog.instance.sync(true);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
