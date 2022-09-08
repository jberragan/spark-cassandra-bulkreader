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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

public abstract class AbstractCdcEventWriter extends ForeachWriter<Row>
{
    private transient volatile Logger logger = LoggerFactory.getLogger(getClass());

    protected abstract void initCqlSchema();

    /**
     * Called to process the cdc events in the executor side. 
     * Just like {@link ForeachWriter#process(Object)}, this method will be called only if open returns true.
     */
    public abstract void processEvent(AbstractCdcEvent event);

    @Override
    public boolean open(long partitionId, long epochId)
    {
        return true;
    }

    @Override
    public void close(Throwable errorOrNull)
    {
        if (errorOrNull != null)
        {
            logger().error("Closing CDC event writer on failure", errorOrNull);
        }
        else
        {
            logger().info("Closing CDC event writer");
        }
    }

    protected Logger logger()
    {
        if (logger == null)
        {
            synchronized (this)
            {
                if (logger == null)
                {
                    logger = LoggerFactory.getLogger(getClass());
                }
            }
        }
        return logger;
    }
}
