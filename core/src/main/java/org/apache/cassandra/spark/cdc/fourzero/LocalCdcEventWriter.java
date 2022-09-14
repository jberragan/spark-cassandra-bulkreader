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

package org.apache.cassandra.spark.cdc.fourzero;

import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.spark.cdc.AbstractCdcEvent;

/**
 * Local impl for _testing_ only
 */
@VisibleForTesting
public class LocalCdcEventWriter extends CdcEventWriter
{
    // Declared as `static` because the writer is serialized and deserialized to worker(s).
    // The instance passed to spark is not the instance that runs in worker(s).
    public static final List<AbstractCdcEvent> events = new ArrayList<>();

    @Override
    public void processEvent(AbstractCdcEvent event)
    {
        // simply collect the events produced in the test
        events.add(event);
    }
}
