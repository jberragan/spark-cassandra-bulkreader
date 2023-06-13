package org.apache.cassandra.spark.utils;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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

public class LoggerHelperTests
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerHelperTests.class);

    @Test
    public void testBuildArgs()
    {
        final LoggerHelper loggerHelper = new LoggerHelper(LOGGER, "a", "1", "b", "2", "c", "3");
        final Object[] additionalArgs = new Object[]{ "d", "4", "e", "5", "f", "6" };
        assertEquals("hello this is the log message a={} b={} c={} d={} e={} f={}",
                     loggerHelper.logMsg("hello this is the log message", additionalArgs));
        assertArrayEquals(new Object[]{ "1", "2", "3", "4", "5", "6" }, loggerHelper.buildArgs(null, additionalArgs));
        final Throwable t = new RuntimeException("Error");
        assertArrayEquals(new Object[]{ "1", "2", "3", "4", "5", "6", t }, loggerHelper.buildArgs(t, additionalArgs));
    }
}
