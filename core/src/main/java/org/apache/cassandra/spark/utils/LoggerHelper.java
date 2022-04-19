package org.apache.cassandra.spark.utils;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;

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

/**
 * Helper class to automatically append fixed logging values for every message.
 */
public class LoggerHelper
{
    private final Logger logger;
    private final String keys;
    private final Object[] fixedArgs;

    public LoggerHelper(final Logger logger, Object... fixedArgs)
    {
        this.logger = logger;
        this.fixedArgs = extractArgs(fixedArgs);
        this.keys = buildKey(fixedArgs);
    }

    private static Object[] extractArgs(Object... args)
    {
        Preconditions.checkArgument(args.length % 2 == 0, "Expect even number of key/value pairs in fixedArgs");
        return IntStream.range(0, args.length).filter(i -> i % 2 != 0).mapToObj(i -> args[i]).toArray(Object[]::new);
    }

    private static String buildKey(Object... args)
    {
        Preconditions.checkArgument(args.length % 2 == 0, "Expect even number of key/value pairs in fixedArgs");
        return " " + IntStream.range(0, args.length).filter(i -> i % 2 == 0)
                              .mapToObj(i -> args[i])
                              .map(s -> s + "={}")
                              .collect(Collectors.joining(" "));
    }

    public void trace(String msg, Object... arguments)
    {
        if (logger.isTraceEnabled())
        {
            logger.trace(logMsg(msg, arguments), buildArgs(arguments));
        }
    }

    public void debug(String msg, Object... arguments)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug(logMsg(msg, arguments), buildArgs(arguments));
        }
    }

    public void info(String msg, Object... arguments)
    {
        logger.info(logMsg(msg, arguments), buildArgs(arguments));
    }

    public void warn(String msg, Throwable throwable, Object... arguments)
    {
        logger.warn(logMsg(msg, arguments), buildArgs(throwable, arguments));
    }

    public void error(String msg, Throwable t, Object... arguments)
    {
        logger.error(logMsg(msg, arguments), buildArgs(t, arguments));
    }

    public String logMsg(String msg, Object... arguments)
    {
        if (arguments.length > 0)
        {
            return msg + keys + buildKey(arguments);
        }
        return msg + keys;
    }

    private Object[] buildArgs(Object... arguments)
    {
        return buildArgs(null, arguments);
    }

    public Object[] buildArgs(@Nullable Throwable t, Object... arguments)
    {
        final Object[] argValues = extractArgs(arguments);
        final Object[] args = new Object[argValues.length + fixedArgs.length + (t == null ? 0 : 1)];
        System.arraycopy(fixedArgs, 0, args, 0, fixedArgs.length);
        System.arraycopy(argValues, 0, args, fixedArgs.length, argValues.length);
        if (t != null)
        {
            args[argValues.length + fixedArgs.length] = t;
        }
        return args;
    }
}