package org.apache.cassandra.spark.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class IOUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IOUtils.class);

    public static long fileLength(@Nullable final Path path)
    {
        try
        {
            return path == null ? 0 : Files.size(path);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void closeQuietly(@Nullable final AutoCloseable closeable)
    {
        if (closeable == null)
        {
            return;
        }

        try
        {
            closeable.close();
        }
        catch (Throwable t)
        {
            LOGGER.warn("Exception closing {}", closeable.getClass().getName(), t);
        }
    }
}
