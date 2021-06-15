package org.apache.cassandra.spark.utils.streaming;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.jetbrains.annotations.NotNull;

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

// wrapper to allow us to pass skip requests down to the base InputStream
public class SkippableDataInputStream extends DataInputStream
{
    /**
     * Creates a DataInputStream that uses the specified
     * underlying InputStream.
     *
     * @param in the specified input stream
     */
    public SkippableDataInputStream(@NotNull InputStream in)
    {
        super(in);
    }

    public static DataInputStream of(InputStream in)
    {
        return new SkippableDataInputStream(in);
    }

    public long skip(long n) throws IOException
    {
        return in.skip(n);
    }
}
