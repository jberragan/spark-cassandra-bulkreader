package org.apache.cassandra.spark.utils;

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

public class ThrowableUtils
{
    /**
     * Find root cause of throwable or this throwable if no prior cause.
     *
     * @param t throwable
     * @return initial cause throwable.
     */
    @NotNull
    public static Throwable rootCause(@NotNull Throwable t)
    {
        while (t.getCause() != null)
        {
            t = t.getCause();
        }
        return t;
    }

    /**
     * Find first throwable of type matching ofType parameter or null if not exists.
     *
     * @param t      throwable
     * @param ofType type of class expected
     * @param <T>    generic type of expected return value
     * @return first throwable of type matching parameter ofType or null if cannot be found.
     */
    @Nullable
    public static <T extends Throwable> T rootCause(@NotNull Throwable t,
                                                    @NotNull final Class<T> ofType)
    {
        while (t.getCause() != null)
        {
            if (ofType.isInstance(t))
            {
                return ofType.cast(t);
            }
            t = t.getCause();
        }

        return ofType.isInstance(t) ? ofType.cast(t) : null;
    }
}
