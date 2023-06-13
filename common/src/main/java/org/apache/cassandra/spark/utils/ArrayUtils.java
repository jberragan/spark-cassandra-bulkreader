package org.apache.cassandra.spark.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

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

public class ArrayUtils
{
    private ArrayUtils()
    {
    }

    public static <T> Stream<T> concatToStream(@NotNull List<T>... lists)
    {
        if (lists.length == 0)
        {
            return Stream.empty();
        }
        Stream<T> curr = lists[0].stream();
        for (int i = 1; i < lists.length; i++)
        {
            curr = Stream.concat(curr, lists[i].stream());
        }
        return curr;
    }

    public static <T> List<T> combine(@NotNull List<T>... lists)
    {
        final List<T> result = new ArrayList<>(Arrays.stream(lists).filter(Objects::nonNull).mapToInt(List::size).sum());
        for (List<T> list : lists)
        {
            if (list != null)
            {
                result.addAll(list);
            }
        }
        return result;
    }

    public static <T> List<T> orElse(@Nullable List<T> v1, @NotNull final List<T> v2)
    {
        return v1 == null ? v2 : v1;
    }

    public static Object[] retain(Object[] src, int srcPos, int len)
    {
        Preconditions.checkArgument(srcPos >= 0 && len >= 0 && src != null);
        Preconditions.checkArgument(srcPos + len <= src.length,
                                    "Requested retain range exceed the src array!");
        Object[] result = new Object[len];
        if (len > 0)
        {
            System.arraycopy(src, srcPos, result, 0, len);
        }
        return result;
    }
}
