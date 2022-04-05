package org.apache.cassandra.spark.utils;

import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.spark.TestUtils;

import static org.apache.cassandra.spark.utils.ArrayUtils.retain;

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

public class ArrayUtilsTest
{
    @Test
    public void testRetain()
    {
        Object[] source = new Object[] {1, 2, 3, 4, 5};
        Assert.assertTrue(TestUtils.equals(new Object[] {1, 2, 3},
                                           retain(source, 0, 3)));
    }

    @Test
    public void testRetainThrows()
    {
        // not using JUnit rule ExpectedException in order to assert multiple throwables.
        expectedThrows(() -> retain(null, 0, 1),
                       t -> Assert.assertSame(IllegalArgumentException.class, t.getClass()));

        expectedThrows(() -> retain(new Object[] {1, 2, 3}, -1, 1),
                       t -> Assert.assertSame(IllegalArgumentException.class, t.getClass()));

        expectedThrows(() -> retain(new Object[] {1, 2, 3}, 0, -1),
                       t -> Assert.assertSame(IllegalArgumentException.class, t.getClass()));

        expectedThrows(() -> retain(new Object[] {1, 2, 3}, 0, 5),
                       t -> Assert.assertSame(IllegalArgumentException.class, t.getClass()));
    }

    private void expectedThrows(Runnable test, Consumer<Throwable> throwableVerifier)
    {
        try
        {
            test.run();
        }
        catch (Throwable t)
        {
            throwableVerifier.accept(t);
        }
    }
}
