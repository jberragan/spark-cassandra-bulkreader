package org.apache.cassandra.spark.utils;

import java.io.IOException;
import java.util.Objects;

import org.junit.Test;

import org.apache.cassandra.spark.exceptions.TransportFailureException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

public class ThrowableUtilTests
{
    @Test
    public void testNoNesting()
    {
        final Throwable t = new RuntimeException();
        assertEquals(t, ThrowableUtils.rootCause(t));
    }

    @Test
    public void testNested()
    {
        final Throwable t2 = new RuntimeException();
        final Throwable t1 = new RuntimeException(t2);
        assertEquals(t2, ThrowableUtils.rootCause(t1));
        assertEquals(t2, ThrowableUtils.rootCause(t2));
    }

    @Test
    public void testMultiNested()
    {
        final Throwable t4 = new RuntimeException();
        final Throwable t3 = new RuntimeException(t4);
        final Throwable t2 = new RuntimeException(t3);
        final Throwable t1 = new RuntimeException(t2);
        assertEquals(t4, ThrowableUtils.rootCause(t1));
        assertEquals(t4, ThrowableUtils.rootCause(t2));
        assertEquals(t4, ThrowableUtils.rootCause(t3));
        assertEquals(t4, ThrowableUtils.rootCause(t4));
    }

    @Test
    public void testOfType()
    {
        final IOException io = new IOException();
        final Throwable t = new RuntimeException(io);
        assertEquals(io, ThrowableUtils.rootCause(t, IOException.class));
        assertEquals(io, ThrowableUtils.rootCause(io, IOException.class));
    }

    @Test
    public void testOfType2()
    {
        final TransportFailureException ex = TransportFailureException.nonretryable(404);
        final Throwable t = new RuntimeException(ex);
        assertEquals(ex, ThrowableUtils.rootCause(t, TransportFailureException.class));
        assertTrue(Objects.requireNonNull(ThrowableUtils.rootCause(t, TransportFailureException.class)).isNotFound());
        assertEquals(ex, ThrowableUtils.rootCause(ex, TransportFailureException.class));
    }

    @Test
    public void testOfTypeNested()
    {
        final Throwable t4 = new RuntimeException();
        final IOException io = new IOException(t4);
        final Throwable t3 = new RuntimeException(io);
        final Throwable t2 = new RuntimeException(t3);
        final Throwable t1 = new RuntimeException(t2);
        assertEquals(io, ThrowableUtils.rootCause(t1, IOException.class));
        assertEquals(io, ThrowableUtils.rootCause(t2, IOException.class));
        assertNull(ThrowableUtils.rootCause(t4, IOException.class));
    }

    @Test
    public void testOfTypeNotFound()
    {
        final Throwable t = new RuntimeException();
        assertNull(ThrowableUtils.rootCause(t, IOException.class));
    }

    @Test
    public void testOfTypeNotExist()
    {
        final Throwable t4 = new RuntimeException();
        final Throwable t3 = new RuntimeException(t4);
        final Throwable t2 = new RuntimeException(t3);
        final Throwable t1 = new RuntimeException(t2);
        assertNull(ThrowableUtils.rootCause(t1, IOException.class));
    }
}
