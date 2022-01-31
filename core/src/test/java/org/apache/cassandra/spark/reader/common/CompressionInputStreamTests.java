package org.apache.cassandra.spark.reader.common;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.apache.cassandra.spark.stats.Stats;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
public class CompressionInputStreamTests
{

    @Test
    public void testRawInputStream() throws IOException
    {
        final String filename = UUID.randomUUID().toString();
        final Path path = Files.createTempFile(filename, "tmp");
        try (final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path.toFile()))))
        {
            dos.writeUTF(filename);
            final int numWrites = 1000;
            dos.writeInt(numWrites);
            for (int i = 0; i < numWrites; i++)
            {
                dos.writeInt(i);
            }
            for (int i = 0; i < numWrites; i++)
            {
                dos.writeLong(i);
            }
            dos.writeBoolean(false);
            dos.writeBoolean(true);
        }

        final byte[] buffer = new byte[1024];
        try (final DataInputStream dis = new DataInputStream(new RawInputStream(new DataInputStream(new BufferedInputStream(Files.newInputStream(path))), buffer, Stats.DoNothingStats.INSTANCE)))
        {
            assertEquals(filename, dis.readUTF());
            final int numReads = dis.readInt();
            for (int i = 0; i < numReads; i++)
            {
                assertEquals(i, dis.readInt());
            }
            for (int i = 0; i < numReads; i++)
            {
                assertEquals(i, dis.readLong());
            }
            assertFalse(dis.readBoolean());
            assertTrue(dis.readBoolean());
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testBigLongArrayIllegalSize()
    {
        new BigLongArray(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testBigLongArrayEmpty()
    {
        final BigLongArray ar = new BigLongArray(0);
        ar.set(0, 0L);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testBigLongArrayOutOfRange()
    {
        final BigLongArray ar = new BigLongArray(500);
        ar.set(501, 999L);
    }

    @Test
    public void testBigLongArrayUnary()
    {
        final BigLongArray ar = new BigLongArray(1);
        ar.set(0, 999L);
        assertEquals(999L, ar.get(0));
    }

    @Test
    public void testBigLongArray()
    {
        final int size = BigLongArray.DEFAULT_PAGE_SIZE * 37;
        final BigLongArray ar = new BigLongArray(size);
        for (int i = 0; i < size; i++)
        {
            ar.set(i, i * 5L);
        }
        for (int i = 0; i < size; i++)
        {
            assertEquals(i * 5L, ar.get(i));
        }
    }
}
