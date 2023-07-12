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

package org.apache.cassandra.spark.reader.fourzero;

import org.junit.Test;

import org.apache.cassandra.spark.reader.common.AbstractCompressionMetadata;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("SameParameterValue")
public class FourZeroIndexReaderTests
{
    @Test
    public void testPartialCompressedSizeWithinChunk()
    {
        assertEquals(64, FourZeroIndexReader.partialCompressedSizeWithinChunk(0, 1024, 64, true));
        assertEquals(32, FourZeroIndexReader.partialCompressedSizeWithinChunk(512, 1024, 64, true));
        assertEquals(16, FourZeroIndexReader.partialCompressedSizeWithinChunk(768, 1024, 64, true));
        assertEquals(2, FourZeroIndexReader.partialCompressedSizeWithinChunk(992, 1024, 64, true));
        assertEquals(2, FourZeroIndexReader.partialCompressedSizeWithinChunk(995, 1024, 64, true));
        assertEquals(1, FourZeroIndexReader.partialCompressedSizeWithinChunk(1008, 1024, 64, true));
        assertEquals(0, FourZeroIndexReader.partialCompressedSizeWithinChunk(1023, 1024, 64, true));
        assertEquals(64, FourZeroIndexReader.partialCompressedSizeWithinChunk(1024, 1024, 64, true));
        assertEquals(64, FourZeroIndexReader.partialCompressedSizeWithinChunk(2048, 1024, 64, true));
        assertEquals(32, FourZeroIndexReader.partialCompressedSizeWithinChunk(2560, 1024, 64, true));

        assertEquals(0, FourZeroIndexReader.partialCompressedSizeWithinChunk(0, 1024, 64, false));
        assertEquals(1, FourZeroIndexReader.partialCompressedSizeWithinChunk(16, 1024, 64, false));
        assertEquals(32, FourZeroIndexReader.partialCompressedSizeWithinChunk(512, 1024, 64, false));
        assertEquals(64, FourZeroIndexReader.partialCompressedSizeWithinChunk(1023, 1024, 64, false));
        assertEquals(32, FourZeroIndexReader.partialCompressedSizeWithinChunk(2560, 1024, 64, false));
    }

    @Test
    public void testCompressedSizeWithinSameChunk()
    {
        // within the same chunk
        assertEquals(64, calculateCompressedSize(128, 256, 0, 512));
        assertEquals(128, calculateCompressedSize(0, 1024, 5, 128));
        assertEquals(25, calculateCompressedSize(32, 64, 5, 800));
    }

    @Test
    public void testCompressedSizeMultipleChunks()
    {
        // partition straddles more than one chunk
        assertEquals(448 + 128, calculateCompressedSize(128, 0, 512, 1536, 1, 256));
        assertEquals(112 + (256 * 10) + 32, calculateCompressedSize(128, 0, 128, 11392, 11, 256));
    }

    private static long calculateCompressedSize(long start, long end, int startIdx, int startCompressedChunkSize)
    {
        return FourZeroIndexReader.calculateCompressedSize(mockMetaData(start, startIdx, startCompressedChunkSize, end), 160000000, start, end);
    }

    private static long calculateCompressedSize(long start, int startIdx, int startCompressedChunkSize,
                                                long end, int endIdx, int endCompressedChunkSize)
    {
        return FourZeroIndexReader.calculateCompressedSize(mockMetaData(start, startIdx, startCompressedChunkSize, end, endIdx, endCompressedChunkSize), 160000000, start, end);
    }

    private static CompressionMetadata mockMetaData(long start, int startIdx, int startCompressedChunkSize, long end)
    {
        return mockMetaData(start, startIdx, startCompressedChunkSize, end, startIdx, startCompressedChunkSize);
    }

    private static CompressionMetadata mockMetaData(long start, int startIdx, int startCompressedChunkSize,
                                                    long end, int endIdx, int endCompressedChunkSize)
    {
        return mockMetaData(start, startIdx, startCompressedChunkSize, end, endIdx, endCompressedChunkSize, 1024);
    }

    private static CompressionMetadata mockMetaData(long start, int startIdx, int startCompressedChunkSize,
                                                    long end, int endIdx, int endCompressedChunkSize,
                                                    int uncompressedChunkLength)
    {
        final CompressionMetadata metadata = mock(CompressionMetadata.class);
        when(metadata.chunkIdx(eq(start))).thenReturn(startIdx);
        when(metadata.chunkIdx(eq(end))).thenReturn(endIdx);
        when(metadata.chunkLength()).thenReturn(uncompressedChunkLength);
        when(metadata.chunkAtIndex(eq(startIdx))).thenReturn(new AbstractCompressionMetadata.Chunk(0, startCompressedChunkSize));
        when(metadata.chunkAtIndex(eq(endIdx))).thenReturn(new AbstractCompressionMetadata.Chunk(0, endCompressedChunkSize));
        for (int idx = startIdx + 1; idx < endIdx; idx++)
        {
            // let intermediate chunks have same compressed size as end
            when(metadata.chunkAtIndex(eq(idx))).thenReturn(new AbstractCompressionMetadata.Chunk(0, endCompressedChunkSize));
        }
        return metadata;
    }
}
