package org.apache.cassandra.spark.reader.fourzero;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.spark.shaded.fourzero.cassandra.io.compress.ICompressor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.CompressionParams;
import org.apache.cassandra.spark.reader.common.AbstractCompressionMetadata;
import org.apache.cassandra.spark.reader.common.BigLongArray;

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
 * Holds metadata about compressed file
 */
class CompressionMetadata extends AbstractCompressionMetadata
{

    private final CompressionParams parameters;

    private CompressionMetadata(final long dataLength,
                                final BigLongArray chunkOffsets,
                                final CompressionParams parameters)
    {
        super(dataLength, chunkOffsets);
        this.parameters = parameters;
    }

    static CompressionMetadata fromInputStream(final InputStream inStream,
                                               final boolean hasCompressedLength) throws IOException
    {
        final long dataLength;
        final BigLongArray chunkOffsets;

        final DataInputStream inData = new DataInputStream(inStream);

        final String compressorName = inData.readUTF();
        final int optionCount = inData.readInt();
        final Map<String, String> options = new HashMap<>(optionCount);
        for (int i = 0; i < optionCount; ++i)
        {
            options.put(inData.readUTF(), inData.readUTF());
        }

        final int chunkLength = inData.readInt();
        int minCompressRatio = 2147483647;
        if (hasCompressedLength)
        {
            minCompressRatio = inData.readInt();
        }

        final CompressionParams params = new CompressionParams(compressorName, chunkLength, minCompressRatio, options);
        params.setCrcCheckChance(AbstractCompressionMetadata.crcCheckChance);

        dataLength = inData.readLong();

        final int chunkCount = inData.readInt();
        chunkOffsets = new BigLongArray(chunkCount);

        for (int i = 0; i < chunkCount; i++)
        {
            try
            {
                chunkOffsets.set(i, inData.readLong());
            }
            catch (final EOFException e)
            {
                throw new EOFException(String.format("Corrupted compression index: read %d but expected %d chunks.", i, chunkCount));
            }
        }

        return new CompressionMetadata(dataLength, chunkOffsets, params);
    }

    ICompressor compressor()
    {
        return parameters.getSstableCompressor();
    }

    @Override
    protected int chunkLength()
    {
        return parameters.chunkLength();
    }

    @Override
    protected double crcCheckChance()
    {
        return parameters.getCrcCheckChance();
    }
}
