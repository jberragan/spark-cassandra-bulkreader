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

package org.apache.cassandra.spark.data;

import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract class representing a single SSTable.
 * Implementations must override hashCode and equals methods
 */
public abstract class SSTable implements Serializable
{
    public static final Pattern OSS_PACKAGE_NAME = Pattern.compile("\\borg\\.apache\\.cassandra\\.(?!spark\\.shaded\\.)");
    public static final String SHADED_PACKAGE_NAME = "org.apache.cassandra.spark.shaded.fourzero.cassandra.";

    public enum FileType
    {
        DATA("Data.db"),
        INDEX("Index.db"),
        FILTER("Filter.db"),
        STATISTICS("Statistics.db"),
        SUMMARY("Summary.db"),
        COMPRESSION_INFO("CompressionInfo.db"),
        TOC("TOC.txt"),
        DIGEST("Digest.sha1"),
        CRC("CRC.db"),
        CRC32("Digest.crc32"),
        COMMITLOG(".log");

        private final String fileSuffix;

        FileType(final String fileSuffix)
        {
            this.fileSuffix = fileSuffix;
        }

        private static final Map<String, FileType> FILE_TYPE_HASH_MAP = new HashMap<>();

        static
        {
            for (final SSTable.FileType fileType : FileType.values())
            {
                FILE_TYPE_HASH_MAP.put(fileType.getFileSuffix(), fileType);
            }
        }

        public static SSTable.FileType fromExtension(final String extension)
        {
            Preconditions.checkArgument(FILE_TYPE_HASH_MAP.containsKey(extension), "Unknown sstable file type: " + extension);
            return FILE_TYPE_HASH_MAP.get(extension);
        }

        @Nullable
        public static Path resolveComponentFile(final FileType fileType, final Path dataFilePath)
        {
            final Path filePath = fileType == FileType.DATA ? dataFilePath : dataFilePath.resolveSibling(dataFilePath.getFileName().toString().replace(FileType.DATA.getFileSuffix(), fileType.getFileSuffix()));
            return Files.exists(filePath) ? filePath : null;
        }

        public String getFileSuffix()
        {
            return fileSuffix;
        }
    }

    public static final long serialVersionUID = 42L;

    public SSTable()
    {

    }

    @Nullable
    protected abstract InputStream openInputStream(final FileType fileType);

    @Nullable
    public InputStream openCompressionStream()
    {
        return openInputStream(FileType.COMPRESSION_INFO);
    }

    @Nullable
    public InputStream openStatsStream()
    {
        return openInputStream(FileType.STATISTICS);
    }

    @Nullable
    public InputStream openSummaryStream()
    {
        return openInputStream(FileType.SUMMARY);
    }

    @Nullable
    public InputStream openPrimaryIndexStream()
    {
        return openInputStream(FileType.INDEX);
    }

    @Nullable
    public InputStream openFilterStream()
    {
        return openInputStream(FileType.FILTER);
    }

    @NotNull
    public InputStream openDataStream()
    {
        return Objects.requireNonNull(openInputStream(FileType.DATA), "Data.db SSTable file component must exist");
    }

    public abstract boolean isMissing(final FileType fileType);

    public void verify() throws IncompleteSSTableException
    {
        // need Data.db file
        if (isMissing(FileType.DATA))
        {
            throw new IncompleteSSTableException(FileType.DATA);
        }
        // need Statistics.db file to open SerializationHeader
        if (isMissing(FileType.STATISTICS))
        {
            throw new IncompleteSSTableException(FileType.STATISTICS);
        }
        // need Summary.db or Index.db to read first/last partition key
        if (isMissing(FileType.SUMMARY) && isMissing(FileType.INDEX))
        {
            throw new IncompleteSSTableException(FileType.SUMMARY, FileType.INDEX);
        }
    }

    public abstract String getDataFileName();
}
