package org.apache.cassandra.spark.utils.streaming;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.jetbrains.annotations.Nullable;

public interface CassandraFile
{
    enum FileType
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
            for (final CassandraFile.FileType fileType : FileType.values())
            {
                FILE_TYPE_HASH_MAP.put(fileType.getFileSuffix(), fileType);
            }
        }

        public static CassandraFile.FileType fromExtension(final String extension)
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
}
