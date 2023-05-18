package org.apache.cassandra.spark.cdc;

import org.apache.cassandra.spark.cdc.watermarker.InMemoryWatermarker;
import org.apache.cassandra.spark.cdc.watermarker.SparkInMemoryWatermarker;
import org.apache.cassandra.spark.cdc.watermarker.Watermarker;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.sparksql.CassandraTableProvider;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

import static org.apache.cassandra.spark.data.LocalDataLayer.getOrThrow;
import static org.apache.cassandra.spark.data.LocalDataLayer.lowerCaseKey;
import static org.mockito.Mockito.spy;

public class SpyWatermarkerDataSource extends CassandraTableProvider implements Serializable
{
    @Override
    public DataLayer getDataLayer(CaseInsensitiveStringMap options)
    {
        return new SpyWaterMarkerDataLayer(CassandraVersion.valueOf(options.getOrDefault(lowerCaseKey("version"), CassandraVersion.THREEZERO.toString())),
                getOrThrow(options, lowerCaseKey("keyspace")),
                getOrThrow(options, lowerCaseKey("createStmt")),
                getOrThrow(options, lowerCaseKey("dirs")).split(","));
    }

    @Override
    public String shortName()
    {
        return "SpyWaterMarkerDataLayer";
    }

    public static class SpyWaterMarkerDataLayer extends LocalDataLayer
    {
        public static final InMemoryWatermarker inMemoryWatermarker = spy(SparkInMemoryWatermarker.INSTANCE);

        public SpyWaterMarkerDataLayer(@NotNull CassandraVersion version, @NotNull String keyspace, @NotNull String createStmt, String... paths)
        {
            super(version, keyspace, createStmt, paths);
        }

        @Override
        public Watermarker cdcWatermarker()
        {
            return inMemoryWatermarker;
        }
    }
}
