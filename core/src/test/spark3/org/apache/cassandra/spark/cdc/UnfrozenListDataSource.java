package org.apache.cassandra.spark.cdc;

import org.apache.cassandra.spark.config.SchemaFeature;
import org.apache.cassandra.spark.config.SchemaFeatureSet;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.spark.sparksql.CassandraTableProvider;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class UnfrozenListDataSource extends CassandraTableProvider implements Serializable
{
    @Override
    public DataLayer getDataLayer(CaseInsensitiveStringMap options)
    {
        return UnfrozenListDataLayer.from(options);
    }

    @Override
    public String shortName() {
        return "UnfrozenListDataLayer";
    }

    private static class UnfrozenListDataLayer extends LocalDataLayer {

        @Override
        public ICassandraSource getCassandraSource()
        {
            return (keySpace, table, columnsToFetch, primaryKeyColumns) -> {
                List<ByteBuffer> byteBuffers = new ArrayList<>();
                byteBuffers.add(ByteBufferUtil.bytes(1));
                byteBuffers.add(ByteBufferUtil.bytes(2));
                byteBuffers.add(ByteBufferUtil.bytes(3));
                byteBuffers.add(ByteBufferUtil.bytes(4));
                return Collections.singletonList(CollectionSerializer.pack(byteBuffers, ByteBufferAccessor.instance, byteBuffers.size(), ProtocolVersion.V3));
            };
        }

        public static UnfrozenListDataLayer from(CaseInsensitiveStringMap options)
        {
            return new UnfrozenListDataLayer(
            CassandraVersion.valueOf(options.getOrDefault(lowerCaseKey("version"), CassandraVersion.THREEZERO.toString())),
            Partitioner.valueOf(options.getOrDefault(lowerCaseKey("partitioner"), Partitioner.Murmur3Partitioner.name())),
            getOrThrow(options, lowerCaseKey("keyspace")),
            getOrThrow(options, lowerCaseKey("createStmt")),
            SchemaFeatureSet.initializeFromOptions(options),
            Arrays.stream(options.getOrDefault(lowerCaseKey("udts"), "").split("\n")).filter(StringUtils::isNotEmpty).collect(Collectors.toSet()),
            getBoolean(options, lowerCaseKey("useSSTableInputStream"), false),
            getBoolean(options, lowerCaseKey("isCdc"), false),
            options.get(lowerCaseKey("statsClass")),
            getOrThrow(options, lowerCaseKey("dirs")).split(",")
            );
        }

        public UnfrozenListDataLayer(@NotNull CassandraVersion version, @NotNull Partitioner partitioner,
                                     @NotNull String keyspace, @NotNull String createStmt, @NotNull List<SchemaFeature> requestedFeatures,
                                     @NotNull Set<String> udts, boolean useSSTableInputStream, boolean isCdc, String statsClass,
                                     String... paths)
        {
            super(version, partitioner, keyspace, createStmt, requestedFeatures, udts,
                    useSSTableInputStream, isCdc, statsClass, paths);
        }

    }
}
