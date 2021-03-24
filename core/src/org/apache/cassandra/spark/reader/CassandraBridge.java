package org.apache.cassandra.spark.reader;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.apache.cassandra.spark.stats.Stats;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.CqlUdt;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.fourzero.FourZero;
import org.apache.cassandra.spark.sparksql.CustomFilter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.jetbrains.annotations.NotNull;

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
 * Provides an abstract interface for all calls to shaded Cassandra code
 */
@SuppressWarnings({ "WeakerAccess", "unused" })
public abstract class CassandraBridge
{

    public enum CassandraVersion
    {
        TWOONE("2.1"), THREEZERO("3.0"), FOURZERO("4.0");
        private final String name;

        CassandraVersion(final String name)
        {
            this.name = name;
        }
    }

    public interface BigNumberConfig
    {
        BigNumberConfig DEFAULT = new BigNumberConfig()
        {
            public int bigIntegerPrecision()
            {
                return 38;
            }

            public int bigIntegerScale()
            {
                return 0;
            }

            public int bigDecimalPrecision()
            {
                return 38;
            }

            public int bigDecimalScale()
            {
                return 19;
            }
        };

        int bigIntegerPrecision();

        int bigIntegerScale();

        int bigDecimalPrecision();

        int bigDecimalScale();
    }

    public static final Set<CassandraVersion> SUPPORTED_VERSIONS = new HashSet<>(Arrays.asList(CassandraVersion.THREEZERO, CassandraVersion.FOURZERO));
    private static final ConcurrentHashMap<CassandraVersion, CassandraBridge> BRIDGES = new ConcurrentHashMap<>(2);

    public static CassandraBridge get(final CassandraVersion version)
    {
        if (!SUPPORTED_VERSIONS.contains(version))
        {
            throw new UnsupportedOperationException("Cassandra " + version.name + " is unsupported");
        }

        if (!BRIDGES.containsKey(version))
        {
            switch (version)
            {
                case THREEZERO:
                case FOURZERO:
                    BRIDGES.putIfAbsent(version, new FourZero());
            }
        }
        return BRIDGES.get(version);
    }

    public abstract Pair<ByteBuffer, BigInteger> getPartitionKey(@NotNull final CqlSchema schema,
                                                                 @NotNull final Partitioner partitioner,
                                                                 @NotNull final String key);

    // Compaction Stream Scanner
    public abstract IStreamScanner getCompactionScanner(@NotNull final CqlSchema schema,
                                                        @NotNull final Partitioner partitionerType,
                                                        @NotNull final SSTablesSupplier ssTables,
                                                        @NotNull final List<CustomFilter> filters,
                                                        @NotNull final Stats stats);

    public abstract CassandraBridge.CassandraVersion getVersion();

    public abstract BigInteger hash(final Partitioner partitioner, final ByteBuffer key);

    public abstract UUID getTimeUUID();

    // CQL Schema

    public abstract CqlSchema buildSchema(final String keyspace,
                                          final String createStmt,
                                          final ReplicationFactor rf,
                                          final Partitioner partitioner,
                                          final Set<String> udts);

    // Data Types

    /*
            SparkSQL      |    Java
            ByteType      |    byte or Byte
            ShortType     |    short or Short
            IntegerType   |    int or Integer
            LongType      |    long or Long
            FloatType     |    float or Float
            DoubleType    |    double or Double
            DecimalType   |    java.math.BigDecimal
            StringType    |    String
            BinaryType    |    byte[]
            BooleanType   |    boolean or Boolean
            TimestampType |    java.sql.Timestamp
            DateType      |    java.sql.Date
            ArrayType     |    java.util.List
            MapType       |    java.util.Map

            see: https://spark.apache.org/docs/latest/sql-reference.html
     */

    public DataType toSparkSQLType(final CqlField.CqlType cql3Type)
    {
        return toSparkSQLType(cql3Type, CassandraBridge.BigNumberConfig.DEFAULT);
    }

    public DataType toSparkSQLType(final CqlField.CqlType cql3Type, final BigNumberConfig bigNumberConfig)
    {
        return CassandraBridge.defaultSparkSQLType(cql3Type, bigNumberConfig);
    }

    public static DataType defaultSparkSQLType(final CqlField.CqlType type)
    {
        return defaultSparkSQLType(type, BigNumberConfig.DEFAULT);
    }

    public static DataType defaultSparkSQLType(final CqlField.CqlType type, final BigNumberConfig bigNumberConfig)
    {
        switch (type.internalType())
        {
            case NativeCql:
                return defaultCqlToSparkSQLType((CqlField.NativeCql3Type) type, bigNumberConfig);
            case Set:
                final CqlField.CqlSet set = (CqlField.CqlSet) type;
                return DataTypes.createArrayType(defaultSparkSQLType(set.type(), bigNumberConfig));
            case List:
                final CqlField.CqlList list = (CqlField.CqlList) type;
                return DataTypes.createArrayType(defaultSparkSQLType(list.type(), bigNumberConfig));
            case Map:
                final CqlField.CqlMap map = (CqlField.CqlMap) type;
                return DataTypes.createMapType(defaultSparkSQLType(map.keyType(), bigNumberConfig), defaultSparkSQLType(map.valueType(), bigNumberConfig));
            case Frozen:
                return defaultSparkSQLType(((CqlField.CqlFrozen) type).inner(), bigNumberConfig);
            case Udt:
                return DataTypes.createStructType(
                ((CqlUdt) type).fields().stream()
                               .map(f -> DataTypes.createStructField(f.name(), defaultSparkSQLType(f.type(), bigNumberConfig), true))
                               .toArray(StructField[]::new)
                );
            case Tuple:
                final CqlField.CqlTuple tuple = (CqlField.CqlTuple) type;
                return DataTypes.createStructType(
                IntStream.range(0, tuple.size())
                         .mapToObj(i -> DataTypes.createStructField(Integer.toString(i), defaultSparkSQLType(tuple.type(i), bigNumberConfig), true))
                         .toArray(StructField[]::new)
                );
            default:
                throw new NotImplementedException(type.toString() + " type not implemented yet");
        }
    }

    public static DataType defaultCqlToSparkSQLType(final CqlField.NativeCql3Type cql3Type, final BigNumberConfig bigNumberConfig)
    {
        switch (cql3Type)
        {
            case TIMEUUID:
            case UUID:
            case ASCII:
            case VARCHAR:
            case TEXT:
                return DataTypes.StringType;
            case INET:
            case BLOB:
                return DataTypes.BinaryType;
            case VARINT:
                return DataTypes.createDecimalType(bigNumberConfig.bigIntegerPrecision(), bigNumberConfig.bigIntegerScale());
            case DECIMAL:
                return DataTypes.createDecimalType(bigNumberConfig.bigDecimalPrecision(), bigNumberConfig.bigDecimalScale());
            case INT:
                return DataTypes.IntegerType;
            case DATE:
                return DataTypes.DateType;
            case BIGINT:
            case TIME:
                return DataTypes.LongType;
            case BOOLEAN:
                return DataTypes.BooleanType;
            case FLOAT:
                return DataTypes.FloatType;
            case DOUBLE:
                return DataTypes.DoubleType;
            case TIMESTAMP:
                return DataTypes.TimestampType;
            case EMPTY:
                return DataTypes.NullType;
            case SMALLINT:
                return DataTypes.ShortType;
            case TINYINT:
                return DataTypes.ByteType;
            default:
                throw new NotImplementedException(cql3Type.toString() + " type not implemented yet");
        }
    }

    public Object deserialize(final CqlField field, final ByteBuffer buf)
    {
        return deserialize(field.type(), buf);
    }

    public abstract Object deserialize(final CqlField.CqlType type, final ByteBuffer buf);

    public abstract Map<String, Object> deserializeUdt(final CqlUdt type, final ByteBuffer buf, final boolean isFrozen);

    public abstract Object[] deserializeTuple(final CqlField.CqlTuple tuple, final ByteBuffer buf, final boolean isFrozen);

    /**
     * test helpers
     */
    public abstract ByteBuffer serialize(final CqlField.CqlType type, final Object value);

    public abstract ByteBuffer serializeUdt(final CqlUdt udt, final Map<String, Object> values);

    public abstract ByteBuffer serializeTuple(final CqlField.CqlTuple udt, final Object[] values);

    public interface IWriter
    {
        void write(Object... values);
    }

    public void writeSSTable(final Partitioner partitioner, final String keyspace, final Path dir, final String createStmt, final String insertStmt, final Consumer<IWriter> writer)
    {
        writeSSTable(partitioner, keyspace, dir, createStmt, insertStmt, Collections.emptySet(), writer);
    }

    public abstract void writeSSTable(final Partitioner partitioner, final String keyspace, final Path dir, final String createStmt, final String insertStmt, final Set<CqlUdt> udts, final Consumer<IWriter> writer);
}
