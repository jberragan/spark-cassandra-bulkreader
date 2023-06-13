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

package org.apache.cassandra.spark.data.fourzero.types.spark;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.SparkCqlField;
import org.jetbrains.annotations.NotNull;

public class FourZeroSparkCqlField
{
    private static final Map<Class<?>, SparkCqlField.SparkCqlType> MAPPER = new HashMap<>();

    static
    {
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Ascii.class, Ascii.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.BigInt.class, BigInt.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Blob.class, Blob.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Boolean.class, Boolean.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Counter.class, Counter.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Date.class, Date.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Decimal.class, Decimal.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Double.class, Double.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Duration.class, Duration.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Empty.class, Empty.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Float.class, Float.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Inet.class, Inet.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Int.class, Int.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.SmallInt.class, SmallInt.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Text.class, Text.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Time.class, Time.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.Timestamp.class, Timestamp.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.TimeUUID.class, TimeUUID.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.TinyInt.class, TinyInt.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.UUID.class, UUID.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.VarChar.class, VarChar.INSTANCE);
        addMapper(org.apache.cassandra.spark.data.fourzero.types.VarInt.class, VarInt.INSTANCE);
    }

    private static <Type extends CqlField.CqlType, MapperType extends Type> void addMapper(Class<Type> tClass, MapperType mapper)
    {
        Preconditions.checkArgument(mapper instanceof SparkCqlField.SparkCqlType);
        MAPPER.put(tClass, (SparkCqlField.SparkCqlType) mapper);
    }

    public static SparkCqlField decorate(CqlField field)
    {
        if (field instanceof SparkCqlField)
        {
            return (SparkCqlField) field;
        }

        return new SparkCqlField(field.isPartitionKey(),
                                 field.isClusteringColumn(),
                                 field.isStaticColumn(),
                                 field.name(),
                                 decorate(field.type()),
                                 field.pos());
    }

    @NotNull
    public static SparkCqlField.SparkCqlType decorate(CqlField.CqlType type)
    {
        if (type instanceof SparkCqlField.SparkCqlType)
        {
            return (SparkCqlField.SparkCqlType) type;
        }

        final SparkCqlField.SparkCqlType sparkCqlType = MAPPER.get(type.getClass());
        if (sparkCqlType == null)
        {
            // non native type, so try complex types
            switch (type.internalType())
            {
                case Frozen:
                    return new org.apache.cassandra.spark.data.fourzero.types.spark.complex.CqlFrozen(((CqlField.CqlFrozen) type).inner());
                case Tuple:
                    return new org.apache.cassandra.spark.data.fourzero.types.spark.complex.CqlTuple((CqlField.CqlTuple) type);
                case Udt:
                    return new org.apache.cassandra.spark.data.fourzero.types.spark.complex.CqlUdt((CqlField.CqlUdt) type);
                case List:
                    return new org.apache.cassandra.spark.data.fourzero.types.spark.complex.CqlList((CqlField.CqlList) type);
                case Map:
                    return new org.apache.cassandra.spark.data.fourzero.types.spark.complex.CqlMap((CqlField.CqlMap) type);
                case Set:
                    return new org.apache.cassandra.spark.data.fourzero.types.spark.complex.CqlSet((CqlField.CqlSet) type);
                default:
                    throw CqlField.notImplemented(type);
            }
        }
        return sparkCqlType;
    }
}
