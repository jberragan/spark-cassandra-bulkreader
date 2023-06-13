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

package org.apache.cassandra.spark.data.fourzero;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.esotericsoftware.kryo.io.Input;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.fourzero.complex.CqlCollection;
import org.apache.cassandra.spark.data.fourzero.complex.CqlFrozen;
import org.apache.cassandra.spark.data.fourzero.complex.CqlList;
import org.apache.cassandra.spark.data.fourzero.complex.CqlMap;
import org.apache.cassandra.spark.data.fourzero.complex.CqlSet;
import org.apache.cassandra.spark.data.fourzero.complex.CqlTuple;
import org.apache.cassandra.spark.data.fourzero.complex.CqlUdt;
import org.apache.cassandra.spark.data.fourzero.types.Ascii;
import org.apache.cassandra.spark.data.fourzero.types.BigInt;
import org.apache.cassandra.spark.data.fourzero.types.Blob;
import org.apache.cassandra.spark.data.fourzero.types.Boolean;
import org.apache.cassandra.spark.data.fourzero.types.Counter;
import org.apache.cassandra.spark.data.fourzero.types.Date;
import org.apache.cassandra.spark.data.fourzero.types.Decimal;
import org.apache.cassandra.spark.data.fourzero.types.Double;
import org.apache.cassandra.spark.data.fourzero.types.Duration;
import org.apache.cassandra.spark.data.fourzero.types.Empty;
import org.apache.cassandra.spark.data.fourzero.types.Float;
import org.apache.cassandra.spark.data.fourzero.types.Inet;
import org.apache.cassandra.spark.data.fourzero.types.Int;
import org.apache.cassandra.spark.data.fourzero.types.SmallInt;
import org.apache.cassandra.spark.data.fourzero.types.Text;
import org.apache.cassandra.spark.data.fourzero.types.Time;
import org.apache.cassandra.spark.data.fourzero.types.TimeUUID;
import org.apache.cassandra.spark.data.fourzero.types.Timestamp;
import org.apache.cassandra.spark.data.fourzero.types.TinyInt;
import org.apache.cassandra.spark.data.fourzero.types.VarChar;
import org.apache.cassandra.spark.data.fourzero.types.VarInt;

public class FourZeroTypes extends CassandraTypes
{
    public static final FourZeroTypes INSTANCE = new FourZeroTypes();

    private final Map<String, CqlField.NativeType> nativeTypes;

    public FourZeroTypes()
    {
        this.nativeTypes = allTypes().stream().collect(Collectors.toMap(CqlField.CqlType::name, Function.identity()));
    }

    @Override
    public Map<String, ? extends CqlField.NativeType> nativeTypeNames()
    {
        return nativeTypes;
    }

    @Override
    public Ascii ascii()
    {
        return Ascii.INSTANCE;
    }

    @Override
    public Blob blob()
    {
        return Blob.INSTANCE;
    }

    @Override
    public Boolean bool()
    {
        return Boolean.INSTANCE;
    }

    @Override
    public Counter counter()
    {
        return Counter.INSTANCE;
    }

    @Override
    public BigInt bigint()
    {
        return BigInt.INSTANCE;
    }

    @Override
    public Date date()
    {
        return Date.INSTANCE;
    }

    @Override
    public Decimal decimal()
    {
        return Decimal.INSTANCE;
    }

    @Override
    public Double aDouble()
    {
        return Double.INSTANCE;
    }

    @Override
    public Duration duration()
    {
        return Duration.INSTANCE;
    }

    @Override
    public Empty empty()
    {
        return Empty.INSTANCE;
    }

    @Override
    public Float aFloat()
    {
        return Float.INSTANCE;
    }

    @Override
    public Inet inet()
    {
        return Inet.INSTANCE;
    }

    @Override
    public Int aInt()
    {
        return Int.INSTANCE;
    }

    @Override
    public SmallInt smallint()
    {
        return SmallInt.INSTANCE;
    }

    @Override
    public Text text()
    {
        return Text.INSTANCE;
    }

    @Override
    public Time time()
    {
        return Time.INSTANCE;
    }

    @Override
    public Timestamp timestamp()
    {
        return Timestamp.INSTANCE;
    }

    @Override
    public TimeUUID timeuuid()
    {
        return TimeUUID.INSTANCE;
    }

    @Override
    public TinyInt tinyint()
    {
        return TinyInt.INSTANCE;
    }

    @Override
    public org.apache.cassandra.spark.data.fourzero.types.UUID uuid()
    {
        return org.apache.cassandra.spark.data.fourzero.types.UUID.INSTANCE;
    }

    @Override
    public VarChar varchar()
    {
        return VarChar.INSTANCE;
    }

    @Override
    public VarInt varint()
    {
        return VarInt.INSTANCE;
    }

    @Override
    public CqlField.CqlType collection(String name, CqlField.CqlType... types)
    {
        return CqlCollection.build(name, types);
    }

    @Override
    public CqlList list(CqlField.CqlType type)
    {
        return CqlCollection.list(type);
    }

    @Override
    public CqlSet set(CqlField.CqlType type)
    {
        return CqlCollection.set(type);
    }

    @Override
    public CqlMap map(CqlField.CqlType keyType, CqlField.CqlType valueType)
    {
        return CqlCollection.map(keyType, valueType);
    }

    @Override
    public CqlTuple tuple(CqlField.CqlType... types)
    {
        return CqlCollection.tuple(types);
    }

    @Override
    public CqlField.CqlType frozen(CqlField.CqlType type)
    {
        return CqlFrozen.build(type);
    }

    @Override
    public CqlField.CqlUdtBuilder udt(final String keyspace, final String name)
    {
        return CqlUdt.builder(keyspace, name);
    }

    public CqlField.CqlType readType(CqlField.CqlType.InternalType type, Input input)
    {
        switch (type)
        {
            case NativeCql:
                return nativeType(input.readString());
            case Set:
            case List:
            case Map:
            case Tuple:
                return CqlCollection.read(type, input);
            case Frozen:
                return CqlFrozen.build(CqlField.CqlType.read(input));
            case Udt:
                return CqlUdt.read(input);
            default:
                throw new IllegalStateException("Unknown cql type, cannot deserialize");
        }
    }
}
