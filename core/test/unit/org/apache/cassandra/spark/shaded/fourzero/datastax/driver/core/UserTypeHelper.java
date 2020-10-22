package org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.NotImplementedException;

import org.apache.cassandra.spark.TestSchema;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlUdt;
import org.apache.cassandra.spark.reader.CassandraBridge;

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
public class UserTypeHelper
{

    public static UDTValue newUDTValue(final UserType userType)
    {
        return new UDTValue(userType);
    }

    public static UserType newUserType(String keyspace, String typeName, boolean frozen, Collection<UserType.Field> fields, ProtocolVersion protocolVersion, CodecRegistry codecRegistry)
    {
        return new UserType(keyspace, typeName, frozen, fields, protocolVersion, codecRegistry);
    }

    public static UserType.Field newField(String name, DataType type)
    {
        return new UserType.Field(name, type);
    }

    public static UserType toUserType(final CqlUdt udt)
    {
        final List<UserType.Field> fields = udt.fields().stream().map(f -> UserTypeHelper.newField(f.name(), UserTypeHelper.toDataType(f.type()))).collect(Collectors.toList());
        return UserTypeHelper.newUserType(udt.keyspace(), udt.name(), true, fields, ProtocolVersion.V3, CodecRegistry.DEFAULT_INSTANCE);
    }

    @SuppressWarnings("unchecked")
    public static UDTValue toUserTypeValue(final CassandraBridge.CassandraVersion version, final CqlUdt udt, final Object value)
    {
        final Map<String, Object> values = (Map<String, Object>) value;
        final UDTValue udtValue = UserTypeHelper.newUDTValue(UserTypeHelper.toUserType(udt));
        int pos = 0;
        for (final CqlField field : udt.fields())
        {
            setInnerValue(version, udtValue, field.type(), pos++, values.get(field.name()));
        }
        return udtValue;
    }

    // set inner value for UDTs or Tuples
    public static void setInnerValue(final CassandraBridge.CassandraVersion version, final SettableByIndexData<?> udtValue,
                                     final CqlField.CqlType type, final int pos, final Object v)
    {
        final Object value = TestSchema.convertForCqlWriter(type, v, version);
        switch (type.internalType())
        {
            case NativeCql:
                switch ((CqlField.NativeCql3Type) type)
                {
                    case TIMEUUID:
                    case UUID:
                        udtValue.setUUID(pos, (UUID) value);
                        return;
                    case ASCII:
                    case VARCHAR:
                    case TEXT:
                        udtValue.setString(pos, (String) value);
                        return;
                    case INET:
                        udtValue.setInet(pos, (InetAddress) value);
                        return;
                    case BLOB:
                        udtValue.setBytes(pos, (ByteBuffer) value);
                        return;
                    case VARINT:
                        udtValue.setVarint(pos, (BigInteger) value);
                        return;
                    case DECIMAL:
                        udtValue.setDecimal(pos, (BigDecimal) value);
                        return;
                    case INT:
                        udtValue.setInt(pos, (int) value);
                        return;
                    case DATE:
                        udtValue.setDate(pos, (LocalDate) value);
                        return;
                    case BIGINT:
                        udtValue.setLong(pos, (long) value);
                    case TIME:
                        udtValue.setTime(pos, (long) value);
                        return;
                    case BOOLEAN:
                        udtValue.setBool(pos, (boolean) value);
                        return;
                    case FLOAT:
                        udtValue.setFloat(pos, (float) value);
                        return;
                    case DOUBLE:
                        udtValue.setDouble(pos, (double) value);
                        return;
                    case TIMESTAMP:
                        udtValue.setTimestamp(pos, (Date) value);
                        return;
                    case EMPTY:
                        udtValue.setToNull(pos);
                        return;
                    case SMALLINT:
                        udtValue.setShort(pos, (short) value);
                        return;
                    case TINYINT:
                        udtValue.setByte(pos, (byte) value);
                        return;
                }
            case Set:
                udtValue.setSet(pos, (Set<?>) value);
                return;
            case List:
                udtValue.setList(pos, (List<?>) value);
                return;
            case Map:
                udtValue.setMap(pos, (Map<?, ?>) value);
                return;
            case Frozen:
                setInnerValue(version, udtValue, ((CqlField.CqlFrozen) type).inner(), pos, v);
                return;
            case Udt:
                udtValue.setUDTValue(pos, (UDTValue) value);
                return;
            case Tuple:
                udtValue.setTupleValue(pos, UserTypeHelper.toTupleValue(version, (CqlField.CqlTuple) type, value));
                return;
            default:
                throw new NotImplementedException(type + " type not implemented yet");
        }
    }

    public static TupleValue toTupleValue(final CassandraBridge.CassandraVersion version, final CqlField.CqlTuple tuple, final Object value)
    {
        if (value instanceof TupleValue) {
            return (TupleValue) value;
        }

        final TupleValue tupleValue = buildTupleValue(tuple);
        final Object[] ar = (Object[]) value;
        for (int pos = 0; pos < ar.length; pos++)
        {
            setInnerValue(version, tupleValue, tuple.type(pos), pos, ar[pos]);
        }
        return tupleValue;
    }

    public static TupleValue buildTupleValue(final CqlField.CqlTuple tuple)
    {
        return new TupleValue(new TupleType(tuple.types().stream().map(UserTypeHelper::toDataType).collect(Collectors.toList()), ProtocolVersion.V3, CodecRegistry.DEFAULT_INSTANCE));
    }

    public static DataType toDataType(final CqlField.CqlType type)
    {
        return UserTypeHelper.toDataType(type, false);
    }

    @SuppressWarnings("ConstantConditions")
    static DataType toDataType(final CqlField.CqlType type, final boolean isFrozen)
    {
        switch (type.internalType())
        {
            case NativeCql:
                switch ((CqlField.NativeCql3Type) type)
                {
                    case TIMEUUID:
                        return DataType.timeuuid();
                    case UUID:
                        return DataType.uuid();
                    case ASCII:
                        return DataType.ascii();
                    case VARCHAR:
                        return DataType.varchar();
                    case TEXT:
                        return DataType.text();
                    case INET:
                        return DataType.inet();
                    case BLOB:
                        return DataType.blob();
                    case VARINT:
                        return DataType.varint();
                    case DECIMAL:
                        return DataType.decimal();
                    case INT:
                        return DataType.cint();
                    case DATE:
                        return DataType.date();
                    case BIGINT:
                        return DataType.bigint();
                    case TIME:
                        return DataType.time();
                    case BOOLEAN:
                        return DataType.cboolean();
                    case FLOAT:
                        return DataType.cfloat();
                    case DOUBLE:
                        return DataType.cdouble();
                    case TIMESTAMP:
                        return DataType.timestamp();
                    case SMALLINT:
                        return DataType.smallint();
                    case TINYINT:
                        return DataType.tinyint();
                }
            case Set:
                return DataType.set(toDataType(((CqlField.CqlSet) type).type()));
            case List:
                return DataType.list(toDataType(((CqlField.CqlList) type).type()));
            case Map:
                final CqlField.CqlMap map = (CqlField.CqlMap) type;
                return DataType.map(toDataType(map.keyType()), toDataType(map.valueType()));
            case Frozen:
                final CqlField.CqlFrozen frozen = (CqlField.CqlFrozen) type;
                return toDataType(frozen.inner(), true);
            case Udt:
                final CqlUdt udt = (CqlUdt) type;
                return UserTypeHelper
                       .newUserType(udt.keyspace(),
                                    udt.name(), isFrozen,
                                    udt.fields().stream().map(f -> UserTypeHelper.newField(f.name(), toDataType(f.type()))).collect(Collectors.toList()),
                                    ProtocolVersion.V3,
                                    CodecRegistry.DEFAULT_INSTANCE);
            case Tuple:
                final CqlField.CqlTuple tuple = (CqlField.CqlTuple) type;
                return new TupleType(tuple.types().stream().map(f -> toDataType(f, isFrozen)).collect(Collectors.toList()), ProtocolVersion.V3, CodecRegistry.DEFAULT_INSTANCE);
            default:
                throw new NotImplementedException(type + " type not implemented yet");
        }
    }
}
