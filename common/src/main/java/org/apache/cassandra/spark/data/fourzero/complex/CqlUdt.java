package org.apache.cassandra.spark.data.fourzero.complex;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.fourzero.FourZeroCqlType;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.UDTValue;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.UserType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.UserTypeHelper;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.Schema;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.spark.utils.ByteBufUtils;
import org.jetbrains.annotations.Nullable;

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

public class CqlUdt extends FourZeroCqlType implements CqlField.CqlUdt
{
    public static final CqlUdt.Serializer SERIALIZER = new CqlUdt.Serializer();
    private final String keyspace, name;
    private final List<CqlField> fields;
    private final Map<String, CqlField> fieldMap;
    private final int hashCode;

    public CqlUdt(final String keyspace, final String name, final List<CqlField> fields)
    {
        this.keyspace = keyspace;
        this.name = name;
        this.fields = Collections.unmodifiableList(fields);
        this.fieldMap = this.fields.stream().collect(Collectors.toMap(CqlField::name, Function.identity()));
        this.hashCode = new HashCodeBuilder(89, 97)
                        .append(internalType().ordinal())
                        .append(this.keyspace)
                        .append(this.name)
                        .append(this.fields)
                        .toHashCode();
    }

    @Override
    public Set<CqlField.CqlUdt> udts()
    {
        final Set<CqlField.CqlUdt> udts = fields.stream()
                                       .map(CqlField::type)
                                       .map(type -> (FourZeroCqlType) type)
                                       .map(CqlField.CqlType::udts)
                                       .flatMap(Collection::stream)
                                       .collect(Collectors.toSet());
        udts.add(this);
        return udts;
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return fields().stream().collect(Collectors.toMap(CqlField::name, f -> Objects.requireNonNull(f.type().randomValue(minCollectionSize))));
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int pos, Object value)
    {
        udtValue.setUDTValue(pos, (UDTValue) value);
    }

    @Override
    public org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return UserTypeHelper
               .newUserType(keyspace(),
                            name(), isFrozen,
                            fields().stream().map(f -> UserTypeHelper.newField(f.name(), ((FourZeroCqlType) f.type()).driverDataType(isFrozen)))
                                    .collect(Collectors.toList()),
                            ProtocolVersion.V3
               );
    }

    @Override
    public Object convertForCqlWriter(Object value, CassandraVersion version)
    {
        return toUserTypeValue(version, this, value);
    }

    @Override
    public String toString()
    {
        return this.cqlName();
    }

    public CqlFrozen frozen()
    {
        return CqlFrozen.build(this);
    }

    public static Builder builder(final String keyspace, final String name)
    {
        return new Builder(keyspace, name);
    }

    public static class Builder implements CqlField.CqlUdtBuilder
    {
        private final String keyspace, name;
        private final List<CqlField> fields = new ArrayList<>();

        public Builder(final String keyspace, final String name)
        {
            this.keyspace = keyspace;
            this.name = name;
        }

        @Override
        public Builder withField(final String name, final CqlField.CqlType type)
        {
            this.fields.add(new CqlField(false, false, false, name, type, this.fields.size()));
            return this;
        }

        @Override
        public CqlUdt build()
        {
            return new CqlUdt(keyspace, name, fields);
        }
    }

    @Override
    public boolean isSupported()
    {
        return true;
    }

    @Override
    public AbstractType<?> dataType()
    {
        return dataType(true);
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        // get UserTypeSerializer from Schema instance to ensure fields are deserialized in correct order
        return Schema.instance.getKeyspaceMetadata(keyspace()).types
               .get(UTF8Serializer.instance.serialize(name()))
               .orElseThrow(() -> new RuntimeException(String.format("UDT '%s' not initialized", name())));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> TypeSerializer<T> serializer()
    {
        // get UserTypeSerializer from Schema instance to ensure fields are deserialized in correct order
        return (TypeSerializer<T>) Schema.instance.getKeyspaceMetadata(keyspace()).types
                                   .get(UTF8Serializer.instance.serialize(name()))
                                   .orElseThrow(() -> new RuntimeException(String.format("UDT '%s' not initialized", name())))
                                   .getSerializer();
    }

    @Nullable
    @Override
    public Object deserializeToJava(ByteBuffer buf)
    {
        return deserializeToJava(buf, false);
    }

    @Nullable
    @Override
    public Object deserializeToJava(ByteBuffer buf, boolean isFrozen)
    {
        return deserializeUdt(buf, isFrozen);
    }

    @Override
    public Map<String, Object> deserializeUdt(final ByteBuffer buf, final boolean isFrozen)
    {
        if (!isFrozen)
        {
            final int fieldCount = buf.getInt();
            Preconditions.checkArgument(fieldCount == size(),
                                        String.format("Unexpected number of fields deserializing UDT '%s', expected %d fields but %d found", cqlName(), size(), fieldCount));
        }

        final Map<String, Object> result = new HashMap<>(size());
        for (final CqlField field : fields())
        {
            if (buf.remaining() < 4)
            {
                break;
            }
            final int len = buf.getInt();
            result.put(field.name(), len <= 0 ? null : field.type().deserializeToJava(ByteBufUtils.readBytes(buf, len), isFrozen));
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ByteBuffer serialize(Object value)
    {
        return serializeUdt((Map<String, Object>) value);
    }

    @Override
    public ByteBuffer serializeUdt(final Map<String, Object> values)
    {
        final List<ByteBuffer> bufs = fields().stream().map(f -> f.serialize(values.get(f.name()))).collect(Collectors.toList());

        final ByteBuffer result = ByteBuffer.allocate(4 + bufs.stream().map(Buffer::remaining).map(a -> a + 4).reduce(Integer::sum).orElse(0));
        result.putInt(bufs.size()); // num fields
        for (final ByteBuffer buf : bufs)
        {
            result.putInt(buf.remaining()); // len
            result.put(buf.duplicate()); // value
        }
        return (ByteBuffer) result.flip();
    }

    public InternalType internalType()
    {
        return InternalType.Udt;
    }

    public String createStmt(final String keyspace)
    {
        return String.format("CREATE TYPE %s.%s (%s);", keyspace, name, fieldsString());
    }

    private String fieldsString()
    {
        return fields.stream().map(CqlUdt::fieldString).collect(Collectors.joining(", "));
    }

    private static String fieldString(final CqlField field)
    {
        return String.format("%s %s", field.name(), field.type().cqlName());
    }

    public String keyspace()
    {
        return keyspace;
    }

    public String name()
    {
        return name;
    }

    public int size()
    {
        return fields.size();
    }

    public List<CqlField> fields()
    {
        return fields;
    }

    public CqlField field(String name)
    {
        return fieldMap.get(name);
    }

    public CqlField field(int i)
    {
        return fields.get(i);
    }

    public CqlField.CqlType type(int i)
    {
        return field(i).type();
    }

    public String cqlName()
    {
        return name;
    }

    public static CqlUdt read(final Input input)
    {
        final Builder builder = CqlUdt.builder(input.readString(), input.readString());
        final int numFields = input.readInt();
        for (int i = 0; i < numFields; i++)
        {
            builder.withField(input.readString(), CqlField.CqlType.read(input));
        }
        return builder.build();
    }

    @Override
    public void write(Output output)
    {
        CqlField.CqlType.write(this, output);
        output.writeString(this.keyspace);
        output.writeString(this.name);
        output.writeInt(this.fields.size());
        for (final CqlField field : this.fields)
        {
            output.writeString(field.name());
            field.type().write(output);
        }
    }

    @Override
    public int hashCode()
    {
        return this.hashCode;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }
        if (obj.getClass() != getClass())
        {
            return false;
        }

        final CqlUdt rhs = (CqlUdt) obj;
        return new EqualsBuilder()
               .append(internalType(), rhs.internalType())
               .append(this.keyspace, rhs.keyspace)
               .append(this.name, rhs.name)
               .append(this.fields, rhs.fields)
               .isEquals();
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CqlUdt>
    {
        @Override
        public CqlUdt read(final Kryo kryo, final Input input, final Class type)
        {
            return CqlUdt.read(input);
        }

        @Override
        public void write(final Kryo kryo, final Output output, final CqlUdt udt)
        {
            udt.write(output);
        }
    }

    @SuppressWarnings("unchecked")
    public static UDTValue toUserTypeValue(final CassandraVersion version, final CqlUdt udt, final Object value)
    {
        final Map<String, Object> values = (Map<String, Object>) value;
        final UDTValue udtValue = UserTypeHelper.newUDTValue(toUserType(udt));
        int pos = 0;
        for (final CqlField field : udt.fields())
        {
            setInnerValue(version, udtValue, (FourZeroCqlType) field.type(), pos++, values.get(field.name()));
        }
        return udtValue;
    }

    // set inner value for UDTs or Tuples
    public static void setInnerValue(final CassandraVersion version, final SettableByIndexData<?> udtValue,
                                     final FourZeroCqlType type, final int pos, final Object v)
    {
        type.setInnerValue(udtValue, pos, type.convertForCqlWriter(v, version));
    }

    public static UserType toUserType(final CqlUdt udt)
    {
        final List<UserType.Field> fields = udt.fields().stream()
                                               .map(f -> UserTypeHelper.newField(f.name(), ((FourZeroCqlType) f.type()).driverDataType()))
                                               .collect(Collectors.toList());
        return UserTypeHelper.newUserType(udt.keyspace(), udt.name(), true, fields, ProtocolVersion.V3);
    }
}
