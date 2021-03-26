package org.apache.cassandra.spark.data.fourzero.complex;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.fourzero.FourZeroCqlType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

public abstract class CqlCollection extends FourZeroCqlType implements CqlField.CqlCollection
{
    public final List<CqlField.CqlType> types;
    private final int hashCode;

    CqlCollection(CqlField.CqlType type)
    {
        this(Collections.singletonList(type));
    }

    CqlCollection(CqlField.CqlType... types)
    {
        this(Arrays.asList(types));
    }

    CqlCollection(List<CqlField.CqlType> types)
    {
        this.types = new ArrayList<>(types);
        this.hashCode = new HashCodeBuilder(29, 31)
                        .append(types.toArray(new CqlField.CqlType[0]))
                        .hashCode();
    }

    @Override
    public Object toSparkSqlType(Object o)
    {
        return toSparkSqlType(o, false);
    }

    @Override
    public AbstractType<?> dataType()
    {
        return this.dataType(true);
    }

    @Override
    public ByteBuffer serialize(Object value)
    {
        return serializer().serialize(value);
    }

    @Override
    public Object deserialize(final ByteBuffer buf)
    {
        return deserialize(buf, false);
    }

    @Override
    public Object deserialize(ByteBuffer buf, boolean isFrozen)
    {
        return toSparkSqlType(serializer().deserialize(buf));
    }

    @Override
    public boolean isSupported()
    {
        return true;
    }

    public static CqlCollection build(final String name, final CqlField.CqlType... types)
    {
        return build(CqlField.CqlType.InternalType.fromString(name), types);
    }

    public static CqlCollection build(final CqlField.CqlType.InternalType internalType, final CqlField.CqlType... types)
    {
        if (types.length < 1 || types[0] == null)
        {
            throw new IllegalArgumentException("Collection type requires a non-null key data type");
        }

        switch (internalType)
        {
            case Set:
                return set(types[0]);
            case List:
                return list(types[0]);
            case Map:
                if (types.length < 2 || types[1] == null)
                {
                    throw new IllegalArgumentException("Map collection type requires a non-null value data type");
                }
                return map(types[0], types[1]);
            case Tuple:
                return tuple(types);
            default:
                throw new IllegalArgumentException("Unknown collection type: " + internalType);
        }
    }

    public static CqlList list(final CqlField.CqlType type)
    {
        return new CqlList(type);
    }

    public static CqlSet set(final CqlField.CqlType type)
    {
        return new CqlSet(type);
    }

    public static CqlMap map(final CqlField.CqlType keyType, final CqlField.CqlType valueType)
    {
        return new CqlMap(keyType, valueType);
    }

    public static CqlTuple tuple(final CqlField.CqlType... types)
    {
        return new CqlTuple(types);
    }

    public int size()
    {
        return this.types.size();
    }

    public List<CqlField.CqlType> types()
    {
        return this.types;
    }

    public CqlField.CqlType type()
    {
        return type(0);
    }

    public CqlField.CqlType type(int i)
    {
        return this.types.get(i);
    }

    public CqlFrozen frozen()
    {
        return CqlFrozen.build(this);
    }

    public String cqlName()
    {
        return String.format("%s<%s>", internalType().name().toLowerCase(), this.types.stream().map(CqlField.CqlType::cqlName).collect(Collectors.joining(", ")));
    }

    @Override
    public Set<CqlField.CqlUdt> udts()
    {
        return this.types.stream()
                         .map(CqlField.CqlType::udts)
                         .flatMap(Collection::stream)
                         .collect(Collectors.toSet());
    }

    @Override
    public String toString()
    {
        return this.cqlName();
    }

    public static CqlCollection read(final CqlField.CqlType.InternalType internalType, final Input input)
    {
        final int numTypes = input.readInt();
        final CqlField.CqlType[] types = new CqlField.CqlType[numTypes];
        for (int i = 0; i < numTypes; i++)
        {
            types[i] = CqlField.CqlType.read(input);
        }
        return CqlCollection.build(internalType, types);
    }

    @Override
    public void write(Output output)
    {
        CqlField.CqlType.write(this, output);
        output.writeInt(this.types.size());
        for (final CqlField.CqlType type : this.types)
        {
            type.write(output);
        }
    }

    @Override
    public int hashCode()
    {
        return hashCode;
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

        final CqlCollection rhs = (CqlCollection) obj;
        return new EqualsBuilder()
               .append(internalType(), rhs.internalType())
               .append(this.types, rhs.types)
               .isEquals();
    }
}
