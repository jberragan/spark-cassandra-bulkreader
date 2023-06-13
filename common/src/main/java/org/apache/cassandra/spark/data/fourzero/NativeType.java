package org.apache.cassandra.spark.data.fourzero;

import java.util.Collections;
import java.util.Set;

import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TypeSerializer;

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

public abstract class NativeType extends FourZeroCqlType implements CqlField.NativeType
{
    private final int hashCode;

    protected NativeType()
    {
        this.hashCode = name().hashCode();
    }

    public CqlField.CqlType.InternalType internalType()
    {
        return CqlField.CqlType.InternalType.NativeCql;
    }

    @Override
    public boolean isSupported()
    {
        return true;
    }

    @Override
    public AbstractType<?> dataType()
    {
        throw CqlField.notImplemented(this);
    }

    @Override
    public Object convertForCqlWriter(final Object value, final CassandraVersion version)
    {
        return value;
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        return this.dataType();
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }

    @Override
    public boolean equals(final Object obj)
    {
        return obj != null &&
               (obj == this || obj.getClass() == getClass());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> TypeSerializer<T> serializer()
    {
        return (TypeSerializer<T>) dataType().getSerializer();
    }

    @Override
    public String cqlName()
    {
        return this.name().toLowerCase();
    }

    @Override
    public void write(Output output)
    {
        CqlField.CqlType.write(this, output);
        output.writeString(name());
    }

    public Set<CqlField.CqlUdt> udts()
    {
        return Collections.emptySet();
    }
}