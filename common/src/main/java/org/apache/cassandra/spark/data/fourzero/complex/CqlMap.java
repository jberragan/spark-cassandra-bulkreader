package org.apache.cassandra.spark.data.fourzero.complex;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.fourzero.FourZeroCqlType;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.MapType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.BufferCell;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.CellPath;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.MapSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.utils.RandomUtils;

import static org.apache.cassandra.spark.data.CqlField.NO_TTL;

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

public class CqlMap extends CqlCollection implements CqlField.CqlMap
{
    public CqlMap(final CqlField.CqlType keyType, final CqlField.CqlType valueType)
    {
        super(keyType, valueType);
    }

    public CqlField.CqlType keyType()
    {
        return type();
    }

    public CqlField.CqlType valueType()
    {
        return type(1);
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        return MapType.getInstance(
        ((FourZeroCqlType) keyType()).dataType(),
        ((FourZeroCqlType) valueType()).dataType(),
        isMultiCell
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> TypeSerializer<T> serializer()
    {
        return (TypeSerializer<T>) MapSerializer.getInstance(
        ((FourZeroCqlType) keyType()).serializer(),
        ((FourZeroCqlType) valueType()).serializer(),
        ((FourZeroCqlType) keyType()).dataType().comparatorSet
        );
    }

    @Override
    public InternalType internalType()
    {
        return InternalType.Map;
    }

    @Override
    public String name()
    {
        return "map";
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return IntStream.range(0, RandomUtils.RANDOM.nextInt(16) + minCollectionSize)
                        .mapToObj(i -> Pair.of(keyType().randomValue(minCollectionSize), valueType().randomValue(minCollectionSize)))
                        .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (a, b) -> a));
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int pos, Object value)
    {
        udtValue.setMap(pos, (Map<?, ?>) value);
    }

    @Override
    public org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.DataType.map(
        ((FourZeroCqlType) keyType()).driverDataType(isFrozen),
        ((FourZeroCqlType) valueType()).driverDataType(isFrozen));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object convertForCqlWriter(Object value, CassandraVersion version)
    {
        final Map<Object, Object> map = (Map<Object, Object>) value;
        return map.entrySet().stream()
                  .collect(Collectors.toMap(
                  e -> keyType().convertForCqlWriter(e.getKey(), version),
                  e -> valueType().convertForCqlWriter(e.getValue(), version)
                  ));
    }

    @Override
    public void addCell(final org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Row.Builder rowBuilder,
                        ColumnMetadata cd, long timestamp, int ttl, int now, Object value)
    {
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet())
        {
            if (ttl != NO_TTL)
            {
                rowBuilder.addCell(BufferCell.expiring(cd, timestamp, ttl, now, valueType().serialize(entry.getValue()),
                                                       CellPath.create(keyType().serialize(entry.getKey()))));
            }
            else
            {
                rowBuilder.addCell(BufferCell.live(cd, timestamp, valueType().serialize(entry.getValue()),
                                                   CellPath.create(keyType().serialize(entry.getKey()))));
            }
        }
    }
}
