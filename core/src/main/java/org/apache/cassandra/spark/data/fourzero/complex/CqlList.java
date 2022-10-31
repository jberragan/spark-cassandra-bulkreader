package org.apache.cassandra.spark.data.fourzero.complex;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.fourzero.FourZeroCqlType;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.ListType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.BufferCell;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.CellPath;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.ListSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.utils.UUIDGen;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;
import scala.collection.mutable.WrappedArray;

import static org.apache.cassandra.spark.cdc.AbstractCdcEvent.NO_TTL;

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

public class CqlList extends CqlCollection implements CqlField.CqlList
{
    public CqlList(final CqlField.CqlType type)
    {
        super(type);
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        return ListType.getInstance(
        ((FourZeroCqlType) type()).dataType(), isMultiCell
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return ArrayData.toArrayData(((Collection<Object>) o)
                                     .stream()
                                     .map(a -> type().toSparkSqlType(a)).toArray());
    }

    @Override
    public InternalType internalType()
    {
        return InternalType.List;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> TypeSerializer<T> serializer()
    {
        return (TypeSerializer<T>) ListSerializer.getInstance(
        ((FourZeroCqlType) type()).serializer()
        );
    }

    @Override
    public boolean equals(Object o1, Object o2)
    {
        return CqlField.equalsArrays(((GenericArrayData) o1).array(), ((GenericArrayData) o2).array(), (pos) -> type());
    }

    @Override
    public String name()
    {
        return "list";
    }

    @Override
    public DataType sparkSqlType(CassandraBridge.BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createArrayType(type().sparkSqlType(bigNumberConfig));
    }

    @Override
    public int compare(Object o1, Object o2)
    {
        return CqlField.compareArrays(((GenericArrayData) o1).array(), ((GenericArrayData) o2).array(), (pos) -> type());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object toTestRowType(Object value)
    {
        return Stream.of((Object[]) ((WrappedArray<Object>) value).array()).map(v -> type().toTestRowType(v)).collect(Collectors.toList());
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int pos, Object value)
    {
        udtValue.setList(pos, (List<?>) value);
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int pos)
    {
        return Arrays.stream(row.getArray(pos).array())
                     .map(o -> type().toTestRowType(o))
                     .collect(Collectors.toList());
    }

    @Override
    public Object sparkSqlRowValue(Row row, int pos)
    {
        return row.getList(pos).stream().map(o -> type().toTestRowType(o)).collect(Collectors.toList());
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return IntStream.range(0, RandomUtils.RANDOM.nextInt(16) + minCollectionSize)
                        .mapToObj(i -> type().randomValue(minCollectionSize))
                        .collect(Collectors.toList());
    }

    @Override
    public org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.DataType.list(
        ((FourZeroCqlType) type()).driverDataType(isFrozen)
        );
    }

    @Override
    public Object convertForCqlWriter(Object value, CassandraBridge.CassandraVersion version)
    {
        return ((List<?>) value).stream()
                                .map(o -> type().convertForCqlWriter(o, version))
                                .collect(Collectors.toList());
    }

    @Override
    public void addCell(final org.apache.cassandra.spark.shaded.fourzero.cassandra.db.rows.Row.Builder rowBuilder,
                        ColumnMetadata cd, long timestamp, int ttl, int now, Object value)
    {
        for (Object o : (List<?>) value)
        {
            if (ttl != NO_TTL)
            {
                rowBuilder.addCell(BufferCell.expiring(cd, timestamp, ttl, now, type().serialize(o),
                                                       CellPath.create(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes()))));
            }
            else
            {
                rowBuilder.addCell(BufferCell.live(cd, timestamp, type().serialize(o),
                                                   CellPath.create(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes()))));
            }
        }
    }
}
