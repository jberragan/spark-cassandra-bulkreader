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

package org.apache.cassandra.spark.data.fourzero.types.spark.complex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.SparkCqlField;
import org.apache.cassandra.spark.data.fourzero.types.spark.FourZeroSparkCqlField;
import org.apache.cassandra.spark.reader.BigNumberConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;
import scala.collection.JavaConverters;

public class CqlMap extends org.apache.cassandra.spark.data.fourzero.complex.CqlMap implements SparkCqlField.ComplexTrait, SparkCqlField.SparkMap
{

    public CqlMap(CqlField.CqlMap map)
    {
        super(FourZeroSparkCqlField.decorate(map.keyType()), FourZeroSparkCqlField.decorate(map.valueType()));
    }

    @Override
    public SparkCqlField.SparkCqlType keyType()
    {
        return (SparkCqlField.SparkCqlType) super.keyType();
    }

    @Override
    public SparkCqlField.SparkCqlType valueType()
    {
        return (SparkCqlField.SparkCqlType) super.valueType();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object toSparkSqlType(@NotNull Object o, boolean isFrozen)
    {
        return mapToSparkSqlType((Map<Object, Object>) o);
    }

    private ArrayBasedMapData mapToSparkSqlType(final Map<Object, Object> map)
    {
        final Object[] keys = new Object[map.size()];
        final Object[] values = new Object[map.size()];
        int pos = 0;
        for (final Map.Entry<Object, Object> entry : map.entrySet())
        {
            keys[pos] = keyType().toSparkSqlType(entry.getKey());
            values[pos] = valueType().toSparkSqlType(entry.getValue());
            pos++;
        }
        return new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values));
    }

    @Override
    public DataType sparkSqlType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createMapType(keyType().sparkSqlType(bigNumberConfig), valueType().sparkSqlType(bigNumberConfig));
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int pos)
    {
        final MapData map = row.getMap(pos);
        final ArrayData keys = map.keyArray();
        final ArrayData values = map.valueArray();
        final Map<Object, Object> result = new HashMap<>(keys.numElements());
        for (int i = 0; i < keys.numElements(); i++)
        {
            final Object key = keyType().toTestRowType(keys.get(i, keyType().sparkSqlType()));
            final Object value = valueType().toTestRowType(values.get(i, valueType().sparkSqlType()));
            result.put(key, value);
        }
        return result;
    }

    @Override
    public Object sparkSqlRowValue(Row row, int pos)
    {
        return row.getJavaMap(pos).entrySet().stream()
                  .collect(Collectors.toMap(
                  e -> keyType().toTestRowType(e.getKey()),
                  e -> valueType().toTestRowType(e.getValue())
                  ));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object toTestRowType(Object value)
    {
        return ((Map<Object, Object>) JavaConverters.mapAsJavaMapConverter(((scala.collection.immutable.Map<?, ?>) value)).asJava())
               .entrySet().stream()
               .collect(Collectors.toMap(
                        e -> keyType().toTestRowType(e.getKey()),
                        e -> valueType().toTestRowType(e.getValue()))
               );
    }

    public SparkCqlField.SparkFrozen freeze()
    {
        return new CqlFrozen(this);
    }

    public List<SparkCqlField.SparkCqlType> sparkTypes()
    {
        return super.types().stream()
                    .map(f -> (SparkCqlField.SparkCqlType) f)
                    .collect(Collectors.toList());
    }

    public SparkCqlField.SparkCqlType sparkType()
    {
        return (SparkCqlField.SparkCqlType) super.type();
    }

    public SparkCqlField.SparkCqlType sparkType(int i)
    {
        return (SparkCqlField.SparkCqlType) super.type(i);
    }

    @Override
    public boolean equals(Object o1, Object o2)
    {
        return SparkCqlField.equalsArrays(((MapData) o1).valueArray().array(), ((MapData) o2).valueArray().array(), (pos) -> valueType());
    }

    @Override
    public int compare(Object o1, Object o2)
    {
        return SparkCqlField.compareArrays(((MapData) o1).valueArray().array(), ((MapData) o2).valueArray().array(), (pos) -> valueType());
    }
}