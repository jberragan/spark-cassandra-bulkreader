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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.SparkCqlField;
import org.apache.cassandra.spark.data.fourzero.types.spark.FourZeroSparkCqlField;

public class CqlList extends org.apache.cassandra.spark.data.fourzero.complex.CqlList implements SparkCqlField.ComplexTrait, SparkCqlField.SparkList, SparkCqlField.ListTrait
{
    public CqlList(CqlField.CqlList list)
    {
        super(FourZeroSparkCqlField.decorate(list.type()));
    }

    @Override
    public SparkCqlField.SparkCqlType type(int i)
    {
        return (SparkCqlField.SparkCqlType) super.type(i);
    }

    @Override
    public SparkCqlField.SparkCqlType type()
    {
        return (SparkCqlField.SparkCqlType) super.type();
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
}
