package org.apache.cassandra.spark.data.fourzero.types;

import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.LongType;
import org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.SettableByIndexData;

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

public class BigInt extends LongBased
{
    public static final BigInt INSTANCE = new BigInt();

    @Override
    public String name()
    {
        return "bigint";
    }

    @Override
    public AbstractType<?> dataType()
    {
        return LongType.instance;
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int pos, Object value)
    {
        udtValue.setLong(pos, (long) value);
    }

    @Override
    public org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.spark.shaded.fourzero.datastax.driver.core.DataType.bigint();
    }
}
