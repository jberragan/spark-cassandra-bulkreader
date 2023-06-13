package org.apache.cassandra.spark.data.fourzero.types;

import org.apache.cassandra.spark.data.fourzero.NativeType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.db.marshal.EmptyType;

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

public class Empty extends NativeType
{
    public static final Empty INSTANCE = new Empty();

    @Override
    public boolean isSupported()
    {
        return false;
    }

    @Override
    public String name()
    {
        return "empty";
    }

    @Override
    public AbstractType<?> dataType()
    {
        return EmptyType.instance;
    }

    @Override
    public int cardinality(int orElse)
    {
        return 1;
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return null;
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int pos, Object value)
    {
        udtValue.setToNull(pos);
    }
}
