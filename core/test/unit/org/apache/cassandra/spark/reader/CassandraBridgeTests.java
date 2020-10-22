package org.apache.cassandra.spark.reader;

import org.junit.Test;

import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.types.DataTypes;

import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;

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
public class CassandraBridgeTests
{
    @Test
    public void testSparkDataTypes()
    {
        qt().forAll(TestUtils.bridges())
            .checkAssert(bridge -> {
                assertEquals(DataTypes.StringType, bridge.toSparkSQLType(CqlField.NativeCql3Type.TIMEUUID));
                assertEquals(DataTypes.StringType, bridge.toSparkSQLType(CqlField.NativeCql3Type.UUID));
                assertEquals(DataTypes.StringType, bridge.toSparkSQLType(CqlField.NativeCql3Type.ASCII));
                assertEquals(DataTypes.StringType, bridge.toSparkSQLType(CqlField.NativeCql3Type.VARCHAR));
                assertEquals(DataTypes.StringType, bridge.toSparkSQLType(CqlField.NativeCql3Type.TEXT));
                assertEquals(DataTypes.BinaryType, bridge.toSparkSQLType(CqlField.NativeCql3Type.INET));
                assertEquals(DataTypes.BinaryType, bridge.toSparkSQLType(CqlField.NativeCql3Type.BLOB));
                assertEquals(DataTypes.IntegerType, bridge.toSparkSQLType(CqlField.NativeCql3Type.INT));
                assertEquals(DataTypes.DateType, bridge.toSparkSQLType(CqlField.NativeCql3Type.DATE));
                assertEquals(DataTypes.LongType, bridge.toSparkSQLType(CqlField.NativeCql3Type.BIGINT));
                assertEquals(DataTypes.LongType, bridge.toSparkSQLType(CqlField.NativeCql3Type.TIME));
                assertEquals(DataTypes.BooleanType, bridge.toSparkSQLType(CqlField.NativeCql3Type.BOOLEAN));
                assertEquals(DataTypes.FloatType, bridge.toSparkSQLType(CqlField.NativeCql3Type.FLOAT));
                assertEquals(DataTypes.DoubleType, bridge.toSparkSQLType(CqlField.NativeCql3Type.DOUBLE));
                assertEquals(DataTypes.TimestampType, bridge.toSparkSQLType(CqlField.NativeCql3Type.TIMESTAMP));
                assertEquals(DataTypes.NullType, bridge.toSparkSQLType(CqlField.NativeCql3Type.EMPTY));
                assertEquals(DataTypes.ShortType, bridge.toSparkSQLType(CqlField.NativeCql3Type.SMALLINT));
                assertEquals(DataTypes.ByteType, bridge.toSparkSQLType(CqlField.NativeCql3Type.TINYINT));
            });
    }
}
