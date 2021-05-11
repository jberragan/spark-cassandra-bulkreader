package org.apache.cassandra.spark.shaded.fourzero.cassandra.cql3.functions.types;

import java.util.stream.Collectors;

import org.apache.cassandra.spark.data.fourzero.FourZeroCqlType;
import org.apache.cassandra.spark.data.fourzero.complex.CqlTuple;
import org.apache.cassandra.spark.shaded.fourzero.cassandra.transport.ProtocolVersion;

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
public class TupleHelper
{
    // helper methods to access package-private Tuple methods

    public static TupleType buildTupleType(CqlTuple tuple, boolean isFrozen)
    {
        return new TupleType(
        tuple.types().stream()
             .map(type -> (FourZeroCqlType) type)
             .map(type -> type.driverDataType(isFrozen)).collect(Collectors.toList()),
        ProtocolVersion.V3, FourZeroCqlType.CODEC_REGISTRY
        );
    }

    public static TupleValue buildTupleValue(final CqlTuple tuple)
    {
        return buildTupleValue(tuple, false);
    }

    public static TupleValue buildTupleValue(final CqlTuple tuple, boolean isFrozen)
    {
        return new TupleValue(buildTupleType(tuple, isFrozen));
    }
}
