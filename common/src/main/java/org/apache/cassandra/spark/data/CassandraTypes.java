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

package org.apache.cassandra.spark.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;

import com.esotericsoftware.kryo.io.Input;
import org.apache.cassandra.spark.data.fourzero.FourZeroTypes;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.CassandraVersion;
import org.jetbrains.annotations.Nullable;

public abstract class CassandraTypes
{
    public static CassandraTypes get(CassandraVersion version)
    {
        switch (version)
        {
            case THREEZERO:
            case FOURZERO:
                return FourZeroTypes.INSTANCE;
            default:
                throw new NotImplementedException("Version not implemented: " + version);
        }
    }

    public static final Pattern COLLECTIONS_PATTERN = Pattern.compile("^(set|list|map|tuple)<(.+)>$", Pattern.CASE_INSENSITIVE);
    public static final Pattern FROZEN_PATTERN = Pattern.compile("^frozen<(.*)>$", Pattern.CASE_INSENSITIVE);

    public CqlTable buildSchema(final String keyspace,
                                final String createStmt,
                                final ReplicationFactor rf,
                                final Partitioner partitioner)
    {
        return buildSchema(keyspace, createStmt, rf, partitioner, Collections.emptySet(), null);
    }

    public CqlTable buildSchema(final String keyspace,
                                final String createStmt,
                                final ReplicationFactor rf,
                                final Partitioner partitioner,
                                final Set<String> udts)
    {
        return buildSchema(keyspace, createStmt, rf, partitioner, udts, null);

    }

    public CqlTable buildSchema(final String keyspace,
                                final String createStmt,
                                final ReplicationFactor rf,
                                final Partitioner partitioner,
                                final Set<String> udts,
                                @Nullable final UUID tableId)
    {
        return buildSchema(keyspace, createStmt, rf, partitioner, udts, tableId, false);
    }

    public abstract CqlTable buildSchema(final String keyspace,
                                         final String createStmt,
                                         final ReplicationFactor rf,
                                         final Partitioner partitioner,
                                         final Set<String> udts,
                                         @Nullable final UUID tableId,
                                         final boolean enabledCdc);

    public List<CqlField.NativeType> allTypes()
    {
        return Arrays.asList(ascii(), bigint(), blob(), bool(), counter(), date(), decimal(), aDouble(), duration(), empty(), aFloat(),
                             inet(), aInt(), smallint(), text(), time(), timestamp(), timeuuid(), tinyint(), uuid(), varchar(), varint());
    }

    public abstract Map<String, ? extends CqlField.NativeType> nativeTypeNames();

    public CqlField.NativeType nativeType(String name)
    {
        return nativeTypeNames().get(name.toLowerCase());
    }

    public List<CqlField.NativeType> supportedTypes()
    {
        return allTypes().stream().filter(CqlField.NativeType::isSupported).collect(Collectors.toList());
    }

    // native

    public abstract CqlField.NativeType ascii();

    public abstract CqlField.NativeType blob();

    public abstract CqlField.NativeType bool();

    public abstract CqlField.NativeType counter();

    public abstract CqlField.NativeType bigint();

    public abstract CqlField.NativeType date();

    public abstract CqlField.NativeType decimal();

    public abstract CqlField.NativeType aDouble();

    public abstract CqlField.NativeType duration();

    public abstract CqlField.NativeType empty();

    public abstract CqlField.NativeType aFloat();

    public abstract CqlField.NativeType inet();

    public abstract CqlField.NativeType aInt();

    public abstract CqlField.NativeType smallint();

    public abstract CqlField.NativeType text();

    public abstract CqlField.NativeType time();

    public abstract CqlField.NativeType timestamp();

    public abstract CqlField.NativeType timeuuid();

    public abstract UUID getTimeUUID();

    public abstract CqlField.NativeType tinyint();

    public abstract CqlField.NativeType uuid();

    public abstract CqlField.NativeType varchar();

    public abstract CqlField.NativeType varint();

    // complex

    public abstract CqlField.CqlType collection(final String name, final CqlField.CqlType... types);

    public abstract CqlField.CqlList list(final CqlField.CqlType type);

    public abstract CqlField.CqlSet set(final CqlField.CqlType type);

    public abstract CqlField.CqlMap map(final CqlField.CqlType keyType, final CqlField.CqlType valueType);

    public abstract CqlField.CqlTuple tuple(final CqlField.CqlType... types);

    public abstract CqlField.CqlType frozen(final CqlField.CqlType type);

    public abstract CqlField.CqlUdtBuilder udt(final String keyspace, final String name);

    public CqlField.CqlType parseType(final String type)
    {
        return parseType(type, Collections.emptyMap());
    }

    public CqlField.CqlType parseType(final String type, final Map<String, CqlField.CqlUdt> udts)
    {
        if (StringUtils.isEmpty(type))
        {
            return null;
        }
        final Matcher matcher = COLLECTIONS_PATTERN.matcher(type);
        if (matcher.find())
        {
            // cql collection
            final String[] types = splitInnerTypes(matcher.group(2));
            return collection(matcher.group(1), Stream.of(types).map(t -> parseType(t, udts)).toArray(CqlField.CqlType[]::new));
        }
        final Matcher frozenMatcher = FROZEN_PATTERN.matcher(type);
        if (frozenMatcher.find())
        {
            // frozen collections
            return frozen(parseType(frozenMatcher.group(1), udts));
        }

        if (udts.containsKey(type))
        {
            // user defined type
            return udts.get(type);
        }

        // native cql 3 type
        return nativeType(type);
    }

    @VisibleForTesting
    public static String[] splitInnerTypes(final String str)
    {
        final List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int parentheses = 0;
        for (int i = 0; i < str.length(); i++)
        {
            final char c = str.charAt(i);
            switch (c)
            {
                case ' ':
                    if (parentheses == 0)
                    {
                        continue;
                    }
                    break;
                case ',':
                    if (parentheses == 0)
                    {
                        if (current.length() > 0)
                        {
                            result.add(current.toString());
                            current = new StringBuilder();
                        }
                        continue;
                    }
                    break;
                case '<':
                    parentheses++;
                    break;
                case '>':
                    parentheses--;
                    break;
            }
            current.append(c);
        }

        if (current.length() > 0 || result.isEmpty())
        {
            result.add(current.toString());
        }

        return result.toArray(new String[0]);
    }

    public abstract CqlField.CqlType readType(CqlField.CqlType.InternalType type, Input input);
}
