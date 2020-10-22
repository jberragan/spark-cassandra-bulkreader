package org.apache.cassandra.spark.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.Decimal;
import org.jetbrains.annotations.NotNull;

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
@SuppressWarnings({ "WeakerAccess", "unused" })
public class CqlField implements Serializable, Comparable<CqlField>
{

    public static final long serialVersionUID = 42L;
    public static final Pattern COLLECTIONS_PATTERN = Pattern.compile("^(set|list|map|tuple)<(.+)>$", Pattern.CASE_INSENSITIVE);
    public static final Pattern FROZEN_PATTERN = Pattern.compile("^frozen<(.*)>$", Pattern.CASE_INSENSITIVE);

    private static final Comparator<String> UUID_COMPARATOR = Comparator.comparing(UUID::fromString);
    private static final Comparator<String> STRING_COMPARATOR = String::compareTo;
    private static final Comparator<Decimal> DECIMAL_COMPARATOR = Comparator.naturalOrder();
    private static final Comparator<Integer> INTEGER_COMPARATOR = Integer::compareTo;
    private static final Comparator<Long> LONG_COMPARATOR = Long::compareTo;
    private static final Comparator<Boolean> BOOLEAN_COMPARATOR = Boolean::compareTo;
    private static final Comparator<Float> FLOAT_COMPARATOR = Float::compareTo;
    private static final Comparator<Double> DOUBLE_COMPARATOR = Double::compareTo;
    private static final Comparator<Void> VOID_COMPARATOR_COMPARATOR = (o1, o2) -> 0;
    private static final Comparator<Short> SHORT_COMPARATOR = Short::compare;
    public static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR = UnsignedBytes.lexicographicalComparator();
    private static final Comparator<Byte> BYTE_COMPARATOR = CqlField::compareBytes;

    private static int compareBytes(final byte a, final byte b)
    {
        return a - b; // safe due to restricted range
    }

    public static final Set<NativeCql3Type> UNSUPPORTED_TYPES = new HashSet<>(Arrays.asList(NativeCql3Type.COUNTER, NativeCql3Type.DURATION, NativeCql3Type.EMPTY));

    public interface CqlType extends Serializable
    {
        enum InternalType
        {
            NativeCql, Set, List, Map, Frozen, Udt, Tuple;

            public static InternalType fromString(final String name)
            {
                switch (name.toLowerCase())
                {
                    case "set":
                        return Set;
                    case "list":
                        return List;
                    case "map":
                        return Map;
                    case "tuple":
                        return Tuple;
                    case "udt":
                        return Udt;
                    case "frozen":
                        return Frozen;
                    default:
                        return NativeCql;
                }
            }
        }

        InternalType internalType();

        String cqlName();

        void write(final Output output);

        Set<CqlUdt> udts();

        static CqlType read(final Input input)
        {
            final NativeCql3Type[] cqlTypes = CqlField.NativeCql3Type.values();
            final int type = input.read();
            final InternalType internalType = InternalType.values()[type];
            switch (internalType)
            {
                case NativeCql:
                    return cqlTypes[input.readInt()];
                case Set:
                case List:
                case Map:
                case Tuple:
                    return CqlCollection.read(internalType, input);
                case Frozen:
                    return CqlFrozen.build(CqlType.read(input));
                case Udt:
                    return CqlUdt.read(input);
                default:
                    throw new IllegalStateException("Unknown cql type, cannot deserialize: " + type);
            }
        }
    }

    public enum NativeCql3Type implements CqlType
    {
        ASCII, BIGINT, BLOB, BOOLEAN, COUNTER, DATE, DECIMAL, DOUBLE, DURATION, EMPTY, FLOAT, INET, INT, SMALLINT, TEXT, TIME, TIMESTAMP, TIMEUUID, TINYINT, UUID, VARCHAR, VARINT;

        public InternalType internalType()
        {
            return InternalType.NativeCql;
        }

        @Override
        public String cqlName()
        {
            return this.name().toLowerCase();
        }

        @Override
        public void write(Output output)
        {
            output.writeByte(internalType().ordinal());
            output.writeInt(this.ordinal());
        }

        public Set<CqlUdt> udts()
        {
            return Collections.emptySet();
        }

        @Override
        public String toString()
        {
            return this.cqlName();
        }
    }

    public static CqlList list(final CqlType type)
    {
        return new CqlList(type);
    }

    public static CqlSet set(final CqlType type)
    {
        return new CqlSet(type);
    }

    public static CqlMap map(final CqlType keyType, final CqlType valueType)
    {
        return new CqlMap(keyType, valueType);
    }

    public static CqlTuple tuple(final CqlType... types)
    {
        return new CqlTuple(types);
    }

    public static CqlType parseType(final String type)
    {
        return CqlField.parseType(type, Collections.emptyMap());
    }

    public static CqlType parseType(final String type, final Map<String, CqlUdt> udts)
    {
        if (StringUtils.isEmpty(type))
        {
            return null;
        }
        final Matcher matcher = CqlField.COLLECTIONS_PATTERN.matcher(type);
        if (matcher.find())
        {
            // cql collection
            final String[] types = splitInnerTypes(matcher.group(2));
            return CqlField.CqlCollection.build(matcher.group(1), Stream.of(types).map(t -> parseType(t, udts)).toArray(CqlType[]::new));
        }
        final Matcher frozenMatcher = CqlField.FROZEN_PATTERN.matcher(type);
        if (frozenMatcher.find())
        {
            // frozen collections
            return CqlFrozen.build(parseType(frozenMatcher.group(1), udts));
        }

        if (udts.containsKey(type))
        {
            // user defined type
            return udts.get(type);
        }

        // native cql 3 type
        return CqlField.NativeCql3Type.valueOf(type.toUpperCase());
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

    public static abstract class CqlCollection implements CqlType
    {
        public final List<CqlType> types;

        CqlCollection(CqlType type)
        {
            this(Collections.singletonList(type));
        }

        CqlCollection(CqlType... types)
        {
            this.types = Arrays.asList(types);
        }

        CqlCollection(List<CqlType> types)
        {
            this.types = new ArrayList<>(types);
        }

        public static CqlCollection build(final String name, final CqlField.CqlType... types)
        {
            return build(InternalType.fromString(name), types);
        }

        public static CqlCollection build(final InternalType internalType, final CqlField.CqlType... types)
        {
            if (types.length < 1 || types[0] == null)
            {
                throw new IllegalArgumentException("Collection type requires a non-null key data type");
            }

            switch (internalType)
            {
                case Set:
                    return set(types[0]);
                case List:
                    return list(types[0]);
                case Map:
                    if (types.length < 2 || types[1] == null)
                    {
                        throw new IllegalArgumentException("Map collection type requires a non-null value data type");
                    }
                    return map(types[0], types[1]);
                case Tuple:
                    return tuple(types);
                default:
                    throw new IllegalArgumentException("Unknown collection type: " + internalType);
            }
        }

        public int size()
        {
            return this.types.size();
        }

        public List<CqlType> types()
        {
            return this.types;
        }

        public CqlType type()
        {
            return type(0);
        }

        public CqlType type(int i)
        {
            return this.types.get(i);
        }

        public CqlFrozen frozen()
        {
            return CqlFrozen.build(this);
        }

        public String cqlName()
        {
            return String.format("%s<%s>", internalType().name().toLowerCase(), this.types.stream().map(CqlType::cqlName).collect(Collectors.joining(", ")));
        }

        @Override
        public Set<CqlUdt> udts()
        {
            return this.types.stream().map(CqlType::udts).flatMap(Collection::stream).collect(Collectors.toSet());
        }

        @Override
        public String toString()
        {
            return this.cqlName();
        }

        public static CqlCollection read(final InternalType internalType, final Input input)
        {
            final int numTypes = input.readInt();
            final CqlType[] types = new CqlType[numTypes];
            for (int i = 0; i < numTypes; i++)
            {
                types[i] = CqlType.read(input);
            }
            return CqlCollection.build(internalType, types);
        }

        @Override
        public void write(Output output)
        {
            output.writeByte(internalType().ordinal());
            output.writeInt(this.types.size());
            for (final CqlType type : this.types)
            {
                type.write(output);
            }
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder(29, 31)
                   .append(internalType().ordinal())
                   .append(this.types)
                   .toHashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (obj == this)
            {
                return true;
            }
            if (obj.getClass() != getClass())
            {
                return false;
            }

            final CqlCollection rhs = (CqlCollection) obj;
            return new EqualsBuilder()
                   .append(internalType(), rhs.internalType())
                   .append(this.types, rhs.types)
                   .isEquals();
        }
    }

    public static class CqlFrozen implements CqlType
    {
        private final CqlType inner;

        public CqlFrozen(final CqlType inner)
        {
            this.inner = inner;
        }

        public static CqlFrozen build(final CqlType inner)
        {
            return new CqlFrozen(inner);
        }

        public InternalType internalType()
        {
            return InternalType.Frozen;
        }

        public CqlType inner()
        {
            return inner;
        }

        public String cqlName()
        {
            return String.format("frozen<%s>", this.inner.cqlName());
        }

        @Override
        public Set<CqlUdt> udts()
        {
            return inner.udts();
        }

        @Override
        public void write(Output output)
        {
            output.writeByte(internalType().ordinal());
            inner.write(output);
        }

        @Override
        public String toString()
        {
            return this.cqlName();
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder(83, 89)
                   .append(internalType().ordinal())
                   .append(inner)
                   .toHashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (obj == this)
            {
                return true;
            }
            if (obj.getClass() != getClass())
            {
                return false;
            }

            final CqlFrozen rhs = (CqlFrozen) obj;
            return new EqualsBuilder()
                   .append(internalType(), rhs.internalType())
                   .append(this.inner, rhs.inner)
                   .isEquals();
        }
    }

    public static class CqlTuple extends CqlCollection
    {
        CqlTuple(final CqlType... types)
        {
            super(types);
        }

        @Override
        public InternalType internalType()
        {
            return InternalType.Tuple;
        }
    }

    public static class CqlSet extends CqlCollection
    {
        public CqlSet(CqlType type)
        {
            super(type);
        }

        @Override
        public InternalType internalType()
        {
            return InternalType.Set;
        }
    }

    public static class CqlList extends CqlCollection
    {
        public CqlList(final CqlType type)
        {
            super(type);
        }

        @Override
        public InternalType internalType()
        {
            return InternalType.List;
        }
    }

    public static class CqlMap extends CqlCollection
    {
        public CqlMap(final CqlType keyType, final CqlType valueType)
        {
            super(keyType, valueType);
        }

        public CqlType keyType()
        {
            return type();
        }

        public CqlType valueType()
        {
            return type(1);
        }

        @Override
        public InternalType internalType()
        {
            return InternalType.Map;
        }
    }

    public enum SortOrder
    {
        ASC, DESC
    }

    private final String name;
    private final boolean isPartitionKey, isClusteringColumn, isStaticColumn;
    private final CqlType type;
    private final int pos;

    public CqlField(final boolean isPartitionKey,
                    final boolean isClusteringColumn,
                    final boolean isStaticColumn,
                    final String name,
                    final CqlType type,
                    final int pos)
    {
        Preconditions.checkArgument(!(isPartitionKey && isClusteringColumn), "Field cannot be both partition key and clustering key");
        Preconditions.checkArgument(!(isPartitionKey && isStaticColumn), "Field cannot be both partition key and static column");
        Preconditions.checkArgument(!(isClusteringColumn && isStaticColumn), "Field cannot be both clustering key and static column");
        this.isPartitionKey = isPartitionKey;
        this.isClusteringColumn = isClusteringColumn;
        this.isStaticColumn = isStaticColumn;
        this.name = name.replaceAll("\"", "");
        this.type = type;
        this.pos = pos;
    }

    public boolean isPartitionKey()
    {
        return isPartitionKey;
    }

    public boolean isClusteringColumn()
    {
        return isClusteringColumn;
    }


    public boolean isStaticColumn()
    {
        return isStaticColumn;
    }

    public boolean isValueColumn()
    {
        return !isPartitionKey && !isClusteringColumn && !isStaticColumn;
    }

    public String name()
    {
        return name;
    }

    public CqlType type()
    {
        return type;
    }

    public String cqlTypeName()
    {
        return type.cqlName();
    }

    public int pos()
    {
        return pos;
    }

    @VisibleForTesting
    public CqlField cloneWithPos(final int pos)
    {
        return new CqlField(isPartitionKey, isClusteringColumn, isStaticColumn, name, type, pos);
    }

    @Override
    public String toString()
    {
        return name + " (" + type + ")";
    }

    @Override
    public int compareTo(@NotNull final CqlField o)
    {
        return Integer.compare(this.pos, o.pos);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(67, 71)
               .append(name)
               .append(isPartitionKey)
               .append(isClusteringColumn)
               .append(isStaticColumn)
               .append(type)
               .append(pos)
               .toHashCode();
    }

    @Override
    public boolean equals(final Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }
        if (obj.getClass() != getClass())
        {
            return false;
        }

        final CqlField rhs = (CqlField) obj;
        return new EqualsBuilder()
               .append(name, rhs.name)
               .append(isPartitionKey, rhs.isPartitionKey)
               .append(isClusteringColumn, rhs.isClusteringColumn)
               .append(isStaticColumn, rhs.isStaticColumn)
               .append(type, rhs.type)
               .append(pos, rhs.pos)
               .isEquals();
    }

    public int compare(final Object o1, final Object o2)
    {
        return CqlField.compare(type(), o1, o2);
    }

    public static int compare(final CqlType type, final Object o1, final Object o2)
    {
        switch (type.internalType())
        {
            case NativeCql:
                return CqlField.compare((NativeCql3Type) type, o1, o2);
            case Frozen:
                return CqlField.compare(((CqlFrozen) type).inner(), o1, o2);
            case Udt:
                return CqlField.compareArrays(((GenericInternalRow) o1).values(), ((GenericInternalRow) o2).values(), (pos) -> ((CqlUdt) type).field(pos).type());
            case Tuple:
                return CqlField.compareArrays(((GenericInternalRow) o1).values(), ((GenericInternalRow) o2).values(), ((CqlTuple) type)::type);
            case Set:
            case List:
                return compareArrays(((GenericArrayData) o1).array(), ((GenericArrayData) o2).array(), (pos) -> ((CqlCollection) type).type());
            case Map:
                return compareArrays(((MapData) o1).valueArray().array(), ((MapData) o2).valueArray().array(), (pos) -> ((CqlMap) type).valueType());
            default:
                throw new UnsupportedOperationException("Comparator for " + type.toString() + " type not implemented yet");
        }
    }

    private static int compareArrays(final Object[] lhs, final Object[] rhs, final Function<Integer, CqlType> types) {
        for (int pos = 0; pos < Math.min(lhs.length, rhs.length); pos++)
        {
            final int c = CqlField.compare(types.apply(pos), lhs[pos], rhs[pos]);
            if (c != 0)
            {
                return c;
            }
        }
        return Integer.compare(lhs.length, rhs.length);
    }

    private static int compare(final NativeCql3Type type, final Object o1, final Object o2)
    {
        if (o1 == null || o2 == null)
        {
            return o1 == o2 ? 0 : (o1 == null ? -1 : 1);
        }
        switch (type)
        {
            case TIMEUUID:
            case UUID:
                return UUID_COMPARATOR.compare(o1.toString(), o2.toString());
            case ASCII:
            case VARCHAR:
            case TEXT:
                return STRING_COMPARATOR.compare(o1.toString(), o2.toString());
            case VARINT:
            case DECIMAL:
                return DECIMAL_COMPARATOR.compare((Decimal) o1, (Decimal) o2);
            case INT:
            case DATE:
                return INTEGER_COMPARATOR.compare((Integer) o1, (Integer) o2);
            case BIGINT:
            case TIME:
            case TIMESTAMP:
                return LONG_COMPARATOR.compare((Long) o1, (Long) o2);
            case BOOLEAN:
                return BOOLEAN_COMPARATOR.compare((Boolean) o1, (Boolean) o2);
            case FLOAT:
                return FLOAT_COMPARATOR.compare((Float) o1, (Float) o2);
            case DOUBLE:
                return DOUBLE_COMPARATOR.compare((Double) o1, (Double) o2);
            case SMALLINT:
                return SHORT_COMPARATOR.compare((Short) o1, (Short) o2);
            case BLOB:
            case INET:
                return BYTE_ARRAY_COMPARATOR.compare((byte[]) o1, (byte[]) o2);
            case TINYINT:
                return BYTE_COMPARATOR.compare((Byte) o1, (Byte) o2);
            case EMPTY:
                return VOID_COMPARATOR_COMPARATOR.compare((Void) o1, (Void) o2);
            default:
                throw new UnsupportedOperationException("Comparator for " + type.toString() + " type not implemented yet");
        }
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CqlField>
    {
        @Override
        public CqlField read(final Kryo kryo, final Input input, final Class type)
        {
            return new CqlField(input.readBoolean(),
                                input.readBoolean(),
                                input.readBoolean(),
                                input.readString(),
                                CqlType.read(input),
                                input.readInt());
        }

        @Override
        public void write(final Kryo kryo, final Output output, final CqlField field)
        {
            output.writeBoolean(field.isPartitionKey());
            output.writeBoolean(field.isClusteringColumn());
            output.writeBoolean(field.isStaticColumn());
            output.writeString(field.name());
            field.type().write(output);
            output.writeInt(field.pos());
        }
    }
}