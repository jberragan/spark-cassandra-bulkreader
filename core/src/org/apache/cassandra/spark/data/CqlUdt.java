package org.apache.cassandra.spark.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class CqlUdt implements CqlField.CqlType
{
    private final String keyspace, name;
    private final List<CqlField> fields;
    private final Map<String, CqlField> fieldMap;

    CqlUdt(final String keyspace, final String name, final List<CqlField> fields)
    {
        this.keyspace = keyspace;
        this.name = name;
        this.fields = Collections.unmodifiableList(fields);
        this.fieldMap = this.fields.stream().collect(Collectors.toMap(CqlField::name, Function.identity()));
    }

    @Override
    public Set<CqlUdt> udts()
    {
        final Set<CqlUdt> udts = fields.stream().map(CqlField::type).map(CqlField.CqlType::udts).flatMap(Collection::stream).collect(Collectors.toSet());
        udts.add(this);
        return udts;
    }

    @Override
    public String toString()
    {
        return this.cqlName();
    }

    public CqlField.CqlFrozen frozen()
    {
        return CqlField.CqlFrozen.build(this);
    }

    public static Builder builder(final String keyspace, final String name)
    {
        return new Builder(keyspace, name);
    }

    public static class Builder
    {
        private final String keyspace, name;
        private final List<CqlField> fields = new ArrayList<>();

        public Builder(final String keyspace, final String name)
        {
            this.keyspace = keyspace;
            this.name = name;
        }

        public Builder withField(final String name, final CqlField.CqlType type)
        {
            this.fields.add(new CqlField(false, false, false, name, type, this.fields.size()));
            return this;
        }

        public CqlUdt build()
        {
            return new CqlUdt(keyspace, name, fields);
        }
    }

    public InternalType internalType()
    {
        return InternalType.Udt;
    }

    public String createStmt(final String keyspace)
    {
        return String.format("CREATE TYPE %s.%s (%s);", keyspace, name, fieldsString());
    }

    private String fieldsString()
    {
        return fields.stream().map(CqlUdt::fieldString).collect(Collectors.joining(", "));
    }

    private static String fieldString(final CqlField field)
    {
        return String.format("%s %s", field.name(), field.type().cqlName());
    }

    public String keyspace()
    {
        return keyspace;
    }

    public String name()
    {
        return name;
    }

    public int size()
    {
        return fields.size();
    }

    public List<CqlField> fields()
    {
        return fields;
    }

    public CqlField field(String name)
    {
        return fieldMap.get(name);
    }

    public CqlField field(int i)
    {
        return fields.get(i);
    }

    public String cqlName()
    {
        return name;
    }

    public static CqlUdt read(final Input input)
    {
        final Builder builder = CqlUdt.builder(input.readString(), input.readString());
        final int numFields = input.readInt();
        for (int i = 0; i < numFields; i++)
        {
            builder.withField(input.readString(), CqlField.CqlType.read(input));
        }
        return builder.build();
    }

    @Override
    public void write(Output output)
    {
        output.writeByte(internalType().ordinal());
        output.writeString(this.keyspace);
        output.writeString(this.name);
        output.writeInt(this.fields.size());
        for (final CqlField field : this.fields)
        {
            output.writeString(field.name());
            field.type().write(output);
        }
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(89, 97)
               .append(internalType().ordinal())
               .append(this.keyspace)
               .append(this.name)
               .append(this.fields)
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

        final CqlUdt rhs = (CqlUdt) obj;
        return new EqualsBuilder()
               .append(internalType(), rhs.internalType())
               .append(this.keyspace, rhs.keyspace)
               .append(this.name, rhs.name)
               .append(this.fields, rhs.fields)
               .isEquals();
    }

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<CqlUdt>
    {
        @Override
        public CqlUdt read(final Kryo kryo, final Input input, final Class type)
        {
            final String keyspace = input.readString();
            final String name = input.readString();
            final int numFields = input.readInt();
            final List<CqlField> fields = new ArrayList<>(numFields);
            for (int i = 0; i < numFields; i++)
            {
                fields.add(kryo.readObject(input, CqlField.class));
            }
            return new CqlUdt(keyspace, name, fields);
        }

        @Override
        public void write(final Kryo kryo, final Output output, final CqlUdt udt)
        {
            output.writeString(udt.keyspace());
            output.writeString(udt.name());
            output.writeInt(udt.fields().size());
            for (final CqlField field : udt.fields())
            {
                kryo.writeObject(output, field);
            }
        }
    }
}
