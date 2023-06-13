package org.apache.cassandra.spark.utils;

import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoUtils
{
    public static <T> byte[] serializeToBytes(final Kryo kryo,
                                              final T obj,
                                              final Serializer<T> serializer)
    {
        try (final Output out = serialize(kryo, obj, serializer))
        {
            return out.getBuffer();
        }
    }

    public static <T> Output serialize(final Kryo kryo,
                                       final Object obj,
                                       Serializer<T> serializer)
    {
        try (final Output out = new Output(1024, -1))
        {
            kryo.writeObject(out, obj, serializer);
            return out;
        }
    }

    public static <T> byte[] serializeToBytes(final Kryo kryo,
                                              final T obj)
    {
        try (final Output out = serialize(kryo, obj))
        {
            return out.getBuffer();
        }
    }

    public static <T> Output serialize(final Kryo kryo,
                                       final Object obj)
    {
        try (final Output out = new Output(1024, -1))
        {
            kryo.writeObject(out, obj);
            return out;
        }
    }

    public static <T> T deserialize(final Kryo kryo,
                                    final byte[] ar,
                                    final Class<T> type)
    {
        try (final Input in = new Input(ar, 0, ar.length))
        {
            return kryo.readObject(in, type);
        }
    }

    public static <T> T deserialize(final Kryo kryo,
                                    final Output out,
                                    final Class<T> type)
    {
        try (final Input in = new Input(out.getBuffer(), 0, (int) out.total()))
        {
            return kryo.readObject(in, type);
        }
    }

    public static <T> T deserialize(final Kryo kryo,
                                    final ByteBuffer buf,
                                    final Class<T> type,
                                    Serializer<T> serializer)
    {
        final byte[] ar = new byte[buf.remaining()];
        buf.get(ar);
        return deserialize(kryo, ar, type, serializer);
    }

    public static <T> T deserialize(final Kryo kryo,
                                    final byte[] ar,
                                    final Class<T> type,
                                    Serializer<T> serializer)
    {
        try (final Input in = new Input(ar, 0, ar.length))
        {
            return kryo.readObject(in, type, serializer);
        }
    }
}