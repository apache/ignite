package de.kp.works.janus;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dr. Krusche & Partner PartG ("Confidential Information").
 *
 * You shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement you
 * entered into with Dr. Krusche & Partner PartG.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.graphdb.database.serialize.DataOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class KeyValueUtil {

    static IgniteSerializer serializer = new IgniteSerializer();

    public static String serialize(Object obj) {

        String serialized = "";
        try {
            ByteArrayOutputStream boas = new ByteArrayOutputStream();

            ObjectOutputStream so = new ObjectOutputStream(boas);
            so.writeObject(obj);
            so.flush();

            serialized = boas.toString();

        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }

        return serialized;
    }

    public static Object deserialize(String serialized) {

        Object obj = null;
        try {

            byte bytes[] = serialized.getBytes();
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

            ObjectInputStream si = new ObjectInputStream(bais);
            obj = si.readObject();

        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }

        return obj;
    }

    public static StaticBuffer toBuffer(long value) {

        byte[] bytes = long2Bytes(value);
        return new StaticArrayBuffer(bytes);

    }

    public static Long fromBuffer(StaticBuffer buffer) {

        ByteBuffer bytes = buffer.asByteBuffer();
        return bytes.getLong();

    }

    public static byte[] long2Bytes(long value) {
        return new byte[] {
                (byte) (value >> 56),
                (byte) (value >> 48),
                (byte) (value >> 40),
                (byte) (value >> 32),
                (byte) (value >> 24),
                (byte) (value >> 16),
                (byte) (value >> 8),
                (byte) value
        };
    }

    /**
     * Extracted from KCVSConfiguration, which is responsible
     * for persisting the configuration
     */
    public static StaticBuffer string2StaticBuffer(final String s) {
        ByteBuffer out = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
        return StaticArrayBuffer.of(out);
    }

    public static String staticBuffer2String(final StaticBuffer s) {
        return new String(s.as(StaticBuffer.ARRAY_FACTORY), StandardCharsets.UTF_8);
    }

    public static<O> StaticBuffer object2StaticBuffer(final O value) {

        if (value==null) throw Graph.Variables.Exceptions.variableValueCanNotBeNull();

        if (!serializer.validDataType(value.getClass()))
            throw Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(value);

        DataOutput out = serializer.getDataOutput(128);
        out.writeClassAndObject(value);

        return out.getStaticBuffer();
    }
    /**
     * This implies that we must know the data type
     * of the provided object explicitly
     */
    @SuppressWarnings("unchecked")
    public static<O> O staticBuffer2Object(final StaticBuffer s, Class<O> dataType) {

        Object value = serializer.readClassAndObject(s.asReadBuffer());
        Preconditions.checkArgument(dataType.isInstance(value),"Could not deserialize to [%s], got: %s",dataType,value);
        return (O)value;
    }

    public static Object staticBuffer2Object(final StaticBuffer s) {
        Object value = serializer.readClassAndObject(s.asReadBuffer());
        return value;

    }
}

