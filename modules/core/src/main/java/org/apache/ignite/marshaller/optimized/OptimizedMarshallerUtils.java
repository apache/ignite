/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.marshaller.optimized;

import java.io.IOException;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

/**
 * Miscellaneous utility methods to facilitate {@link OptimizedMarshaller}.
 */
class OptimizedMarshallerUtils {
    /** */
    static final long HASH_SET_MAP_OFF;

    /** */
    static final byte JDK = -2;

    /** */
    static final byte HANDLE = -1;

    /** */
    static final byte NULL = 0;

    /** */
    static final byte BYTE = 1;

    /** */
    static final byte SHORT = 2;

    /** */
    static final byte INT = 3;

    /** */
    static final byte LONG = 4;

    /** */
    static final byte FLOAT = 5;

    /** */
    static final byte DOUBLE = 6;

    /** */
    static final byte CHAR = 7;

    /** */
    static final byte BOOLEAN = 8;

    /** */
    static final byte BYTE_ARR = 9;

    /** */
    static final byte SHORT_ARR = 10;

    /** */
    static final byte INT_ARR = 11;

    /** */
    static final byte LONG_ARR = 12;

    /** */
    static final byte FLOAT_ARR = 13;

    /** */
    static final byte DOUBLE_ARR = 14;

    /** */
    static final byte CHAR_ARR = 15;

    /** */
    static final byte BOOLEAN_ARR = 16;

    /** */
    static final byte OBJ_ARR = 17;

    /** */
    static final byte STR = 18;

    /** */
    static final byte UUID = 19;

    /** */
    static final byte PROPS = 20;

    /** */
    static final byte ARRAY_LIST = 21;

    /** */
    static final byte HASH_MAP = 22;

    /** */
    static final byte HASH_SET = 23;

    /** */
    static final byte LINKED_LIST = 24;

    /** */
    static final byte LINKED_HASH_MAP = 25;

    /** */
    static final byte LINKED_HASH_SET = 26;

    /** */
    static final byte DATE = 27;

    /** */
    static final byte CLS = 28;

    /** */
    static final byte PROXY = 29;

    /** */
    static final byte ENUM = 100;

    /** */
    static final byte EXTERNALIZABLE = 101;

    /** */
    static final byte SERIALIZABLE = 102;

    /** UTF-8 character name. */
    static final Charset UTF_8 = Charset.forName("UTF-8");

    /** JDK marshaller. */
    static final JdkMarshaller JDK_MARSH = new JdkMarshaller();

    static {
        long mapOff;

        try {
            mapOff = GridUnsafe.objectFieldOffset(HashSet.class.getDeclaredField("map"));
        }
        catch (NoSuchFieldException ignored) {
            try {
                // Workaround for legacy IBM JRE.
                mapOff = GridUnsafe.objectFieldOffset(HashSet.class.getDeclaredField("backingMap"));
            }
            catch (NoSuchFieldException e2) {
                throw new IgniteException("Initialization failure.", e2);
            }
        }

        HASH_SET_MAP_OFF = mapOff;
    }

    /**
     */
    private OptimizedMarshallerUtils() {
        // No-op.
    }

    /**
     * Gets descriptor for provided class.
     *
     * @param clsMap Class descriptors by class map.
     * @param cls Class.
     * @param ctx Context.
     * @param mapper ID mapper.
     * @return Descriptor.
     * @throws IOException In case of error.
     */
    static OptimizedClassDescriptor classDescriptor(
        ConcurrentMap<Class, OptimizedClassDescriptor> clsMap,
        Class cls,
        MarshallerContext ctx,
        OptimizedMarshallerIdMapper mapper)
        throws IOException
    {
        OptimizedClassDescriptor desc = clsMap.get(cls);

        if (desc == null) {
            int typeId = resolveTypeId(cls.getName(), mapper);

            boolean registered;

            try {
                registered = ctx.registerClass(typeId, cls);
            }
            catch (IgniteCheckedException e) {
                throw new IOException("Failed to register class: " + cls.getName(), e);
            }

            desc = new OptimizedClassDescriptor(cls, registered ? typeId : 0, clsMap, ctx, mapper);

            if (registered) {
                OptimizedClassDescriptor old = clsMap.putIfAbsent(cls, desc);

                if (old != null)
                    desc = old;
            }
        }

        return desc;
    }

    /**
     * @param clsName Class name.
     * @param mapper Mapper.
     * @return Type ID.
     */
    private static int resolveTypeId(String clsName, OptimizedMarshallerIdMapper mapper) {
        int typeId;

        if (mapper != null) {
            typeId = mapper.typeId(clsName);

            if (typeId == 0)
                typeId = clsName.hashCode();
        }
        else
            typeId = clsName.hashCode();

        return typeId;
    }

    /**
     * Gets descriptor for provided ID.
     *
     * @param clsMap Class descriptors by class map.
     * @param id ID.
     * @param ldr Class loader.
     * @param ctx Context.
     * @param mapper ID mapper.
     * @return Descriptor.
     * @throws IOException In case of error.
     * @throws ClassNotFoundException If class was not found.
     */
    static OptimizedClassDescriptor classDescriptor(
        ConcurrentMap<Class, OptimizedClassDescriptor> clsMap,
        int id,
        ClassLoader ldr,
        MarshallerContext ctx,
        OptimizedMarshallerIdMapper mapper) throws IOException, ClassNotFoundException {
        Class cls;

        try {
            cls = ctx.getClass(id, ldr);
        }
        catch (IgniteCheckedException e) {
            throw new IOException("Failed to resolve class for ID: " + id, e);
        }

        OptimizedClassDescriptor desc = clsMap.get(cls);

        if (desc == null) {
            OptimizedClassDescriptor old = clsMap.putIfAbsent(cls, desc =
                new OptimizedClassDescriptor(cls, resolveTypeId(cls.getName(), mapper), clsMap, ctx, mapper));

            if (old != null)
                desc = old;
        }

        return desc;
    }

    /**
     * Computes the serial version UID value for the given class. The code is taken from {@link
     * ObjectStreamClass#computeDefaultSUID(Class)}.
     *
     * @param cls A class.
     * @param fields Fields.
     * @return A serial version UID.
     * @throws IOException If failed.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    static short computeSerialVersionUid(Class cls, List<Field> fields) throws IOException {
        if (Serializable.class.isAssignableFrom(cls) && !Enum.class.isAssignableFrom(cls)) {
            try {
                Field field = cls.getDeclaredField("serialVersionUID");

                if (field.getType() == long.class) {
                    int mod = field.getModifiers();

                    if (Modifier.isStatic(mod) && Modifier.isFinal(mod)) {
                        field.setAccessible(true);

                        return (short)field.getLong(null);
                    }
                }
            }
            catch (NoSuchFieldException ignored) {
                // No-op.
            }
            catch (IllegalAccessException e) {
                throw new IOException(e);
            }

            if (OptimizedMarshaller.USE_DFLT_SUID)
                return (short)ObjectStreamClass.lookup(cls).getSerialVersionUID();
        }

        MessageDigest md;

        try {
            md = MessageDigest.getInstance("SHA");
        }
        catch (NoSuchAlgorithmException e) {
            throw new IOException("Failed to get digest for SHA.", e);
        }

        md.update(cls.getName().getBytes(UTF_8));

        if (!F.isEmpty(fields)) {
            for (int i = 0; i < fields.size(); i++) {
                Field f = fields.get(i);

                md.update(f.getName().getBytes(UTF_8));
                md.update(f.getType().getName().getBytes(UTF_8));
            }
        }

        byte[] hashBytes = md.digest();

        long hash = 0;

        // Composes a single-long hash from the byte[] hash.
        for (int i = Math.min(hashBytes.length, 8) - 1; i >= 0; i--)
            hash = (hash << 8) | (hashBytes[i] & 0xFF);

        return (short)hash;
    }

    /**
     * Gets byte field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Byte value.
     */
    static byte getByte(Object obj, long off) {
        return GridUnsafe.getByteField(obj, off);
    }

    /**
     * Sets byte field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setByte(Object obj, long off, byte val) {
        GridUnsafe.putByteField(obj, off, val);
    }

    /**
     * Gets short field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Short value.
     */
    static short getShort(Object obj, long off) {
        return GridUnsafe.getShortField(obj, off);
    }

    /**
     * Sets short field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setShort(Object obj, long off, short val) {
        GridUnsafe.putShortField(obj, off, val);
    }

    /**
     * Gets integer field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Integer value.
     */
    static int getInt(Object obj, long off) {
        return GridUnsafe.getIntField(obj, off);
    }

    /**
     * Sets integer field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setInt(Object obj, long off, int val) {
        GridUnsafe.putIntField(obj, off, val);
    }

    /**
     * Gets long field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Long value.
     */
    static long getLong(Object obj, long off) {
        return GridUnsafe.getLongField(obj, off);
    }

    /**
     * Sets long field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setLong(Object obj, long off, long val) {
        GridUnsafe.putLongField(obj, off, val);
    }

    /**
     * Gets float field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Float value.
     */
    static float getFloat(Object obj, long off) {
        return GridUnsafe.getFloatField(obj, off);
    }

    /**
     * Sets float field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setFloat(Object obj, long off, float val) {
        GridUnsafe.putFloatField(obj, off, val);
    }

    /**
     * Gets double field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Double value.
     */
    static double getDouble(Object obj, long off) {
        return GridUnsafe.getDoubleField(obj, off);
    }

    /**
     * Sets double field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setDouble(Object obj, long off, double val) {
        GridUnsafe.putDoubleField(obj, off, val);
    }

    /**
     * Gets char field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Char value.
     */
    static char getChar(Object obj, long off) {
        return GridUnsafe.getCharField(obj, off);
    }

    /**
     * Sets char field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setChar(Object obj, long off, char val) {
        GridUnsafe.putCharField(obj, off, val);
    }

    /**
     * Gets boolean field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Boolean value.
     */
    static boolean getBoolean(Object obj, long off) {
        return GridUnsafe.getBooleanField(obj, off);
    }

    /**
     * Sets boolean field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setBoolean(Object obj, long off, boolean val) {
        GridUnsafe.putBooleanField(obj, off, val);
    }

    /**
     * Gets field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Value.
     */
    static Object getObject(Object obj, long off) {
        return GridUnsafe.getObjectField(obj, off);
    }

    /**
     * Sets field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setObject(Object obj, long off, Object val) {
        GridUnsafe.putObjectField(obj, off, val);
    }
}
