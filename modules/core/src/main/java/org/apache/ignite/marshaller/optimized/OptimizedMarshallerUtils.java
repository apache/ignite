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

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.jdk8.backport.*;
import sun.misc.*;

import java.io.*;
import java.lang.reflect.*;
import java.nio.charset.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Miscellaneous utility methods to facilitate {@link OptimizedMarshaller}.
 */
class OptimizedMarshallerUtils {
    /** Unsafe. */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** {@code Null} object reference. */
    static final byte NULL = (byte)0x70;

    /** Handle reference. */
    static final byte HANDLE = (byte)0x71;

    /** Object reference. */
    static final byte OBJECT = (byte)0x72;

    /** UTF-8 character name. */
    static final Charset UTF_8 = Charset.forName("UTF-8");

    /** Predefined classes. */
    private static final Class[] PREDEFINED = new Class[] {
        byte[].class,
        Boolean.class,
        Integer.class,
        String.class,
        UUID.class,
        ArrayList.class,
        LinkedList.class,
        HashSet.class,
        HashMap.class,

        GridDhtPartitionMap.class,
        GridDhtPartitionFullMap.class,
        GridCacheMvccCandidate.class,
        GridCacheVersion.class,
        IgniteTxEntry.class,
        IgnitePredicate[].class,
        IgniteExternalizableExpiryPolicy.class,
        IgniteTxKey.class,
        GridCacheReturn.class,
        GridTuple4.class,
        GridCacheEntryInfo.class,
        GridLeanMap.class
    };

    /** Predefined class names. */
    private static final String[] PREDEFINED_NAMES = new String[] {
        "org.apache.ignite.internal.GridTopic$T7",
        "org.apache.ignite.internal.util.lang.GridFunc$37",
        "org.apache.ignite.internal.util.GridLeanMap$Map3"
    };

    /** Class descriptors by class. */
    private static final ConcurrentMap<Class, OptimizedClassDescriptor> DESC_BY_CLS = new ConcurrentHashMap8<>(256);

    /** Classes by ID. */
    private static final ConcurrentMap<Integer, IgniteBiTuple<Class, Boolean>> CLS_BY_ID =
        new ConcurrentHashMap8<>(256);

    static {
        for (Class cls : PREDEFINED)
            CLS_BY_ID.put(cls.getName().hashCode(), F.t(cls, true));

        try {
            for (String clsName : PREDEFINED_NAMES) {
                Class cls = U.forName(clsName, OptimizedMarshallerUtils.class.getClassLoader());

                CLS_BY_ID.put(cls.getName().hashCode(), F.t(cls, true));
            }
        }
        catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     */
    private OptimizedMarshallerUtils() {
        // No-op.
    }

    /**
     * Gets descriptor for provided class.
     *
     * @param cls Class.
     * @param ctx Context.
     * @return Descriptor.
     * @throws IOException In case of error.
     */
    static OptimizedClassDescriptor classDescriptor(Class cls, MarshallerContext ctx) throws IOException {
        OptimizedClassDescriptor desc = DESC_BY_CLS.get(cls);

        if (desc == null) {
            desc = new OptimizedClassDescriptor(cls, ctx);

            if (CLS_BY_ID.putIfAbsent(desc.typeId(), F.t(cls, false)) == null)
                ctx.registerClass(desc.typeId(), cls.getName());

            OptimizedClassDescriptor old = DESC_BY_CLS.putIfAbsent(cls, desc);

            if (old != null)
                desc = old;
        }

        return desc;
    }

    /**
     * Gets descriptor for provided ID.
     *
     * @param id ID.
     * @param ldr Class loader.
     * @param ctx Context.
     * @return Descriptor.
     * @throws IOException In case of error.
     * @throws ClassNotFoundException If class was not found.
     */
    static OptimizedClassDescriptor classDescriptor(int id, ClassLoader ldr, MarshallerContext ctx)
        throws IOException, ClassNotFoundException {
        Class cls = CLS_BY_ID.get(id).get1();

        if (cls == null) {
            String clsName = ctx.className(id);

            assert clsName != null : id;

            cls = U.forName(clsName, ldr);

            IgniteBiTuple<Class, Boolean> old = CLS_BY_ID.putIfAbsent(id, F.t(cls, false));

            if (old != null)
                cls = old.get1();
        }

        return classDescriptor(cls, ctx);
    }

    /**
     * Undeployment callback.
     *
     * @param ldr Undeployed class loader.
     */
    public static void onUndeploy(ClassLoader ldr) {
        for (Class<?> cls : DESC_BY_CLS.keySet()) {
            if (ldr.equals(cls.getClassLoader()))
                DESC_BY_CLS.remove(cls);
        }

        for (Map.Entry<Integer, IgniteBiTuple<Class, Boolean>> e : CLS_BY_ID.entrySet()) {
            if (!e.getValue().get2() && ldr.equals(e.getValue().get1().getClassLoader()))
                CLS_BY_ID.remove(e.getKey());
        }
    }

    /**
     * Intended for test purposes only.
     */
    public static void clearCache() {
        DESC_BY_CLS.clear();

        for (Map.Entry<Integer, IgniteBiTuple<Class, Boolean>> e : CLS_BY_ID.entrySet()) {
            if (!e.getValue().get2())
                CLS_BY_ID.remove(e.getKey());
        }
    }

    /**
     * Computes the serial version UID value for the given class.
     * The code is taken from {@link ObjectStreamClass#computeDefaultSUID(Class)}.
     *
     * @param cls A class.
     * @param fields Fields.
     * @return A serial version UID.
     * @throws IOException If failed.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    static short computeSerialVersionUid(Class cls, List<Field> fields) throws IOException {
        if (Serializable.class.isAssignableFrom(cls) && !Enum.class.isAssignableFrom(cls))
            return (short)ObjectStreamClass.lookup(cls).getSerialVersionUID();

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
        return UNSAFE.getByte(obj, off);
    }

    /**
     * Sets byte field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setByte(Object obj, long off, byte val) {
        UNSAFE.putByte(obj, off, val);
    }

    /**
     * Gets short field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Short value.
     */
    static short getShort(Object obj, long off) {
        return UNSAFE.getShort(obj, off);
    }

    /**
     * Sets short field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setShort(Object obj, long off, short val) {
        UNSAFE.putShort(obj, off, val);
    }

    /**
     * Gets integer field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Integer value.
     */
    static int getInt(Object obj, long off) {
        return UNSAFE.getInt(obj, off);
    }

    /**
     * Sets integer field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setInt(Object obj, long off, int val) {
        UNSAFE.putInt(obj, off, val);
    }

    /**
     * Gets long field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Long value.
     */
    static long getLong(Object obj, long off) {
        return UNSAFE.getLong(obj, off);
    }

    /**
     * Sets long field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setLong(Object obj, long off, long val) {
        UNSAFE.putLong(obj, off, val);
    }

    /**
     * Gets float field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Float value.
     */
    static float getFloat(Object obj, long off) {
        return UNSAFE.getFloat(obj, off);
    }

    /**
     * Sets float field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setFloat(Object obj, long off, float val) {
        UNSAFE.putFloat(obj, off, val);
    }

    /**
     * Gets double field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Double value.
     */
    static double getDouble(Object obj, long off) {
        return UNSAFE.getDouble(obj, off);
    }

    /**
     * Sets double field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setDouble(Object obj, long off, double val) {
        UNSAFE.putDouble(obj, off, val);
    }

    /**
     * Gets char field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Char value.
     */
    static char getChar(Object obj, long off) {
        return UNSAFE.getChar(obj, off);
    }

    /**
     * Sets char field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setChar(Object obj, long off, char val) {
        UNSAFE.putChar(obj, off, val);
    }

    /**
     * Gets boolean field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Boolean value.
     */
    static boolean getBoolean(Object obj, long off) {
        return UNSAFE.getBoolean(obj, off);
    }

    /**
     * Sets boolean field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setBoolean(Object obj, long off, boolean val) {
        UNSAFE.putBoolean(obj, off, val);
    }

    /**
     * Gets field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Value.
     */
    static Object getObject(Object obj, long off) {
        return UNSAFE.getObject(obj, off);
    }

    /**
     * Sets field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setObject(Object obj, long off, Object val) {
        UNSAFE.putObject(obj, off, val);
    }
}
