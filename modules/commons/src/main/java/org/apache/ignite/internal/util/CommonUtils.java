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

package org.apache.ignite.internal.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Collection of utility methods used in 'ignite-commons' and throughout the system.
 */
public abstract class CommonUtils {
    /**
     * The maximum size of array to allocate.
     * Some VMs reserve some header words in an array.
     * Attempts to allocate larger arrays may result in
     * OutOfMemoryError: Requested array size exceeds VM limit
     * @see java.util.ArrayList#MAX_ARRAY_SIZE
     */
    public static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    /** Sun-specific JDK constructor factory for objects that don't have empty constructor. */
    private static final Method CTOR_FACTORY;

    /** Sun JDK reflection factory. */
    private static final Object SUN_REFLECT_FACTORY;

    /** Public {@code java.lang.Object} no-argument constructor. */
    private static final Constructor OBJECT_CTOR;

    /** System line separator. */
    static final String NL = System.getProperty("line.separator");

    static {
        try {
            OBJECT_CTOR = Object.class.getConstructor();
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError("Object class does not have empty constructor (is JDK corrupted?).", e);
        }

        // Constructor factory.
        Method ctorFac = null;
        Object refFac = null;

        try {
            Class<?> refFactoryCls = Class.forName("sun.reflect.ReflectionFactory");

            refFac = refFactoryCls.getMethod("getReflectionFactory").invoke(null);

            ctorFac = refFac.getClass().getMethod("newConstructorForSerialization", Class.class,
                Constructor.class);
        }
        catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InvocationTargetException ignored) {
            // No-op.
        }

        CTOR_FACTORY = ctorFac;
        SUN_REFLECT_FACTORY = refFac;
    }

    /**
     * Creates new instance of a class even if it does not have public constructor.
     *
     * @param cls Class to instantiate.
     * @return New instance of the class or {@code null} if empty constructor could not be assigned.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static <T> T forceNewInstance(Class<?> cls) throws IgniteCheckedException {
        Constructor ctor = forceEmptyConstructor(cls);

        if (ctor == null)
            return null;

        boolean set = false;

        try {

            if (!ctor.isAccessible()) {
                ctor.setAccessible(true);

                set = true;
            }

            return (T)ctor.newInstance();
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IgniteCheckedException("Failed to create new instance for class: " + cls, e);
        }
        finally {
            if (set)
                ctor.setAccessible(false);
        }
    }

    /**
     * Gets empty constructor for class even if the class does not have empty constructor
     * declared. This method is guaranteed to work with SUN JDK and other JDKs still need
     * to be tested.
     *
     * @param cls Class to get empty constructor for.
     * @return Empty constructor if one could be found or {@code null} otherwise.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static Constructor<?> forceEmptyConstructor(Class<?> cls) throws IgniteCheckedException {
        Constructor<?> ctor = null;

        try {
            return cls.getDeclaredConstructor();
        }
        catch (Exception ignore) {
            Method ctorFac = CTOR_FACTORY;
            Object sunRefFac = sunReflectionFactory();

            if (ctorFac != null && sunRefFac != null)
                try {
                    ctor = (Constructor)ctorFac.invoke(sunRefFac, cls, OBJECT_CTOR);
                }
                catch (IllegalAccessException | InvocationTargetException e) {
                    throw new IgniteCheckedException("Failed to get object constructor for class: " + cls, e);
                }
        }

        return ctor;
    }

    /**
     * SUN JDK specific reflection factory for objects without public constructor.
     *
     * @return Reflection factory for objects without public constructor.
     */
    @Nullable public static Object sunReflectionFactory() {
        return SUN_REFLECT_FACTORY;
    }

    /**
     * Returns a capacity that is sufficient to keep the map from being resized as
     * long as it grows no larger than expSize and the load factor is >= its
     * default (0.75).
     *
     * Copy pasted from guava. See com.google.common.collect.Maps#capacity(int)
     *
     * @param expSize Expected size of created map.
     * @return Capacity.
     */
    public static int capacity(int expSize) {
        if (expSize < 3)
            return expSize + 1;

        if (expSize < (1 << 30))
            return expSize + expSize / 3;

        return Integer.MAX_VALUE; // any large value
    }

    /**
     * @return {@code line.separator} system property.
     */
    public static String nl() {
        return NL;
    }

    /**
     * Converts primitive {@code long} type to byte array.
     *
     * @param l Long value.
     * @return Array of bytes.
     */
    public static byte[] longToBytes(long l) {
        return toBytes(l, new byte[8], 0, 8);
    }

    /**
     * Converts primitive {@code long} type to byte array and stores it in specified
     * byte array.
     *
     * @param l Long value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int longToBytes(long l, byte[] bytes, int off) {
        toBytes(l, bytes, off, 8);

        return off + 8;
    }

    /**
     * Converts primitive {@code int} type to byte array.
     *
     * @param i Integer value.
     * @return Array of bytes.
     */
    public static byte[] intToBytes(int i) {
        return toBytes(i, new byte[4], 0, 4);
    }

    /**
     * Converts primitive {@code int} type to byte array and stores it in specified
     * byte array.
     *
     * @param i Integer value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int intToBytes(int i, byte[] bytes, int off) {
        toBytes(i, bytes, off, 4);

        return off + 4;
    }

    /**
     * Converts primitive {@code short} type to byte array.
     *
     * @param s Short value.
     * @return Array of bytes.
     */
    public static byte[] shortToBytes(short s) {
        return toBytes(s, new byte[2], 0, 2);
    }

    /**
     * Converts primitive {@code short} type to byte array and stores it in specified
     * byte array.
     *
     * @param s Short value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int shortToBytes(short s, byte[] bytes, int off) {
        toBytes(s, bytes, off, 2);

        return off + 2;
    }

    /**
     * Encodes {@link java.util.UUID} into a sequence of bytes using the {@link java.nio.ByteBuffer},
     * storing the result into a new byte array.
     *
     * @param uuid Unique identifier.
     * @param arr Byte array to fill with result.
     * @param off Offset in {@code arr}.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int uuidToBytes(@Nullable UUID uuid, byte[] arr, int off) {
        ByteBuffer buf = ByteBuffer.wrap(arr, off, 16);

        buf.order(ByteOrder.BIG_ENDIAN);

        if (uuid != null) {
            buf.putLong(uuid.getMostSignificantBits());
            buf.putLong(uuid.getLeastSignificantBits());
        }
        else {
            buf.putLong(0);
            buf.putLong(0);
        }

        return off + 16;
    }

    /**
     * Converts an UUID to byte array.
     *
     * @param uuid UUID value.
     * @return Encoded into byte array {@link java.util.UUID}.
     */
    public static byte[] uuidToBytes(@Nullable UUID uuid) {
        byte[] bytes = new byte[(Long.SIZE >> 3) * 2];

        uuidToBytes(uuid, bytes, 0);

        return bytes;
    }

    /**
     * Constructs {@code short} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Short value.
     */
    public static short bytesToShort(byte[] bytes, int off) {
        return (short)fromBytes(bytes, off, 2);
    }

    /**
     * Constructs {@code int} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Integer value.
     */
    public static int bytesToInt(byte[] bytes, int off) {
        return (int)fromBytes(bytes, off, 4);
    }

    /**
     * Constructs {@code long} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Long value.
     */
    public static long bytesToLong(byte[] bytes, int off) {
        return fromBytes(bytes, off, 8);
    }

    /**
     * Reads an {@link java.util.UUID} form byte array.
     * If given array contains all 0s then {@code null} will be returned.
     *
     * @param bytes array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return UUID value or {@code null}.
     */
    public static UUID bytesToUuid(byte[] bytes, int off) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, off, 16);

        buf.order(ByteOrder.BIG_ENDIAN);

        long most = buf.getLong();
        long least = buf.getLong();

        return most != 0 && least != 0 ? new UUID(most, least) : null;
    }

    /**
     * Converts primitive {@code long} type to byte array and stores it in specified
     * byte array. The highest byte in the value is the first byte in result array.
     *
     * @param l Unsigned long value.
     * @param bytes Bytes array to write result to.
     * @param off Offset in the target array to write result to.
     * @param limit Limit of bytes to write into output.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    private static byte[] toBytes(long l, byte[] bytes, int off, int limit) {
        assert bytes != null;
        assert limit <= 8;
        assert bytes.length >= off + limit;

        for (int i = limit - 1; i >= 0; i--) {
            bytes[off + i] = (byte)(l & 0xFF);
            l >>>= 8;
        }

        return bytes;
    }

    /**
     * Constructs {@code long} from byte array. The first byte in array is the highest byte in result.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @param limit Amount of bytes to use in the source array.
     * @return Unsigned long value.
     */
    private static long fromBytes(byte[] bytes, int off, int limit) {
        assert bytes != null;
        assert limit <= 8;
        assert bytes.length >= off + limit;

        long res = 0;

        for (int i = 0; i < limit; i++) {
            res <<= 8;
            res |= bytes[off + i] & 0xFF;
        }

        return res;
    }
}
