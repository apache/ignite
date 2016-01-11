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

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import sun.misc.Unsafe;

/**
 * Provides handle on Unsafe class from SUN which cannot be instantiated directly.
 */
public abstract class GridUnsafe {
    /** Unsafe. */
    private static final Unsafe UNSAFE = unsafe();

    /** Unaligned flag. */
    private static final boolean UNALIGNED = unaligned();

    /** Big endian. */
    private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    /** Address size. */
    public static final int ADDR_SIZE = UNSAFE.addressSize();

    /** */
    public static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** */
    public static final long SHORT_ARR_OFF = UNSAFE.arrayBaseOffset(short[].class);

    /** */
    public static final long INT_ARR_OFF = UNSAFE.arrayBaseOffset(int[].class);

    /** */
    public static final long LONG_ARR_OFF = UNSAFE.arrayBaseOffset(long[].class);

    /** */
    public static final long FLOAT_ARR_OFF = UNSAFE.arrayBaseOffset(float[].class);

    /** */
    public static final long DOUBLE_ARR_OFF = UNSAFE.arrayBaseOffset(double[].class);

    /** */
    public static final long CHAR_ARR_OFF = UNSAFE.arrayBaseOffset(char[].class);

    /** */
    public static final long BOOLEAN_ARR_OFF = UNSAFE.arrayBaseOffset(boolean[].class);

    /**
     * Ensure singleton.
     */
    private GridUnsafe() {
        // No-op.
    }

    /**
     * Gets boolean value from object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Boolean value from object field.
     */
    public static boolean getBoolean(Object obj, long off) {
        return UNSAFE.getBoolean(obj, off);
    }

    /**
     * Stores boolean value into object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putBoolean(Object obj, long off, boolean val) {
        UNSAFE.putBoolean(obj, off, val);
    }

    /**
     * Gets byte value from object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Byte value from object field.
     */
    public static byte getByte(Object obj, long off) {
        return UNSAFE.getByte(obj, off);
    }

    /**
     * Stores byte value into object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putByte(Object obj, long off, byte val) {
        UNSAFE.putByte(obj, off, val);
    }

    /**
     * Gets short value from object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Short value from object field.
     */
    public static short getShort(Object obj, long off) {
        return UNSAFE.getShort(obj, off);
    }

    /**
     * Stores short value into object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putShort(Object obj, long off, short val) {
        UNSAFE.putShort(obj, off, val);
    }

    /**
     * Gets char value from object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Char value from object field.
     */
    public static char getChar(Object obj, long off) {
        return UNSAFE.getChar(obj, off);
    }

    /**
     * Stores char value into object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putChar(Object obj, long off, char val) {
        UNSAFE.putChar(obj, off, val);
    }

    /**
     * Gets integer value from object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Integer value from object field.
     */
    public static int getInt(Object obj, long off) {
        return UNSAFE.getInt(obj, off);
    }

    /**
     * Stores integer value into object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putInt(Object obj, long off, int val) {
        UNSAFE.putInt(obj, off, val);
    }

    /**
     * Gets long value from object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Long value from object field.
     */
    public static long getLong(Object obj, long off) {
        return UNSAFE.getLong(obj, off);
    }

    /**
     * Stores long value into object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putLong(Object obj, long off, long val) {
        UNSAFE.putLong(obj, off, val);
    }

    /**
     * Gets float value from object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Float value from object field.
     */
    public static float getFloat(Object obj, long off) {
        return UNSAFE.getFloat(obj, off);
    }

    /**
     * Stores float value into object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putFloat(Object obj, long off, float val) {
        UNSAFE.putFloat(obj, off, val);
    }

    /**
     * Gets double value from object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Double value from object field.
     */
    public static double getDouble(Object obj, long off) {
        return UNSAFE.getDouble(obj, off);
    }

    /**
     * Stores double value into object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putDouble(Object obj, long off, double val) {
        UNSAFE.putDouble(obj, off, val);
    }

    /**
     * Gets reference from object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Reference from object field.
     */
    public static Object getObject(Object obj, long off) {
        return UNSAFE.getObject(obj, off);
    }

    /**
     * Stores reference value into object field.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putObject(Object obj, long off, Object val) {
        UNSAFE.putObject(obj, off, val);
    }

    /**
     * Gets short value from object field. Alignment aware.
     *
     * @param obj Object.
     * @param off Address.
     * @return Short value from object field.
     */
    public static short getShortAligned(Object obj, long off) {
        return UNALIGNED ? UNSAFE.getShort(obj, off) : getShortByByte(obj, off);
    }

    /**
     * Stores short value into object field. Alignment aware.
     *
     * @param obj Object.
     * @param off Address.
     * @param val Value.
     */
    public static void putShortAligned(Object obj, long off, short val) {
        if (UNALIGNED)
            UNSAFE.putShort(obj, off, val);
        else
            putShortByByte(obj, off, val);
    }

    /**
     * Gets char value from object field. Alignment aware.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Char value from object field.
     */
    public static char getCharAligned(Object obj, long off) {
        if (UNALIGNED)
            return UNSAFE.getChar(obj, off);
        else
            return getCharByByte(obj, off);
    }

    /**
     * Stores char value into object field. Alignment aware.
     *
     * @param obj Object.
     * @param addr Address.
     * @param val Value.
     */
    public static void putCharAligned(Object obj, long addr, char val) {
        if (UNALIGNED)
            UNSAFE.putChar(obj, addr, val);
        else
            putCharByByte(obj, addr, val);
    }

    /**
     * Gets integer value from object field. Alignment aware.
     *
     * @param obj Object.
     * @param addr Address.
     * @return Integer value from object field.
     */
    public static int getIntAligned(Object obj, long addr) {
        return UNALIGNED ? UNSAFE.getInt(obj, addr) : getIntByByte(obj, addr);
    }

    /**
     * Stores integer value into object field. Alignment aware.
     *
     * @param obj Object.
     * @param addr Address.
     * @param val Value.
     */
    public static void putIntAligned(Object obj, long addr, int val) {
        if (UNALIGNED)
            UNSAFE.putInt(obj, addr, val);
        else
            putIntByByte(obj, addr, val);
    }

    /**
     * Gets long value from object field. Alignment aware.
     *
     * @param obj Object.
     * @param addr Address.
     * @return Long value from object field.
     */
    public static long getLongAligned(Object obj, long addr) {
        return UNALIGNED ? UNSAFE.getLong(obj, addr) : getLongByByte(obj, addr);
    }

    /**
     * Stores long value into object field. Alignment aware.
     *
     * @param obj Object.
     * @param addr Address.
     * @param val Value.
     */
    public static void putLongAligned(Object obj, long addr, long val) {
        if (UNALIGNED)
            UNSAFE.putLong(obj, addr, val);
        else
            putLongByByte(obj, addr, val);
    }

    /**
     * Gets float value from object field. Alignment aware.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Float value from object field.
     */
    public static float getFloatAligned(Object obj, long off) {
        return UNALIGNED ? UNSAFE.getFloat(obj, off) : Float.intBitsToFloat(getIntByByte(obj, off));
    }

    /**
     * Stores float value into object field. Alignment aware.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putFloatAligned(Object obj, long off, float val) {
        if (UNALIGNED)
            UNSAFE.putFloat(obj, off, val);
        else
            putIntByByte(obj, off, Float.floatToIntBits(val));
    }

    /**
     * Gets double value from object field. Alignment aware.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Double value from object field. Alignment aware.
     */
    public static double getDoubleAligned(Object obj, long off) {
        return UNALIGNED ? UNSAFE.getDouble(obj, off) : Double.longBitsToDouble(getLongByByte(obj, off));
    }

    /**
     * Stores double value into object field. Alignment aware.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putDoubleAligned(Object obj, long off, double val) {
        if (UNALIGNED)
            UNSAFE.putDouble(obj, off, val);
        else
            putLongByByte(obj, off, Double.doubleToLongBits(val));
    }

    /**
     * Gets byte value from given address.
     *
     * @param addr Address.
     * @return Byte value from given address.
     */
    public static byte getByte(long addr) {
        return UNSAFE.getByte(addr);
    }

    /**
     * Stores given byte value.
     *
     * @param addr Address.
     * @param val Value.
     */
    public static void putByte(long addr, byte val) {
        UNSAFE.putByte(addr, val);
    }

    /**
     * Gets short value from given address. Alignment aware.
     *
     * @param addr Address.
     * @return Short value from given address.
     */
    public static short getShort(long addr) {
        return UNALIGNED ? UNSAFE.getShort(addr) : getShortByByte(addr);
    }

    /**
     * Stores given short value. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public static void putShort(long addr, short val) {
        if (UNALIGNED)
            UNSAFE.putShort(addr, val);
        else
            putShortByByte(addr, val);
    }

    /**
     * Gets char value from given address. Alignment aware.
     *
     * @param addr Address.
     * @return Char value from given address.
     */
    public static char getChar(long addr) {
        return UNALIGNED ? UNSAFE.getChar(addr) : getCharByByte(addr);
    }

    /**
     * Stores given char value. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public static void putChar(long addr, char val) {
        if (UNALIGNED)
            UNSAFE.putChar(addr, val);
        else
            putCharByByte(addr, val);
    }

    /**
     * Gets integer value from given address. Alignment aware.
     *
     * @param addr Address.
     * @return Integer value from given address.
     */
    public static int getInt(long addr) {
        return UNALIGNED ? UNSAFE.getInt(addr) : getIntByByte(addr);
    }

    /**
     * Stores given integer value. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public static void putInt(long addr, int val) {
        if (UNALIGNED)
            UNSAFE.putInt(addr, val);
        else
            putIntByByte(addr, val);
    }

    /**
     * Gets long value from given address. Alignment aware.
     *
     * @param addr Address.
     * @return Long value from given address.
     */
    public static long getLong(long addr) {
        return UNALIGNED ? UNSAFE.getLong(addr) : getLongByByte(addr);
    }

    /**
     * Stores given integer value. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public static void putLong(long addr, long val) {
        if (UNALIGNED)
            UNSAFE.putLong(addr, val);
        else
            putLongByByte(addr, val);
    }

    /**
     * Gets float value from given address. Alignment aware.
     *
     * @param addr Address.
     * @return Float value from given address.
     */
    public static float getFloat(long addr) {
        return UNALIGNED ? UNSAFE.getFloat(addr) : Float.intBitsToFloat(getIntByByte(addr));
    }

    /**
     * Stores given float value. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public static void putFloat(long addr, float val) {
        if (UNALIGNED)
            UNSAFE.putFloat(addr, val);
        else
            putIntByByte(addr, Float.floatToIntBits(val));
    }

    /**
     * Gets double value from given address. Alignment aware.
     *
     * @param addr Address.
     * @return Double value from given address.
     */
    public static double getDouble(long addr) {
        return UNALIGNED ? UNSAFE.getDouble(addr) : Double.longBitsToDouble(getLongByByte(addr));
    }

    /**
     * Stores given double value. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public static void putDouble(long addr, double val) {
        if (UNALIGNED)
            UNSAFE.putDouble(addr, val);
        else
            putLongByByte(addr, Double.doubleToLongBits(val));
    }

    /**
     * Returns static field offset.
     *
     * @param field Field.
     * @return Static field offset.
     */
    public static long staticFieldOffset(Field field) {
        return UNSAFE.staticFieldOffset(field);
    }

    /**
     * Returns object field offset.
     *
     * @param field Field.
     * @return Object field offset.
     */
    public static long objectFieldOffset(Field field) {
        return UNSAFE.objectFieldOffset(field);
    }

    /**
     * Returns static field base.
     *
     * @param field Field.
     * @return Static field base.
     */
    public static Object staticFieldBase(Field field) {
        return UNSAFE.staticFieldBase(field);
    }

    /**
     * Allocates memory.
     *
     * @param size Size.
     * @return address.
     */
    public static long allocateMemory(long size) {
        return UNSAFE.allocateMemory(size);
    }

    /**
     * Reallocates memory.
     *
     * @param addr Address.
     * @param len Length.
     * @return address.
     */
    public static long reallocateMemory(long addr, long len) {
        return UNSAFE.reallocateMemory(addr, len);
    }

    /**
     * Fills memory with given value.
     *
     * @param addr Address.
     * @param len Length.
     * @param val Value.
     */
    public static void setMemory(long addr, long len, byte val) {
        UNSAFE.setMemory(addr, len, val);
    }

    /**
     * Copies memory.
     *
     * @param src Source.
     * @param dst Dst.
     * @param len Length.
     */
    public static void copyMemory(long src, long dst, long len) {
        UNSAFE.copyMemory(src, dst, len);
    }

    /**
     * Sets all bytes in a given block of memory to a copy of another block.
     *
     * @param srcBase Source base.
     * @param srcOff Source offset.
     * @param dstBase Dst base.
     * @param dstOff Dst offset.
     * @param len Length.
     */
    public static void copyMemory(Object srcBase, long srcOff, Object dstBase, long dstOff, long len) {
        UNSAFE.copyMemory(srcBase, srcOff, dstBase, dstOff, len);
    }

    /**
     * Frees memory.
     *
     * @param addr Address.
     */
    public static void freeMemory(long addr) {
        UNSAFE.freeMemory(addr);
    }

    /**
     * Returns the offset of the first element in the storage allocation of a given array class.
     *
     * @param cls Class.
     * @return the offset of the first element in the storage allocation of a given array class.
     */
    public static int arrayBaseOffset(Class cls) {
        return UNSAFE.arrayBaseOffset(cls);
    }

    /**
     * Allocates instance of given class.
     *
     * @param cls Class.
     * @return Allocated instance.
     */
    public static Object allocateInstance(Class cls) throws InstantiationException {
        return UNSAFE.allocateInstance(cls);
    }

    /**
     * Acquires monitor lock.
     *
     * @param obj Object.
     */
    public static void monitorEnter(Object obj) {
        UNSAFE.monitorEnter(obj);
    }

    /**
     * Releases monitor lock.
     *
     * @param obj Object.
     */
    public static void monitorExit(Object obj) {
        UNSAFE.monitorExit(obj);
    }

    /**
     * Integer CAS.
     *
     * @param obj Object.
     * @param off Offset.
     * @param exp Expected.
     * @param upd Upd.
     * @return {@code True} if operation completed successfully, {@code false} - otherwise.
     */
    public static boolean compareAndSwapInt(Object obj, long off, int exp, int upd) {
        return UNSAFE.compareAndSwapInt(obj, off, exp, upd);
    }

    /**
     * Long CAS.
     *
     * @param obj Object.
     * @param off Offset.
     * @param exp Expected.
     * @param upd Upd.
     * @return {@code True} if operation completed successfully, {@code false} - otherwise.
     */
    public static boolean compareAndSwapLong(Object obj, long off, long exp, long upd) {
        return UNSAFE.compareAndSwapLong(obj, off, exp, upd);
    }

    /**
     * Gets byte value with volatile semantic.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Byte value.
     */
    public static byte getByteVolatile(Object obj, long off) {
        return UNSAFE.getByteVolatile(obj, off);
    }

    /**
     * Stores byte value with volatile semantic.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putByteVolatile(Object obj, long off, byte val) {
        UNSAFE.putByteVolatile(obj, off, val);
    }

    /**
     * Gets integer value with volatile semantic.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Integer value.
     */
    public static int getIntVolatile(Object obj, long off) {
        return UNSAFE.getIntVolatile(obj, off);
    }

    /**
     * Stores integer value with volatile semantic.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putIntVolatile(Object obj, long off, int val) {
        UNSAFE.putIntVolatile(obj, off, val);
    }

    /**
     * Gets long value with volatile semantic.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Long value.
     */
    public static long getLongVolatile(Object obj, long off) {
        return UNSAFE.getLongVolatile(obj, off);
    }

    /**
     * Stores long value with volatile semantic.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putLongVolatile(Object obj, long off, long val) {
        UNSAFE.putLongVolatile(obj, off, val);
    }

    /**
     * Stores reference value with volatile semantic.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public static void putObjectVolatile(Object obj, long off, Object val) {
        UNSAFE.putObjectVolatile(obj, off, val);
    }

    /**
     * Returns unaligned flag.
     */
    private static boolean unaligned() {
        String arch = System.getProperty("os.arch");

        return arch.equals("i386") || arch.equals("x86") || arch.equals("amd64") || arch.equals("x86_64");
    }

    /**
     * @return Instance of Unsafe class.
     */
    private static Unsafe unsafe() {
        try {
            return Unsafe.getUnsafe();
        }
        catch (SecurityException ignored) {
            try {
                return AccessController.doPrivileged
                    (new PrivilegedExceptionAction<Unsafe>() {
                        @Override public Unsafe run() throws Exception {
                            Field f = Unsafe.class.getDeclaredField("theUnsafe");

                            f.setAccessible(true);

                            return (Unsafe)f.get(null);
                        }
                    });
            }
            catch (PrivilegedActionException e) {
                throw new RuntimeException("Could not initialize intrinsics.", e.getCause());
            }
        }
    }

    /**
     * @param obj Object.
     * @param off Offset.
     */
    private static short getShortByByte(Object obj, long off) {
        if (BIG_ENDIAN)
            return (short)(UNSAFE.getByte(obj, off) << 8 | (UNSAFE.getByte(obj, off + 1) & 0xff));
        else
            return (short)(UNSAFE.getByte(obj, off + 1) << 8 | (UNSAFE.getByte(obj, off) & 0xff));
    }

    /**
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    private static void putShortByByte(Object obj, long off, short val) {
        if (BIG_ENDIAN) {
            UNSAFE.putByte(obj, off, (byte)(val >> 8));
            UNSAFE.putByte(obj, off + 1, (byte)val);
        }
        else {
            UNSAFE.putByte(obj, off + 1, (byte)(val >> 8));
            UNSAFE.putByte(obj, off, (byte)val);
        }
    }

    /**
     * @param obj Object.
     * @param off Offset.
     */
    private static char getCharByByte(Object obj, long off) {
        if (BIG_ENDIAN)
            return (char)(UNSAFE.getByte(obj, off) << 8 | (UNSAFE.getByte(obj, off + 1) & 0xff));
        else
            return (char)(UNSAFE.getByte(obj, off + 1) << 8 | (UNSAFE.getByte(obj, off) & 0xff));
    }

    /**
     * @param obj Object.
     * @param addr Address.
     * @param val Value.
     */
    private static void putCharByByte(Object obj, long addr, char val) {
        if (BIG_ENDIAN) {
            UNSAFE.putByte(obj, addr, (byte)(val >> 8));
            UNSAFE.putByte(obj, addr + 1, (byte)val);
        }
        else {
            UNSAFE.putByte(obj, addr + 1, (byte)(val >> 8));
            UNSAFE.putByte(obj, addr, (byte)val);
        }
    }

    /**
     * @param obj Object.
     * @param addr Address.
     */
    private static int getIntByByte(Object obj, long addr) {
        if (BIG_ENDIAN) {
            return (((int)UNSAFE.getByte(obj, addr)) << 24) |
                (((int)UNSAFE.getByte(obj, addr + 1) & 0xff) << 16) |
                (((int)UNSAFE.getByte(obj, addr + 2) & 0xff) << 8) |
                (((int)UNSAFE.getByte(obj, addr + 3) & 0xff));
        }
        else {
            return (((int)UNSAFE.getByte(obj, addr + 3)) << 24) |
                (((int)UNSAFE.getByte(obj, addr + 2) & 0xff) << 16) |
                (((int)UNSAFE.getByte(obj, addr + 1) & 0xff) << 8) |
                (((int)UNSAFE.getByte(obj, addr) & 0xff));
        }
    }

    /**
     * @param obj Object.
     * @param addr Address.
     * @param val Value.
     */
    private static void putIntByByte(Object obj, long addr, int val) {
        if (BIG_ENDIAN) {
            UNSAFE.putByte(obj, addr, (byte)(val >> 24));
            UNSAFE.putByte(obj, addr + 1, (byte)(val >> 16));
            UNSAFE.putByte(obj, addr + 2, (byte)(val >> 8));
            UNSAFE.putByte(obj, addr + 3, (byte)(val));
        }
        else {
            UNSAFE.putByte(obj, addr + 3, (byte)(val >> 24));
            UNSAFE.putByte(obj, addr + 2, (byte)(val >> 16));
            UNSAFE.putByte(obj, addr + 1, (byte)(val >> 8));
            UNSAFE.putByte(obj, addr, (byte)(val));
        }
    }

    /**
     * @param obj Object.
     * @param addr Address.
     */
    private static long getLongByByte(Object obj, long addr) {
        if (BIG_ENDIAN) {
            return (((long)UNSAFE.getByte(obj, addr)) << 56) |
                (((long)UNSAFE.getByte(obj, addr + 1) & 0xff) << 48) |
                (((long)UNSAFE.getByte(obj, addr + 2) & 0xff) << 40) |
                (((long)UNSAFE.getByte(obj, addr + 3) & 0xff) << 32) |
                (((long)UNSAFE.getByte(obj, addr + 4) & 0xff) << 24) |
                (((long)UNSAFE.getByte(obj, addr + 5) & 0xff) << 16) |
                (((long)UNSAFE.getByte(obj, addr + 6) & 0xff) << 8) |
                (((long)UNSAFE.getByte(obj, addr + 7) & 0xff));
        }
        else {
            return (((long)UNSAFE.getByte(obj, addr + 7)) << 56) |
                (((long)UNSAFE.getByte(obj, addr + 6) & 0xff) << 48) |
                (((long)UNSAFE.getByte(obj, addr + 5) & 0xff) << 40) |
                (((long)UNSAFE.getByte(obj, addr + 4) & 0xff) << 32) |
                (((long)UNSAFE.getByte(obj, addr + 3) & 0xff) << 24) |
                (((long)UNSAFE.getByte(obj, addr + 2) & 0xff) << 16) |
                (((long)UNSAFE.getByte(obj, addr + 1) & 0xff) << 8) |
                (((long)UNSAFE.getByte(obj, addr) & 0xff));
        }
    }

    /**
     * @param obj Object.
     * @param addr Address.
     * @param val Value.
     */
    private static void putLongByByte(Object obj, long addr, long val) {
        if (BIG_ENDIAN) {
            UNSAFE.putByte(obj, addr, (byte)(val >> 56));
            UNSAFE.putByte(obj, addr + 1, (byte)(val >> 48));
            UNSAFE.putByte(obj, addr + 2, (byte)(val >> 40));
            UNSAFE.putByte(obj, addr + 3, (byte)(val >> 32));
            UNSAFE.putByte(obj, addr + 4, (byte)(val >> 24));
            UNSAFE.putByte(obj, addr + 5, (byte)(val >> 16));
            UNSAFE.putByte(obj, addr + 6, (byte)(val >> 8));
            UNSAFE.putByte(obj, addr + 7, (byte)(val));
        }
        else {
            UNSAFE.putByte(obj, addr + 7, (byte)(val >> 56));
            UNSAFE.putByte(obj, addr + 6, (byte)(val >> 48));
            UNSAFE.putByte(obj, addr + 5, (byte)(val >> 40));
            UNSAFE.putByte(obj, addr + 4, (byte)(val >> 32));
            UNSAFE.putByte(obj, addr + 3, (byte)(val >> 24));
            UNSAFE.putByte(obj, addr + 2, (byte)(val >> 16));
            UNSAFE.putByte(obj, addr + 1, (byte)(val >> 8));
            UNSAFE.putByte(obj, addr, (byte)(val));
        }
    }

    /**
     * @param addr Address.
     */
    private static short getShortByByte(long addr) {
        if (BIG_ENDIAN)
            return (short)(UNSAFE.getByte(addr) << 8 | (UNSAFE.getByte(addr + 1) & 0xff));
        else
            return (short)(UNSAFE.getByte(addr + 1) << 8 | (UNSAFE.getByte(addr) & 0xff));
    }

    /**
     * @param addr Address.
     * @param val Value.
     */
    private static void putShortByByte(long addr, short val) {
        if (BIG_ENDIAN) {
            UNSAFE.putByte(addr, (byte)(val >> 8));
            UNSAFE.putByte(addr + 1, (byte)val);
        }
        else {
            UNSAFE.putByte(addr + 1, (byte)(val >> 8));
            UNSAFE.putByte(addr, (byte)val);
        }
    }

    /**
     * @param addr Address.
     */
    private static char getCharByByte(long addr) {
        if (BIG_ENDIAN)
            return (char)(UNSAFE.getByte(addr) << 8 | (UNSAFE.getByte(addr + 1) & 0xff));
        else
            return (char)(UNSAFE.getByte(addr + 1) << 8 | (UNSAFE.getByte(addr) & 0xff));
    }

    /**
     * @param addr Address.
     * @param val Value.
     */
    private static void putCharByByte(long addr, char val) {
        if (BIG_ENDIAN) {
            UNSAFE.putByte(addr, (byte)(val >> 8));
            UNSAFE.putByte(addr + 1, (byte)val);
        }
        else {
            UNSAFE.putByte(addr + 1, (byte)(val >> 8));
            UNSAFE.putByte(addr, (byte)val);
        }
    }

    /**
     * @param addr Address.
     */
    private static int getIntByByte(long addr) {
        if (BIG_ENDIAN) {
            return (((int)UNSAFE.getByte(addr)) << 24) |
                (((int)UNSAFE.getByte(addr + 1) & 0xff) << 16) |
                (((int)UNSAFE.getByte(addr + 2) & 0xff) << 8) |
                (((int)UNSAFE.getByte(addr + 3) & 0xff));
        }
        else {
            return (((int)UNSAFE.getByte(addr + 3)) << 24) |
                (((int)UNSAFE.getByte(addr + 2) & 0xff) << 16) |
                (((int)UNSAFE.getByte(addr + 1) & 0xff) << 8) |
                (((int)UNSAFE.getByte(addr) & 0xff));
        }
    }

    /**
     * @param addr Address.
     * @param val Value.
     */
    private static void putIntByByte(long addr, int val) {
        if (BIG_ENDIAN) {
            UNSAFE.putByte(addr, (byte)(val >> 24));
            UNSAFE.putByte(addr + 1, (byte)(val >> 16));
            UNSAFE.putByte(addr + 2, (byte)(val >> 8));
            UNSAFE.putByte(addr + 3, (byte)(val));
        }
        else {
            UNSAFE.putByte(addr + 3, (byte)(val >> 24));
            UNSAFE.putByte(addr + 2, (byte)(val >> 16));
            UNSAFE.putByte(addr + 1, (byte)(val >> 8));
            UNSAFE.putByte(addr, (byte)(val));
        }
    }

    /**
     * @param addr Address.
     */
    private static long getLongByByte(long addr) {
        if (BIG_ENDIAN) {
            return (((long)UNSAFE.getByte(addr)) << 56) |
                (((long)UNSAFE.getByte(addr + 1) & 0xff) << 48) |
                (((long)UNSAFE.getByte(addr + 2) & 0xff) << 40) |
                (((long)UNSAFE.getByte(addr + 3) & 0xff) << 32) |
                (((long)UNSAFE.getByte(addr + 4) & 0xff) << 24) |
                (((long)UNSAFE.getByte(addr + 5) & 0xff) << 16) |
                (((long)UNSAFE.getByte(addr + 6) & 0xff) << 8) |
                (((long)UNSAFE.getByte(addr + 7) & 0xff));
        }
        else {
            return (((long)UNSAFE.getByte(addr + 7)) << 56) |
                (((long)UNSAFE.getByte(addr + 6) & 0xff) << 48) |
                (((long)UNSAFE.getByte(addr + 5) & 0xff) << 40) |
                (((long)UNSAFE.getByte(addr + 4) & 0xff) << 32) |
                (((long)UNSAFE.getByte(addr + 3) & 0xff) << 24) |
                (((long)UNSAFE.getByte(addr + 2) & 0xff) << 16) |
                (((long)UNSAFE.getByte(addr + 1) & 0xff) << 8) |
                (((long)UNSAFE.getByte(addr) & 0xff));
        }
    }

    /**
     * @param addr Address.
     * @param val Value.
     */
    private static void putLongByByte(long addr, long val) {
        if (BIG_ENDIAN) {
            UNSAFE.putByte(addr, (byte)(val >> 56));
            UNSAFE.putByte(addr + 1, (byte)(val >> 48));
            UNSAFE.putByte(addr + 2, (byte)(val >> 40));
            UNSAFE.putByte(addr + 3, (byte)(val >> 32));
            UNSAFE.putByte(addr + 4, (byte)(val >> 24));
            UNSAFE.putByte(addr + 5, (byte)(val >> 16));
            UNSAFE.putByte(addr + 6, (byte)(val >> 8));
            UNSAFE.putByte(addr + 7, (byte)(val));
        }
        else {
            UNSAFE.putByte(addr + 7, (byte)(val >> 56));
            UNSAFE.putByte(addr + 6, (byte)(val >> 48));
            UNSAFE.putByte(addr + 5, (byte)(val >> 40));
            UNSAFE.putByte(addr + 4, (byte)(val >> 32));
            UNSAFE.putByte(addr + 3, (byte)(val >> 24));
            UNSAFE.putByte(addr + 2, (byte)(val >> 16));
            UNSAFE.putByte(addr + 1, (byte)(val >> 8));
            UNSAFE.putByte(addr, (byte)(val));
        }
    }
}