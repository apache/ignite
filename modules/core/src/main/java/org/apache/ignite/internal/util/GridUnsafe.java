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

import lib.llpl.MemoryBlock;
import lib.llpl.Transactional;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;
import sun.misc.JavaNioAccess;
import sun.misc.SharedSecrets;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class GridUnsafe {

    /** */
    public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

    /** Direct byte buffer factory. */
    protected static final JavaNioAccess nioAccess = SharedSecrets.getJavaNioAccess();

    /** Unsafe. */
    public static final Unsafe UNSAFE = unsafe();

    /** Unaligned flag. */
    public static final boolean UNALIGNED = unaligned();

    /** Per-byte copy threshold. */
    protected static final long PER_BYTE_THRESHOLD =
            IgniteSystemProperties.getLong(IgniteSystemProperties.IGNITE_MEMORY_PER_BYTE_COPY_THRESHOLD, 0L);

    /** Big endian. */
    public static final boolean BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    /** Address size. */
    public static final int ADDR_SIZE = UNSAFE.addressSize();

    /** */
    public static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** */
    public static final int BYTE_ARR_INT_OFF = UNSAFE.arrayBaseOffset(byte[].class);

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
     * @param ptr Pointer to wrap.
     * @param len Memory location length.
     * @return Byte buffer wrapping the given memory.
     */
    public ByteBuffer wrapPointer(long ptr, int len) {
        ByteBuffer buf = nioAccess.newDirectByteBuffer(ptr, len, null);

        assert buf instanceof DirectBuffer;

        buf.order(NATIVE_BYTE_ORDER);

        return buf;
    }

    /**
     * @param len Length.
     * @return Allocated direct buffer.
     */
    public ByteBuffer allocateBuffer(int len) {
        long ptr = allocateMemory(len);

        return wrapPointer(ptr, len);
    }

    /**
     * @param buf Direct buffer allocated by {@link #allocateBuffer(int)}.
     */
    public void freeBuffer(ByteBuffer buf) {
        long ptr = bufferAddress(buf);

        freeMemory(ptr);
    }

    public MemoryBlock<MemoryBlock.Kind> getSchemaRegistryBlock() {
        /* no-op */
        return null;
    }

    public PersistentLinkedList<Transactional, Long> getPersistentList() {
        /* no-op */
        return null;
    }
    /**
     * Gets boolean value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Boolean value from object field.
     */
    public boolean getBooleanField(Object obj, long fieldOff) {
        return UNSAFE.getBoolean(obj, fieldOff);
    }

    /**
     * Stores boolean value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public void putBooleanField(Object obj, long fieldOff, boolean val) {
        UNSAFE.putBoolean(obj, fieldOff, val);
    }

    /**
     * Gets byte value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Byte value from object field.
     */
    public byte getByteField(Object obj, long fieldOff) {
        return UNSAFE.getByte(obj, fieldOff);
    }

    /**
     * Stores byte value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public void putByteField(Object obj, long fieldOff, byte val) {
        UNSAFE.putByte(obj, fieldOff, val);
    }

    /**
     * Gets short value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Short value from object field.
     */
    public short getShortField(Object obj, long fieldOff) {
        return UNSAFE.getShort(obj, fieldOff);
    }

    /**
     * Stores short value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public void putShortField(Object obj, long fieldOff, short val) {
        UNSAFE.putShort(obj, fieldOff, val);
    }

    /**
     * Gets char value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Char value from object field.
     */
    public char getCharField(Object obj, long fieldOff) {
        return UNSAFE.getChar(obj, fieldOff);
    }

    /**
     * Stores char value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public void putCharField(Object obj, long fieldOff, char val) {
        UNSAFE.putChar(obj, fieldOff, val);
    }

    /**
     * Gets integer value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Integer value from object field.
     */
    public int getIntField(Object obj, long fieldOff) {
        return UNSAFE.getInt(obj, fieldOff);
    }

    /**
     * Stores integer value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public void putIntField(Object obj, long fieldOff, int val) {
        UNSAFE.putInt(obj, fieldOff, val);
    }

    /**
     * Gets long value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Long value from object field.
     */
    public long getLongField(Object obj, long fieldOff) {
        return UNSAFE.getLong(obj, fieldOff);
    }

    /**
     * Stores long value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public void putLongField(Object obj, long fieldOff, long val) {
        UNSAFE.putLong(obj, fieldOff, val);
    }

    /**
     * Gets float value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Float value from object field.
     */
    public float getFloatField(Object obj, long fieldOff) {
        return UNSAFE.getFloat(obj, fieldOff);
    }

    /**
     * Stores float value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public void putFloatField(Object obj, long fieldOff, float val) {
        UNSAFE.putFloat(obj, fieldOff, val);
    }

    /**
     * Gets double value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Double value from object field.
     */
    public double getDoubleField(Object obj, long fieldOff) {
        return UNSAFE.getDouble(obj, fieldOff);
    }

    /**
     * Stores double value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public void putDoubleField(Object obj, long fieldOff, double val) {
        UNSAFE.putDouble(obj, fieldOff, val);
    }

    /**
     * Gets reference from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Reference from object field.
     */
    public Object getObjectField(Object obj, long fieldOff) {
        return UNSAFE.getObject(obj, fieldOff);
    }

    /**
     * Stores reference value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public void putObjectField(Object obj, long fieldOff, Object val) {
        UNSAFE.putObject(obj, fieldOff, val);
    }

    /**
     * Gets boolean value from byte array.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @return Boolean value from byte array.
     */
    public boolean getBoolean(byte[] arr, long off) {
        return UNSAFE.getBoolean(arr, off);
    }

    /**
     * Stores boolean value into byte array.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @param val Value.
     */
    public void putBoolean(byte[] arr, long off, boolean val) {
        UNSAFE.putBoolean(arr, off, val);
    }

    /**
     * Gets byte value from byte array.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @return Byte value from byte array.
     */
    public byte getByte(byte[] arr, long off) {
        return UNSAFE.getByte(arr, off);
    }

    /**
     * Stores byte value into byte array.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @param val Value.
     */
    public void putByte(byte[] arr, long off, byte val) {
        UNSAFE.putByte(arr, off, val);
    }

    /**
     * Gets short value from byte array. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @return Short value from byte array.
     */
    public short getShort(byte[] arr, long off) {
        return UNALIGNED ? UNSAFE.getShort(arr, off) : getShortByByte(arr, off, BIG_ENDIAN);
    }

    /**
     * Stores short value into byte array. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @param val Value.
     */
    public void putShort(byte[] arr, long off, short val) {
        if (UNALIGNED)
            UNSAFE.putShort(arr, off, val);
        else
            putShortByByte(arr, off, val, BIG_ENDIAN);
    }

    /**
     * Gets char value from byte array. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @return Char value from byte array.
     */
    public char getChar(byte[] arr, long off) {
        return UNALIGNED ? UNSAFE.getChar(arr, off) : getCharByByte(arr, off, BIG_ENDIAN);
    }

    /**
     * Stores char value into byte array. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @param val Value.
     */
    public void putChar(byte[] arr, long off, char val) {
        if (UNALIGNED)
            UNSAFE.putChar(arr, off, val);
        else
            putCharByByte(arr, off, val, BIG_ENDIAN);
    }

    /**
     * Gets integer value from byte array. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @return Integer value from byte array.
     */
    public int getInt(byte[] arr, long off) {
        return UNALIGNED ? UNSAFE.getInt(arr, off) : getIntByByte(arr, off, BIG_ENDIAN);
    }

    /**
     * Stores integer value into byte array. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @param val Value.
     */
    public void putInt(byte[] arr, long off, int val) {
        if (UNALIGNED)
            UNSAFE.putInt(arr, off, val);
        else
            putIntByByte(arr, off, val, BIG_ENDIAN);
    }

    /**
     * Gets long value from byte array. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @return Long value from byte array.
     */
    public long getLong(byte[] arr, long off) {
        return UNALIGNED ? UNSAFE.getLong(arr, off) : getLongByByte(arr, off, BIG_ENDIAN);
    }

    /**
     * Stores long value into byte array. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @param val Value.
     */
    public void putLong(byte[] arr, long off, long val) {
        if (UNALIGNED)
            UNSAFE.putLong(arr, off, val);
        else
            putLongByByte(arr, off, val, BIG_ENDIAN);
    }

    /**
     * Gets float value from byte array. Alignment aware.
     *
     * @param arr Object.
     * @param off Offset.
     * @return Float value from byte array.
     */
    public float getFloat(byte[] arr, long off) {
        return UNALIGNED ? UNSAFE.getFloat(arr, off) : Float.intBitsToFloat(getIntByByte(arr, off, BIG_ENDIAN));
    }

    /**
     * Stores float value into byte array. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @param val Value.
     */
    public void putFloat(byte[] arr, long off, float val) {
        if (UNALIGNED)
            UNSAFE.putFloat(arr, off, val);
        else
            putIntByByte(arr, off, Float.floatToIntBits(val), BIG_ENDIAN);
    }

    /**
     * Gets double value from byte array. Alignment aware.
     *
     * @param arr byte array.
     * @param off Offset.
     * @return Double value from byte array. Alignment aware.
     */
    public double getDouble(byte[] arr, long off) {
        return UNALIGNED ? UNSAFE.getDouble(arr, off) : Double.longBitsToDouble(getLongByByte(arr, off, BIG_ENDIAN));
    }

    /**
     * Stores double value into byte array. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @param val Value.
     */
    public void putDouble(byte[] arr, long off, double val) {
        if (UNALIGNED)
            UNSAFE.putDouble(arr, off, val);
        else
            putLongByByte(arr, off, Double.doubleToLongBits(val), BIG_ENDIAN);
    }

    /**
     * Gets short value from byte array assuming that value stored in little-endian byte order and native byte order
     * is big-endian. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @return Short value from byte array.
     */
    public short getShortLE(byte[] arr, long off) {
        return UNALIGNED ? Short.reverseBytes(UNSAFE.getShort(arr, off)) : getShortByByte(arr, off, false);
    }

    /**
     * Stores short value into byte array assuming that value should be stored in little-endian byte order and native
     * byte order is big-endian. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @param val Value.
     */
    public void putShortLE(byte[] arr, long off, short val) {
        if (UNALIGNED)
            UNSAFE.putShort(arr, off, Short.reverseBytes(val));
        else
            putShortByByte(arr, off, val, false);
    }

    /**
     * Gets char value from byte array assuming that value stored in little-endian byte order and native byte order
     * is big-endian. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @return Char value from byte array.
     */
    public char getCharLE(byte[] arr, long off) {
        return UNALIGNED ? Character.reverseBytes(UNSAFE.getChar(arr, off)) : getCharByByte(arr, off, false);
    }

    /**
     * Stores char value into byte array assuming that value should be stored in little-endian byte order and native
     * byte order is big-endian. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @param val Value.
     */
    public void putCharLE(byte[] arr, long off, char val) {
        if (UNALIGNED)
            UNSAFE.putChar(arr, off, Character.reverseBytes(val));
        else
            putCharByByte(arr, off, val, false);
    }

    /**
     * Gets integer value from byte array assuming that value stored in little-endian byte order and native byte order
     * is big-endian. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @return Integer value from byte array.
     */
    public int getIntLE(byte[] arr, long off) {
        return UNALIGNED ? Integer.reverseBytes(UNSAFE.getInt(arr, off)) : getIntByByte(arr, off, false);
    }

    /**
     * Stores integer value into byte array assuming that value should be stored in little-endian byte order and
     * native byte order is big-endian. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @param val Value.
     */
    public void putIntLE(byte[] arr, long off, int val) {
        if (UNALIGNED)
            UNSAFE.putInt(arr, off, Integer.reverseBytes(val));
        else
            putIntByByte(arr, off, val, false);
    }

    /**
     * Gets long value from byte array assuming that value stored in little-endian byte order and native byte order
     * is big-endian. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @return Long value from byte array.
     */
    public long getLongLE(byte[] arr, long off) {
        return UNALIGNED ? Long.reverseBytes(UNSAFE.getLong(arr, off)) : getLongByByte(arr, off, false);
    }

    /**
     * Stores long value into byte array assuming that value should be stored in little-endian byte order and native
     * byte order is big-endian. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @param val Value.
     */
    public void putLongLE(byte[] arr, long off, long val) {
        if (UNALIGNED)
            UNSAFE.putLong(arr, off, Long.reverseBytes(val));
        else
            putLongByByte(arr, off, val, false);
    }

    /**
     * Gets float value from byte array assuming that value stored in little-endian byte order and native byte order
     * is big-endian. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @return Float value from byte array.
     */
    public float getFloatLE(byte[] arr, long off) {
        return Float.intBitsToFloat(
                UNALIGNED ? Integer.reverseBytes(UNSAFE.getInt(arr, off)) : getIntByByte(arr, off, false)
        );
    }

    /**
     * Stores float value into byte array assuming that value should be stored in little-endian byte order and native
     * byte order is big-endian. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @param val Value.
     */
    public void putFloatLE(byte[] arr, long off, float val) {
        int intVal = Float.floatToIntBits(val);

        if (UNALIGNED)
            UNSAFE.putInt(arr, off, Integer.reverseBytes(intVal));
        else
            putIntByByte(arr, off, intVal, false);
    }

    /**
     * Gets double value from byte array assuming that value stored in little-endian byte order and native byte order
     * is big-endian. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @return Double value from byte array.
     */
    public double getDoubleLE(byte[] arr, long off) {
        return Double.longBitsToDouble(
                UNALIGNED ? Long.reverseBytes(UNSAFE.getLong(arr, off)) : getLongByByte(arr, off, false)
        );
    }

    /**
     * Stores double value into byte array assuming that value should be stored in little-endian byte order and
     * native byte order is big-endian. Alignment aware.
     *
     * @param arr Byte array.
     * @param off Offset.
     * @param val Value.
     */
    public void putDoubleLE(byte[] arr, long off, double val) {
        long longVal = Double.doubleToLongBits(val);

        if (UNALIGNED)
            UNSAFE.putLong(arr, off, Long.reverseBytes(longVal));
        else
            putLongByByte(arr, off, longVal, false);
    }

    /**
     * Gets byte value from given address.
     *
     * @param addr Address.
     * @return Byte value from given address.
     */
    public byte getByte(long addr) {
        return UNSAFE.getByte(addr);
    }

    /**
     * Stores given byte value.
     *
     * @param addr Address.
     * @param val Value.
     */
    public void putByte(long addr, byte val) {
        UNSAFE.putByte(addr, val);
    }

    /**
     * Gets short value from given address. Alignment aware.
     *
     * @param addr Address.
     * @return Short value from given address.
     */
    public short getShort(long addr) {
        return UNALIGNED ? UNSAFE.getShort(addr) : getShortByByte(addr, BIG_ENDIAN);
    }

    /**
     * Stores given short value. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public void putShort(long addr, short val) {
        if (UNALIGNED)
            UNSAFE.putShort(addr, val);
        else
            putShortByByte(addr, val, BIG_ENDIAN);
    }

    /**
     * Gets char value from given address. Alignment aware.
     *
     * @param addr Address.
     * @return Char value from given address.
     */
    public char getChar(long addr) {
        return UNALIGNED ? UNSAFE.getChar(addr) : getCharByByte(addr, BIG_ENDIAN);
    }

    /**
     * Stores given char value. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public void putChar(long addr, char val) {
        if (UNALIGNED)
            UNSAFE.putChar(addr, val);
        else
            putCharByByte(addr, val, BIG_ENDIAN);
    }

    /**
     * Gets integer value from given address. Alignment aware.
     *
     * @param addr Address.
     * @return Integer value from given address.
     */
    public int getInt(long addr) {
        return UNALIGNED ? UNSAFE.getInt(addr) : getIntByByte(addr, BIG_ENDIAN);
    }

    /**
     * Stores given integer value. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public void putInt(long addr, int val) {
        if (UNALIGNED)
            UNSAFE.putInt(addr, val);
        else
            putIntByByte(addr, val, BIG_ENDIAN);
    }

    /**
     * Gets long value from given address. Alignment aware.
     *
     * @param addr Address.
     * @return Long value from given address.
     */
    public long getLong(long addr) {
        return UNALIGNED ? UNSAFE.getLong(addr) : getLongByByte(addr, BIG_ENDIAN);
    }

    /**
     * Stores given integer value. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public void putLong(long addr, long val) {
        if (UNALIGNED)
            UNSAFE.putLong(addr, val);
        else
            putLongByByte(addr, val, BIG_ENDIAN);
    }

    /**
     * Gets float value from given address. Alignment aware.
     *
     * @param addr Address.
     * @return Float value from given address.
     */
    public float getFloat(long addr) {
        return UNALIGNED ? UNSAFE.getFloat(addr) : Float.intBitsToFloat(getIntByByte(addr, BIG_ENDIAN));
    }

    /**
     * Stores given float value. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public void putFloat(long addr, float val) {
        if (UNALIGNED)
            UNSAFE.putFloat(addr, val);
        else
            putIntByByte(addr, Float.floatToIntBits(val), BIG_ENDIAN);
    }

    /**
     * Gets double value from given address. Alignment aware.
     *
     * @param addr Address.
     * @return Double value from given address.
     */
    public double getDouble(long addr) {
        return UNALIGNED ? UNSAFE.getDouble(addr) : Double.longBitsToDouble(getLongByByte(addr, BIG_ENDIAN));
    }

    /**
     * Stores given double value. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public void putDouble(long addr, double val) {
        if (UNALIGNED)
            UNSAFE.putDouble(addr, val);
        else
            putLongByByte(addr, Double.doubleToLongBits(val), BIG_ENDIAN);
    }

    /**
     * Gets short value from given address assuming that value stored in little-endian byte order and native byte order
     * is big-endian. Alignment aware.
     *
     * @param addr Address.
     * @return Short value from given address.
     */
    public short getShortLE(long addr) {
        return UNALIGNED ? Short.reverseBytes(UNSAFE.getShort(addr)) : getShortByByte(addr, false);
    }

    /**
     * Stores given short value assuming that value should be stored in little-endian byte order and native byte
     * order is big-endian. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public void putShortLE(long addr, short val) {
        if (UNALIGNED)
            UNSAFE.putShort(addr, Short.reverseBytes(val));
        else
            putShortByByte(addr, val, false);
    }

    /**
     * Gets char value from given address assuming that value stored in little-endian byte order and native byte order
     * is big-endian. Alignment aware.
     *
     * @param addr Address.
     * @return Char value from given address.
     */
    public char getCharLE(long addr) {
        return UNALIGNED ? Character.reverseBytes(UNSAFE.getChar(addr)) : getCharByByte(addr, false);
    }

    /**
     * Stores given char value assuming that value should be stored in little-endian byte order and native byte order
     * is big-endian. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public void putCharLE(long addr, char val) {
        if (UNALIGNED)
            UNSAFE.putChar(addr, Character.reverseBytes(val));
        else
            putCharByByte(addr, val, false);
    }

    /**
     * Gets integer value from given address assuming that value stored in little-endian byte order
     * and native byte order is big-endian. Alignment aware.
     *
     * @param addr Address.
     * @return Integer value from given address.
     */
    public int getIntLE(long addr) {
        return UNALIGNED ? Integer.reverseBytes(UNSAFE.getInt(addr)) : getIntByByte(addr, false);
    }

    /**
     * Stores given integer value assuming that value should be stored in little-endian byte order
     * and native byte order is big-endian. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public void putIntLE(long addr, int val) {
        if (UNALIGNED)
            UNSAFE.putInt(addr, Integer.reverseBytes(val));
        else
            putIntByByte(addr, val, false);
    }

    /**
     * Gets long value from given address assuming that value stored in little-endian byte order
     * and native byte order is big-endian. Alignment aware.
     *
     * @param addr Address.
     * @return Long value from given address.
     */
    public long getLongLE(long addr) {
        return UNALIGNED ? Long.reverseBytes(UNSAFE.getLong(addr)) : getLongByByte(addr, false);
    }

    /**
     * Stores given integer value assuming that value should be stored in little-endian byte order
     * and native byte order is big-endian. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public void putLongLE(long addr, long val) {
        if (UNALIGNED)
            UNSAFE.putLong(addr, Long.reverseBytes(val));
        else
            putLongByByte(addr, val, false);
    }

    /**
     * Gets float value from given address assuming that value stored in little-endian byte order
     * and native byte order is big-endian. Alignment aware.
     *
     * @param addr Address.
     * @return Float value from given address.
     */
    public float getFloatLE(long addr) {
        return Float.intBitsToFloat(UNALIGNED ? Integer.reverseBytes(UNSAFE.getInt(addr)) : getIntByByte(addr, false));
    }

    /**
     * Stores given float value assuming that value should be stored in little-endian byte order
     * and native byte order is big-endian. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public void putFloatLE(long addr, float val) {
        int intVal = Float.floatToIntBits(val);

        if (UNALIGNED)
            UNSAFE.putInt(addr, Integer.reverseBytes(intVal));
        else
            putIntByByte(addr, intVal, false);
    }

    /**
     * Gets double value from given address assuming that value stored in little-endian byte order
     * and native byte order is big-endian. Alignment aware.
     *
     * @param addr Address.
     * @return Double value from given address.
     */
    public double getDoubleLE(long addr) {
        return Double.longBitsToDouble(
                UNALIGNED ? Long.reverseBytes(UNSAFE.getLong(addr)) : getLongByByte(addr, false)
        );
    }

    /**
     * Stores given double value assuming that value should be stored in little-endian byte order
     * and native byte order is big-endian. Alignment aware.
     *
     * @param addr Address.
     * @param val Value.
     */
    public void putDoubleLE(long addr, double val) {
        long longVal = Double.doubleToLongBits(val);

        if (UNALIGNED)
            UNSAFE.putLong(addr, Long.reverseBytes(longVal));
        else
            putLongByByte(addr, longVal, false);
    }

    /**
     * Returns static field offset.
     *
     * @param field Field.
     * @return Static field offset.
     */
    public long staticFieldOffset(Field field) {
        return UNSAFE.staticFieldOffset(field);
    }

    /**
     * Returns object field offset.
     *
     * @param field Field.
     * @return Object field offset.
     */
    public long objectFieldOffset(Field field) {
        return UNSAFE.objectFieldOffset(field);
    }

    /**
     * Returns static field base.
     *
     * @param field Field.
     * @return Static field base.
     */
    public Object staticFieldBase(Field field) {
        return UNSAFE.staticFieldBase(field);
    }

    /**
     * Allocates memory.
     *
     * @param size Size.
     * @return address.
     */
    public long allocateMemory(long size) {
        return UNSAFE.allocateMemory(size);
    }

    /**
     *
     * @param size
     * @return
     */
    public long allocateUnsafeMemory(String regionName, int index, long size) {
        return allocateMemory(size);
    }

    /**
     * Reallocates memory.
     *
     * @param addr Address.
     * @param len Length.
     * @return address.
     */
    public long reallocateMemory(long addr, long len) {
        return UNSAFE.reallocateMemory(addr, len);
    }

    /**
     * Fills memory with given value.
     *
     * @param addr Address.
     * @param len Length.
     * @param val Value.
     */
    public void setMemory(long addr, long len, byte val) {
        UNSAFE.setMemory(addr, len, val);
    }

    /**
     * Copy memory between offheap locations.
     *
     * @param srcAddr Source address.
     * @param dstAddr Destination address.
     * @param len Length.
     */
    public void copyOffheapOffheap(long srcAddr, long dstAddr, long len) {
        if (len <= PER_BYTE_THRESHOLD) {
            for (int i = 0; i < len; i++)
                UNSAFE.putByte(dstAddr + i, UNSAFE.getByte(srcAddr + i));
        }
        else
            UNSAFE.copyMemory(srcAddr, dstAddr, len);
    }

    /**
     * Copy memory from offheap to heap.
     *
     * @param srcAddr Source address.
     * @param dstBase Destination base.
     * @param dstOff Destination offset.
     * @param len Length.
     */
    public void copyOffheapHeap(long srcAddr, Object dstBase, long dstOff, long len) {
        if (len <= PER_BYTE_THRESHOLD) {
            for (int i = 0; i < len; i++)
                UNSAFE.putByte(dstBase, dstOff + i, UNSAFE.getByte(srcAddr + i));
        }
        else
            UNSAFE.copyMemory(null, srcAddr, dstBase, dstOff, len);
    }

    /**
     * Copy memory from heap to offheap.
     *
     * @param srcBase Source base.
     * @param srcOff Source offset.
     * @param dstAddr Destination address.
     * @param len Length.
     */
    public void copyHeapOffheap(Object srcBase, long srcOff, long dstAddr, long len) {
        if (len <= PER_BYTE_THRESHOLD) {
            for (int i = 0; i < len; i++)
                UNSAFE.putByte(dstAddr + i, UNSAFE.getByte(srcBase, srcOff + i));
        }
        else
            UNSAFE.copyMemory(srcBase, srcOff, null, dstAddr, len);
    }

    /**
     * Copies memory.
     *
     * @param src Source.
     * @param dst Dst.
     * @param len Length.
     */
    public void copyMemory(long src, long dst, long len) {
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
    public void copyMemory(Object srcBase, long srcOff, Object dstBase, long dstOff, long len) {
        if (len <= PER_BYTE_THRESHOLD && srcBase != null && dstBase != null) {
            for (int i = 0; i < len; i++)
                UNSAFE.putByte(dstBase, dstOff + i, UNSAFE.getByte(srcBase, srcOff + i));
        }
        else
            UNSAFE.copyMemory(srcBase, srcOff, dstBase, dstOff, len);
    }

    /**
     * Frees off-heap memory.
     *
     * @param addr Address.
     */
    public void freeMemory(long addr) {
        UNSAFE.freeMemory(addr);
    }

    public void freeUnsafeMemory(long addr) {
        /* do nothing */
    }

    public void freeMemoryBlocks() {
        /* do nothing */
    }

    public void freeMemoryBlock(long addr) {
        /* do nothing */
    }

    /**
     * Returns the offset of the first element in the storage allocation of a given array class.
     *
     * @param cls Class.
     * @return the offset of the first element in the storage allocation of a given array class.
     */
    public int arrayBaseOffset(Class cls) {
        return UNSAFE.arrayBaseOffset(cls);
    }

    /**
     * Allocates instance of given class.
     *
     * @param cls Class.
     * @return Allocated instance.
     */
    public Object allocateInstance(Class cls) throws InstantiationException {
        return UNSAFE.allocateInstance(cls);
    }

    /**
     * Acquires monitor lock.
     *
     * @param obj Object.
     */
    public void monitorEnter(Object obj) {
        UNSAFE.monitorEnter(obj);
    }

    /**
     * Releases monitor lock.
     *
     * @param obj Object.
     */
    public void monitorExit(Object obj) {
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
    public boolean compareAndSwapInt(Object obj, long off, int exp, int upd) {
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
    public boolean compareAndSwapLong(Object obj, long off, long exp, long upd) {
        return UNSAFE.compareAndSwapLong(obj, off, exp, upd);
    }

    /**
     * Gets byte value with volatile semantic.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Byte value.
     */
    public byte getByteVolatile(Object obj, long off) {
        return UNSAFE.getByteVolatile(obj, off);
    }

    /**
     * Stores byte value with volatile semantic.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public void putByteVolatile(Object obj, long off, byte val) {
        UNSAFE.putByteVolatile(obj, off, val);
    }

    /**
     * Gets integer value with volatile semantic.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Integer value.
     */
    public int getIntVolatile(Object obj, long off) {
        return UNSAFE.getIntVolatile(obj, off);
    }

    /**
     * Stores integer value with volatile semantic.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public void putIntVolatile(Object obj, long off, int val) {
        UNSAFE.putIntVolatile(obj, off, val);
    }

    /**
     * Gets long value with volatile semantic.
     *
     * @param obj Object.
     * @param off Offset.
     * @return Long value.
     */
    public long getLongVolatile(Object obj, long off) {
        return UNSAFE.getLongVolatile(obj, off);
    }

    /**
     * Stores long value with volatile semantic.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public void putLongVolatile(Object obj, long off, long val) {
        UNSAFE.putLongVolatile(obj, off, val);
    }

    /**
     * Stores reference value with volatile semantic.
     *
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     */
    public void putObjectVolatile(Object obj, long off, Object val) {
        UNSAFE.putObjectVolatile(obj, off, val);
    }

    /**
     * Returns unaligned flag.
     */
    private static boolean unaligned() {
        String arch = System.getProperty("os.arch");

        boolean res = arch.equals("i386") || arch.equals("x86") || arch.equals("amd64") || arch.equals("x86_64");

        if (!res)
            res = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_MEMORY_UNALIGNED_ACCESS, false);

        return res;
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
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static short getShortByByte(Object obj, long off, boolean bigEndian) {
        if (bigEndian)
            return (short)(UNSAFE.getByte(obj, off) << 8 | (UNSAFE.getByte(obj, off + 1) & 0xff));
        else
            return (short)(UNSAFE.getByte(obj, off + 1) << 8 | (UNSAFE.getByte(obj, off) & 0xff));
    }

    /**
     * @param obj Object.
     * @param off Offset.
     * @param val Value.
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static void putShortByByte(Object obj, long off, short val, boolean bigEndian) {
        if (bigEndian) {
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
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static char getCharByByte(Object obj, long off, boolean bigEndian) {
        if (bigEndian)
            return (char)(UNSAFE.getByte(obj, off) << 8 | (UNSAFE.getByte(obj, off + 1) & 0xff));
        else
            return (char)(UNSAFE.getByte(obj, off + 1) << 8 | (UNSAFE.getByte(obj, off) & 0xff));
    }

    /**
     * @param obj Object.
     * @param addr Address.
     * @param val Value.
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static void putCharByByte(Object obj, long addr, char val, boolean bigEndian) {
        if (bigEndian) {
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
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static int getIntByByte(Object obj, long addr, boolean bigEndian) {
        if (bigEndian) {
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
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static void putIntByByte(Object obj, long addr, int val, boolean bigEndian) {
        if (bigEndian) {
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
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static long getLongByByte(Object obj, long addr, boolean bigEndian) {
        if (bigEndian) {
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
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static void putLongByByte(Object obj, long addr, long val, boolean bigEndian) {
        if (bigEndian) {
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
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static short getShortByByte(long addr, boolean bigEndian) {
        if (bigEndian)
            return (short)(UNSAFE.getByte(addr) << 8 | (UNSAFE.getByte(addr + 1) & 0xff));
        else
            return (short)(UNSAFE.getByte(addr + 1) << 8 | (UNSAFE.getByte(addr) & 0xff));
    }

    /**
     * @param addr Address.
     * @param val Value.
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static void putShortByByte(long addr, short val, boolean bigEndian) {
        if (bigEndian) {
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
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static char getCharByByte(long addr, boolean bigEndian) {
        if (bigEndian)
            return (char)(UNSAFE.getByte(addr) << 8 | (UNSAFE.getByte(addr + 1) & 0xff));
        else
            return (char)(UNSAFE.getByte(addr + 1) << 8 | (UNSAFE.getByte(addr) & 0xff));
    }

    /**
     * @param addr Address.
     * @param val Value.
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static void putCharByByte(long addr, char val, boolean bigEndian) {
        if (bigEndian) {
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
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static int getIntByByte(long addr, boolean bigEndian) {
        if (bigEndian) {
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
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static void putIntByByte(long addr, int val, boolean bigEndian) {
        if (bigEndian) {
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
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static long getLongByByte(long addr, boolean bigEndian) {
        if (bigEndian) {
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
     * @param bigEndian Order of value bytes in memory. If {@code true} - big-endian, otherwise little-endian.
     */
    private static void putLongByByte(long addr, long val, boolean bigEndian) {
        if (bigEndian) {
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

    /**
     * @param buf Direct buffer.
     * @return Buffer memory address.
     */
    public long bufferAddress(ByteBuffer buf) {
        return ((DirectBuffer)buf).address();
    }
}
