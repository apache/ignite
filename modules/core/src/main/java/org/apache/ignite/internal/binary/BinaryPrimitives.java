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

package org.apache.ignite.internal.binary;

import org.apache.ignite.internal.util.GridUnsafe;
import sun.misc.Unsafe;

import java.nio.ByteOrder;

/**
 * Primitives writer.
 */
public abstract class BinaryPrimitives {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** */
    private static final long CHAR_ARR_OFF = UNSAFE.arrayBaseOffset(char[].class);

    /** Whether little endian is set. */
    private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeByte(byte[] arr, int off, byte val) {
        UNSAFE.putByte(arr, BYTE_ARR_OFF + off, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static byte readByte(byte[] arr, int off) {
        return UNSAFE.getByte(arr, BYTE_ARR_OFF + off);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static byte readByte(long ptr, int off) {
        return UNSAFE.getByte(ptr + off);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static byte[] readByteArray(byte[] arr, int off, int len) {
        byte[] arr0 = new byte[len];

        UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, BYTE_ARR_OFF, len);

        return arr0;
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static byte[] readByteArray(long ptr, int off, int len) {
        byte[] arr0 = new byte[len];

        UNSAFE.copyMemory(null, ptr + off, arr0, BYTE_ARR_OFF, len);

        return arr0;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeBoolean(byte[] arr, int off, boolean val) {
        writeByte(arr, off, val ? (byte)1 : (byte)0);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static boolean readBoolean(byte[] arr, int off) {
        return readByte(arr, off) == 1;
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static boolean readBoolean(long ptr, int off) {
        return readByte(ptr, off) == 1;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeShort(byte[] arr, int off, short val) {
        if (BIG_ENDIAN)
            val = Short.reverseBytes(val);

        UNSAFE.putShort(arr, BYTE_ARR_OFF + off, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static short readShort(byte[] arr, int off) {
        short val = UNSAFE.getShort(arr, BYTE_ARR_OFF + off);

        if (BIG_ENDIAN)
            val = Short.reverseBytes(val);

        return val;
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static short readShort(long ptr, int off) {
        short val = UNSAFE.getShort(ptr + off);

        if (BIG_ENDIAN)
            val = Short.reverseBytes(val);

        return val;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeChar(byte[] arr, int off, char val) {
        if (BIG_ENDIAN)
            val = Character.reverseBytes(val);

        UNSAFE.putChar(arr, BYTE_ARR_OFF + off, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static char readChar(byte[] arr, int off) {
        char val = UNSAFE.getChar(arr, BYTE_ARR_OFF + off);

        if (BIG_ENDIAN)
            val = Character.reverseBytes(val);

        return val;
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static char readChar(long ptr, int off) {
        char val = UNSAFE.getChar(ptr + off);

        if (BIG_ENDIAN)
            val = Character.reverseBytes(val);

        return val;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static char[] readCharArray(byte[] arr, int off, int len) {
        char[] arr0 = new char[len];

        UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, CHAR_ARR_OFF, len << 1);

        if (BIG_ENDIAN) {
            for (int i = 0; i < len; i++)
                arr0[i] = Character.reverseBytes(arr0[i]);
        }

        return arr0;
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static char[] readCharArray(long ptr, int off, int len) {
        char[] arr0 = new char[len];

        UNSAFE.copyMemory(null, ptr + off, arr0, CHAR_ARR_OFF, len << 1);

        if (BIG_ENDIAN) {
            for (int i = 0; i < len; i++)
                arr0[i] = Character.reverseBytes(arr0[i]);
        }

        return arr0;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeInt(byte[] arr, int off, int val) {
        if (BIG_ENDIAN)
            val = Integer.reverseBytes(val);

        UNSAFE.putInt(arr, BYTE_ARR_OFF + off, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static int readInt(byte[] arr, int off) {
        int val = UNSAFE.getInt(arr, BYTE_ARR_OFF + off);

        if (BIG_ENDIAN)
            val = Integer.reverseBytes(val);

        return val;
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static int readInt(long ptr, int off) {
        int val = UNSAFE.getInt(ptr + off);

        if (BIG_ENDIAN)
            val = Integer.reverseBytes(val);

        return val;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeLong(byte[] arr, int off, long val) {
        if (BIG_ENDIAN)
            val = Long.reverseBytes(val);

        UNSAFE.putLong(arr, BYTE_ARR_OFF + off, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static long readLong(byte[] arr, int off) {
        long val = UNSAFE.getLong(arr, BYTE_ARR_OFF + off);

        if (BIG_ENDIAN)
            val = Long.reverseBytes(val);

        return val;
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static long readLong(long ptr, int off) {
        long val = UNSAFE.getLong(ptr + off);

        if (BIG_ENDIAN)
            val = Long.reverseBytes(val);

        return val;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeFloat(byte[] arr, int off, float val) {
        int val0 = Float.floatToIntBits(val);

        writeInt(arr, off, val0);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static float readFloat(byte[] arr, int off) {
        int val = readInt(arr, off);

        return Float.intBitsToFloat(val);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static float readFloat(long ptr, int off) {
        int val = readInt(ptr, off);

        return Float.intBitsToFloat(val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeDouble(byte[] arr, int off, double val) {
        long val0 = Double.doubleToLongBits(val);

        writeLong(arr, off, val0);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static double readDouble(byte[] arr, int off) {
        long val = readLong(arr, off);

        return Double.longBitsToDouble(val);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static double readDouble(long ptr, int off) {
        long val = readLong(ptr, off);

        return Double.longBitsToDouble(val);
    }
}
