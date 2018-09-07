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

import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.GridUnsafe;
import static org.apache.ignite.internal.util.GridUnsafe.BIG_ENDIAN;

/**
 * Primitives writer.
 */
public abstract class BinaryPrimitives {
    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeByte(byte[] arr, int off, byte val) {
        Ignition.GRID_UNSAFE.putByte(arr, GridUnsafe.BYTE_ARR_OFF + off, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static byte readByte(byte[] arr, int off) {
        return Ignition.GRID_UNSAFE.getByte(arr, GridUnsafe.BYTE_ARR_OFF + off);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static byte readByte(long ptr, int off) {
        return Ignition.GRID_UNSAFE.getByte(ptr + off);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static byte[] readByteArray(byte[] arr, int off, int len) {
        byte[] arr0 = new byte[len];

        System.arraycopy(arr, off, arr0, 0, len);

        return arr0;
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static byte[] readByteArray(long ptr, int off, int len) {
        byte[] arr0 = new byte[len];

        Ignition.GRID_UNSAFE.copyOffheapHeap(ptr + off, arr0, GridUnsafe.BYTE_ARR_OFF, len);

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
        long pos = Ignition.GRID_UNSAFE.BYTE_ARR_OFF + off;

        if (BIG_ENDIAN)
            Ignition.GRID_UNSAFE.putShortLE(arr, pos, val);
        else
            Ignition.GRID_UNSAFE.putShort(arr, pos, val);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeShort(long ptr, int off, short val) {
        if (BIG_ENDIAN)
            Ignition.GRID_UNSAFE.putShortLE(ptr + off, val);
        else
            Ignition.GRID_UNSAFE.putShort(ptr + off, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static short readShort(byte[] arr, int off) {
        long pos = Ignition.GRID_UNSAFE.BYTE_ARR_OFF + off;

        return BIG_ENDIAN ? Ignition.GRID_UNSAFE.getShortLE(arr, pos) : Ignition.GRID_UNSAFE.getShort(arr, pos);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static short readShort(long ptr, int off) {
        long addr = ptr + off;

        return BIG_ENDIAN ? Ignition.GRID_UNSAFE.getShortLE(addr) : Ignition.GRID_UNSAFE.getShort(addr);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeChar(byte[] arr, int off, char val) {
        long pos = Ignition.GRID_UNSAFE.BYTE_ARR_OFF + off;

        if (BIG_ENDIAN)
            Ignition.GRID_UNSAFE.putCharLE(arr, pos, val);
        else
            Ignition.GRID_UNSAFE.putChar(arr, pos, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static char readChar(byte[] arr, int off) {
        long pos = Ignition.GRID_UNSAFE.BYTE_ARR_OFF + off;

        return BIG_ENDIAN ? Ignition.GRID_UNSAFE.getCharLE(arr, pos): Ignition.GRID_UNSAFE.getChar(arr, pos);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static char readChar(long ptr, int off) {
        long addr = ptr + off;

        return BIG_ENDIAN ? Ignition.GRID_UNSAFE.getCharLE(addr) : Ignition.GRID_UNSAFE.getChar(addr);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static char[] readCharArray(byte[] arr, int off, int len) {
        char[] arr0 = new char[len];

        Ignition.GRID_UNSAFE.copyMemory(arr, Ignition.GRID_UNSAFE.BYTE_ARR_OFF + off, arr0, Ignition.GRID_UNSAFE.CHAR_ARR_OFF, len << 1);

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

        Ignition.GRID_UNSAFE.copyOffheapHeap(ptr + off, arr0, Ignition.GRID_UNSAFE.CHAR_ARR_OFF, len << 1);

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
        long pos = Ignition.GRID_UNSAFE.BYTE_ARR_OFF + off;

        if (BIG_ENDIAN)
            Ignition.GRID_UNSAFE.putIntLE(arr, pos, val);
        else
            Ignition.GRID_UNSAFE.putInt(arr, pos, val);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeInt(long ptr, int off, int val) {
        if (BIG_ENDIAN)
            Ignition.GRID_UNSAFE.putIntLE(ptr + off, val);
        else
            Ignition.GRID_UNSAFE.putInt(ptr + off, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static int readInt(byte[] arr, int off) {
        long pos = Ignition.GRID_UNSAFE.BYTE_ARR_OFF + off;

        return BIG_ENDIAN ? Ignition.GRID_UNSAFE.getIntLE(arr, pos) : Ignition.GRID_UNSAFE.getInt(arr, pos);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static int readInt(long ptr, int off) {
        long addr = ptr + off;

        return BIG_ENDIAN ? Ignition.GRID_UNSAFE.getIntLE(addr) : Ignition.GRID_UNSAFE.getInt(addr);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeLong(byte[] arr, int off, long val) {
        long pos = Ignition.GRID_UNSAFE.BYTE_ARR_OFF + off;

        if (BIG_ENDIAN)
            Ignition.GRID_UNSAFE.putLongLE(arr, pos, val);
        else
            Ignition.GRID_UNSAFE.putLong(arr, pos, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static long readLong(byte[] arr, int off) {
        long pos = Ignition.GRID_UNSAFE.BYTE_ARR_OFF + off;

        return BIG_ENDIAN ? Ignition.GRID_UNSAFE.getLongLE(arr, pos) : Ignition.GRID_UNSAFE.getLong(arr, pos);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static long readLong(long ptr, int off) {
        long addr = ptr + off;

        return BIG_ENDIAN ? Ignition.GRID_UNSAFE.getLongLE(addr) : Ignition.GRID_UNSAFE.getLong(addr);
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
