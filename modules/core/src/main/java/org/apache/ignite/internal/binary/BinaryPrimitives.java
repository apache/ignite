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

import org.apache.ignite.binary.BinaryObjectException;
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
        GridUnsafe.putByte(arr, GridUnsafe.BYTE_ARR_OFF + off, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static byte readByte(byte[] arr, int off) {
        return GridUnsafe.getByte(arr, GridUnsafe.BYTE_ARR_OFF + off);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static byte readByte(long ptr, int off) {
        return GridUnsafe.getByte(ptr + off);
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

        GridUnsafe.copyOffheapHeap(ptr + off, arr0, GridUnsafe.BYTE_ARR_OFF, len);

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
        long pos = GridUnsafe.BYTE_ARR_OFF + off;

        if (BIG_ENDIAN)
            GridUnsafe.putShortLE(arr, pos, val);
        else
            GridUnsafe.putShort(arr, pos, val);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeShort(long ptr, int off, short val) {
        if (BIG_ENDIAN)
            GridUnsafe.putShortLE(ptr + off, val);
        else
            GridUnsafe.putShort(ptr + off, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static short readShort(byte[] arr, int off) {
        long pos = GridUnsafe.BYTE_ARR_OFF + off;

        return BIG_ENDIAN ? GridUnsafe.getShortLE(arr, pos) : GridUnsafe.getShort(arr, pos);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static short readShort(long ptr, int off) {
        long addr = ptr + off;

        return BIG_ENDIAN ? GridUnsafe.getShortLE(addr) : GridUnsafe.getShort(addr);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeChar(byte[] arr, int off, char val) {
        long pos = GridUnsafe.BYTE_ARR_OFF + off;

        if (BIG_ENDIAN)
            GridUnsafe.putCharLE(arr, pos, val);
        else
            GridUnsafe.putChar(arr, pos, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static char readChar(byte[] arr, int off) {
        long pos = GridUnsafe.BYTE_ARR_OFF + off;

        return BIG_ENDIAN ? GridUnsafe.getCharLE(arr, pos): GridUnsafe.getChar(arr, pos);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static char readChar(long ptr, int off) {
        long addr = ptr + off;

        return BIG_ENDIAN ? GridUnsafe.getCharLE(addr) : GridUnsafe.getChar(addr);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static char[] readCharArray(byte[] arr, int off, int len) {
        char[] arr0 = new char[len];

        GridUnsafe.copyMemory(arr, GridUnsafe.BYTE_ARR_OFF + off, arr0, GridUnsafe.CHAR_ARR_OFF, len << 1);

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

        GridUnsafe.copyOffheapHeap(ptr + off, arr0, GridUnsafe.CHAR_ARR_OFF, len << 1);

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
        long pos = GridUnsafe.BYTE_ARR_OFF + off;

        if (BIG_ENDIAN)
            GridUnsafe.putIntLE(arr, pos, val);
        else
            GridUnsafe.putInt(arr, pos, val);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeInt(long ptr, int off, int val) {
        if (BIG_ENDIAN)
            GridUnsafe.putIntLE(ptr + off, val);
        else
            GridUnsafe.putInt(ptr + off, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static int readInt(byte[] arr, int off) {
        long pos = GridUnsafe.BYTE_ARR_OFF + off;

        return BIG_ENDIAN ? GridUnsafe.getIntLE(arr, pos) : GridUnsafe.getInt(arr, pos);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static int readInt(long ptr, int off) {
        long addr = ptr + off;

        return BIG_ENDIAN ? GridUnsafe.getIntLE(addr) : GridUnsafe.getInt(addr);
    }

    /**
     * Reads integer value which is presented in varint encoding.
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">More information about varint.</a>
     *
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     * @throws BinaryObjectException if have been read more than 5 bytes.
     */
    public static int readVarint(long ptr, int off) throws BinaryObjectException {
        int val = 0;
        int n = 0;
        int b;

        while (((b = readByte(ptr, off++)) & 0x80) != 0) {
            val |= (b & 0x7F) << n;
            n += 7;

            if (n > 35)
                throw new BinaryObjectException("Failed to read varint, variable length is too long");
        }

        return val | (b << n);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public static void writeLong(byte[] arr, int off, long val) {
        long pos = GridUnsafe.BYTE_ARR_OFF + off;

        if (BIG_ENDIAN)
            GridUnsafe.putLongLE(arr, pos, val);
        else
            GridUnsafe.putLong(arr, pos, val);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public static long readLong(byte[] arr, int off) {
        long pos = GridUnsafe.BYTE_ARR_OFF + off;

        return BIG_ENDIAN ? GridUnsafe.getLongLE(arr, pos) : GridUnsafe.getLong(arr, pos);
    }

    /**
     * @param ptr Pointer.
     * @param off Offset.
     * @return Value.
     */
    public static long readLong(long ptr, int off) {
        long addr = ptr + off;

        return BIG_ENDIAN ? GridUnsafe.getLongLE(addr) : GridUnsafe.getLong(addr);
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
