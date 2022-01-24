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

package org.apache.ignite.internal.pagememory.util;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Page utils.
 */
public class PageUtils {
    /**
     * Reads a byte from the memory.
     *
     * @param addr Start address.
     * @param off  Offset.
     * @return Byte value from given address.
     */
    public static byte getByte(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        return GridUnsafe.getByte(addr + off);
    }

    /**
     * Reads an unsigned byte from the memory.
     *
     * @param addr Start address.
     * @param off  Offset.
     * @return Byte value from given address.
     */
    public static int getUnsignedByte(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        return GridUnsafe.getByte(addr + off) & 0xFF;
    }

    /**
     * Reads a byte array from the memory.
     *
     * @param addr Start address.
     * @param off  Offset.
     * @param len  Bytes length.
     * @return Bytes from given address.
     */
    public static byte[] getBytes(long addr, int off, int len) {
        assert addr > 0 : addr;
        assert off >= 0;
        assert len >= 0;

        byte[] bytes = new byte[len];

        GridUnsafe.copyMemory(null, addr + off, bytes, GridUnsafe.BYTE_ARR_OFF, len);

        return bytes;
    }

    /**
     * Reads a byte array from the memory.
     *
     * @param srcAddr Source address.
     * @param srcOff  Source offset.
     * @param dst     Destination array.
     * @param dstOff  Destination offset.
     * @param len     Length.
     */
    public static void getBytes(long srcAddr, int srcOff, byte[] dst, int dstOff, int len) {
        assert srcAddr > 0;
        assert srcOff > 0;
        assert dst != null;
        assert dstOff >= 0;
        assert len >= 0;

        GridUnsafe.copyMemory(null, srcAddr + srcOff, dst, GridUnsafe.BYTE_ARR_OFF + dstOff, len);
    }

    /**
     * Reads a short value from the memory.
     *
     * @param addr Address.
     * @param off  Offset.
     * @return Value.
     */
    public static short getShort(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        return GridUnsafe.getShort(addr + off);
    }

    /**
     * Reads an int value from the memory.
     *
     * @param addr Address.
     * @param off  Offset.
     * @return Value.
     */
    public static int getInt(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        return GridUnsafe.getInt(addr + off);
    }

    /**
     * Reads a long value from the memory.
     *
     * @param addr Address.
     * @param off  Offset.
     * @return Value.
     */
    public static long getLong(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        return GridUnsafe.getLong(addr + off);
    }

    /**
     * Writes a byte array into the memory.
     *
     * @param addr  Address.
     * @param off   Offset.
     * @param bytes Bytes.
     */
    public static void putBytes(long addr, int off, byte[] bytes) {
        putBytes(addr, off, bytes, 0, bytes.length);
    }

    /**
     * Writes a byte array into the memory.
     *
     * @param addr     Address.
     * @param off      Offset.
     * @param bytes    Bytes array.
     * @param bytesOff Bytes array offset.
     */
    public static void putBytes(long addr, int off, byte[] bytes, int bytesOff) {
        putBytes(addr, off, bytes, bytesOff, bytes.length - bytesOff);
    }

    /**
     * Writes a byte array into the memory.
     *
     * @param addr     Address.
     * @param off      Offset.
     * @param bytes    Bytes array.
     * @param bytesOff Bytes array offset.
     * @param len      Length.
     */
    public static void putBytes(long addr, int off, byte[] bytes, int bytesOff, int len) {
        assert addr > 0 : addr;
        assert off >= 0;
        assert bytes != null;
        assert bytesOff >= 0 && (bytesOff < bytes.length || bytes.length == 0) : bytesOff;
        assert len >= 0 && (bytesOff + len <= bytes.length);

        GridUnsafe.copyMemory(bytes, GridUnsafe.BYTE_ARR_OFF + bytesOff, null, addr + off, len);
    }

    /**
     * Writes a byte into the memory.
     *
     * @param addr Address.
     * @param off  Offset.
     * @param v    Value.
     */
    public static void putByte(long addr, int off, byte v) {
        assert addr > 0 : addr;
        assert off >= 0;

        GridUnsafe.putByte(addr + off, v);
    }

    /**
     * Writes an unsigned byte array into the memory.
     *
     * @param addr Address.
     * @param off  Offset.
     * @param v    Value.
     */
    public static void putUnsignedByte(long addr, int off, int v) {
        assert addr > 0 : addr;
        assert off >= 0;
        assert v >= 0 && v <= 255;

        GridUnsafe.putByte(addr + off, (byte) v);
    }

    /**
     * Writes a short value into the memory.
     *
     * @param addr Address.
     * @param off  Offset.
     * @param v    Value.
     */
    public static void putShort(long addr, int off, short v) {
        assert addr > 0 : addr;
        assert off >= 0;

        GridUnsafe.putShort(addr + off, v);
    }

    /**
     * Writes an int value into the memory.
     *
     * @param addr Address.
     * @param off  Offset.
     * @param v    Value.
     */
    public static void putInt(long addr, int off, int v) {
        assert addr > 0 : addr;
        assert off >= 0;

        GridUnsafe.putInt(addr + off, v);
    }

    /**
     * Writes a long value into the memory.
     *
     * @param addr Address.
     * @param off  Offset.
     * @param v    Value.
     */
    public static void putLong(long addr, int off, long v) {
        assert addr > 0 : addr;
        assert off >= 0;

        GridUnsafe.putLong(addr + off, v);
    }

    /**
     * Copies memory from one buffer to another.
     *
     * @param src    Source.
     * @param srcOff Source offset in bytes.
     * @param dst    Destination.
     * @param dstOff Destination offset in bytes.
     * @param cnt    Bytes count to copy.
     */
    public static void copyMemory(ByteBuffer src, long srcOff, ByteBuffer dst, long dstOff, long cnt) {
        byte[] srcArr = src.hasArray() ? src.array() : null;
        byte[] dstArr = dst.hasArray() ? dst.array() : null;
        long srcArrOff = src.hasArray() ? src.arrayOffset() + GridUnsafe.BYTE_ARR_OFF : 0;
        long dstArrOff = dst.hasArray() ? dst.arrayOffset() + GridUnsafe.BYTE_ARR_OFF : 0;

        long srcPtr = src.isDirect() ? GridUnsafe.bufferAddress(src) : 0;
        long dstPtr = dst.isDirect() ? GridUnsafe.bufferAddress(dst) : 0;

        GridUnsafe.copyMemory(srcArr, srcPtr + srcArrOff + srcOff, dstArr, dstPtr + dstArrOff + dstOff, cnt);
    }

    /**
     * Copies memory from one address to another.
     *
     * @param srcAddr Source.
     * @param srcOff  Source offset in bytes.
     * @param dstAddr Destination.
     * @param dstOff  Destination offset in bytes.
     * @param cnt     Bytes count to copy.
     */
    public static void copyMemory(long srcAddr, long srcOff, long dstAddr, long dstOff, long cnt) {
        GridUnsafe.copyMemory(null, srcAddr + srcOff, null, dstAddr + dstOff, cnt);
    }
}
