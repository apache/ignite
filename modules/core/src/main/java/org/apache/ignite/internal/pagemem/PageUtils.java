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

package org.apache.ignite.internal.pagemem;

import org.apache.ignite.internal.util.GridUnsafe;

/**
 *
 */
public class PageUtils {
    /**
     * @param addr Start address. 
     * @param off Offset.
     * @return Byte value from given address.
     */
    public static byte getByte(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        return GridUnsafe.getByte(addr + off);
    }

    /**
     *
     * @param addr Start address.
     * @param off Offset.
     * @return Byte value from given address.
     */
    public static int getUnsignedByte(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        return GridUnsafe.getByte(addr + off) & 0xFF;
    }

    /**
     * @param addr Start address.
     * @param off Offset.
     * @param len Bytes length.
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
     * @param srcAddr Source address.
     * @param srcOff Source offset.
     * @param dst Destination array.
     * @param dstOff Destination offset.
     * @param len Length.
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
     * @param addr Address.
     * @param off Offset.
     * @return Value.
     */
    public static short getShort(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        return GridUnsafe.getShort(addr + off);
    }

    /**
     * @param addr Address.
     * @param off Offset.
     * @return Value.
     */
    public static int getInt(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        return GridUnsafe.getInt(addr + off);
    }

    /**
     * @param addr Address.
     * @param off Offset.
     * @return Value.
     */
    public static long getLong(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        return GridUnsafe.getLong(addr + off);
    }

    /**
     * @param addr Address/
     * @param off Offset.
     * @param bytes Bytes.
     */
    public static void putBytes(long addr, int off, byte[] bytes) {
        assert addr > 0 : addr;
        assert off >= 0;
        assert bytes != null;

        GridUnsafe.copyMemory(bytes, GridUnsafe.BYTE_ARR_OFF, null, addr + off, bytes.length);
    }

    /**
     * @param addr Address.
     * @param off Offset.
     * @param bytes Bytes array.
     * @param bytesOff Bytes array offset.
     */
    public static void putBytes(long addr, int off, byte[] bytes, int bytesOff) {
        assert addr > 0 : addr;
        assert off >= 0;
        assert bytes != null;
        assert bytesOff >= 0 && (bytesOff < bytes.length || bytes.length == 0) : bytesOff;

        GridUnsafe.copyMemory(bytes, GridUnsafe.BYTE_ARR_OFF + bytesOff, null, addr + off, bytes.length - bytesOff);
    }

    /**
     * @param addr Address.
     * @param off Offset.
     * @param bytes Bytes array.
     * @param bytesOff Bytes array offset.
     * @param len Length.
     */
    public static void putBytes(long addr, int off, byte[] bytes, int bytesOff, int len) {
        assert addr > 0 : addr;
        assert off >= 0;
        assert bytes != null;
        assert bytesOff >= 0 && (bytesOff < bytes.length || bytes.length == 0) : bytesOff;

        GridUnsafe.copyMemory(bytes, GridUnsafe.BYTE_ARR_OFF + bytesOff, null, addr + off, len);
    }

    /**
     * @param addr Address.
     * @param off Offset.
     * @param v Value.
     */
    public static void putByte(long addr, int off, byte v) {
        assert addr > 0 : addr;
        assert off >= 0;

        GridUnsafe.putByte(addr + off, v);
    }

    /**
     * @param addr Address.
     * @param off Offset.
     * @param v Value.
     */
    public static void putUnsignedByte(long addr, int off, int v) {
        assert addr > 0 : addr;
        assert off >= 0;
        assert v >= 0 && v <= 255;

        GridUnsafe.putByte(addr + off, (byte) v);
    }

    /**
     * @param addr Address.
     * @param off Offset.
     * @param v Value.
     */
    public static void putShort(long addr, int off, short v) {
        assert addr > 0 : addr;
        assert off >= 0;

        GridUnsafe.putShort(addr + off, v);
    }

    /**
     * @param addr Address.
     * @param off Offset.
     * @param v Value.
     */
    public static void putInt(long addr, int off, int v) {
        assert addr > 0 : addr;
        assert off >= 0;

        GridUnsafe.putInt(addr + off, v);
    }

    /**
     * @param addr Address.
     * @param off Offset.
     * @param v Value.
     */
    public static void putLong(long addr, int off, long v) {
        assert addr > 0 : addr;
        assert off >= 0;

        GridUnsafe.putLong(addr + off, v);
    }
}
