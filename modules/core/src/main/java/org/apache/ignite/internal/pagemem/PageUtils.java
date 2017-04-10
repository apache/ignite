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

import java.util.UUID;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class PageUtils {
    /** */
    private final static int IGNITE_UUID_NULL_SIZE = 1;

    /** */
    public final static int IGNITE_UUID_SIZE = 1 /* Null indicator */ + 8 /* MostSignificantBits */
        + 8 /* LeastSignificantBits */ + 8 /* Local ID */;

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

    /**
     * @param addr Address.
     * @param v Value.
     */
    public static int putIgniteUuid(long addr, @Nullable IgniteUuid v) {
        assert addr > 0 : addr;

        GridUnsafe.putByte(addr, (v == null ? (byte)0 : (byte)1));
        addr += 1;

        if (v != null) {
            GridUnsafe.putLong(addr, v.globalId().getMostSignificantBits());
            addr+=8;

            GridUnsafe.putLong(addr, v.globalId().getLeastSignificantBits());
            addr+=8;

            GridUnsafe.putLong(addr, v.localId());
        }
        else
            return IGNITE_UUID_NULL_SIZE;

        return IGNITE_UUID_SIZE;
    }

    /**
     * @param addr Address.
     * @param off Offset.
     * @return Value.
     */
    public static IgniteUuid getIgniteUuid(long addr, int off) {
        assert addr > 0 : addr;
        assert off >= 0;

        byte isNull = GridUnsafe.getByte(addr + off);

        if (isNull == 0)
            return null;

        addr+=1;

        long mostSignBits = GridUnsafe.getLong(addr + off);
        addr+=8;

        long lestSignBits = GridUnsafe.getLong(addr + off);
        addr+=8;

        long locId = GridUnsafe.getLong(addr + off);

        return new IgniteUuid(new UUID(mostSignBits, lestSignBits), locId);
    }

    /**
     * @param v Ignite UUID.
     * @return Size.
     */
    public static int sizeIgniteUuid(IgniteUuid v) {
        return (v == null ? IGNITE_UUID_NULL_SIZE : IGNITE_UUID_SIZE);
    }

    /**
     * @param b Ignite UUID null flag.
     * @return Size.
     */
    public static int sizeIgniteUuid(Byte b) {
        return (b == 0 ? IGNITE_UUID_NULL_SIZE : IGNITE_UUID_SIZE);
    }

    /**
     * @param v Ignite UUIDs.
     * @param v1 Ignite UUIDs.
     * @return Size.
     */
    public static int sizeIgniteUuids(IgniteUuid v, IgniteUuid v1) {
        return (v == null ? IGNITE_UUID_NULL_SIZE : IGNITE_UUID_SIZE)
            + (v1 == null ? IGNITE_UUID_NULL_SIZE : IGNITE_UUID_SIZE);
    }
}
