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

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import sun.misc.Unsafe;

/**
 * Swap entry.
 */
public class GridCacheSwapEntryImpl implements GridCacheSwapEntry {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** */
    static final int EXPIRE_TIME_OFFSET = 8;

    /** */
    static final int VERSION_OFFSET = 16;

    /** */
    static final int VERSION_SIZE = 24;

    /** */
    static final int VERSION_EX_SIZE = 48;

    /** */
    static final int GUID_SIZE = 24;

    /** Value bytes. */
    private ByteBuffer valBytes;

    /** Value. */
    private CacheObject val;

    /** Type. */
    private byte type;

    /** Class loader ID. */
    private IgniteUuid keyClsLdrId;

    /** Class loader ID. */
    private IgniteUuid valClsLdrId;

    /** Version. */
    private GridCacheVersion ver;

    /** Time to live. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /**
     * @param valBytes Value.
     * @param type Type.
     * @param ver Version.
     * @param ttl Entry time to live.
     * @param expireTime Expire time.
     * @param keyClsLdrId Class loader ID for entry key (can be {@code null} for local class loader).
     * @param valClsLdrId Class loader ID for entry value (can be {@code null} for local class loader).
     */
    public GridCacheSwapEntryImpl(
        ByteBuffer valBytes,
        byte type,
        GridCacheVersion ver,
        long ttl,
        long expireTime,
        @Nullable IgniteUuid keyClsLdrId,
        @Nullable IgniteUuid valClsLdrId) {
        assert ver != null;

        this.valBytes = valBytes;
        this.type = type;
        this.ver = ver;
        this.ttl = ttl;
        this.expireTime = expireTime;
        this.valClsLdrId = valClsLdrId;
        this.keyClsLdrId = keyClsLdrId;
    }

    /**
     * @param bytes Entry bytes.
     * @return TTL.
     */
    public static long timeToLive(byte[] bytes) {
        return UNSAFE.getLong(bytes, BYTE_ARR_OFF);
    }

    /**
     * @param bytes Entry bytes.
     * @return Expire time.
     */
    public static long expireTime(byte[] bytes) {
        return UNSAFE.getLong(bytes, BYTE_ARR_OFF + EXPIRE_TIME_OFFSET);
    }

    /**
     * @param bytes Entry bytes.
     * @return Version.
     */
    public static GridCacheVersion version(byte[] bytes) {
        int off = VERSION_OFFSET; // Skip ttl, expire time.

        boolean verEx = bytes[off++] != 0;

        return U.readVersion(bytes, off, verEx);
    }

    /**
     * @param bytes Entry bytes.
     * @return Value if value is byte array, otherwise {@code null}.
     */
    @Nullable public static IgniteBiTuple<byte[], Byte> getValue(byte[] bytes) {
        long off = BYTE_ARR_OFF + VERSION_OFFSET; // Skip ttl, expire time.

        boolean verEx = UNSAFE.getByte(bytes, off++) != 0;

        off += verEx ? VERSION_EX_SIZE : VERSION_SIZE;

        int arrLen = UNSAFE.getInt(bytes, off);

        off += 4;

        byte type = UNSAFE.getByte(bytes, off++);

        byte[] valBytes = new byte[arrLen];

        UNSAFE.copyMemory(bytes, off, valBytes, BYTE_ARR_OFF, arrLen);

        return new IgniteBiTuple<>(valBytes, type);
    }

    /**
     * @param bytes Entry bytes.
     * @return Value bytes offset.
     */
    public static int valueOffset(byte[] bytes) {
        assert bytes.length > 40 : bytes.length;

        int off = VERSION_OFFSET; // Skip ttl, expire time.

        boolean verEx = bytes[off++] != 0;

        off += verEx ? VERSION_EX_SIZE : VERSION_SIZE;

        off += 5; // Byte array flag + array size.

        assert bytes.length >= off;

        return off;
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes() {
        if (valBytes != null) {
            assert valBytes.capacity() == valBytes.limit();

            return valBytes.array();
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void valueBytes(@Nullable byte[] valBytes) {
        this.valBytes = valBytes != null ? ByteBuffer.wrap(valBytes) : null;
    }

    /** {@inheritDoc} */
    @Override public CacheObject value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public void value(CacheObject val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public byte type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public long ttl() {
        return ttl;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return expireTime;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteUuid keyClassLoaderId() {
        return keyClsLdrId;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteUuid valueClassLoaderId() {
        return valClsLdrId;
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        return 0;
    }

    /**
     * @return Entry bytes.
     */
    public byte[] marshal() {
        // Ttl + expire time + Ex Version flag + Version.
        int size = 16 + 1 + ((ver instanceof GridCacheVersionEx) ? VERSION_EX_SIZE : VERSION_SIZE);

        size += 1; // Plain byte array flag.

        int len = valBytes.limit();

        size += len + 4; // Value bytes.

        size += (valClsLdrId == null ? 1 : (1 + GUID_SIZE));

        size += (keyClsLdrId == null ? 1 : (1 + GUID_SIZE));

        byte[] arr = new byte[size];

        long off = BYTE_ARR_OFF;

        UNSAFE.putLong(arr, off, ttl);

        off += 8;

        UNSAFE.putLong(arr, off, expireTime);

        off += 8;

        off = U.writeVersion(arr, off, ver);

        UNSAFE.putInt(arr, off, len);

        off += 4;

        UNSAFE.putByte(arr, off++, type);

        UNSAFE.copyMemory(valBytes.array(), BYTE_ARR_OFF, arr, off, len);

        off += len;

        off = U.writeGridUuid(arr, off, valClsLdrId);

        U.writeGridUuid(arr, off, keyClsLdrId);

        return arr;
    }

    /**
     * @param arr Entry bytes.
     * @return Entry.
     */
    public static GridCacheSwapEntryImpl unmarshal(byte[] arr) {
        long off = BYTE_ARR_OFF;

        long ttl = UNSAFE.getLong(arr, off);

        off += 8;

        long expireTime = UNSAFE.getLong(arr, off);

        off += 8;

        boolean verEx = UNSAFE.getBoolean(arr, off++);

        GridCacheVersion ver = U.readVersion(arr, off, verEx);

        off += verEx ? VERSION_EX_SIZE : VERSION_SIZE;

        int arrLen = UNSAFE.getInt(arr, off);

        off += 4;

        byte type = UNSAFE.getByte(arr, off++);

        byte[] valBytes = new byte[arrLen];

        UNSAFE.copyMemory(arr, off, valBytes, BYTE_ARR_OFF, arrLen);

        off += arrLen;

        IgniteUuid valClsLdrId = U.readGridUuid(arr, off);

        off += valClsLdrId == null ? 1 : (1 + GUID_SIZE);

        IgniteUuid keyClsLdrId = U.readGridUuid(arr, off);

        return new GridCacheSwapEntryImpl(ByteBuffer.wrap(valBytes),
            type,
            ver,
            ttl,
            expireTime,
            keyClsLdrId,
            valClsLdrId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSwapEntryImpl.class, this);
    }
}