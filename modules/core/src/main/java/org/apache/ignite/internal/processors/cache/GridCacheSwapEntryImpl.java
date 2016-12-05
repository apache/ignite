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

/**
 * Swap entry.
 */
public class GridCacheSwapEntryImpl implements GridCacheSwapEntry {
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
        return GridUnsafe.getLong(bytes, GridUnsafe.BYTE_ARR_OFF);
    }

    /**
     * @param bytes Entry bytes.
     * @return Expire time.
     */
    public static long expireTime(byte[] bytes) {
        return GridUnsafe.getLong(bytes, GridUnsafe.BYTE_ARR_OFF + EXPIRE_TIME_OFFSET);
    }

    /**
     * @param bytes Entry bytes.
     * @return Version.
     */
    public static GridCacheVersion version(byte[] bytes) {
        long off = GridUnsafe.BYTE_ARR_OFF + VERSION_OFFSET; // Skip ttl, expire time.

        boolean verEx = GridUnsafe.getByte(bytes, off++) != 0;

        return U.readVersion(bytes, off, verEx);
    }

    /**
     * @param bytes Entry bytes.
     * @return Value if value is byte array, otherwise {@code null}.
     */
    @Nullable public static IgniteBiTuple<byte[], Byte> getValue(byte[] bytes) {
        long off = GridUnsafe.BYTE_ARR_OFF + VERSION_OFFSET; // Skip ttl, expire time.

        boolean verEx = GridUnsafe.getByte(bytes, off++) != 0;

        off += verEx ? VERSION_EX_SIZE : VERSION_SIZE;

        int arrLen = GridUnsafe.getInt(bytes, off);

        off += 4;

        byte type = GridUnsafe.getByte(bytes, off++);

        byte[] valBytes = new byte[arrLen];

        GridUnsafe.copyMemory(bytes, off, valBytes, GridUnsafe.BYTE_ARR_OFF, arrLen);

        return new IgniteBiTuple<>(valBytes, type);
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

        long off = GridUnsafe.BYTE_ARR_OFF;

        GridUnsafe.putLong(arr, off, ttl);

        off += 8;

        GridUnsafe.putLong(arr, off, expireTime);

        off += 8;

        off = U.writeVersion(arr, off, ver);

        GridUnsafe.putInt(arr, off, len);

        off += 4;

        GridUnsafe.putByte(arr, off++, type);

        GridUnsafe.copyMemory(valBytes.array(), GridUnsafe.BYTE_ARR_OFF, arr, off, len);

        off += len;

        off = U.writeGridUuid(arr, off, valClsLdrId);

        U.writeGridUuid(arr, off, keyClsLdrId);

        return arr;
    }

    /**
     * @param arr Entry bytes.
     * @param valOnly If {@code true} unmarshalls only entry value.
     * @return Entry.
     */
    public static GridCacheSwapEntryImpl unmarshal(byte[] arr, boolean valOnly) {
        if (valOnly) {
            long off = GridUnsafe.BYTE_ARR_OFF + VERSION_OFFSET; // Skip ttl, expire time.

            boolean verEx = GridUnsafe.getByte(arr, off++) != 0;

            off += verEx ? VERSION_EX_SIZE : VERSION_SIZE;

            int arrLen = GridUnsafe.getInt(arr, off);

            off += 4;

            byte type = GridUnsafe.getByte(arr, off++);

            byte[] valBytes = new byte[arrLen];

            GridUnsafe.copyMemory(arr, off, valBytes, GridUnsafe.BYTE_ARR_OFF, arrLen);

            return new GridCacheSwapEntryImpl(ByteBuffer.wrap(valBytes),
                type,
                null,
                0L,
                0L,
                null,
                null);
        }

        long off = GridUnsafe.BYTE_ARR_OFF;

        long ttl = GridUnsafe.getLong(arr, off);

        off += 8;

        long expireTime = GridUnsafe.getLong(arr, off);

        off += 8;

        boolean verEx = GridUnsafe.getBoolean(arr, off++);

        GridCacheVersion ver = U.readVersion(arr, off, verEx);

        off += verEx ? VERSION_EX_SIZE : VERSION_SIZE;

        int arrLen = GridUnsafe.getInt(arr, off);

        off += 4;

        byte type = GridUnsafe.getByte(arr, off++);

        byte[] valBytes = new byte[arrLen];

        GridUnsafe.copyMemory(arr, off, valBytes, GridUnsafe.BYTE_ARR_OFF, arrLen);

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