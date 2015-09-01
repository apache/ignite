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

import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import sun.misc.Unsafe;

/**
 * GridCacheSwapEntry over offheap pointer.
 * <p>
 * Offheap pointer points to marshalled {@link GridCacheSwapEntryImpl} instance, marshalled data:
 * <ul>
 *     <li>TTL</li>
 *     <li>Expire time</li>
 *     <li>GridCacheVersion or GridCacheVersionEx</li>
 *     <li>Value is byte array flag</li>
 *     <li>Value byte array (marshalled with portable or grid marshaller)</li>
 *     <li>Value classloader UUID</li>
 *     <li>Key classloader UUID</li>
 * </ul>
 */
public class GridCacheOffheapSwapEntry implements GridCacheSwapEntry {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private final long ptr;

    /** */
    private final long valPtr;

    /** */
    private final GridCacheVersion ver;

    /** */
    private CacheObject val;

    /** */
    private final byte type;

    /**
     * @param ptr Value pointer.
     * @param size Value size.
     */
    public GridCacheOffheapSwapEntry(long ptr, int size) {
        assert ptr > 0 : ptr;
        assert size > 40 : size;

        this.ptr = ptr;

        long readPtr = ptr + GridCacheSwapEntryImpl.VERSION_OFFSET;

        boolean verEx = UNSAFE.getByte(readPtr++) != 0;

        ver = U.readVersion(readPtr, verEx);

        readPtr += verEx ? GridCacheSwapEntryImpl.VERSION_EX_SIZE : GridCacheSwapEntryImpl.VERSION_SIZE;

        type = UNSAFE.getByte(readPtr + 4);

        valPtr = readPtr;

        assert (ptr + size) > (UNSAFE.getInt(valPtr) + valPtr + 5);
    }

    /**
     * @param ptr Marshaled swap entry address.
     * @param size Marshalled data size.
     * @return Value data address.
     */
    public static long valueAddress(long ptr, int size) {
        assert ptr > 0 : ptr;
        assert size > 40 : size;

        ptr += GridCacheSwapEntryImpl.VERSION_OFFSET; // Skip ttl, expire time.

        boolean verEx = UNSAFE.getByte(ptr++) != 0;

        ptr += verEx ? GridCacheSwapEntryImpl.VERSION_EX_SIZE : GridCacheSwapEntryImpl.VERSION_SIZE;

        assert (ptr + size) > (UNSAFE.getInt(ptr) + ptr + 5);

        return ptr;
    }

    /**
     * @param ptr Marshaled swap entry address.
     * @return TTL.
     */
    public static long timeToLive(long ptr) {
        return UNSAFE.getLong(ptr);
    }

    /**
     * @param ptr Marshaled swap entry address.
     * @return Expire time.
     */
    public static long expireTime(long ptr) {
        return UNSAFE.getLong(ptr + GridCacheSwapEntryImpl.EXPIRE_TIME_OFFSET);
    }

    /**
     * @param ptr Marshaled swap entry address.
     * @return Version.
     */
    public static GridCacheVersion version(long ptr) {
        long addr = ptr + GridCacheSwapEntryImpl.VERSION_OFFSET;

        boolean verEx = UNSAFE.getByte(addr) != 0;

        addr++;

        return U.readVersion(addr, verEx);
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void valueBytes(byte[] valBytes) {
        throw new UnsupportedOperationException();
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
        return UNSAFE.getLong(ptr);
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return UNSAFE.getLong(ptr + GridCacheSwapEntryImpl.EXPIRE_TIME_OFFSET);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteUuid keyClassLoaderId() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        return valPtr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteUuid valueClassLoaderId() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheOffheapSwapEntry.class, this);
    }
}