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

package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Represents Data Entry ({@link #key}, {@link #val value}) pair update {@link #op operation} in WAL log.
 */
public class DataEntry {
    /** Empty flags. */
    public static final byte EMPTY_FLAGS = 0;

    /** */
    public static final byte PRIMARY_FLAG = 0b00000001;

    /** */
    public static final byte PRELOAD_FLAG = 0b00000010;

    /** */
    public static final byte FROM_STORE_FLAG = 0b00000100;

    /** Cache ID. */
    @GridToStringInclude
    protected int cacheId;

    /** Cache object key. */
    protected KeyCacheObject key;

    /** Cache object value. May be {@code} null for {@link GridCacheOperation#DELETE} */
    @Nullable protected CacheObject val;

    /** Entry operation performed. */
    @GridToStringInclude
    protected GridCacheOperation op;

    /** Near transaction version. */
    @GridToStringInclude
    protected GridCacheVersion nearXidVer;

    /** Write version. */
    @GridToStringInclude
    protected GridCacheVersion writeVer;

    /** Expire time. */
    @GridToStringInclude
    protected long expireTime;

    /** Partition ID. */
    @GridToStringInclude
    protected int partId;

    /** */
    @GridToStringInclude
    protected long partCnt;

    /**
     * Bit flags.
     * <ul>
     *  <li>0 bit - primary - seted when current node is primary for entry partition.</li>
     *  <li>1 bit - preload - seted when entry logged during preload(rebalance).</li>
     *  <li>2 bit - fromStore - seted when entry loaded from third-party store.</li>
     * </ul>
     */
    @GridToStringInclude
    protected byte flags;

    /** Constructor. */
    private DataEntry() {
        // No-op, used from factory methods.
    }

    /**
     * @param cacheId Cache ID.
     * @param key Key.
     * @param val Value or null for delete operation.
     * @param op Operation.
     * @param nearXidVer Near transaction version.
     * @param writeVer Write version.
     * @param expireTime Expire time.
     * @param partId Partition ID.
     * @param partCnt Partition counter.
     * @param flags Entry flags.
     */
    public DataEntry(
        int cacheId,
        KeyCacheObject key,
        @Nullable CacheObject val,
        GridCacheOperation op,
        GridCacheVersion nearXidVer,
        GridCacheVersion writeVer,
        long expireTime,
        int partId,
        long partCnt,
        byte flags
    ) {
        this.cacheId = cacheId;
        this.key = key;
        this.val = val;
        this.op = op;
        this.nearXidVer = nearXidVer;
        this.writeVer = writeVer;
        this.expireTime = expireTime;
        this.partId = partId;
        this.partCnt = partCnt;
        this.flags = flags;

        // Only READ, CREATE, UPDATE and DELETE operations should be stored in WAL.
        assert op == GridCacheOperation.READ
            || op == GridCacheOperation.CREATE
            || op == GridCacheOperation.UPDATE
            || op == GridCacheOperation.DELETE : op;
    }

    /**
     * @param primary {@code True} if node is primary for partition in the moment of logging.
     * @return Flags value.
     */
    public static byte flags(boolean primary) {
        return flags(primary, false, false);
    }

    /**
     * @param primary {@code True} if node is primary for partition in the moment of logging.
     * @param preload {@code True} if logged during preload(rebalance).
     * @param fromStore {@code True} if logged during loading from third-party store.
     * @return Flags value.
     */
    public static byte flags(boolean primary, boolean preload, boolean fromStore) {
        byte val = EMPTY_FLAGS;

        val |= primary ? PRIMARY_FLAG : EMPTY_FLAGS;
        val |= preload ? PRELOAD_FLAG : EMPTY_FLAGS;
        val |= fromStore ? FROM_STORE_FLAG : EMPTY_FLAGS;

        return val;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return Key cache object.
     */
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @return Value cache object.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @return Cache operation.
     */
    public GridCacheOperation op() {
        return op;
    }

    /**
     * @return Near transaction version if the write was transactional.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @return Write version.
     */
    public GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * @return Partition ID.
     */
    public int partitionId() {
        return partId;
    }

    /**
     * @return Partition counter.
     */
    public long partitionCounter() {
        return partCnt;
    }

    /**
     * Sets partition update counter to entry.
     *
     * @param partCnt Partition update counter.
     * @return {@code this} for chaining.
     */
    public DataEntry partitionCounter(long partCnt) {
        this.partCnt = partCnt;

        return this;
    }

    /**
     * @return Expire time.
     */
    public long expireTime() {
        return expireTime;
    }

    /**
     * Entry flags.
     * @see #flags
     */
    public byte flags() {
        return flags;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataEntry.class, this);
    }
}
