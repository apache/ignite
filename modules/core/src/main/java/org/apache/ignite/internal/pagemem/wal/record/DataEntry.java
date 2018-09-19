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
    protected GridCacheVersion nearXidVer;

    /** Write version. */
    @GridToStringInclude
    protected GridCacheVersion writeVer;

    /** Expire time. */
    protected long expireTime;

    /** Partition ID. */
    @GridToStringInclude
    protected int partId;

    /** */
    @GridToStringInclude
    protected long partCnt;

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
        long partCnt
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

        // Only READ, CREATE, UPDATE and DELETE operations should be stored in WAL.
        assert op == GridCacheOperation.READ || op == GridCacheOperation.CREATE || op == GridCacheOperation.UPDATE || op == GridCacheOperation.DELETE : op;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataEntry.class, this);
    }
}
