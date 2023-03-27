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
package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheRawVersionedEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Holder for the enlisted entries data.
 */
public class MvccTxEntry {
    /** */
    private KeyCacheObject key;

    /** */
    private CacheObject val;

    /** */
    private int cacheId;

    /** */
    private GridCacheVersion ver;

    /** */
    private CacheObject oldVal;

    /** */
    private boolean primary;

    /** */
    private AffinityTopologyVersion topVer;

    /** */
    private MvccVersion mvccVer;

    /** */
    private long ttl;

    /** */
    private long expireTime;

    /** */
    private long updCntr;

    /**
     * @param key Key.
     * @param val New value.
     * @param ttl Time to live.
     * @param expireTime Expire time.
     * @param ver Tx grig cache version.
     * @param oldVal Old value.
     * @param primary {@code True} if this is a primary node.
     * @param topVer Topology version.
     * @param mvccVer Mvcc version.
     * @param cacheId Cache id.
     */
    public MvccTxEntry(KeyCacheObject key,
        @Nullable CacheObject val,
        long ttl,
        long expireTime,
        GridCacheVersion ver,
        CacheObject oldVal,
        boolean primary,
        AffinityTopologyVersion topVer,
        MvccVersion mvccVer,
        int cacheId) {
        assert key != null;
        assert mvccVer != null;

        this.key = key;
        this.val = val;
        this.ttl = ttl;
        this.expireTime = expireTime;
        this.ver = ver;
        this.oldVal = oldVal;
        this.primary = primary;
        this.topVer = topVer;
        this.mvccVer = mvccVer;
        this.cacheId = cacheId;
    }

    /**
     * @return Versioned entry (for DR).
     */
    public GridCacheRawVersionedEntry versionedEntry() {
        return new GridCacheRawVersionedEntry(key, val, ttl, expireTime, ver);
    }

    /**
     * @return Key.
     */
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @return Value.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @return Time to live.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @return Expire time.
     */
    public long expireTime() {
        return expireTime;
    }

    /**
     * @return Version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Old value.
     */
    public CacheObject oldValue() {
        return oldVal;
    }

    /**
     * @param oldVal Old value.
     */
    public void oldValue(CacheObject oldVal) {
        this.oldVal = oldVal;
    }

    /**
     * @return {@code True} if this entry is created on a primary node.
     */
    public boolean isPrimary() {
        return primary;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Mvcc version.
     */
    public MvccVersion mvccVersion() {
        return mvccVer;
    }

    /**
     * @return Cache id.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return Update counter.
     */
    public long updateCounter() {
        return updCntr;
    }

    /**
     * @param updCntr Update counter.
     */
    public void updateCounter(long updCntr) {
        this.updCntr = updCntr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccTxEntry.class, this);
    }
}
