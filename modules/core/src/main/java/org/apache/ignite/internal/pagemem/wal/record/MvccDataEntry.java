/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Represents Data Entry ({@link #key}, {@link #val value}) pair for mvcc update {@link #op operation} in WAL log.
 */
public class MvccDataEntry extends DataEntry {
    /** Entry version. */
    private MvccVersion mvccVer;

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
     * @param mvccVer Mvcc version.
     */
    public MvccDataEntry(
        int cacheId,
        KeyCacheObject key,
        @Nullable CacheObject val,
        GridCacheOperation op,
        GridCacheVersion nearXidVer,
        GridCacheVersion writeVer,
        long expireTime,
        int partId,
        long partCnt,
        MvccVersion mvccVer
    ) {
        super(cacheId, key, val, op, nearXidVer, writeVer, expireTime, partId, partCnt);

        this.mvccVer = mvccVer;
    }

    /**
     * @return Mvcc version.
     */
    public MvccVersion mvccVer() {
        return mvccVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccDataEntry.class, this);
    }
}