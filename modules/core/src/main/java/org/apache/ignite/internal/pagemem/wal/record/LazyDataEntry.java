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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;

/**
 *
 */
public class LazyDataEntry extends DataEntry {
    /** */
    private GridCacheSharedContext cctx;

    /** */
    private byte keyType;

    /** */
    private byte[] keyBytes;

    /** */
    private byte valType;

    /** */
    private byte[] valBytes;

    /**
     * @param cacheId Cache ID.
     * @param op Operation.
     * @param nearXidVer Near transaction version.
     * @param writeVer Write version.
     * @param partId Partition ID.
     * @param partCnt Partition counter.
     */
    public LazyDataEntry(
        GridCacheSharedContext cctx,
        int cacheId,
        byte keyType,
        byte[] keyBytes,
        byte valType,
        byte[] valBytes,
        GridCacheOperation op,
        GridCacheVersion nearXidVer,
        GridCacheVersion writeVer,
        long expireTime,
        int partId,
        long partCnt
    ) {
        super(cacheId, null, null, op, nearXidVer, writeVer, expireTime, partId, partCnt);

        this.cctx = cctx;
        this.keyType = keyType;
        this.keyBytes = keyBytes;
        this.valType = valType;
        this.valBytes = valBytes;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key() {
        try {
            if (key == null) {
                GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                if (cacheCtx == null)
                    throw new IgniteException("Failed to find cache context for the given cache ID: " + cacheId);

                IgniteCacheObjectProcessor co = cctx.kernalContext().cacheObjects();

                key = co.toKeyCacheObject(cacheCtx.cacheObjectContext(), keyType, keyBytes);
            }

            return key;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheObject value() {
        if (val == null && valBytes != null) {
            GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

            if (cacheCtx == null)
                throw new IgniteException("Failed to find cache context for the given cache ID: " + cacheId);

            IgniteCacheObjectProcessor co = cctx.kernalContext().cacheObjects();

            val = co.toCacheObject(cacheCtx.cacheObjectContext(), valType, valBytes);
        }

        return val;
    }
}
