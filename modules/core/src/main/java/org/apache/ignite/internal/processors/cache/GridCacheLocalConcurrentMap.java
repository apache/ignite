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
 *
 */

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.Nullable;

/**
 * GridCacheConcurrentMap implementation for local and near caches.
 */
public class GridCacheLocalConcurrentMap extends GridCacheConcurrentMapImpl {
    /** */
    private final int cacheId;

    /** */
    private final CacheMapHolder entryMap;

    /**
     * @param cctx Cache context.
     * @param factory Entry factory.
     * @param initCap Initial capacity.
     */
    public GridCacheLocalConcurrentMap(GridCacheContext cctx, GridCacheMapEntryFactory factory, int initCap) {
        super(factory);

        this.cacheId = cctx.cacheId();
        this.entryMap = new CacheMapHolder(cctx,
            new ConcurrentHashMap<KeyCacheObject, GridCacheMapEntry>(initCap, 0.75f, Runtime.getRuntime().availableProcessors() * 2));
    }

    /** {@inheritDoc} */
    @Override public int internalSize() {
        return entryMap.map.size();
    }

    /** {@inheritDoc} */
    @Nullable @Override protected CacheMapHolder entriesMap(GridCacheContext cctx) {
        return entryMap;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected CacheMapHolder entriesMapIfExists(Integer cacheId) {
        return entryMap;
    }

    /** {@inheritDoc} */
    @Override public int publicSize(int cacheId) {
        assert this.cacheId == cacheId;

        return entryMap.size.get();
    }

    /** {@inheritDoc} */
    @Override public void incrementPublicSize(CacheMapHolder hld, GridCacheEntryEx e) {
        assert cacheId == e.context().cacheId();

        entryMap.size.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void decrementPublicSize(CacheMapHolder hld, GridCacheEntryEx e) {
        assert cacheId == e.context().cacheId();

        entryMap.size.decrementAndGet();
    }
}
