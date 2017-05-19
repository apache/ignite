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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.jsr166.ConcurrentHashMap8;

/**
 * GridCacheConcurrentMap implementation for local and near caches.
 */
public class GridCacheLocalConcurrentMap extends GridCacheConcurrentMapImpl {
    /** */
    private final AtomicInteger pubSize = new AtomicInteger();

    /** */
    private final int cacheId;

    /** */
    private final ConcurrentMap<KeyCacheObject, GridCacheMapEntry> entryMap;

    /**
     * @param cacheId Cache ID.
     * @param factory Entry factory.
     * @param initCap Initial capacity.
     */
    public GridCacheLocalConcurrentMap(int cacheId, GridCacheMapEntryFactory factory, int initCap) {
        super(factory);

        this.cacheId = cacheId;
        this.entryMap = new ConcurrentHashMap8<>(initCap, 0.75f, Runtime.getRuntime().availableProcessors() * 2);
    }

    /** {@inheritDoc} */
    @Override public int internalSize() {
        return entryMap.size();
    }

    /** {@inheritDoc} */
    @Override protected ConcurrentMap<KeyCacheObject, GridCacheMapEntry> entriesMap(int cacheId, boolean create) {
        assert this.cacheId == cacheId;

        return entryMap;
    }

    /** {@inheritDoc} */
    @Override public int publicSize(int cacheId) {
        assert this.cacheId == cacheId;

        return pubSize.get();
    }

    /** {@inheritDoc} */
    @Override public void incrementPublicSize(GridCacheEntryEx e) {
        assert cacheId == e.context().cacheId();

        pubSize.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void decrementPublicSize(GridCacheEntryEx e) {
        assert cacheId == e.context().cacheId();

        pubSize.decrementAndGet();
    }
}
