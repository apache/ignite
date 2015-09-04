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

package org.apache.ignite.internal.processors.cache.distributed.dht.colocated;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Detached cache entry.
 */
public class GridDhtDetachedCacheEntry extends GridDistributedCacheEntry {
    /**
     * @param ctx Cache context.
     * @param key Cache key.
     * @param hash Key hash value.
     * @param val Entry value.
     * @param next Next entry in the linked list.
     * @param hdrId Header ID.
     */
    public GridDhtDetachedCacheEntry(GridCacheContext ctx, KeyCacheObject key, int hash, CacheObject val,
        GridCacheMapEntry next, int hdrId) {
        super(ctx, key, hash, val, next, hdrId);
    }

    /**
     * Sets value to detached entry so it can be retrieved in transactional gets.
     *
     * @param val Value.
     * @param ver Version.
     */
    public void resetFromPrimary(CacheObject val, GridCacheVersion ver) {
        value(val);

        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject unswap(boolean needVal) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void value(@Nullable CacheObject val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override protected CacheObject valueBytesUnlocked() {
        return val;
    }

    /** {@inheritDoc} */
    @Override protected void updateIndex(CacheObject val, long expireTime,
        GridCacheVersion ver, CacheObject old) throws IgniteCheckedException {
        // No-op for detached entries, index is updated on primary nodes.
    }

    /** {@inheritDoc} */
    @Override protected void clearIndex(CacheObject val) throws IgniteCheckedException {
        // No-op for detached entries, index is updated on primary or backup nodes.
    }

    /** {@inheritDoc} */
    @Override public boolean detached() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return S.toString(GridDhtDetachedCacheEntry.class, this, "super", super.toString());
    }

    /** {@inheritDoc} */
    @Override public boolean addRemoved(GridCacheVersion ver) {
        // No-op for detached cache entry.
        return true;
    }
}