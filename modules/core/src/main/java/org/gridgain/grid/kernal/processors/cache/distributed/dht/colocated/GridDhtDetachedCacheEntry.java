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

package org.gridgain.grid.kernal.processors.cache.distributed.dht.colocated;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * Detached cache entry.
 */
public class GridDhtDetachedCacheEntry<K, V> extends GridDistributedCacheEntry<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param ctx Cache context.
     * @param key Cache key.
     * @param hash Key hash value.
     * @param val Entry value.
     * @param next Next entry in the linked list.
     * @param ttl Time to live.
     * @param hdrId Header ID.
     */
    public GridDhtDetachedCacheEntry(GridCacheContext<K, V> ctx, K key, int hash, V val,
        GridCacheMapEntry<K, V> next, long ttl, int hdrId) {
        super(ctx, key, hash, val, next, ttl, hdrId);
    }

    /**
     * Sets value to detached entry so it can be retrieved in transactional gets.
     *
     * @param val Value.
     * @param valBytes Value bytes.
     * @param ver Version.
     * @throws IgniteCheckedException If value unmarshalling failed.
     */
    public void resetFromPrimary(V val, byte[] valBytes, GridCacheVersion ver)
        throws IgniteCheckedException {
       if (valBytes != null && val == null)
            val = cctx.marshaller().unmarshal(valBytes, cctx.deploy().globalLoader());

        value(val, valBytes);

        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V unswap(boolean ignoreFlags, boolean needVal) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void value(@Nullable V val, @Nullable byte[] valBytes) {
        this.val = val;
        this.valBytes = valBytes;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheValueBytes valueBytesUnlocked() {
        return (val != null && val instanceof byte[]) ? GridCacheValueBytes.plain(val) :
            valBytes == null ? GridCacheValueBytes.nil() : GridCacheValueBytes.marshaled(valBytes);
    }

    /** {@inheritDoc} */
    @Override protected void updateIndex(V val, byte[] valBytes, long expireTime,
        GridCacheVersion ver, V old) throws IgniteCheckedException {
        // No-op for detached entries, index is updated on primary nodes.
    }

    /** {@inheritDoc} */
    @Override protected void clearIndex(V val) throws IgniteCheckedException {
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
