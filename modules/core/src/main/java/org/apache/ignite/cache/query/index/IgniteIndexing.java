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

package org.apache.ignite.cache.query.index;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineInnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineLeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.LeafIO;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.jetbrains.annotations.Nullable;


/**
 * Implementation of IndexingSpi that tracks all cache indexes.
 */
public class IgniteIndexing implements IndexingSpi {

    /**
     * Register inline IOs for sorted indexes.
     */
    static {
        PageIO.registerH2(InnerIO.VERSIONS, LeafIO.VERSIONS, null, null);

        AbstractInlineInnerIO.register();
        AbstractInlineLeafIO.register();
    }

    /**
     * Registry of all indexes. High key is a cache name, lower key is an index name.
     */
    private final Map<String, Map<String, Index>> cacheToIdx = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<?, ?>> query(@Nullable String cacheName, Collection<Object> params,
        @Nullable IndexingQueryFilter filters) throws IgniteSpiException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void store(@Nullable String cacheName, Object key, Object val,
        long expirationTime) throws IgniteSpiException {

    }

    /** {@inheritDoc} */
    @Override public void store(GridCacheContext cctx, CacheDataRow newRow, @Nullable CacheDataRow prevRow)
        throws IgniteSpiException {

        String cacheName = cctx.cache().name();
        Map<String, Index> indexes = cacheToIdx.get(cacheName);

        if (indexes == null)
            return;

        try {
            for (Index idx: indexes.values())
                idx.onUpdate(prevRow, newRow);

        } catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to update indexes.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable String cacheName, Object key) throws IgniteSpiException {
    }

    /** {@inheritDoc} */
    @Override public void remove(String cacheName, @Nullable CacheDataRow prevRow) throws IgniteSpiException {
        Map<String, Index> indexes = cacheToIdx.get(cacheName);

        if (indexes == null)
            return;

        try {
            for (Index idx: indexes.values())
                idx.onUpdate(prevRow, null);

        } catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to remove item.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Index createIndex(IndexFactory factory, IndexDefinition definition) {
        Index idx = factory.createIndex(definition);
        String cacheName = definition.getCacheName();

        if (!cacheToIdx.containsKey(cacheName))
            cacheToIdx.put(cacheName, new ConcurrentHashMap<>());

        cacheToIdx.get(cacheName).put(idx.name(), idx);

        return idx;
    }

    /** {@inheritDoc} */
    @Override public void removeIndex(String cacheName, String idxName, boolean softDelete) {
        Map<String, Index> idxs = cacheToIdx.get(cacheName);

        Index idx = idxs.remove(idxName);

        if (idx != null)
            idx.destroy(softDelete);
    }
















    @Override public String getName() {
        return null;
    }

    @Override public Map<String, Object> getNodeAttributes() throws IgniteSpiException {
        return null;
    }

    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {

    }

    @Override public void onContextInitialized(IgniteSpiContext spiCtx) throws IgniteSpiException {

    }

    @Override public void onContextDestroyed() {

    }

    @Override public void spiStop() throws IgniteSpiException {

    }

    @Override public void onClientDisconnected(IgniteFuture<?> reconnectFut) {

    }

    @Override public void onClientReconnected(boolean clusterRestarted) {

    }
}
