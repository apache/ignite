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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineInnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineLeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.LeafIO;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
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
     * Registry of all indexes. High key is a cache name, lower key is an unique index name.
     */
    private final Map<String, Map<String, Index>> cacheToIdx = new ConcurrentHashMap<>();

    /**
     * Registry of all index definitions. Key is {@link Index#id()}, value is IndexDefinition used for creating index.
     */
    private final Map<UUID, IndexDefinition> idxDefs = new ConcurrentHashMap<>();

    /** Exclusive lock for DDL operations. */
    private final ReentrantReadWriteLock ddlLock = new ReentrantReadWriteLock();

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<?, ?>> query(@Nullable String cacheName, Collection<Object> params,
        @Nullable IndexingQueryFilter filters) throws IgniteSpiException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void store(@Nullable String cacheName, Object key, Object val,
        long expirationTime) throws IgniteSpiException {
        throw new IgniteSpiException("Not implemented.");
    }

    /** {@inheritDoc} */
    @Override public void store(GridCacheContext<?, ?> cctx, CacheDataRow newRow, @Nullable CacheDataRow prevRow,
        boolean prevRowAvailable)
        throws IgniteSpiException {
        try {
            updateIndexes(cctx.name(), newRow, prevRow, prevRowAvailable);

        } catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to store row in cache", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void store(Collection<? extends Index> idxs, CacheDataRow newRow, @Nullable CacheDataRow prevRow,
        boolean prevRowAvailable) throws IgniteSpiException {
        IgniteCheckedException err = null;

        ddlLock.readLock().lock();

        try {
            for (Index idx : idxs)
                err = updateIndex(idx, newRow, prevRow, prevRowAvailable, err);

            if (err != null)
                throw err;

        } catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to store row in index", e);

        } finally {
            ddlLock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable String cacheName, Object key) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void remove(String cacheName, @Nullable CacheDataRow prevRow) throws IgniteSpiException {
        try {
            updateIndexes(cacheName, null, prevRow, true);

        } catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to remove row in cache", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Index createIndex(GridCacheContext<?, ?> cctx, IndexFactory factory, IndexDefinition definition) {
        ddlLock.writeLock().lock();

        try {
            String cacheName = definition.getIdxName().cacheName();

            cacheToIdx.putIfAbsent(cacheName, new ConcurrentHashMap<>());

            String uniqIdxName = definition.getIdxName().fqdnIdxName();

            // GridQueryProcessor already checked schema operation for index duplication.
            assert cacheToIdx.get(cacheName).get(uniqIdxName) == null : "Duplicated index name " + uniqIdxName;

            Index idx = factory.createIndex(cctx, definition);

            cacheToIdx.get(cacheName).put(uniqIdxName, idx);

            idxDefs.put(idx.id(), definition);

            return idx;

        } finally {
            ddlLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void removeIndex(GridCacheContext<?, ?> cctx, IndexDefinition def, boolean softDelete) {
        ddlLock.writeLock().lock();

        try {
            String cacheName = def.getIdxName().cacheName();

            Map<String, Index> idxs = cacheToIdx.get(cacheName);

            assert idxs != null : "Try remove index for non registered cache " + cacheName;

            Index idx = idxs.remove(def.getIdxName().fqdnIdxName());

            if (idx != null)
                idx.destroy(softDelete);

        } finally {
            ddlLock.writeLock().unlock();
        }
    }

    /** */
    private void updateIndexes(String cacheName, CacheDataRow newRow, CacheDataRow prevRow,
        boolean prevRowAvailable) throws IgniteCheckedException {
        IgniteCheckedException err = null;

        ddlLock.readLock().lock();

        try {
            Map<String, Index> indexes = cacheToIdx.get(cacheName);

            if (F.isEmpty(indexes))
                return;

            for (Index idx: indexes.values())
                err = updateIndex(idx, newRow, prevRow, prevRowAvailable, err);

        } finally {
            ddlLock.readLock().unlock();
        }

        if (err != null)
            throw err;
    }

    /** {@inheritDoc} */
    @Override public void markRebuildIndexesForCache(GridCacheContext<?, ?> cctx, boolean val) {
        ddlLock.readLock().lock();

        try {
            if (!cacheToIdx.containsKey(cctx.name()))
                return;

            Collection<Index> idxs = cacheToIdx.get(cctx.name()).values();

            for (Index idx: idxs) {
                if (idx instanceof AbstractIndex)
                    ((AbstractIndex) idx).markIndexRebuild(val);
            }

        } finally {
            ddlLock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<Index> getIndexes(GridCacheContext<?, ?> cctx) {
        ddlLock.readLock().lock();

        try {
            Map<String, Index> idxs = cacheToIdx.get(cctx.name());

            if (idxs == null)
                return Collections.emptyList();

            return idxs.values();

        } finally {
            ddlLock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public IndexDefinition getIndexDefinition(UUID idxId) {
        return idxDefs.get(idxId);
    }

    /**
     * Add row to index.
     * @param idx Index to add row to.
     * @param row Row to add to index.
     * @param prevRow Previous row state, if any.
     * @param prevRowAvailable Whether previous row is available.
     * @param prevErr Error on index add.
     */
    private IgniteCheckedException updateIndex(
        Index idx, CacheDataRow row, CacheDataRow prevRow, boolean prevRowAvailable, IgniteCheckedException prevErr
    ) throws IgniteCheckedException {
        try {
            if (row != null && !idx.belongsToIndex(row))
                return prevErr;

            if (prevRow != null && !idx.belongsToIndex(prevRow))
                return prevErr;

            idx.onUpdate(prevRow, row, prevRowAvailable);

            return prevErr;
        }
        catch (Throwable t) {
            IgniteSQLException ex = X.cause(t, IgniteSQLException.class);

            if (ex != null && ex.statusCode() == IgniteQueryErrorCode.FIELD_TYPE_MISMATCH) {
                if (prevErr != null) {
                    prevErr.addSuppressed(t);

                    return prevErr;
                }
                else
                    return new IgniteCheckedException("Error on add row to index '" + getName() + '\'', t);
            }
            else
                throw t;
        }
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return "IginiteIndexingSpi";
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getNodeAttributes() throws IgniteSpiException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {

    }

    /** {@inheritDoc} */
    @Override public void onContextInitialized(IgniteSpiContext spiCtx) throws IgniteSpiException {

    }

    /** {@inheritDoc} */
    @Override public void onContextDestroyed() {

    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {

    }

    /** {@inheritDoc} */
    @Override public void onClientDisconnected(IgniteFuture<?> reconnectFut) {

    }

    /** {@inheritDoc} */
    @Override public void onClientReconnected(boolean clusterRestarted) {

    }
}
