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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCacheRestartingException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.store.CacheStoreManager;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;

/**
 *
 */
public class IgniteTxImplicitSingleStateImpl extends IgniteTxLocalStateAdapter {
    /** */
    private GridCacheContext<?, ?> cacheCtx;

    /** Entry is stored as singleton list for performance optimization. */
    private List<IgniteTxEntry> entry;

    /** */
    private volatile boolean init;

    /** */
    private boolean recovery;

    /** */
    private volatile boolean useMvccCaching;

    /** {@inheritDoc} */
    @Override public void addActiveCache(GridCacheContext cacheCtx, boolean recovery, IgniteTxAdapter tx)
        throws IgniteCheckedException {
        synchronized (this) {
            assert this.cacheCtx == null : "Cache already set [cur=" + cacheCtx.name() + ", new=" + cacheCtx.name() + ']';
            assert tx.local();

            this.cacheCtx = cacheCtx;
            this.recovery = recovery;
        }

        tx.activeCachesDeploymentEnabled(cacheCtx.deploymentEnabled());

        useMvccCaching = cacheCtx.mvccEnabled() && (cacheCtx.isDrEnabled() || cacheCtx.hasContinuousQueryListeners(tx));
    }

    /** {@inheritDoc} */
    @Override public int[] cacheIds() {
        return new int[] {cacheCtx().cacheId()};
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheContext<?, ?> singleCacheContext(GridCacheSharedContext ctx) {
        return cacheCtx();
    }

    /** {@inheritDoc} */
    @Nullable @Override public synchronized Integer firstCacheId() {
        return cacheCtx != null ? cacheCtx.cacheId() : null;
    }

    /** {@inheritDoc} */
    @Override public void unwindEvicts(GridCacheSharedContext ctx) {
        List<IgniteTxEntry> entry = entry();

        if (entry == null || entry.isEmpty())
            return;

        assert entry.size() == 1;

        GridCacheContext<?, ?> cctx = ctx.cacheContext(entry.get(0).cacheId());

        if (cctx != null)
            CU.unwindEvicts(cctx);
    }

    /** {@inheritDoc} */
    @Override public void awaitLastFuture(GridCacheSharedContext ctx) {
        GridCacheContext<?, ?> cctx = cacheCtx();

        if (cctx == null)
            return;

        cctx.cache().awaitLastFut();
    }

    /** {@inheritDoc} */
    @Override public boolean implicitSingle() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException validateTopology(
        GridCacheSharedContext ctx,
        boolean read,
        GridDhtTopologyFuture topFut
    ) {
        GridCacheContext<?, ?> cctx = cacheCtx();

        if (cctx == null)
            return null;

        Throwable err = null;

        List<IgniteTxEntry> entry = entry();

        if (entry != null) {
            // An entry is a singleton list here, so a key is taken from a first element.
            KeyCacheObject key = entry.get(0).key();

            err = topFut.validateCache(cctx, recovery(), read, key, null);
        }

        if (err != null) {
            return new IgniteCheckedException(
                "Failed to perform cache operation (cache topology is not valid): "
                    + U.maskName(cctx.name()), err);
        }

        if (CU.affinityNodes(cctx, topFut.topologyVersion()).isEmpty()) {
            return new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all " +
                "partition nodes left the grid): " + cctx.name());
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public synchronized CacheWriteSynchronizationMode syncMode(GridCacheSharedContext ctx) {
        return cacheCtx != null ? cacheCtx.config().getWriteSynchronizationMode() : FULL_ASYNC;
    }

    /** {@inheritDoc} */
    @Override public GridDhtTopologyFuture topologyReadLock(GridCacheSharedContext ctx, GridFutureAdapter<?> fut) {
        GridCacheContext<?, ?> cctx = cacheCtx();

        if (cctx == null)
            return ctx.exchange().lastTopologyFuture();

        cctx.topology().readLock();

        if (cctx.topology().stopping()) {
            fut.onDone(
                ctx.cache().isCacheRestarting(cctx.name()) ?
                    new IgniteCacheRestartingException(cctx.name()) :
                    new CacheStoppedException(cctx.name()));

            return null;
        }

        return cctx.topology().topologyVersionFuture();
    }

    /** {@inheritDoc} */
    @Override public void topologyReadUnlock(GridCacheSharedContext ctx) {
        GridCacheContext<?, ?> cctx = cacheCtx();

        if (cctx == null)
            return;

        cctx.topology().readUnlock();
    }

    /** {@inheritDoc} */
    @Override public boolean storeWriteThrough(GridCacheSharedContext ctx) {
        GridCacheContext<?, ?> cctx = cacheCtx();

        if (cctx == null)
            return false;

        CacheStoreManager store = cctx.store();

        return store.configured() && store.isWriteThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean hasInterceptor(GridCacheSharedContext ctx) {
        GridCacheContext<?, ?> cctx = cacheCtx();

        return cctx != null && cctx.config().getInterceptor() != null;
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheStoreManager> stores(GridCacheSharedContext ctx) {
        GridCacheContext<?, ?> cctx = cacheCtx();

        if (cctx == null)
            return null;

        CacheStoreManager store = cctx.store();

        if (store.configured())
            return Collections.singleton(store);

        return null;
    }

    /** {@inheritDoc} */
    @Override public void onTxEnd(GridCacheSharedContext ctx, IgniteInternalTx tx, boolean commit) {
        GridCacheContext<?, ?> cctx = cacheCtx();

        if (cctx != null)
            onTxEnd(cctx, tx, commit);
    }

    /** {@inheritDoc} */
    @Override public IgniteTxEntry entry(IgniteTxKey key) {
        List<IgniteTxEntry> entry = entry();

        if (entry != null && entry.get(0).txKey().equals(key))
            return entry.get(0);

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasWriteKey(IgniteTxKey key) {
        List<IgniteTxEntry> entry = entry();

        return entry != null && entry.get(0).txKey().equals(key);
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteTxKey> readSet() {
        return Collections.emptySet();
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteTxKey> writeSet() {
        List<IgniteTxEntry> entry = entry();

        if (entry != null) {
            Set<IgniteTxKey> set = new HashSet<>(3, 0.75f);

            set.add(entry.get(0).txKey());

            return set;
        }
        else
            return Collections.emptySet();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> writeEntries() {
        List<IgniteTxEntry> entry = entry();

        return entry != null ? entry : Collections.<IgniteTxEntry>emptyList();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> readEntries() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> writeMap() {
        List<IgniteTxEntry> entry = entry();
        
        return entry != null ? F.asMap(entry.get(0).txKey(), entry.get(0)) :
            Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> readMap() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean empty() {
        return entry == null;
    }

    /** {@inheritDoc} */
    @Override public synchronized Collection<IgniteTxEntry> allEntries() {
        return entry != null ? entry : Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public boolean init(int txSize) {
        if (!init) {
            init = true;

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean initialized() {
        return init;
    }

    /** {@inheritDoc} */
    @Override public synchronized void addEntry(IgniteTxEntry entry) {
        assert this.entry == null : "Entry already set [cur=" + this.entry + ", new=" + entry + ']';

        this.entry = Collections.singletonList(entry);
    }

    /** {@inheritDoc} */
    @Override public void removeEntry(IgniteTxKey key) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void seal() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteTxEntry singleWrite() {
        List<IgniteTxEntry> entry = entry();
        
        return entry != null ? entry.get(0) : null;
    }

    /** {@inheritDoc} */
    @Override public boolean mvccEnabled() {
        GridCacheContext<?, ?> cctx = cacheCtx();

        return cctx != null && cctx.mvccEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean useMvccCaching(int cacheId) {
        GridCacheContext<?, ?> cctx = cacheCtx();
        
        assert cctx == null || cctx.cacheId() == cacheId;

        return useMvccCaching;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean recovery() {
        return recovery;
    }

    /** */
    private synchronized List<IgniteTxEntry> entry() {
        return entry;
    }

    /** */
    private synchronized GridCacheContext<?, ?> cacheCtx() {
        return cacheCtx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteTxImplicitSingleStateImpl.class, this);
    }
}
