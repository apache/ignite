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
import org.apache.ignite.internal.util.GridIntList;
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
    private GridCacheContext cacheCtx;

    /** Entry is stored as singleton list for performance optimization. */
    private List<IgniteTxEntry> entry;

    /** */
    private boolean init;

    /** */
    private boolean recovery;

    /** */
    private volatile boolean useMvccCaching;

    /** {@inheritDoc} */
    @Override public void addActiveCache(GridCacheContext ctx, boolean recovery, IgniteTxAdapter tx)
        throws IgniteCheckedException {
        assert cacheCtx == null : "Cache already set [cur=" + cacheCtx.name() + ", new=" + ctx.name() + ']';
        assert tx.local();

        cacheCtx = ctx;
        this.recovery = recovery;

        tx.activeCachesDeploymentEnabled(cacheCtx.deploymentEnabled());

        useMvccCaching = cacheCtx.mvccEnabled() && (cacheCtx.isDrEnabled() || cacheCtx.hasContinuousQueryListeners(tx));
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridIntList cacheIds() {
        return GridIntList.asList(cacheCtx.cacheId());
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheContext singleCacheContext(GridCacheSharedContext cctx) {
        return cacheCtx;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Integer firstCacheId() {
        return cacheCtx != null ? cacheCtx.cacheId() : null;
    }

    /** {@inheritDoc} */
    @Override public void unwindEvicts(GridCacheSharedContext cctx) {
        if (entry == null || entry.isEmpty())
            return;

        assert entry.size() == 1;

        GridCacheContext ctx = cctx.cacheContext(entry.get(0).cacheId());

        if (ctx != null)
            CU.unwindEvicts(ctx);
    }

    /** {@inheritDoc} */
    @Override public void awaitLastFuture(GridCacheSharedContext ctx) {
        if (cacheCtx == null)
            return;

        cacheCtx.cache().awaitLastFut();
    }

    /** {@inheritDoc} */
    @Override public boolean implicitSingle() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException validateTopology(
        GridCacheSharedContext cctx,
        boolean read,
        GridDhtTopologyFuture topFut
    ) {
        if (cacheCtx == null)
            return null;

        Throwable err = null;

        if (entry != null) {
            // An entry is a singleton list here, so a key is taken from a first element.
            KeyCacheObject key = entry.get(0).key();

            err = topFut.validateCache(cacheCtx, recovery, read, key, null);
        }

        if (err != null) {
            return new IgniteCheckedException(
                "Failed to perform cache operation (cache topology is not valid): "
                    + U.maskName(cacheCtx.name()), err);
        }

        if (CU.affinityNodes(cacheCtx, topFut.topologyVersion()).isEmpty()) {
            return new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all " +
                "partition nodes left the grid): " + cacheCtx.name());
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheWriteSynchronizationMode syncMode(GridCacheSharedContext cctx) {
        return cacheCtx != null ? cacheCtx.config().getWriteSynchronizationMode() : FULL_ASYNC;
    }

    /** {@inheritDoc} */
    @Override public GridDhtTopologyFuture topologyReadLock(GridCacheSharedContext cctx, GridFutureAdapter<?> fut) {
        if (cacheCtx == null || cacheCtx.isLocal())
            return cctx.exchange().lastTopologyFuture();

        cacheCtx.topology().readLock();

        if (cacheCtx.topology().stopping()) {
            fut.onDone(
                cctx.cache().isCacheRestarting(cacheCtx.name()) ?
                    new IgniteCacheRestartingException(cacheCtx.name()) :
                    new CacheStoppedException(cacheCtx.name()));

            return null;
        }

        return cacheCtx.topology().topologyVersionFuture();
    }

    /** {@inheritDoc} */
    @Override public void topologyReadUnlock(GridCacheSharedContext cctx) {
        if (cacheCtx == null || cacheCtx.isLocal())
            return;

        cacheCtx.topology().readUnlock();
    }

    /** {@inheritDoc} */
    @Override public boolean storeWriteThrough(GridCacheSharedContext cctx) {
        if (cacheCtx == null)
            return false;

        CacheStoreManager store = cacheCtx.store();

        return store.configured() && store.isWriteThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean hasInterceptor(GridCacheSharedContext cctx) {
        GridCacheContext ctx0 = cacheCtx;

        return ctx0 != null && ctx0.config().getInterceptor() != null;
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheStoreManager> stores(GridCacheSharedContext cctx) {
        if (cacheCtx == null)
            return null;

        CacheStoreManager store = cacheCtx.store();

        if (store.configured())
            return Collections.singleton(store);

        return null;
    }

    /** {@inheritDoc} */
    @Override public void onTxEnd(GridCacheSharedContext cctx, IgniteInternalTx tx, boolean commit) {
        if (cacheCtx != null)
            onTxEnd(cacheCtx, tx, commit);
    }

    /** {@inheritDoc} */
    @Override public IgniteTxEntry entry(IgniteTxKey key) {
        if (entry != null && entry.get(0).txKey().equals(key))
            return entry.get(0);

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasWriteKey(IgniteTxKey key) {
        return entry != null && entry.get(0).txKey().equals(key);
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteTxKey> readSet() {
        return Collections.emptySet();
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteTxKey> writeSet() {
        if (entry != null) {
            Set<IgniteTxKey> set = new HashSet<>(3, 0.75f);

            set.add(entry.get(0).txKey());

            return set;
        }
        else
            return Collections.<IgniteTxKey>emptySet();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> writeEntries() {
        return entry != null ? entry : Collections.<IgniteTxEntry>emptyList();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> readEntries() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> writeMap() {
        return entry != null ? F.asMap(entry.get(0).txKey(), entry.get(0)) :
            Collections.<IgniteTxKey, IgniteTxEntry>emptyMap();
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> readMap() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public boolean empty() {
        return entry == null;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> allEntries() {
        return entry != null ? entry : Collections.<IgniteTxEntry>emptyList();
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
    @Override public void addEntry(IgniteTxEntry entry) {
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
        return entry != null ? entry.get(0) : null;
    }

    /** {@inheritDoc} */
    @Override public boolean mvccEnabled() {
        GridCacheContext ctx0 = cacheCtx;

        return ctx0 != null && ctx0.mvccEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean useMvccCaching(int cacheId) {
        assert cacheCtx == null || cacheCtx.cacheId() == cacheId;

        return useMvccCaching;
    }

    /** {@inheritDoc} */
    @Override public boolean recovery() {
        return recovery;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteTxImplicitSingleStateImpl.class, this);
    }
}
