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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.store.CacheStoreManager;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 *
 */
public class IgniteTxStateImpl extends IgniteTxLocalStateAdapter {
    /** Active cache IDs. */
    private GridIntList activeCacheIds = new GridIntList();

    /** Per-transaction read map. */
    @GridToStringExclude
    protected Map<IgniteTxKey, IgniteTxEntry> txMap;

    /** Read view on transaction map. */
    @GridToStringExclude
    protected IgniteTxMap readView;

    /** Write view on transaction map. */
    @GridToStringExclude
    protected IgniteTxMap writeView;

    /** */
    @GridToStringInclude
    protected Boolean recovery;

    /** List of savepoints for this transaction, which is used to retrieve previous states. */
    private LinkedList<TxSavepoint> savepoints;

    /** {@inheritDoc} */
    @Override public boolean implicitSingle() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Integer firstCacheId() {
        return activeCacheIds.isEmpty() ? null : activeCacheIds.get(0);
    }

    /** {@inheritDoc} */
    @Override public void unwindEvicts(GridCacheSharedContext cctx) {
        for (int i = 0; i < activeCacheIds.size(); i++) {
            int cacheId = activeCacheIds.get(i);

            GridCacheContext ctx = cctx.cacheContext(cacheId);

            if (ctx != null)
                CU.unwindEvicts(ctx);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheContext singleCacheContext(GridCacheSharedContext cctx) {
        if (activeCacheIds.size() == 1) {
            int cacheId = activeCacheIds.get(0);

            return cctx.cacheContext(cacheId);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void awaitLastFuture(GridCacheSharedContext cctx) {
        for (int i = 0; i < activeCacheIds.size(); i++) {
            int cacheId = activeCacheIds.get(i);

            cctx.cacheContext(cacheId).cache().awaitLastFut();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException validateTopology(
        GridCacheSharedContext cctx,
        boolean read,
        GridDhtTopologyFuture topFut
    ) {

        Map<Integer, Set<KeyCacheObject>> keysByCacheId = new HashMap<>();

        for (IgniteTxKey key : txMap.keySet()) {
            Set<KeyCacheObject> set = keysByCacheId.get(key.cacheId());

            if (set == null)
                keysByCacheId.put(key.cacheId(), set = new HashSet<>());

            set.add(key.key());
        }

        StringBuilder invalidCaches = null;

        for (Map.Entry<Integer, Set<KeyCacheObject>> e : keysByCacheId.entrySet()) {
            int cacheId = e.getKey();

            GridCacheContext ctx = cctx.cacheContext(cacheId);

            assert ctx != null : cacheId;

            Throwable err = topFut.validateCache(ctx, recovery != null && recovery, read, null, e.getValue());

            if (err != null) {
                if (invalidCaches != null)
                    invalidCaches.append(", ");
                else
                    invalidCaches = new StringBuilder();

                invalidCaches.append(U.maskName(ctx.name()));
            }
        }

        if (invalidCaches != null) {
            return new IgniteCheckedException("Failed to perform cache operation (cache topology is not valid): " +
                invalidCaches);
        }

        for (int i = 0; i < activeCacheIds.size(); i++) {
            int cacheId = activeCacheIds.get(i);

            GridCacheContext<?, ?> cacheCtx = cctx.cacheContext(cacheId);

            if (CU.affinityNodes(cacheCtx, topFut.topologyVersion()).isEmpty()) {
                return new ClusterTopologyServerNotFoundException("Failed to map keys for cache (all " +
                    "partition nodes left the grid): " + cacheCtx.name());
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheWriteSynchronizationMode syncMode(GridCacheSharedContext cctx) {
        CacheWriteSynchronizationMode syncMode = CacheWriteSynchronizationMode.FULL_ASYNC;

        for (int i = 0; i < activeCacheIds.size(); i++) {
            int cacheId = activeCacheIds.get(i);

            CacheWriteSynchronizationMode cacheSyncMode =
                cctx.cacheContext(cacheId).config().getWriteSynchronizationMode();

            switch (cacheSyncMode) {
                case FULL_SYNC:
                    return FULL_SYNC;

                case PRIMARY_SYNC: {
                    if (syncMode == FULL_ASYNC)
                        syncMode = PRIMARY_SYNC;

                    break;
                }

                case FULL_ASYNC:
                    break;
            }
        }

        return syncMode;
    }

    /** {@inheritDoc} */
    @Override public void addActiveCache(GridCacheContext cacheCtx, boolean recovery, IgniteTxLocalAdapter tx)
        throws IgniteCheckedException {
        GridCacheSharedContext cctx = cacheCtx.shared();

        int cacheId = cacheCtx.cacheId();

        if (this.recovery != null && this.recovery != recovery)
            throw new IgniteCheckedException("Failed to enlist an entry to existing transaction " +
                "(cannot transact between recovery and non-recovery caches).");

        this.recovery = recovery;

        // Check if we can enlist new cache to transaction.
        if (!activeCacheIds.contains(cacheId)) {
            String err = cctx.verifyTxCompatibility(tx, activeCacheIds, cacheCtx);

            if (err != null) {
                StringBuilder cacheNames = new StringBuilder();

                int idx = 0;

                for (int i = 0; i < activeCacheIds.size(); i++) {
                    int activeCacheId = activeCacheIds.get(i);

                    cacheNames.append(cctx.cacheContext(activeCacheId).name());

                    if (idx++ < activeCacheIds.size() - 1)
                        cacheNames.append(", ");
                }

                throw new IgniteCheckedException("Failed to enlist new cache to existing transaction (" +
                    err +
                    ") [activeCaches=[" + cacheNames + "]" +
                    ", cacheName=" + cacheCtx.name() +
                    ", cacheSystem=" + cacheCtx.systemTx() +
                    ", txSystem=" + tx.system() + ']');
            }
            else
                activeCacheIds.add(cacheId);

            if (activeCacheIds.size() == 1)
                tx.activeCachesDeploymentEnabled(cacheCtx.deploymentEnabled());
        }
    }

    /** {@inheritDoc} */
    @Override public GridDhtTopologyFuture topologyReadLock(GridCacheSharedContext cctx, GridFutureAdapter<?> fut) {
        if (activeCacheIds.isEmpty())
            return cctx.exchange().lastTopologyFuture();

        GridCacheContext<?, ?> nonLocCtx = null;

        for (int i = 0; i < activeCacheIds.size(); i++) {
            int cacheId = activeCacheIds.get(i);

            GridCacheContext<?, ?> cacheCtx = cctx.cacheContext(cacheId);

            if (!cacheCtx.isLocal()) {
                nonLocCtx = cacheCtx;

                break;
            }
        }

        if (nonLocCtx == null)
            return cctx.exchange().lastTopologyFuture();

        nonLocCtx.topology().readLock();

        if (nonLocCtx.topology().stopping()) {
            fut.onDone(new CacheStoppedException(nonLocCtx.name()));

            return null;
        }

        return nonLocCtx.topology().topologyVersionFuture();
    }

    /** {@inheritDoc} */
    @Override public void topologyReadUnlock(GridCacheSharedContext cctx) {
        if (!activeCacheIds.isEmpty()) {
            GridCacheContext<?, ?> nonLocCtx = null;

            for (int i = 0; i < activeCacheIds.size(); i++) {
                int cacheId = activeCacheIds.get(i);

                GridCacheContext<?, ?> cacheCtx = cctx.cacheContext(cacheId);

                if (!cacheCtx.isLocal()) {
                    nonLocCtx = cacheCtx;

                    break;
                }
            }

            if (nonLocCtx != null)
                nonLocCtx.topology().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean storeWriteThrough(GridCacheSharedContext cctx) {
        if (!activeCacheIds.isEmpty()) {
            for (int i = 0; i < activeCacheIds.size(); i++) {
                int cacheId = activeCacheIds.get(i);

                CacheStoreManager store = cctx.cacheContext(cacheId).store();

                if (store.configured() && store.isWriteThrough())
                    return true;
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean hasInterceptor(GridCacheSharedContext cctx) {
        for (int i = 0; i < activeCacheIds.size(); i++) {
            int cacheId = activeCacheIds.get(i);

            CacheInterceptor interceptor = cctx.cacheContext(cacheId).config().getInterceptor();

            if (interceptor != null)
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheStoreManager> stores(GridCacheSharedContext cctx) {
        GridIntList cacheIds = activeCacheIds;

        if (!cacheIds.isEmpty()) {
            Collection<CacheStoreManager> stores = new ArrayList<>(cacheIds.size());

            for (int i = 0; i < cacheIds.size(); i++) {
                int cacheId = cacheIds.get(i);

                CacheStoreManager store = cctx.cacheContext(cacheId).store();

                if (store.configured())
                    stores.add(store);
            }

            return stores;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void onTxEnd(GridCacheSharedContext cctx, IgniteInternalTx tx, boolean commit) {
        for (int i = 0; i < activeCacheIds.size(); i++) {
            int cacheId = activeCacheIds.get(i);

            GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

            onTxEnd(cacheCtx, tx, commit);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean init(int txSize) {
        if (txMap == null) {
            txMap = U.newLinkedHashMap(txSize > 0 ? txSize : 16);

            readView = new IgniteTxMap(txMap, CU.reads());
            writeView = new IgniteTxMap(txMap, CU.writes());

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean initialized() {
        return txMap != null;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> allEntries() {
        return txMap == null ? Collections.<IgniteTxEntry>emptySet() : txMap.values();
    }

    /**
     * @return All entries. Returned collection is copy of internal collection.
     */
    public synchronized Collection<IgniteTxEntry> allEntriesCopy() {
        return txMap == null ? Collections.<IgniteTxEntry>emptySet() : new HashSet<>(txMap.values());
    }

    /** {@inheritDoc} */
    @Override public IgniteTxEntry entry(IgniteTxKey key) {
        return txMap == null ? null : txMap.get(key);
    }

    /** {@inheritDoc} */
    @Override public boolean hasWriteKey(IgniteTxKey key) {
        return writeView.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteTxKey> readSet() {
        return txMap == null ? Collections.<IgniteTxKey>emptySet() : readView.keySet();
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteTxKey> writeSet() {
        return txMap == null ? Collections.<IgniteTxKey>emptySet() : writeView.keySet();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> writeEntries() {
        return writeView == null ? Collections.<IgniteTxEntry>emptyList() : writeView.values();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> readEntries() {
        return readView == null ? Collections.<IgniteTxEntry>emptyList() : readView.values();
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> writeMap() {
        return writeView == null ? Collections.<IgniteTxKey, IgniteTxEntry>emptyMap() : writeView;
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> readMap() {
        return readView == null ? Collections.<IgniteTxKey, IgniteTxEntry>emptyMap() : readView;
    }

    /** {@inheritDoc} */
    @Override public boolean empty() {
        return txMap.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public synchronized void addEntry(IgniteTxEntry entry) {
        txMap.put(entry.txKey(), entry);
    }

    /** {@inheritDoc} */
    @Override public void seal() {
        if (readView != null)
            readView.seal();

        if (writeView != null)
            writeView.seal();
    }

    /** {@inheritDoc} */
    @Override public IgniteTxEntry singleWrite() {
        return writeView != null && writeView.size() == 1 ? F.firstValue(writeView) : null;
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(IgniteTxStateImpl.class, this, "txMap", allEntriesCopy());
    }

    /**
     * Creates savepoint.
     *
     * @param name Savepoint ID.
     * @param overwrite If {@code true} - already created savepoint with the same name will be replaced.
     */
    public void savepoint(String name, boolean overwrite) {
        assert name != null;

        ListIterator<TxSavepoint> spIter = findSP(name);

        if (spIter != null) {
            if (overwrite)
                savepoints.subList(spIter.nextIndex(), savepoints.size()).clear();
            else {
                throw new IllegalArgumentException("Savepoint \"" + name + "\" already exists. " +
                    "Savepoints with the same name aren't available. " +
                    "If you want to rewrite savepoints - use savepoint(name, true) method.");
            }
        }

        createSavepoint(name);
    }

    /**
     * Rollback this transaction to previous state with given savepoint name. Also deletes all afterward savepoints.
     *
     * @param name Savepoint ID.
     * @throws IgniteCheckedException If failed.
     */
    public void rollbackToSavepoint(String name, GridNearTxLocal tx) throws IgniteCheckedException {
        assert name != null;
        assert tx != null;

        ListIterator<TxSavepoint> spIter = findSP(name);

        if (spIter == null)
            throw new IllegalArgumentException("No such savepoint. [name=" + name + ']');

        rollbackToSavepoint(spIter.next(), tx);

        savepoints.subList(spIter.nextIndex(), savepoints.size()).clear();
    }

    /**
     * Delete savepoint if it exist. Do nothing if there is no savepoint with such name.
     *
     * @param name Savepoint ID.
     */
    public void releaseSavepoint(String name) {
        assert name != null;

        ListIterator<TxSavepoint> spIter = findSP(name);

        if (spIter == null)
            return;

        savepoints.subList(spIter.nextIndex(), savepoints.size()).clear();
    }

    /**
     * Creates savepoint containing changes from last created savepoint.
     *
     * @param name Savepoint ID.
     */
    private void createSavepoint(String name) {
        Map<IgniteTxKey, IgniteTxEntry> txMapSnapshotPiece = new LinkedHashMap<>();

        if (savepoints == null)
            savepoints = new LinkedList<>();

        for (IgniteTxEntry txEntry : allEntries()) {
            if (!isKeyAlreadySaved(txEntry))
                txMapSnapshotPiece.put(txEntry.txKey(), txEntry.copy());
        }

        savepoints.add(new TxSavepoint(name, txMapSnapshotPiece));
    }

    /**
     * Checks that entry is already saved in any existing savepoint.
     *
     * @param txEntry Entry.
     * @return {code True} if entry is saved, otherwise - {code false}.
     */
    private boolean isKeyAlreadySaved(IgniteTxEntry txEntry) {
        for (TxSavepoint sp : savepoints) {
            IgniteTxEntry savedEntry = sp.getTxMapSnapshotPiece().get(txEntry.txKey());

            if (savedEntry != null && savedEntry.equals(txEntry))
                return true;
        }

        return false;
    }

    /**
     * Find savepoint with given name and returns ListIterator with cursor on this savepoint.
     *
     * @param name Savepoint ID.
     * @return ListIterator with cursor position on the named savepoint or null if there is no such savepoint.
     */
    private ListIterator<TxSavepoint> findSP(String name) {
        if (savepoints == null)
            return null;

        ListIterator<TxSavepoint> iter = savepoints.listIterator(savepoints.size());

        while (iter.hasPrevious()) {
            TxSavepoint sp = iter.previous();

            if (sp.getName().equals(name))
                return iter;
        }

        return null;
    }

    /**
     * Replace current state by the state, saved in savepoint.
     *
     * @param savepoint Savepoint.
     * @param tx Transaction where this state exists.
     * @throws IgniteCheckedException If failed.
     */
    public void rollbackToSavepoint(TxSavepoint savepoint, GridNearTxLocal tx) throws IgniteCheckedException {
        assert savepoint != null;
        assert tx != null;

        if (txMap == null) {
            tx.log().info("Nothing to rollback. " +
                "Savepoints are available only for transactional caches on the same node as transaction. " +
                "Atomic caches and caches from other nodes have nothing to save and rollback " +
                "because they have non transactional behaviour.");

            return;
        }

        Map<IgniteTxKey, IgniteTxEntry> initTxMap = extractSavepoint(savepoint);

        if (tx.optimistic())
            return;

        Collection<IgniteTxKey> keysToUnlock = gatherKeysToUnlock(initTxMap);

        if (keysToUnlock.isEmpty())
            return;

        Map<ClusterNode, Map<GridCacheAdapter, List<KeyCacheObject>>> mapping = mapKeysToCaches(
            keysToUnlock, initTxMap);

        tx.sendRollbackToSavepointMessages(mapping);

        tx.removeMappings(keysToUnlock);
    }

    /**
     * @param savepoint Savepoint to rollback.
     * @return Copy of current txMap.
     */
    private Map<IgniteTxKey, IgniteTxEntry> extractSavepoint(TxSavepoint savepoint) {
        Map<IgniteTxKey, IgniteTxEntry> curTxMap = new HashMap<>(txMap);

        txMap.clear();

        txMap.putAll(extractValues(savepoint));

        return curTxMap;
    }

    /**
     * @param keysToUnlock Keys to map.
     * @param replacedTxMap Tx state before rollback.
     * @return Map with caches and their keys.
     */
    private Map<ClusterNode, Map<GridCacheAdapter, List<KeyCacheObject>>> mapKeysToCaches(
        Collection<IgniteTxKey> keysToUnlock,
        Map<IgniteTxKey, IgniteTxEntry> replacedTxMap
    ) {
        Map<ClusterNode, Map<GridCacheAdapter, List<KeyCacheObject>>> caches = new HashMap<>();

        for (IgniteTxKey key : keysToUnlock) {
            GridCacheAdapter cache = replacedTxMap.get(key).context().cache();

            ClusterNode node = cache.affinity().mapPartitionToNode(key.key().partition());

            Map<GridCacheAdapter, List<KeyCacheObject>> cache2Keys = caches.computeIfAbsent(node, c -> new HashMap<>());

            List<KeyCacheObject> keys = cache2Keys.computeIfAbsent(cache, k -> new ArrayList<>());

            keys.add(key.key());
        }

        return caches;
    }

    /**
     * @param replacedTxMap Tx state before rollback.
     * @return Keys which primary node isn't local.
     */
    private Collection<IgniteTxKey> gatherKeysToUnlock(Map<IgniteTxKey, IgniteTxEntry> replacedTxMap) {
        Collection<IgniteTxKey> keysToUnlock = new ArrayList<>(replacedTxMap.size());

        for (Map.Entry<IgniteTxKey, IgniteTxEntry> entry : replacedTxMap.entrySet()) {
            if (!txMap.containsKey(entry.getKey()))
                keysToUnlock.add(entry.getKey());
        }

        return keysToUnlock;
    }

    /**
     * @return Copy of values from txMap snapshot.
     */
    private Map<IgniteTxKey, IgniteTxEntry> extractValues(TxSavepoint savepoint) {
        Map<IgniteTxKey, IgniteTxEntry> map = new LinkedHashMap<>();

        for (TxSavepoint sp : savepoints) {
            for (IgniteTxEntry entry : sp.getTxMapSnapshotPiece().values())
                map.put(entry.txKey(), entry.copy());

            if (sp == savepoint)
                break;
        }

        return map;
    }

    /**
     * Removes entries with specified cacheId from txMap.
     *
     * @param caches Caches with keys to remove from txState.
     */
    public void clearDhtEntries(Map<Integer, List<KeyCacheObject>> caches) {
        txMap.keySet().removeIf(key -> {
            for (Map.Entry<Integer, List<KeyCacheObject>> e : caches.entrySet()) {
                if (key.cacheId() == e.getKey() && e.getValue().contains(key.key()))
                    return true;
            }

            return false;
        });
    }
}
