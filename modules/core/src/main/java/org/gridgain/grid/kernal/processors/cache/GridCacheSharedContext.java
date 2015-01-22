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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.managers.discovery.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.apache.ignite.internal.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.cache.GridCacheFlag.*;

/**
 * Shared context.
 */
public class GridCacheSharedContext<K, V> {
    /** Kernal context. */
    private GridKernalContext kernalCtx;

    /** Managers in starting order. */
    private List<GridCacheSharedManager<K, V>> mgrs = new LinkedList<>();

    /** Cache transaction manager. */
    private IgniteTxManager<K, V> txMgr;

    /** Partition exchange manager. */
    private GridCachePartitionExchangeManager<K, V> exchMgr;

    /** Version manager. */
    private GridCacheVersionManager<K, V> verMgr;

    /** Lock manager. */
    private GridCacheMvccManager<K, V> mvccMgr;

    /** IO Manager. */
    private GridCacheIoManager<K, V> ioMgr;

    /** Deployment manager. */
    private GridCacheDeploymentManager<K, V> depMgr;

    /** Cache contexts map. */
    private Map<Integer, GridCacheContext<K, V>> ctxMap;

    /** Tx metrics. */
    private volatile IgniteTxMetricsAdapter txMetrics;

    /** Preloaders start future. */
    private IgniteFuture<Object> preloadersStartFut;

    /**
     * @param txMgr Transaction manager.
     * @param verMgr Version manager.
     * @param mvccMgr MVCC manager.
     */
    public GridCacheSharedContext(
        GridKernalContext kernalCtx,
        IgniteTxManager<K, V> txMgr,
        GridCacheVersionManager<K, V> verMgr,
        GridCacheMvccManager<K, V> mvccMgr,
        GridCacheDeploymentManager<K, V> depMgr,
        GridCachePartitionExchangeManager<K, V> exchMgr,
        GridCacheIoManager<K, V> ioMgr
    ) {
        this.kernalCtx = kernalCtx;
        this.mvccMgr = add(mvccMgr);
        this.verMgr = add(verMgr);
        this.txMgr = add(txMgr);
        this.depMgr = add(depMgr);
        this.exchMgr = add(exchMgr);
        this.ioMgr = add(ioMgr);

        txMetrics = new IgniteTxMetricsAdapter();

        ctxMap = new HashMap<>();
    }

    /**
     * Gets all cache contexts for local node.
     *
     * @return Collection of all cache contexts.
     */
    public Collection<GridCacheContext<K, V>> cacheContexts() {
        return ctxMap.values();
    }

    /**
     * Adds cache context to shared cache context.
     *
     * @param cacheCtx Cache context.
     */
    @SuppressWarnings("unchecked")
    public void addCacheContext(GridCacheContext cacheCtx) throws IgniteCheckedException {
        if (ctxMap.containsKey(cacheCtx.cacheId())) {
            GridCacheContext<K, V> existing = ctxMap.get(cacheCtx.cacheId());

            throw new IgniteCheckedException("Failed to start cache due to conflicting cache ID " +
                "(change cache name and restart grid) [cacheName=" + cacheCtx.name() +
                ", conflictingCacheName=" + existing.name() + ']');
        }

        ctxMap.put(cacheCtx.cacheId(), cacheCtx);
    }

    /**
     * @return List of shared context managers in starting order.
     */
    public List<GridCacheSharedManager<K, V>> managers() {
        return mgrs;
    }

    /**
     * Gets cache context by cache ID.
     *
     * @param cacheId Cache ID.
     * @return Cache context.
     */
    public GridCacheContext<K, V> cacheContext(int cacheId) {
        return ctxMap.get(cacheId);
    }

    /**
     * @return Grid name.
     */
    public String gridName() {
        return kernalCtx.gridName();
    }

    /**
     * Gets transactions configuration.
     *
     * @return Transactions configuration.
     */
    public TransactionsConfiguration txConfig() {
        return kernalCtx.config().getTransactionsConfiguration();
    }

    /**
     * @return Timeout for initial map exchange before preloading. We make it {@code 4} times
     * bigger than network timeout by default.
     */
    public long preloadExchangeTimeout() {
        long t1 = gridConfig().getNetworkTimeout() * 4;
        long t2 = gridConfig().getNetworkTimeout() * gridConfig().getCacheConfiguration().length * 2;

        long timeout = Math.max(t1, t2);

        return timeout < 0 ? Long.MAX_VALUE : timeout;
    }

    /**
     * @return Deployment enabled flag.
     */
    public boolean deploymentEnabled() {
        return kernalContext().deploy().enabled();
    }

    /**
     * @return Data center ID.
     */
    public byte dataCenterId() {
        // Data center ID is same for all caches, so grab the first one.
        GridCacheContext<K, V> cacheCtx = F.first(cacheContexts());

        return cacheCtx.dataCenterId();
    }

    /**
     * @return Compound preloaders start future.
     */
    public IgniteFuture<Object> preloadersStartFuture() {
        if (preloadersStartFut == null) {
            GridCompoundFuture<Object, Object> compound = null;

            for (GridCacheContext<K, V> cacheCtx : cacheContexts()) {
                IgniteFuture<Object> startFut = cacheCtx.preloader().startFuture();

                if (!startFut.isDone()) {
                    if (compound == null)
                        compound = new GridCompoundFuture<>();

                    compound.add(startFut);
                }
            }

            if (compound != null) {
                compound.markInitialized();

                return preloadersStartFut = compound;
            }
            else
                return preloadersStartFut = new GridFinishedFuture<>();
        }
        else
            return preloadersStartFut;
    }

    /**
     * @return Transactional metrics adapter.
     */
    public IgniteTxMetricsAdapter txMetrics() {
        return txMetrics;
    }

    /**
     * Resets tx metrics.
     */
    public void resetTxMetrics() {
        txMetrics = new IgniteTxMetricsAdapter();
    }

    /**
     * @return Cache transaction manager.
     */
    public IgniteTxManager<K, V> tm() {
        return txMgr;
    }

    /**
     * @return Exchange manager.
     */
    public GridCachePartitionExchangeManager<K, V> exchange() {
        return exchMgr;
    }

    /**
     * @return Lock order manager.
     */
    public GridCacheVersionManager<K, V> versions() {
        return verMgr;
    }

    /**
     * @return Lock manager.
     */
    public GridCacheMvccManager<K, V> mvcc() {
        return mvccMgr;
    }

    /**
     * @return IO manager.
     */
    public GridCacheIoManager<K, V> io() {
        return ioMgr;
    }

    /**
     * @return Cache deployment manager.
     */
    public GridCacheDeploymentManager<K, V> deploy() {
        return depMgr;
    }

    /**
     * @return Marshaller.
     */
    public IgniteMarshaller marshaller() {
        return kernalCtx.config().getMarshaller();
    }

    /**
     * @return Grid configuration.
     */
    public IgniteConfiguration gridConfig() {
        return kernalCtx.config();
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return kernalCtx;
    }

    /**
     * @return Grid IO manager.
     */
    public GridIoManager gridIO() {
        return kernalCtx.io();
    }

    /**
     * @return Grid deployment manager.
     */
    public GridDeploymentManager gridDeploy() {
        return kernalCtx.deploy();
    }

    /**
     * @return Grid event storage manager.
     */
    public GridEventStorageManager gridEvents() {
        return kernalCtx.event();
    }

    /**
     * @return Discovery manager.
     */
    public GridDiscoveryManager discovery() {
        return kernalCtx.discovery();
    }

    /**
     * @return Timeout processor.
     */
    public GridTimeoutProcessor time() {
        return kernalCtx.timeout();
    }

    /**
     * @return Node ID.
     */
    public UUID localNodeId() {
        return kernalCtx.localNodeId();
    }

    /**
     * @return Local node.
     */
    public ClusterNode localNode() {
        return kernalCtx.discovery().localNode();
    }

    /**
     * @param nodeId Node ID.
     * @return Node or {@code null}.
     */
    public ClusterNode node(UUID nodeId) {
        return kernalCtx.discovery().node(nodeId);
    }

    /**
     * Gets grid logger for given class.
     *
     * @param cls Class to get logger for.
     * @return GridLogger instance.
     */
    public IgniteLogger logger(Class<?> cls) {
        return kernalCtx.log(cls);
    }

    /**
     * @param category Category.
     * @return Logger.
     */
    public IgniteLogger logger(String category) {
        return kernalCtx.log().getLogger(category);
    }

    /**
     * Waits for partition locks and transactions release.
     *
     * @param topVer Topology version.
     * @return {@code true} if waiting was successful.
     */
    @SuppressWarnings({"unchecked"})
    public IgniteFuture<?> partitionReleaseFuture(long topVer) {
        GridCompoundFuture f = new GridCompoundFuture(kernalCtx);

        f.add(mvcc().finishExplicitLocks(topVer));
        f.add(tm().finishTxs(topVer));
        f.add(mvcc().finishAtomicUpdates(topVer));

        f.markInitialized();

        return f;
    }

    /**
     * @param tx Transaction to check.
     * @param activeCacheIds Active cache IDs.
     * @param cacheCtx Cache context.
     * @return {@code True} if cross-cache transaction can include this new cache.
     */
    public boolean txCompatible(IgniteTxEx<K, V> tx, Iterable<Integer> activeCacheIds, GridCacheContext<K, V> cacheCtx) {
        if (cacheCtx.system() ^ tx.system())
            return false;

        for (Integer cacheId : activeCacheIds) {
            GridCacheContext<K, V> activeCacheCtx = cacheContext(cacheId);

            // Check that caches have the same store.
            if (activeCacheCtx.store().store() != cacheCtx.store().store())
                return false;
        }

        return true;
    }

    /**
     * @param flags Flags to turn on.
     * @throws GridCacheFlagException If given flags are conflicting with given transaction.
     */
    public void checkTxFlags(@Nullable Collection<GridCacheFlag> flags) throws GridCacheFlagException {
        IgniteTxEx tx = tm().userTxx();

        if (tx == null || F.isEmpty(flags))
            return;

        assert flags != null;

        if (flags.contains(INVALIDATE) && !tx.isInvalidate())
            throw new GridCacheFlagException(INVALIDATE);

        if (flags.contains(SYNC_COMMIT) && !tx.syncCommit())
            throw new GridCacheFlagException(SYNC_COMMIT);
    }

    /**
     * Nulling references to potentially leak-prone objects.
     */
    public void cleanup() {
        mvccMgr = null;

        mgrs.clear();
    }

    /**
     * @param tx Transaction to close.
     * @throws IgniteCheckedException If failed.
     */
    public void endTx(IgniteTxEx<K, V> tx) throws IgniteCheckedException {
        Collection<Integer> cacheIds = tx.activeCacheIds();

        if (!cacheIds.isEmpty()) {
            for (Integer cacheId : cacheIds)
                cacheContext(cacheId).cache().awaitLastFut();
        }

        tx.close();
    }

    /**
     * @param tx Transaction to commit.
     * @return Commit future.
     */
    public IgniteFuture<IgniteTx> commitTxAsync(IgniteTxEx<K, V> tx) {
        Collection<Integer> cacheIds = tx.activeCacheIds();

        if (cacheIds.isEmpty())
            return tx.commitAsync();
        else if (cacheIds.size() == 1) {
            int cacheId = F.first(cacheIds);

            return cacheContext(cacheId).cache().commitTxAsync(tx);
        }
        else {
            for (Integer cacheId : cacheIds)
                cacheContext(cacheId).cache().awaitLastFut();

            return tx.commitAsync();
        }
    }

    /**
     * @param tx Transaction to rollback.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteFuture rollbackTxAsync(IgniteTxEx<K, V> tx) throws IgniteCheckedException {
        Collection<Integer> cacheIds = tx.activeCacheIds();

        if (!cacheIds.isEmpty()) {
            for (Integer cacheId : cacheIds)
                cacheContext(cacheId).cache().awaitLastFut();
        }

        return tx.rollbackAsync();
    }

    /**
     * @param mgr Manager to add.
     * @return Added manager.
     */
    @Nullable private <T extends GridCacheSharedManager<K, V>> T add(@Nullable T mgr) {
        if (mgr != null)
            mgrs.add(mgr);

        return mgr;
    }
}
