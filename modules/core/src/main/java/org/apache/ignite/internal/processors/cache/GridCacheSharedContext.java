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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManager;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.PartitionsEvictManager;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.jta.CacheJtaManagerAdapter;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCachingManager;
import org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.processors.cache.store.CacheStoreManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.PluginProvider;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOCAL_STORE_KEEPS_PRIMARY_ONLY;
import static org.apache.ignite.transactions.TransactionState.MARKED_ROLLBACK;

/**
 * Shared context.
 */
@GridToStringExclude
public class GridCacheSharedContext<K, V> {
    /** Kernal context. */
    private GridKernalContext kernalCtx;

    /** Managers in starting order. */
    private List<GridCacheSharedManager<K, V>> mgrs = new LinkedList<>();

    /** Cache transaction manager. */
    private IgniteTxManager txMgr;

    /** JTA manager. */
    private CacheJtaManagerAdapter jtaMgr;

    /** Partition exchange manager. */
    private GridCachePartitionExchangeManager<K, V> exchMgr;

    /** Version manager. */
    private GridCacheVersionManager verMgr;

    /** Lock manager. */
    private GridCacheMvccManager mvccMgr;

    /** IO Manager. */
    private GridCacheIoManager ioMgr;

    /** Deployment manager. */
    private GridCacheDeploymentManager<K, V> depMgr;

    /** Write ahead log manager. {@code Null} if persistence is not enabled. */
    @Nullable private IgniteWriteAheadLogManager walMgr;

    /** Write ahead log state manager. */
    private WalStateManager walStateMgr;

    /** Database manager. */
    private IgniteCacheDatabaseSharedManager dbMgr;

    /** Snp manager. */
    private IgniteCacheSnapshotManager snpMgr;

    /** Page store manager. {@code Null} if persistence is not enabled. */
    @Nullable private IgnitePageStoreManager pageStoreMgr;

    /** Affinity manager. */
    private CacheAffinitySharedManager affMgr;

    /** Ttl cleanup manager. */
    private GridCacheSharedTtlCleanupManager ttlMgr;

    /** */
    private PartitionsEvictManager evictMgr;

    /** Mvcc caching manager. */
    private MvccCachingManager mvccCachingMgr;

    /** Cache contexts map. */
    private ConcurrentHashMap<Integer, GridCacheContext<K, V>> ctxMap;

    /** Tx metrics. */
    private final TransactionMetricsAdapter txMetrics;

    /** Store session listeners. */
    private Collection<CacheStoreSessionListener> storeSesLsnrs;

    /** Local store count. */
    private final AtomicInteger locStoreCnt;

    /** Indicating whether local store keeps primary only. */
    private final boolean locStorePrimaryOnly = IgniteSystemProperties.getBoolean(IGNITE_LOCAL_STORE_KEEPS_PRIMARY_ONLY);

    /** */
    private final IgniteLogger msgLog;

    /** */
    private final IgniteLogger atomicMsgLog;

    /** */
    private final IgniteLogger txPrepareMsgLog;

    /** */
    private final IgniteLogger txFinishMsgLog;

    /** */
    private final IgniteLogger txLockMsgLog;

    /** */
    private final IgniteLogger txRecoveryMsgLog;

    /** Concurrent DHT atomic updates counters. */
    private AtomicIntegerArray dhtAtomicUpdCnt;

    /** Rebalance enabled flag. */
    private boolean rebalanceEnabled = true;

    /** */
    private final List<IgniteChangeGlobalStateSupport> stateAwareMgrs;

    /** Cluster is in read-only mode. */
    private volatile boolean readOnlyMode;

    /**
     * @param kernalCtx  Context.
     * @param txMgr Transaction manager.
     * @param verMgr Version manager.
     * @param mvccMgr MVCC manager.
     * @param pageStoreMgr Page store manager. {@code Null} if persistence is not enabled.
     * @param walMgr WAL manager. {@code Null} if persistence is not enabled.
     * @param walStateMgr WAL state manager.
     * @param depMgr Deployment manager.
     * @param exchMgr Exchange manager.
     * @param affMgr Affinity manager.
     * @param ioMgr IO manager.
     * @param ttlMgr Ttl cleanup manager.
     * @param jtaMgr JTA manager.
     * @param storeSesLsnrs Store session listeners.
     */
    public GridCacheSharedContext(
        GridKernalContext kernalCtx,
        IgniteTxManager txMgr,
        GridCacheVersionManager verMgr,
        GridCacheMvccManager mvccMgr,
        @Nullable IgnitePageStoreManager pageStoreMgr,
        @Nullable IgniteWriteAheadLogManager walMgr,
        WalStateManager walStateMgr,
        IgniteCacheDatabaseSharedManager dbMgr,
        IgniteCacheSnapshotManager snpMgr,
        GridCacheDeploymentManager<K, V> depMgr,
        GridCachePartitionExchangeManager<K, V> exchMgr,
        CacheAffinitySharedManager<K, V> affMgr,
        GridCacheIoManager ioMgr,
        GridCacheSharedTtlCleanupManager ttlMgr,
        PartitionsEvictManager evictMgr,
        CacheJtaManagerAdapter jtaMgr,
        Collection<CacheStoreSessionListener> storeSesLsnrs,
        MvccCachingManager mvccCachingMgr
    ) {
        this.kernalCtx = kernalCtx;

        setManagers(
            mgrs,
            txMgr,
            jtaMgr,
            verMgr,
            mvccMgr,
            pageStoreMgr,
            walMgr,
            walStateMgr,
            dbMgr,
            snpMgr,
            depMgr,
            exchMgr,
            affMgr,
            ioMgr,
            ttlMgr,
            evictMgr,
            mvccCachingMgr
        );

        this.storeSesLsnrs = storeSesLsnrs;

        txMetrics = new TransactionMetricsAdapter(kernalCtx);

        ctxMap = new ConcurrentHashMap<>();

        locStoreCnt = new AtomicInteger();

        if (dbMgr != null && CU.isPersistenceEnabled(kernalCtx.config()))
            dhtAtomicUpdCnt = new AtomicIntegerArray(kernalCtx.config().getSystemThreadPoolSize());

        msgLog = kernalCtx.log(CU.CACHE_MSG_LOG_CATEGORY);
        atomicMsgLog = kernalCtx.log(CU.ATOMIC_MSG_LOG_CATEGORY);
        txPrepareMsgLog = kernalCtx.log(CU.TX_MSG_PREPARE_LOG_CATEGORY);
        txFinishMsgLog = kernalCtx.log(CU.TX_MSG_FINISH_LOG_CATEGORY);
        txLockMsgLog = kernalCtx.log(CU.TX_MSG_LOCK_LOG_CATEGORY);
        txRecoveryMsgLog = kernalCtx.log(CU.TX_MSG_RECOVERY_LOG_CATEGORY);

        stateAwareMgrs = new ArrayList<>();

        if (pageStoreMgr != null)
            stateAwareMgrs.add(pageStoreMgr);

        if (walMgr != null)
            stateAwareMgrs.add(walMgr);

        stateAwareMgrs.add(dbMgr);

        stateAwareMgrs.add(snpMgr);

        for (PluginProvider prv : kernalCtx.plugins().allProviders())
            if (prv instanceof IgniteChangeGlobalStateSupport)
                stateAwareMgrs.add(((IgniteChangeGlobalStateSupport)prv));
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void activate() throws IgniteCheckedException {
        long time = System.currentTimeMillis();

        for (IgniteChangeGlobalStateSupport mgr : stateAwareMgrs)
            mgr.onActivate(kernalCtx);

        if (msgLog.isInfoEnabled())
            msgLog.info("Components activation performed in " + (System.currentTimeMillis() - time) + " ms.");
    }

    /**
     *
     */
    public void deactivate() {
        for (int i = stateAwareMgrs.size() - 1; i >= 0; i--)
            stateAwareMgrs.get(i).onDeActivate(kernalCtx);
    }

    /**
     * @return Logger.
     */
    public IgniteLogger messageLogger() {
        return msgLog;
    }

    /**
     * @return Logger.
     */
    public IgniteLogger atomicMessageLogger() {
        return atomicMsgLog;
    }

    /**
     * @return Logger.
     */
    public IgniteLogger txPrepareMessageLogger() {
        return txPrepareMsgLog;
    }

    /**
     * @return Logger.
     */
    public IgniteLogger txFinishMessageLogger() {
        return txFinishMsgLog;
    }

    /**
     * @return Logger.
     */
    public IgniteLogger txLockMessageLogger() {
        return txLockMsgLog;
    }

    /**
     * @return Logger.
     */
    public IgniteLogger txRecoveryMessageLogger() {
        return txRecoveryMsgLog;
    }

    /**
     * @return rebalance enabled flag.
     */
    public boolean isRebalanceEnabled() {
        return this.rebalanceEnabled;
    }

    /**
     * @param rebalanceEnabled rebalance enabled flag.
     */
    public void rebalanceEnabled(boolean rebalanceEnabled) {
        this.rebalanceEnabled = rebalanceEnabled;

        if (rebalanceEnabled)
            cache().enableRebalance();
    }

    /**
     * @param reconnectFut Reconnect future.
     * @throws IgniteCheckedException If failed.
     */
    void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        for (ListIterator<? extends GridCacheSharedManager<?, ?>> it = mgrs.listIterator(mgrs.size());
            it.hasPrevious();) {
            GridCacheSharedManager<?, ?> mgr = it.previous();

            mgr.onDisconnected(reconnectFut);

            if (restartOnDisconnect(mgr))
                mgr.onKernalStop(true);
        }

        for (ListIterator<? extends GridCacheSharedManager<?, ?>> it = mgrs.listIterator(mgrs.size()); it.hasPrevious();) {
            GridCacheSharedManager<?, ?> mgr = it.previous();

            if (restartOnDisconnect(mgr))
                mgr.stop(true);
        }

        deactivate();
    }

    /**
     * @param active Active flag.
     * @throws IgniteCheckedException If failed.
     */
    void onReconnected(boolean active) throws IgniteCheckedException {
        List<GridCacheSharedManager<K, V>> mgrs = new LinkedList<>();

        setManagers(
            mgrs,
            txMgr,
            jtaMgr,
            verMgr,
            mvccMgr,
            pageStoreMgr,
            walMgr,
            walStateMgr,
            dbMgr,
            snpMgr,
            new GridCacheDeploymentManager<K, V>(),
            new GridCachePartitionExchangeManager<K, V>(),
            affMgr,
            ioMgr,
            ttlMgr,
            evictMgr,
            mvccCachingMgr
        );

        this.mgrs = mgrs;

        for (GridCacheSharedManager<K, V> mgr : mgrs) {
            if (restartOnDisconnect(mgr))
                mgr.start(this);

            mgr.onReconnected(active);
        }

        kernalCtx.query().onCacheReconnect();

        if (!active)
            affinity().clearGroupHoldersAndRegistry();

        exchMgr.onKernalStart(active, true);
    }

    /**
     * @param mgr Manager.
     * @return {@code True} if manager is restarted cn reconnect.
     */
    private boolean restartOnDisconnect(GridCacheSharedManager<?, ?> mgr) {
        return mgr instanceof GridCacheDeploymentManager || mgr instanceof GridCachePartitionExchangeManager;
    }

    /**
     * @param mgrs Managers list.
     * @param txMgr Transaction manager.
     * @param jtaMgr JTA manager.
     * @param verMgr Version manager.
     * @param mvccMgr MVCC manager.
     * @param pageStoreMgr Page store manager. {@code Null} if persistence is not enabled.
     * @param walStateMgr WAL state manager.
     * @param depMgr Deployment manager.
     * @param exchMgr Exchange manager.
     * @param affMgr Affinity manager.
     * @param ioMgr IO manager.
     * @param ttlMgr Ttl cleanup manager.
     */
    @SuppressWarnings("unchecked")
    private void setManagers(
        List<GridCacheSharedManager<K, V>> mgrs,
        IgniteTxManager txMgr,
        CacheJtaManagerAdapter jtaMgr,
        GridCacheVersionManager verMgr,
        GridCacheMvccManager mvccMgr,
        @Nullable IgnitePageStoreManager pageStoreMgr,
        IgniteWriteAheadLogManager walMgr,
        WalStateManager walStateMgr,
        IgniteCacheDatabaseSharedManager dbMgr,
        IgniteCacheSnapshotManager snpMgr,
        GridCacheDeploymentManager<K, V> depMgr,
        GridCachePartitionExchangeManager<K, V> exchMgr,
        CacheAffinitySharedManager affMgr,
        GridCacheIoManager ioMgr,
        GridCacheSharedTtlCleanupManager ttlMgr,
        PartitionsEvictManager evictMgr,
        MvccCachingManager mvccCachingMgr
    ) {
        this.mvccMgr = add(mgrs, mvccMgr);
        this.verMgr = add(mgrs, verMgr);
        this.txMgr = add(mgrs, txMgr);
        this.pageStoreMgr = add(mgrs, pageStoreMgr);
        this.walMgr = add(mgrs, walMgr);
        this.walStateMgr = add(mgrs, walStateMgr);
        this.dbMgr = add(mgrs, dbMgr);
        this.snpMgr = add(mgrs, snpMgr);
        this.jtaMgr = add(mgrs, jtaMgr);
        this.depMgr = add(mgrs, depMgr);
        this.exchMgr = add(mgrs, exchMgr);
        this.affMgr = add(mgrs, affMgr);
        this.ioMgr = add(mgrs, ioMgr);
        this.ttlMgr = add(mgrs, ttlMgr);
        this.evictMgr = add(mgrs, evictMgr);
        this.mvccCachingMgr = add(mgrs, mvccCachingMgr);
    }

    /**
     * Gets all cache contexts for local node.
     *
     * @return Collection of all cache contexts.
     */
    public Collection<GridCacheContext> cacheContexts() {
        return (Collection)ctxMap.values();
    }

    /**
     * @param c Cache context closure.
     */
    void forAllCaches(final IgniteInClosure<GridCacheContext> c) {
        for (Integer cacheId : ctxMap.keySet()) {
            ctxMap.computeIfPresent(cacheId,
                (cacheId1, ctx) -> {
                    c.apply(ctx);

                    return ctx;
                }
            );
        }
    }

    /**
     * @return Cache processor.
     */
    public GridCacheProcessor cache() {
        return kernalCtx.cache();
    }

    /**
     * Adds cache context to shared cache context.
     *
     * @param cacheCtx Cache context to add.
     * @throws IgniteCheckedException If cache ID conflict detected.
     */
    @SuppressWarnings("unchecked")
    public void addCacheContext(GridCacheContext cacheCtx) throws IgniteCheckedException {
        if (ctxMap.containsKey(cacheCtx.cacheId())) {
            GridCacheContext<K, V> existing = ctxMap.get(cacheCtx.cacheId());

            throw new IgniteCheckedException("Failed to start cache due to conflicting cache ID " +
                "(change cache name and restart grid) [cacheName=" + cacheCtx.name() +
                ", conflictingCacheName=" + existing.name() + ']');
        }

        CacheStoreManager mgr = cacheCtx.store();

        if (mgr.configured() && mgr.isLocal())
            locStoreCnt.incrementAndGet();

        ctxMap.put(cacheCtx.cacheId(), cacheCtx);
    }

    /**
     * @param cacheCtx Cache context to remove.
     */
    void removeCacheContext(GridCacheContext cacheCtx) {
        int cacheId = cacheCtx.cacheId();

        ctxMap.remove(cacheId, cacheCtx);

        CacheStoreManager mgr = cacheCtx.store();

        if (mgr.configured() && mgr.isLocal())
            locStoreCnt.decrementAndGet();

        // Safely clean up the message listeners.
        ioMgr.removeCacheHandlers(cacheId);
    }

    /**
     * Checks if cache context is closed.
     *
     * @param ctx Cache context to check.
     * @return {@code True} if cache context is closed.
     */
    public boolean closed(GridCacheContext ctx) {
        return !ctxMap.containsKey(ctx.cacheId());
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
     * Returns cache object context if created or creates new and caches it until cache started.
     *
     * @param cacheId Cache id.
     */
    @Nullable public CacheObjectContext cacheObjectContext(int cacheId) throws IgniteCheckedException {
        GridCacheContext<K, V> ctx = ctxMap.get(cacheId);

        if (ctx != null)
            return ctx.cacheObjectContext();

        DynamicCacheDescriptor desc = cache().cacheDescriptor(cacheId);

        return desc != null ? desc.cacheObjectContext(kernalContext().cacheObjects()) : null;
    }

    /**
     * @return Ignite instance name.
     */
    public String igniteInstanceName() {
        return kernalCtx.igniteInstanceName();
    }

    /**
     * Gets transactions configuration.
     *
     * @return Transactions configuration.
     */
    public TransactionConfiguration txConfig() {
        return kernalCtx.config().getTransactionConfiguration();
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
        GridCacheContext<?, ?> cacheCtx = F.first(cacheContexts());

        return cacheCtx.dataCenterId();
    }

    /**
     * @return Transactional metrics adapter.
     */
    public TransactionMetricsAdapter txMetrics() {
        return txMetrics;
    }

    /**
     * Resets tx metrics.
     */
    public void resetTxMetrics() {
        txMetrics.reset();
    }

    /**
     * @return Cache transaction manager.
     */
    public IgniteTxManager tm() {
        return txMgr;
    }

    /**
     * @return JTA manager.
     */
    public CacheJtaManagerAdapter jta() {
        return jtaMgr;
    }

    /**
     * @return Exchange manager.
     */
    public GridCachePartitionExchangeManager<K, V> exchange() {
        return exchMgr;
    }

    /**
     * @return Affinity manager.
     */
    public CacheAffinitySharedManager<K, V> affinity() {
        return affMgr;
    }

    /**
     * @return Lock order manager.
     */
    public GridCacheVersionManager versions() {
        return verMgr;
    }

    /**
     * @return Lock manager.
     */
    public GridCacheMvccManager mvcc() {
        return mvccMgr;
    }

    /**
     * @return Database manager.
     */
    public IgniteCacheDatabaseSharedManager database() {
        return dbMgr;
    }

    /**
     * @return Snapshot manager.
     */
    public IgniteCacheSnapshotManager snapshot() {
        return snpMgr;
    }

    /**
     * @return Page store manager. {@code Null} if persistence is not enabled.
     */
    @Nullable public IgnitePageStoreManager pageStore() {
        return pageStoreMgr;
    }

    /**
     * @return Write ahead log manager.
     */
    public IgniteWriteAheadLogManager wal() {
        return walMgr;
    }

    /**
     * @return WAL state manager.
     */
    public WalStateManager walState() {
       return walStateMgr;
    }

    /**
     * @return IO manager.
     */
    public GridCacheIoManager io() {
        return ioMgr;
    }

    /**
     * @return Ttl cleanup manager.
     */
    public GridCacheSharedTtlCleanupManager ttl() {
        return ttlMgr;
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
    public Marshaller marshaller() {
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
     * @return Cache mvcc coordinator manager.
     */
    public MvccProcessor coordinators() {
        return kernalCtx.coordinators();
    }

    /**
     * @return Partition evict manager.
     */
    public PartitionsEvictManager evict() {
        return evictMgr;
    }

    /**
     * @return Mvcc transaction enlist caching manager.
     */
    public MvccCachingManager mvccCaching() {
        return mvccCachingMgr;
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
     * @return Count of caches with configured local stores.
     */
    public int getLocalStoreCount() {
        return locStoreCnt.get();
    }

    /**
     * @param nodeId Node ID.
     * @return Node or {@code null}.
     */
    @Nullable public ClusterNode node(UUID nodeId) {
        return kernalCtx.discovery().node(nodeId);
    }

    /** Indicating whether local store keeps primary only. */
    public boolean localStorePrimaryOnly() {
        return locStorePrimaryOnly;
    }

    /**
     * Gets grid logger for given class.
     *
     * @param cls Class to get logger for.
     * @return IgniteLogger instance.
     */
    public IgniteLogger logger(Class<?> cls) {
        return kernalCtx.log(cls);
    }

    /**
     * @param category Category.
     * @return Logger.
     */
    public IgniteLogger logger(String category) {
        return kernalCtx.log(category);
    }

    /**
     * Captures all ongoing operations that we need to wait before we can proceed to the next topology version.
     * This method must be called only after
     * {@link GridDhtPartitionTopology#updateTopologyVersion}
     * method is called so that all new updates will wait to switch to the new version.
     * This method will capture:
     * <ul>
     *     <li>All non-released cache locks</li>
     *     <li>All non-committed transactions (local and remote)</li>
     *     <li>All pending atomic updates</li>
     *     <li>All pending DataStreamer updates</li>
     * </ul>
     *
     * Captured updates are wrapped in a future that will be completed once pending objects are released.
     *
     * @param topVer Topology version.
     * @return {@code true} if waiting was successful.
     */
    @SuppressWarnings({"unchecked"})
    public IgniteInternalFuture<?> partitionReleaseFuture(AffinityTopologyVersion topVer) {
        GridCompoundFuture f = new CacheObjectsReleaseFuture("Partition", topVer);

        f.add(mvcc().finishExplicitLocks(topVer));
        f.add(mvcc().finishAtomicUpdates(topVer));
        f.add(mvcc().finishDataStreamerUpdates(topVer));

        IgniteInternalFuture<?> finishLocalTxsFuture = tm().finishLocalTxs(topVer);
        // To properly track progress of finishing local tx updates we explicitly add this future to compound set.
        f.add(finishLocalTxsFuture);
        f.add(tm().finishAllTxs(finishLocalTxsFuture, topVer));

        f.markInitialized();

        return f;
    }

    /**
     * Gets ready future for the next affinity topology version (used in cases when a node leaves grid).
     *
     * @param curVer Current topology version (before a node left grid).
     * @return Ready future.
     */
    public IgniteInternalFuture<?> nextAffinityReadyFuture(AffinityTopologyVersion curVer) {
        if (curVer == null)
            return null;

        AffinityTopologyVersion nextVer = new AffinityTopologyVersion(curVer.topologyVersion() + 1);

        IgniteInternalFuture<?> fut = exchMgr.affinityReadyFuture(nextVer);

        return fut == null ? new GridFinishedFuture<>() : fut;
    }

    /**
     * @param tx Transaction to check.
     * @param activeCacheIds Active cache IDs.
     * @param cacheCtx Cache context.
     * @return Error message if transactions are incompatible.
     */
    @Nullable public String verifyTxCompatibility(IgniteInternalTx tx, GridIntList activeCacheIds,
        GridCacheContext<K, V> cacheCtx) {
        if (cacheCtx.systemTx() && !tx.system())
            return "system cache can be enlisted only in system transaction";

        if (!cacheCtx.systemTx() && tx.system())
            return "non-system cache can't be enlisted in system transaction";

        for (int i = 0; i < activeCacheIds.size(); i++) {
            int cacheId = activeCacheIds.get(i);

            GridCacheContext<K, V> activeCacheCtx = cacheContext(cacheId);

            if (cacheCtx.systemTx()) {
                if (activeCacheCtx.cacheId() != cacheCtx.cacheId())
                    return "system transaction can include only one cache";
            }

            CacheStoreManager store = cacheCtx.store();
            CacheStoreManager activeStore = activeCacheCtx.store();

            if (store.isLocal() != activeStore.isLocal())
                return "caches with local and non-local stores can't be enlisted in one transaction";

            if (store.isWriteBehind() != activeStore.isWriteBehind())
                return "caches with different write-behind setting can't be enlisted in one transaction";

            if (activeCacheCtx.deploymentEnabled() != cacheCtx.deploymentEnabled())
                return "caches with enabled and disabled deployment modes can't be enlisted in one transaction";

            // If local and write-behind validations passed, this must be true.
            assert store.isWriteToStoreFromDht() == activeStore.isWriteToStoreFromDht();
        }

        return null;
    }

    /**
     * @param ignore Transaction to ignore.
     * @return Not null topology version if current thread holds lock preventing topology change.
     */
    @Nullable public AffinityTopologyVersion lockedTopologyVersion(IgniteInternalTx ignore) {
        long threadId = Thread.currentThread().getId();

        AffinityTopologyVersion topVer = txMgr.lockedTopologyVersion(threadId, ignore);

        if (topVer == null)
            topVer = mvccMgr.lastExplicitLockTopologyVersion(threadId);

        return topVer;
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
    public void endTx(GridNearTxLocal tx) throws IgniteCheckedException {
        boolean clearThreadMap = txMgr.threadLocalTx(null) == tx;

        if (clearThreadMap)
            tx.txState().awaitLastFuture(this);
        else
            tx.state(MARKED_ROLLBACK);

        tx.close(clearThreadMap);
    }

    /**
     * @param tx Transaction to commit.
     * @return Commit future.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<IgniteInternalTx> commitTxAsync(GridNearTxLocal tx) {
        GridCacheContext ctx = tx.txState().singleCacheContext(this);

        if (ctx == null) {
            tx.txState().awaitLastFuture(this);

            return tx.commitNearTxLocalAsync();
        }
        else
            return ctx.cache().commitTxAsync(tx);
    }

    /**
     * @param tx Transaction to rollback.
     * @throws IgniteCheckedException If failed.
     * @return Rollback future.
     */
    public IgniteInternalFuture rollbackTxAsync(GridNearTxLocal tx) throws IgniteCheckedException {
        boolean clearThreadMap = txMgr.threadLocalTx(null) == tx;

        if (clearThreadMap)
            tx.txState().awaitLastFuture(this);
        else
            tx.state(MARKED_ROLLBACK);

        return tx.rollbackNearTxLocalAsync(clearThreadMap, false);
    }

    /**
     * Suspends transaction. It could be resume later. Supported only for optimistic transactions.
     *
     * @param tx Transaction to suspend.
     * @throws IgniteCheckedException If suspension failed.
     */
    public void suspendTx(GridNearTxLocal tx) throws IgniteCheckedException {
        tx.txState().awaitLastFuture(this);

        tx.suspend();
    }

    /**
     * Resume transaction if it was previously suspended.
     *
     * @param tx Transaction to resume.
     * @throws IgniteCheckedException If resume failed.
     */
    public void resumeTx(GridNearTxLocal tx) throws IgniteCheckedException {
        tx.txState().awaitLastFuture(this);

        tx.resume();
    }

    /**
     * @return Store session listeners.
     */
    @Nullable public Collection<CacheStoreSessionListener> storeSessionListeners() {
        return storeSesLsnrs;
    }

    /**
     * @param mgrs Managers list.
     * @param mgr Manager to add.
     * @return Added manager.
     */
    @Nullable private <T extends GridCacheSharedManager<K, V>> T add(List<GridCacheSharedManager<K, V>> mgrs,
        @Nullable T mgr) {
        if (mgr != null)
            mgrs.add(mgr);

        return mgr;
    }

    /**
     * Reset thread-local context for transactional cache.
     */
    public void txContextReset() {
        mvccMgr.contextReset();
    }

    /**
     * @param ver DHT atomic update future version.
     * @return Amount of active DHT atomic updates.
     */
    public int startDhtAtomicUpdate(GridCacheVersion ver) {
        assert dhtAtomicUpdCnt != null;

        return dhtAtomicUpdCnt.incrementAndGet(dhtAtomicUpdateIndex(ver));
    }

    /**
     * @param ver DHT atomic update future version.
     */
    public void finishDhtAtomicUpdate(GridCacheVersion ver) {
        assert dhtAtomicUpdCnt != null;

        dhtAtomicUpdCnt.decrementAndGet(dhtAtomicUpdateIndex(ver));
    }

    /**
     * @param ver Version.
     * @return Index.
     */
    private int dhtAtomicUpdateIndex(GridCacheVersion ver) {
        return U.safeAbs(ver.hashCode()) % dhtAtomicUpdCnt.length();
    }

    /**
     * @return {@code true} if cluster is in read-only mode.
     */
    public boolean readOnlyMode() {
        return readOnlyMode;
    }

    /**
     * @param readOnlyMode Read-only flag.
     */
    public void readOnlyMode(boolean readOnlyMode) {
        this.readOnlyMode = readOnlyMode;
    }
}
