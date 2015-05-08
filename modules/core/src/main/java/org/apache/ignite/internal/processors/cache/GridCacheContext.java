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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.managers.discovery.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.managers.swapspace.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.datastructures.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.jta.*;
import org.apache.ignite.internal.processors.cache.local.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.cache.query.continuous.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.cacheobject.*;
import org.apache.ignite.internal.processors.closure.*;
import org.apache.ignite.internal.processors.offheap.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.offheap.unsafe.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.plugin.security.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.expiry.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMemoryMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.*;
import static org.apache.ignite.internal.processors.cache.CacheFlag.*;

/**
 * Cache context.
 */
@GridToStringExclude
public class GridCacheContext<K, V> implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<IgniteBiTuple<String, String>> stash = new ThreadLocal<IgniteBiTuple<String, String>>() {
        @Override protected IgniteBiTuple<String, String> initialValue() {
            return F.t2();
        }
    };

    /** Empty cache version array. */
    private static final GridCacheVersion[] EMPTY_VERSION = new GridCacheVersion[0];

    /** Kernal context. */
    private GridKernalContext ctx;

    /** Cache shared context. */
    private GridCacheSharedContext<K, V> sharedCtx;

    /** Logger. */
    private IgniteLogger log;

    /** Cache configuration. */
    private CacheConfiguration cacheCfg;

    /** Unsafe memory object for direct memory allocation. */
    private GridUnsafeMemory unsafeMemory;

    /** Affinity manager. */
    private GridCacheAffinityManager affMgr;

    /** Event manager. */
    private GridCacheEventManager evtMgr;

    /** Query manager. */
    private GridCacheQueryManager<K, V> qryMgr;

    /** Continuous query manager. */
    private CacheContinuousQueryManager contQryMgr;

    /** Swap manager. */
    private GridCacheSwapManager swapMgr;

    /** Evictions manager. */
    private GridCacheEvictionManager evictMgr;

    /** Data structures manager. */
    private CacheDataStructuresManager dataStructuresMgr;

    /** Eager TTL manager. */
    private GridCacheTtlManager ttlMgr;

    /** Store manager. */
    private GridCacheStoreManager storeMgr;

    /** Replication manager. */
    private GridCacheDrManager drMgr;

    /** JTA manager. */
    private CacheJtaManagerAdapter jtaMgr;

    /** Managers. */
    private List<GridCacheManager<K, V>> mgrs = new LinkedList<>();

    /** Cache gateway. */
    private GridCacheGateway<K, V> gate;

    /** Grid cache. */
    private GridCacheAdapter<K, V> cache;

    /** Cached local rich node. */
    private ClusterNode locNode;

    /**
     * Thread local projection. If it's set it means that method call was initiated
     * by child projection of initial cache.
     */
    private ThreadLocal<GridCacheProjectionImpl<K, V>> prjPerCall = new ThreadLocal<>();

    /** Thread local forced flags that affect any projection in the same thread. */
    private ThreadLocal<CacheFlag[]> forcedFlags = new ThreadLocal<>();

    /** Constant array to avoid recreation. */
    private static final CacheFlag[] FLAG_LOCAL_READ = new CacheFlag[]{LOCAL, READ};

    /** Local flag array. */
    private static final CacheFlag[] FLAG_LOCAL = new CacheFlag[]{LOCAL};

    /** Cache name. */
    private String cacheName;

    /** Cache ID. */
    private int cacheId;

    /** System cache flag. */
    private boolean sys;

    /** IO policy. */
    private GridIoPolicy plc;

    /** Default expiry policy. */
    private ExpiryPolicy expiryPlc;

    /** Cache weak query iterator holder. */
    private CacheWeakQueryIteratorsHolder<Map.Entry<K, V>> itHolder;

    /** Affinity node. */
    private boolean affNode;

    /** Conflict resolver. */
    private GridCacheVersionAbstractConflictResolver conflictRslvr;

    /** */
    private CacheObjectContext cacheObjCtx;

    /** */
    private CountDownLatch startLatch = new CountDownLatch(1);

    /** Start topology version. */
    private AffinityTopologyVersion startTopVer;

    /** Dynamic cache deployment ID. */
    private IgniteUuid dynamicDeploymentId;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheContext() {
        // No-op.
    }

    /**
     * @param ctx Kernal context.
     * @param sharedCtx Cache shared context.
     * @param cacheCfg Cache configuration.
     * @param evtMgr Cache event manager.
     * @param swapMgr Cache swap manager.
     * @param storeMgr Store manager.
     * @param evictMgr Cache eviction manager.
     * @param qryMgr Cache query manager.
     * @param contQryMgr Continuous query manager.
     * @param affMgr Affinity manager.
     * @param dataStructuresMgr Cache dataStructures manager.
     * @param ttlMgr TTL manager.
     * @param drMgr Data center replication manager.
     * @param jtaMgr JTA manager.
     */
    @SuppressWarnings({"unchecked"})
    public GridCacheContext(
        GridKernalContext ctx,
        GridCacheSharedContext sharedCtx,
        CacheConfiguration cacheCfg,
        boolean affNode,

        /*
         * Managers in starting order!
         * ===========================
         */

        GridCacheEventManager evtMgr,
        GridCacheSwapManager swapMgr,
        GridCacheStoreManager storeMgr,
        GridCacheEvictionManager evictMgr,
        GridCacheQueryManager<K, V> qryMgr,
        CacheContinuousQueryManager contQryMgr,
        GridCacheAffinityManager affMgr,
        CacheDataStructuresManager dataStructuresMgr,
        GridCacheTtlManager ttlMgr,
        GridCacheDrManager drMgr,
        CacheJtaManagerAdapter jtaMgr) {
        assert ctx != null;
        assert sharedCtx != null;
        assert cacheCfg != null;

        assert evtMgr != null;
        assert swapMgr != null;
        assert storeMgr != null;
        assert evictMgr != null;
        assert qryMgr != null;
        assert contQryMgr != null;
        assert affMgr != null;
        assert dataStructuresMgr != null;
        assert ttlMgr != null;

        this.ctx = ctx;
        this.sharedCtx = sharedCtx;
        this.cacheCfg = cacheCfg;
        this.affNode = affNode;

        /*
         * Managers in starting order!
         * ===========================
         */
        this.evtMgr = add(evtMgr);
        this.swapMgr = add(swapMgr);
        this.storeMgr = add(storeMgr);
        this.evictMgr = add(evictMgr);
        this.qryMgr = add(qryMgr);
        this.contQryMgr = add(contQryMgr);
        this.affMgr = add(affMgr);
        this.dataStructuresMgr = add(dataStructuresMgr);
        this.ttlMgr = add(ttlMgr);
        this.drMgr = add(drMgr);
        this.jtaMgr = add(jtaMgr);

        log = ctx.log(getClass());

        // Create unsafe memory only if writing values
        unsafeMemory = (cacheCfg.getMemoryMode() == OFFHEAP_VALUES || cacheCfg.getMemoryMode() == OFFHEAP_TIERED) ?
            new GridUnsafeMemory(cacheCfg.getOffHeapMaxMemory()) : null;

        gate = new GridCacheGateway<>(this);

        cacheName = cacheCfg.getName();

        cacheId = CU.cacheId(cacheName);

        sys = ctx.cache().systemCache(cacheName);

        plc = CU.isMarshallerCache(cacheName) ? MARSH_CACHE_POOL : sys ? UTILITY_CACHE_POOL : SYSTEM_POOL;

        Factory<ExpiryPolicy> factory = cacheCfg.getExpiryPolicyFactory();

        expiryPlc = factory != null ? factory.create() : null;

        if (expiryPlc instanceof EternalExpiryPolicy)
            expiryPlc = null;

        itHolder = new CacheWeakQueryIteratorsHolder(log);
    }

    /**
     * @param dynamicDeploymentId Dynamic deployment ID.
     */
    void dynamicDeploymentId(IgniteUuid dynamicDeploymentId) {
        this.dynamicDeploymentId = dynamicDeploymentId;
    }

    /**
     * @return Dynamic deployment ID.
     */
    public IgniteUuid dynamicDeploymentId() {
        return dynamicDeploymentId;
    }

    /**
     * Initialize conflict resolver after all managers are started.
     */
    void initConflictResolver() {
        // Conflict resolver is determined in two stages:
        // 1. If DR receiver hub is enabled, then pick it from DR manager.
        // 2. Otherwise instantiate default resolver in case local store is configured.
        conflictRslvr = drMgr.conflictResolver();

        if (conflictRslvr == null && storeMgr.isLocalStore())
            conflictRslvr = new GridCacheVersionConflictResolver();
    }

    /**
     * @return {@code True} if local node is affinity node.
     */
    public boolean affinityNode() {
        return affNode;
    }

    /**
     * @throws IgniteCheckedException If failed to wait.
     */
    public void awaitStarted() throws IgniteCheckedException {
        U.await(startLatch);

        GridCachePreloader<K, V> prldr = preloader();

        if (prldr != null)
            prldr.startFuture().get();
    }

    /**
     * @return Started flag.
     */
    public boolean started() {
        if (startLatch.getCount() != 0)
            return false;

        GridCachePreloader<K, V> prldr = preloader();

        return prldr == null || prldr.startFuture().isDone();
    }

    /**
     *
     */
    public void onStarted() {
        startLatch.countDown();
    }

    /**
     * @return Start topology version.
     */
    public AffinityTopologyVersion startTopologyVersion() {
        return startTopVer;
    }

    /**
     * @param startTopVer Start topology version.
     */
    public void startTopologyVersion(AffinityTopologyVersion startTopVer) {
        this.startTopVer = startTopVer;
    }

    /**
     * @return Cache default {@link ExpiryPolicy}.
     */
    @Nullable public ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /**
     * @param txEntry TX entry.
     * @return Expiry policy for the given TX entry.
     */
    @Nullable public ExpiryPolicy expiryForTxEntry(IgniteTxEntry txEntry) {
        ExpiryPolicy plc = txEntry.expiry();

        return plc != null ? plc : expiryPlc;
    }

    /**
     * @param mgr Manager to add.
     * @return Added manager.
     */
    @Nullable private <T extends GridCacheManager<K, V>> T add(@Nullable T mgr) {
        if (mgr != null)
            mgrs.add(mgr);

        return mgr;
    }

    /**
     * @return Cache managers.
     */
    public List<GridCacheManager<K, V>> managers() {
        return mgrs;
    }

    /**
     * @return Shared cache context.
     */
    public GridCacheSharedContext<K, V> shared() {
        return sharedCtx;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return System cache flag.
     */
    public boolean system() {
        return sys;
    }

    /**
     * @return IO policy for the given cache.
     */
    public GridIoPolicy ioPolicy() {
        return plc;
    }

    /**
     * @param cache Cache.
     */
    public void cache(GridCacheAdapter<K, V> cache) {
        this.cache = cache;
    }

    /**
     * @return Local cache.
     */
    public GridLocalCache<K, V> local() {
        return (GridLocalCache<K, V>)cache;
    }

    /**
     * @return {@code True} if cache is DHT.
     */
    public boolean isDht() {
        return cache != null && cache.isDht();
    }

    /**
     * @return {@code True} if cache is DHT atomic.
     */
    public boolean isDhtAtomic() {
        return cache != null && cache.isDhtAtomic();
    }

    /**
     * @return {@code True} if cache is colocated (dht with near disabled).
     */
    public boolean isColocated() {
        return cache != null && cache.isColocated();
    }

    /**
     * @return {@code True} if cache is near cache.
     */
    public boolean isNear() {
        return cache != null && cache.isNear();
    }

    /**
     * @return {@code True} if cache is local.
     */
    public boolean isLocal() {
        return cache != null && cache.isLocal();
    }

    /**
     * @return {@code True} if cache is replicated cache.
     */
    public boolean isReplicated() {
        return cacheCfg.getCacheMode() == CacheMode.REPLICATED;
    }

    /**
     * @return {@code True} in case replication is enabled.
     */
    public boolean isDrEnabled() {
        return dr().enabled();
    }

    /**
     * @return {@code True} if entries should not be deleted from cache immediately.
     */
    public boolean deferredDelete() {
        return isDht() || isDhtAtomic() || isColocated() || (isNear() && atomic());
    }

    /**
     * @param e Entry.
     */
    public void incrementPublicSize(GridCacheMapEntry e) {
        assert deferredDelete();
        assert e != null;
        assert !e.isInternal();

        cache.map().incrementSize(e);

        if (isDht() || isColocated() || isDhtAtomic()) {
            GridDhtLocalPartition part = topology().localPartition(e.partition(), AffinityTopologyVersion.NONE, false);

            if (part != null)
                part.incrementPublicSize();
        }
    }

    /**
     * @param e Entry.
     */
    public void decrementPublicSize(GridCacheMapEntry e) {
        assert deferredDelete();
        assert e != null;
        assert !e.isInternal();

        cache.map().decrementSize(e);

        if (isDht() || isColocated() || isDhtAtomic()) {
            GridDhtLocalPartition part = topology().localPartition(e.partition(), AffinityTopologyVersion.NONE, false);

            if (part != null)
                part.decrementPublicSize();
        }
    }

    /**
     * @return DHT cache.
     */
    public GridDhtCacheAdapter<K, V> dht() {
        return (GridDhtCacheAdapter<K, V>)cache;
    }

    /**
     * @return Transactional DHT cache.
     */
    public GridDhtTransactionalCacheAdapter<K, V> dhtTx() {
        return (GridDhtTransactionalCacheAdapter<K, V>)cache;
    }

    /**
     * @return Colocated cache.
     */
    public GridDhtColocatedCache<K, V> colocated() {
        return (GridDhtColocatedCache<K, V>)cache;
    }

    /**
     * @return Near cache.
     */
    public GridNearCacheAdapter<K, V> near() {
        return (GridNearCacheAdapter<K, V>)cache;
    }

    /**
     * @return Near cache for transactional mode.
     */
    public GridNearTransactionalCache<K, V> nearTx() {
        return (GridNearTransactionalCache<K, V>)cache;
    }

    /**
     * @return Cache gateway.
     */
    public GridCacheGateway<K, V> gate() {
        return gate;
    }

    /**
     * @return Instance of {@link GridUnsafeMemory} object.
     */
    @Nullable public GridUnsafeMemory unsafeMemory() {
        return unsafeMemory;
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return ctx;
    }

    /**
     * @return Grid instance.
     */
    public IgniteEx grid() {
        return ctx.grid();
    }

    /**
     * @return Grid name.
     */
    public String gridName() {
        return ctx.gridName();
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return cacheName;
    }

    /**
     * Gets public name for cache.
     *
     * @return Public name of the cache.
     */
    public String namex() {
        return isDht() ? dht().near().name() : name();
    }

    /**
     * Gets public cache name substituting null name by {@code 'default'}.
     *
     * @return Public cache name substituting null name by {@code 'default'}.
     */
    public String namexx() {
        String name = namex();

        return name == null ? "default" : name;
    }

    /**
     * @param key Key to construct tx key for.
     * @return Transaction key.
     */
    public IgniteTxKey txKey(KeyCacheObject key) {
        return new IgniteTxKey(key, cacheId);
    }

    /**
     * @param op Operation to check.
     * @throws GridSecurityException If security check failed.
     */
    public void checkSecurity(GridSecurityPermission op) throws GridSecurityException {
        if (CU.isSystemCache(name()))
            return;

        ctx.security().authorize(name(), op, null);
    }

    /**
     * @return Preloader.
     */
    public GridCachePreloader<K, V> preloader() {
        return cache().preloader();
    }

    /**
     * @return Local node ID.
     */
    public UUID nodeId() {
        return ctx.localNodeId();
    }

    /**
     * @return {@code True} if rebalance is enabled.
     */
    public boolean rebalanceEnabled() {
        return cacheCfg.getRebalanceMode() != NONE;
    }

    /**
     * @return {@code True} if atomic.
     */
    public boolean atomic() {
        return cacheCfg.getAtomicityMode() == ATOMIC;
    }

    /**
     * @return {@code True} if transactional.
     */
    public boolean transactional() {
        return cacheCfg.getAtomicityMode() == TRANSACTIONAL;
    }

    /**
     * @return Local node.
     */
    public ClusterNode localNode() {
        if (locNode == null)
            locNode = ctx.discovery().localNode();

        return locNode;
    }

    /**
     * @return Local node ID.
     */
    public UUID localNodeId() {
        return ctx.localNodeId();
    }

    /**
     * @param n Node to check.
     * @return {@code True} if node is local.
     */
    public boolean isLocalNode(ClusterNode n) {
        assert n != null;

        return localNode().id().equals(n.id());
    }

    /**
     * @param id Node ID to check.
     * @return {@code True} if node ID is local.
     */
    public boolean isLocalNode(UUID id) {
        assert id != null;

        return localNode().id().equals(id);
    }

    /**
     * @param nodeId Node id.
     * @return Node.
     */
    @Nullable public ClusterNode node(UUID nodeId) {
        assert nodeId != null;

        return ctx.discovery().node(nodeId);
    }

    /**
     * @return Partition topology.
     */
    public GridDhtPartitionTopology topology() {
        assert isNear() || isDht() || isColocated() || isDhtAtomic();

        return isNear() ? near().dht().topology() : dht().topology();
    }

    /**
     * @return Topology version future.
     */
    public GridDhtTopologyFuture topologyVersionFuture() {
        assert isNear() || isDht() || isColocated() || isDhtAtomic();

        GridDhtTopologyFuture fut = null;

        if (!isDhtAtomic()) {
            GridDhtCacheAdapter<K, V> cache = isNear() ? near().dht() : colocated();

            fut = cache.multiUpdateTopologyFuture();
        }

        return fut == null ? topology().topologyVersionFuture() : fut;
    }

    /**
     * @return Marshaller.
     */
    public Marshaller marshaller() {
        return ctx.config().getMarshaller();
    }

    /**
     * @param ctgr Category to log.
     * @return Logger.
     */
    public IgniteLogger logger(String ctgr) {
        return new GridCacheLogger(this, ctgr);
    }

    /**
     * @param cls Class to log.
     * @return Logger.
     */
    public IgniteLogger logger(Class<?> cls) {
        return logger(cls.getName());
    }

    /**
     * @return Grid configuration.
     */
    public IgniteConfiguration gridConfig() {
        return ctx.config();
    }

    /**
     * @return Grid communication manager.
     */
    public GridIoManager gridIO() {
        return ctx.io();
    }

    /**
     * @return Grid timeout processor.
     */
    public GridTimeoutProcessor time() {
        return ctx.timeout();
    }

    /**
     * @return Grid off-heap processor.
     */
    public GridOffHeapProcessor offheap() {
        return ctx.offheap();
    }

    /**
     * @return Grid deployment manager.
     */
    public GridDeploymentManager gridDeploy() {
        return ctx.deploy();
    }

    /**
     * @return Grid swap space manager.
     */
    public GridSwapSpaceManager gridSwap() {
        return ctx.swap();
    }

    /**
     * @return Grid event storage manager.
     */
    public GridEventStorageManager gridEvents() {
        return ctx.event();
    }

    /**
     * @return Closures processor.
     */
    public GridClosureProcessor closures() {
        return ctx.closure();
    }

    /**
     * @return Grid discovery manager.
     */
    public GridDiscoveryManager discovery() {
        return ctx.discovery();
    }

    /**
     * @return Cache instance.
     */
    public GridCacheAdapter<K, V> cache() {
        return cache;
    }

    /**
     * @return Cache configuration for given cache instance.
     */
    public CacheConfiguration config() {
        return cacheCfg;
    }

    /**
     * @return {@code True} If store writes should be performed from dht transactions. This happens if both
     *      {@code writeBehindEnabled} and {@code writeBehindPreferPrimary} cache configuration properties
     *      are set to {@code true} or the store is local.
     */
    public boolean writeToStoreFromDht() {
        return store().isLocalStore() || cacheCfg.isWriteBehindEnabled();
    }

    /**
     * @return Cache transaction manager.
     */
    public IgniteTxManager tm() {
         return sharedCtx.tm();
    }

    /**
     * @return Lock order manager.
     */
    public GridCacheVersionManager versions() {
        return sharedCtx.versions();
    }

    /**
     * @return Lock manager.
     */
    public GridCacheMvccManager mvcc() {
        return sharedCtx.mvcc();
    }

    /**
     * @return Event manager.
     */
    public GridCacheEventManager events() {
        return evtMgr;
    }

    /**
     * @return Cache affinity manager.
     */
    public GridCacheAffinityManager affinity() {
        return affMgr;
    }

    /**
     * @return Query manager, {@code null} if disabled.
     */
    public GridCacheQueryManager<K, V> queries() {
        return qryMgr;
    }

    /**
     * @return Continuous query manager, {@code null} if disabled.
     */
    public CacheContinuousQueryManager continuousQueries() {
        return contQryMgr;
    }

    /**
     * @return Iterators Holder.
     */
    public CacheWeakQueryIteratorsHolder<Map.Entry<K, V>> itHolder() {
        return itHolder;
    }

    /**
     * @return Swap manager.
     */
    public GridCacheSwapManager swap() {
        return swapMgr;
    }

    /**
     * @return Store manager.
     */
    public GridCacheStoreManager store() {
        return storeMgr;
    }

    /**
     * @return Cache deployment manager.
     */
    public GridCacheDeploymentManager<K, V> deploy() {
        return sharedCtx.deploy();
    }

    /**
     * @return Cache communication manager.
     */
    public GridCacheIoManager io() {
        return sharedCtx.io();
    }

    /**
     * @return Eviction manager.
     */
    public GridCacheEvictionManager evicts() {
        return evictMgr;
    }

    /**
     * @return Data structures manager.
     */
    public CacheDataStructuresManager dataStructures() {
        return dataStructuresMgr;
    }

    /**
     * @return DR manager.
     */
    public GridCacheDrManager dr() {
        return drMgr;
    }

    /**
     * @return TTL manager.
     */
    public GridCacheTtlManager ttl() {
        return ttlMgr;
    }

    /**
     * @return JTA manager.
     */
    public CacheJtaManagerAdapter jta() {
        return jtaMgr;
    }

    /**
     * @param p Predicate.
     * @return {@code True} if given predicate is filter for {@code putIfAbsent} operation.
     */
    public boolean putIfAbsentFilter(@Nullable CacheEntryPredicate[] p) {
        if (p == null || p.length == 0)
            return false;

        for (CacheEntryPredicate p0 : p) {
            if ((p0 instanceof CacheEntrySerializablePredicate) &&
               ((CacheEntrySerializablePredicate)p0).predicate() instanceof CacheEntryPredicateNoValue)
            return true;
        }

        return false;
    }

    /**
     * @return No value filter.
     */
    public CacheEntryPredicate[] noValArray() {
        return new CacheEntryPredicate[]{new CacheEntrySerializablePredicate(new CacheEntryPredicateNoValue())};
    }

    /**
     * @return Has value filter.
     */
    public CacheEntryPredicate[] hasValArray() {
        return new CacheEntryPredicate[]{new CacheEntrySerializablePredicate(new CacheEntryPredicateHasValue())};
    }

    /**
     * @param val Value to check.
     * @return Predicate array that checks for value.
     */
    public CacheEntryPredicate[] equalsValArray(V val) {
        return new CacheEntryPredicate[]{new CacheEntryPredicateContainsValue(toCacheObject(val))};
    }

    /**
     * @param val Value to check.
     * @return Predicate that checks for value.
     */
    public CacheEntryPredicate equalsValue(V val) {
        return new CacheEntryPredicateContainsValue(toCacheObject(val));
    }

    /**
     * @return Empty cache version array.
     */
    public GridCacheVersion[] emptyVersion() {
        return EMPTY_VERSION;
    }

    /**
     * @return Default affinity key mapper.
     */
    public AffinityKeyMapper defaultAffMapper() {
        return cacheObjCtx.defaultAffMapper();
    }

    /**
     * Sets cache object context.
     *
     * @param cacheObjCtx Cache object context.
     */
    public void cacheObjectContext(CacheObjectContext cacheObjCtx) {
        this.cacheObjCtx = cacheObjCtx;
    }

    /**
     * @param p Single predicate.
     * @return Array containing single predicate.
     */
    @SuppressWarnings({"unchecked"})
    public IgnitePredicate<Cache.Entry<K, V>>[] vararg(IgnitePredicate<Cache.Entry<K, V>> p) {
        return p == null ? CU.<K, V>empty() : new IgnitePredicate[]{p};
    }

    /**
     * Same as {@link GridFunc#isAll(Object, IgnitePredicate[])}, but safely unwraps exceptions.
     *
     * @param e Element.
     * @param p Predicates.
     * @return {@code True} if predicates passed.
     * @throws IgniteCheckedException If failed.
     */
    public <K1, V1> boolean isAll(
        GridCacheEntryEx e,
        @Nullable IgnitePredicate<Cache.Entry<K1, V1>>[] p
    ) throws IgniteCheckedException {
        return F.isEmpty(p) || isAll(e.<K1, V1>wrapLazyValue(), p);
    }

    /**
     * Same as {@link GridFunc#isAll(Object, IgnitePredicate[])}, but safely unwraps exceptions.
     *
     * @param e Element.
     * @param p Predicates.
     * @param <E> Element type.
     * @return {@code True} if predicates passed.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"ErrorNotRethrown"})
    public <E> boolean isAll(E e, @Nullable IgnitePredicate<? super E>[] p) throws IgniteCheckedException {
        if (F.isEmpty(p))
            return true;

        // We should allow only local read-only operations within filter checking.
        CacheFlag[] oldFlags = forceFlags(FLAG_LOCAL_READ);

        try {
            boolean pass = F.isAll(e, p);

            if (log.isDebugEnabled())
                log.debug("Evaluated filters for entry [pass=" + pass + ", entry=" + e + ", filters=" +
                    Arrays.toString(p) + ']');

            return pass;
        }
        catch (RuntimeException ex) {
            throw U.cast(ex);
        }
        finally {
            forceFlags(oldFlags);
        }
    }

    /**
     * @param e Entry.
     * @param p Predicates.
     * @return {@code True} if predicates passed.
     * @throws IgniteCheckedException If failed.
     */
    public boolean isAll(GridCacheEntryEx e, CacheEntryPredicate[] p) throws IgniteCheckedException {
        if (p == null || p.length == 0)
            return true;

        try {
            for (CacheEntryPredicate p0 : p) {
                if (p0 != null && !p0.apply(e))
                    return false;
            }
        }
        catch (RuntimeException ex) {
            throw U.cast(ex);
        }

        return true;
    }

    /**
     * @param e Entry.
     * @param p Predicates.
     * @return {@code True} if predicates passed.
     * @throws IgniteCheckedException If failed.
     */
    public boolean isAllLocked(GridCacheEntryEx e, CacheEntryPredicate[] p) throws IgniteCheckedException {
        if (p == null || p.length == 0)
            return true;

        try {
            for (CacheEntryPredicate p0 : p) {
                if (p0 != null) {
                    p0.entryLocked(true);

                    if (!p0.apply(e))
                        return false;
                }
            }
        }
        catch (RuntimeException ex) {
            throw U.cast(ex);
        }
        finally {
            for (CacheEntryPredicate p0 : p) {
                if (p0 != null)
                    p0.entryLocked(false);
            }
        }

        return true;
    }

    /**
     * Forces LOCAL flag.
     *
     * @return Previously forced flags.
     */
    @Nullable public CacheFlag[] forceLocal() {
        return forceFlags(FLAG_LOCAL);
    }

    /**
     * Forces LOCAL and READ flags.
     *
     * @return Forced flags that were set prior to method call.
     */
    @Nullable public CacheFlag[] forceLocalRead() {
        return forceFlags(FLAG_LOCAL_READ);
    }

    /**
     * Force projection flags for the current thread. These flags will affect all
     * projections (even without flags) used within the current thread.
     *
     * @param flags Flags to force.
     * @return Forced flags that were set prior to method call.
     */
    @Nullable public CacheFlag[] forceFlags(@Nullable CacheFlag[] flags) {
        CacheFlag[] oldFlags = forcedFlags.get();

        forcedFlags.set(F.isEmpty(flags) ? null : flags);

        return oldFlags;
    }

    /**
     * Gets forced flags for current thread.
     *
     * @return Forced flags.
     */
    public CacheFlag[] forcedFlags() {
        return forcedFlags.get();
    }

    /**
     * Clone cached object.
     *
     * @param obj Object to clone
     * @return Clone of the given object.
     * @throws IgniteCheckedException If failed to clone object.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <T> T cloneValue(@Nullable T obj) throws IgniteCheckedException {
        return obj == null ? null : X.cloneObject(obj, false, true);
    }

    /**
     * Sets thread local projection.
     *
     * @param prj Flags to set.
     */
    public void projectionPerCall(@Nullable GridCacheProjectionImpl<K, V> prj) {
        if (nearContext())
            dht().near().context().prjPerCall.set(prj);
        else
            prjPerCall.set(prj);
    }

    /**
     * Gets thread local projection.
     *
     * @return Projection per call.
     */
    public GridCacheProjectionImpl<K, V> projectionPerCall() {
        return nearContext() ? dht().near().context().prjPerCall.get() : prjPerCall.get();
    }

    /**
     * Gets subject ID per call.
     *
     * @param subjId Optional already existing subject ID.
     * @return Subject ID per call.
     */
    public UUID subjectIdPerCall(@Nullable UUID subjId) {
        if (subjId != null)
            return subjId;

        return subjectIdPerCall(subjId, projectionPerCall());
    }

    /**
     * Gets subject ID per call.
     *
     * @param subjId Optional already existing subject ID.
     * @param prj Optional thread local projection.
     * @return Subject ID per call.
     */
    public UUID subjectIdPerCall(@Nullable UUID subjId, @Nullable GridCacheProjectionImpl<K, V> prj) {
        if (prj != null)
            subjId = prj.subjectId();

        if (subjId == null)
            subjId = ctx.localNodeId();

        return subjId;
    }

    /**
     *
     * @param flag Flag to check.
     * @return {@code true} if the given flag is set.
     */
    public boolean hasFlag(CacheFlag flag) {
        assert flag != null;

        if (nearContext())
            return dht().near().context().hasFlag(flag);

        GridCacheProjectionImpl<K, V> prj = prjPerCall.get();

        CacheFlag[] forced = forcedFlags.get();

        return (prj != null && prj.flags().contains(flag)) || (forced != null && U.containsObjectArray(forced, flag));
    }

    /**
     * Checks whether any of the given flags is set.
     *
     * @param flags Flags to check.
     * @return {@code true} if any of the given flags is set.
     */
    public boolean hasAnyFlags(CacheFlag[] flags) {
        assert !F.isEmpty(flags);

        if (nearContext())
            return dht().near().context().hasAnyFlags(flags);

        GridCacheProjectionImpl<K, V> prj = prjPerCall.get();

        if (prj == null && F.isEmpty(forcedFlags.get()))
            return false;

        for (CacheFlag f : flags)
            if (hasFlag(f))
                return true;

        return false;
    }

    /**
     * Checks whether any of the given flags is set.
     *
     * @param flags Flags to check.
     * @return {@code true} if any of the given flags is set.
     */
    public boolean hasAnyFlags(Collection<CacheFlag> flags) {
        assert !F.isEmpty(flags);

        if (nearContext())
            return dht().near().context().hasAnyFlags(flags);

        GridCacheProjectionImpl<K, V> prj = prjPerCall.get();

        if (prj == null && F.isEmpty(forcedFlags.get()))
            return false;

        for (CacheFlag f : flags)
            if (hasFlag(f))
                return true;

        return false;
    }

    /**
     * @return {@code True} if need check near cache context.
     */
    private boolean nearContext() {
        return isDht() || (isDhtAtomic() && dht().near() != null);
    }

    /**
     * @param flag Flag to check.
     */
    public void denyOnFlag(CacheFlag flag) {
        assert flag != null;

        if (hasFlag(flag))
            throw new CacheFlagException(flag);
    }

    /**
     *
     */
    public void denyOnLocalRead() {
        denyOnFlags(FLAG_LOCAL_READ);
    }

    /**
     * @param flags Flags.
     */
    public void denyOnFlags(CacheFlag[] flags) {
        assert !F.isEmpty(flags);

        if (hasAnyFlags(flags))
            throw new CacheFlagException(flags);
    }

    /**
     * @param flags Flags.
     */
    public void denyOnFlags(Collection<CacheFlag> flags) {
        assert !F.isEmpty(flags);

        if (hasAnyFlags(flags))
            throw new CacheFlagException(flags);
    }

    /**
     * Clones cached object depending on whether or not {@link CacheFlag#CLONE} flag
     * is set thread locally.
     *
     * @param obj Object to clone.
     * @return Clone of the given object.
     * @throws IgniteCheckedException If failed to clone.
     */
    @Nullable public <T> T cloneOnFlag(@Nullable T obj) throws IgniteCheckedException {
        return hasFlag(CLONE) ? cloneValue(obj) : obj;
    }

    /**
     * @param f Target future.
     * @return Wrapped future that is aware of cloning behaviour.
     */
    public IgniteInternalFuture<V> wrapClone(IgniteInternalFuture<V> f) {
        if (!hasFlag(CLONE))
            return f;

        return f.chain(new CX1<IgniteInternalFuture<V>, V>() {
            @Override public V applyx(IgniteInternalFuture<V> f) throws IgniteCheckedException {
                return cloneValue(f.get());
            }
        });
    }

    /**
     * Creates Runnable that can be executed safely in a different thread inheriting
     * the same thread local projection as for the current thread. If no projection is
     * set for current thread then there's no need to create new object and method simply
     * returns given Runnable.
     *
     * @param r Runnable.
     * @return Runnable that can be executed in a different thread with the same
     *      projection as for current thread.
     */
    public Runnable projectSafe(final Runnable r) {
        assert r != null;

        // Have to get projection per call used by calling thread to use it in a new thread.
        final GridCacheProjectionImpl<K, V> prj = projectionPerCall();

        // Get flags in the same thread.
        final CacheFlag[] flags = forcedFlags();

        if (prj == null && F.isEmpty(flags))
            return r;

        return new GPR() {
            @Override public void run() {
                GridCacheProjectionImpl<K, V> oldPrj = projectionPerCall();

                projectionPerCall(prj);

                CacheFlag[] oldFlags = forceFlags(flags);

                try {
                    r.run();
                }
                finally {
                    projectionPerCall(oldPrj);

                    forceFlags(oldFlags);
                }
            }
        };
    }

    /**
     * Creates callable that can be executed safely in a different thread inheriting
     * the same thread local projection as for the current thread. If no projection is
     * set for current thread then there's no need to create new object and method simply
     * returns given callable.
     *
     * @param r Callable.
     * @return Callable that can be executed in a different thread with the same
     *      projection as for current thread.
     */
    public <T> Callable<T> projectSafe(final Callable<T> r) {
        assert r != null;

        // Have to get projection per call used by calling thread to use it in a new thread.
        final GridCacheProjectionImpl<K, V> prj = projectionPerCall();

        // Get flags in the same thread.
        final CacheFlag[] flags = forcedFlags();

        if (prj == null && F.isEmpty(flags))
            return r;

        return new GPC<T>() {
            @Override public T call() throws Exception {
                GridCacheProjectionImpl<K, V> oldPrj = projectionPerCall();

                projectionPerCall(prj);

                CacheFlag[] oldFlags = forceFlags(flags);

                try {
                    return r.call();
                }
                finally {
                    projectionPerCall(oldPrj);

                    forceFlags(oldFlags);
                }
            }
        };
    }

    /**
     * @return {@code True} if deployment enabled.
     */
    public boolean deploymentEnabled() {
        return ctx.deploy().enabled();
    }

    /**
     * @return {@code True} if swap store of off-heap cache are enabled.
     */
    public boolean isSwapOrOffheapEnabled() {
        return (swapMgr.swapEnabled() && !hasFlag(SKIP_SWAP)) || isOffHeapEnabled();
    }

    /**
     * @return {@code True} if offheap storage is enabled.
     */
    public boolean isOffHeapEnabled() {
        return swapMgr.offHeapEnabled();
    }

    /**
     * @return {@code True} if store read-through mode is enabled.
     */
    public boolean readThrough() {
        return cacheCfg.isReadThrough() && !hasFlag(SKIP_STORE);
    }

    /**
     * @return {@code True} if store read-through mode is enabled.
     */
    public boolean loadPreviousValue() {
        return cacheCfg.isLoadPreviousValue();
    }

    /**
     * @return {@code True} if store write-through is enabled.
     */
    public boolean writeThrough() {
        return cacheCfg.isWriteThrough() && !hasFlag(SKIP_STORE);
    }

    /**
     * @return {@code True} if invalidation is enabled.
     */
    public boolean isInvalidate() {
        return cacheCfg.isInvalidate() || hasFlag(INVALIDATE);
    }

    /**
     * @return {@code True} if synchronous commit is enabled.
     */
    public boolean syncCommit() {
        return cacheCfg.getWriteSynchronizationMode() == FULL_SYNC || hasFlag(SYNC_COMMIT);
    }

    /**
     * @return {@code True} if synchronous rollback is enabled.
     */
    public boolean syncRollback() {
        return cacheCfg.getWriteSynchronizationMode() == FULL_SYNC;
    }

    /**
     * @return {@code True} if only primary node should be updated synchronously.
     */
    public boolean syncPrimary() {
        return cacheCfg.getWriteSynchronizationMode() == PRIMARY_SYNC;
    }

    /**
     * @param nearNodeId Near node ID.
     * @param topVer Topology version.
     * @param entry Entry.
     * @param log Log.
     * @param dhtMap Dht mappings.
     * @param nearMap Near mappings.
     * @return {@code True} if mapped.
     * @throws GridCacheEntryRemovedException If reader for entry is removed.
     */
    public boolean dhtMap(
        UUID nearNodeId,
        AffinityTopologyVersion topVer,
        GridDhtCacheEntry entry,
        IgniteLogger log,
        Map<ClusterNode, List<GridDhtCacheEntry>> dhtMap,
        @Nullable Map<ClusterNode, List<GridDhtCacheEntry>> nearMap
    ) throws GridCacheEntryRemovedException {
        assert !AffinityTopologyVersion.NONE.equals(topVer);

        Collection<ClusterNode> dhtNodes = dht().topology().nodes(entry.partition(), topVer);

        if (log.isDebugEnabled())
            log.debug("Mapping entry to DHT nodes [nodes=" + U.nodeIds(dhtNodes) + ", entry=" + entry + ']');

        Collection<ClusterNode> dhtRemoteNodes = F.view(dhtNodes, F.remoteNodes(nodeId())); // Exclude local node.

        boolean ret = map(entry, dhtRemoteNodes, dhtMap);

        if (nearMap != null) {
            Collection<UUID> readers = entry.readers();

            Collection<ClusterNode> nearNodes = null;

            if (!F.isEmpty(readers)) {
                nearNodes = discovery().nodes(readers, F0.notEqualTo(nearNodeId));

                if (log.isDebugEnabled())
                    log.debug("Mapping entry to near nodes [nodes=" + U.nodeIds(nearNodes) + ", entry=" + entry + ']');
            }
            else if (log.isDebugEnabled())
                log.debug("Entry has no near readers: " + entry);

            if (nearNodes != null && !nearNodes.isEmpty())
                ret |= map(entry, F.view(nearNodes, F.notIn(dhtNodes)), nearMap);
        }

        return ret;
    }

    /**
     * @param entry Entry.
     * @param nodes Nodes.
     * @param map Map.
     * @return {@code True} if mapped.
     */
    private boolean map(GridDhtCacheEntry entry, Iterable<ClusterNode> nodes,
        Map<ClusterNode, List<GridDhtCacheEntry>> map) {
        boolean ret = false;

        if (nodes != null) {
            for (ClusterNode n : nodes) {
                List<GridDhtCacheEntry> entries = map.get(n);

                if (entries == null)
                    map.put(n, entries = new LinkedList<>());

                entries.add(entry);

                ret = true;
            }
        }

        return ret;
    }

    /**
     * Checks if at least one of the given keys belongs to one of the given partitions.
     *
     * @param keys Collection of keys to check.
     * @param movingParts Collection of partitions to check against.
     * @return {@code True} if there exist a key in collection {@code keys} that belongs
     *      to one of partitions in {@code movingParts}
     */
    public boolean hasKey(Iterable<? extends K> keys, Collection<Integer> movingParts) {
        for (K key : keys) {
            if (movingParts.contains(affinity().partition(key)))
                return true;
        }

        return false;
    }

    /**
     * Check whether conflict resolution is required.
     *
     * @return {@code True} in case DR is required.
     */
    public boolean conflictNeedResolve() {
        return conflictRslvr != null;
    }

    /**
     * Resolve DR conflict.
     *
     * @param oldEntry Old entry.
     * @param newEntry New entry.
     * @param atomicVerComp Whether to use atomic version comparator.
     * @return Conflict resolution result.
     * @throws IgniteCheckedException In case of exception.
     */
    public GridCacheVersionConflictContext<K, V> conflictResolve(GridCacheVersionedEntryEx<K, V> oldEntry,
        GridCacheVersionedEntryEx<K, V> newEntry, boolean atomicVerComp) throws IgniteCheckedException {
        assert conflictRslvr != null : "Should not reach this place.";

        GridCacheVersionConflictContext<K, V> ctx = conflictRslvr.resolve(oldEntry, newEntry, atomicVerComp);

        if (ctx.isManualResolve())
            drMgr.onReceiveCacheConflictResolved(ctx.isUseNew(), ctx.isUseOld(), ctx.isMerge());

        return ctx;
    }

    /**
     * @return Data center ID.
     */
    public byte dataCenterId() {
        return dr().dataCenterId();
    }

    /**
     * @param entry Entry.
     * @param ver Version.
     */
    public void onDeferredDelete(GridCacheEntryEx entry, GridCacheVersion ver) {
        assert entry != null;
        assert !Thread.holdsLock(entry);
        assert ver != null;
        assert deferredDelete();

        cache.onDeferredDelete(entry, ver);
    }

    /**
     * @param interceptorRes Result of {@link CacheInterceptor#onBeforeRemove} callback.
     * @return {@code True} if interceptor cancels remove.
     */
    public boolean cancelRemove(@Nullable IgniteBiTuple<Boolean, ?> interceptorRes) {
        if (interceptorRes != null) {
            if (interceptorRes.get1() == null) {
                U.warn(log, "CacheInterceptor must not return null as cancellation flag value from " +
                    "'onBeforeRemove' method.");

                return false;
            }
            else
                return interceptorRes.get1();
        }
        else {
            U.warn(log, "CacheInterceptor must not return null from 'onBeforeRemove' method.");

            return false;
        }
    }

    /**
     * @return Portable processor.
     */
    public IgniteCacheObjectProcessor cacheObjects() {
        return kernalContext().cacheObjects();
    }

    /**
     * @return Keep portable flag.
     */
    public boolean keepPortable() {
        GridCacheProjectionImpl<K, V> prj = projectionPerCall();

        return prj != null && prj.isKeepPortable();
    }

    /**
     * @return {@code True} if OFFHEAP_TIERED memory mode is enabled.
     */
    public boolean offheapTiered() {
        return cacheCfg.getMemoryMode() == OFFHEAP_TIERED && isOffHeapEnabled();
    }

    /**
     * Converts temporary offheap object to heap-based.
     *
     * @param obj Object.
     * @return Heap-based object.
     */
    @Nullable public <T> T unwrapTemporary(@Nullable Object obj) {
        if (!offheapTiered())
            return (T)obj;

        return (T) cacheObjects().unwrapTemporary(this, obj);
    }

    /**
     * Unwraps collection.
     *
     * @param col Collection to unwrap.
     * @param keepPortable Keep portable flag.
     * @return Unwrapped collection.
     */
    public Collection<Object> unwrapPortablesIfNeeded(Collection<Object> col, boolean keepPortable) {
        return cacheObjCtx.unwrapPortablesIfNeeded(col, keepPortable);
    }

    /**
     * Unwraps object for portables.
     *
     * @param o Object to unwrap.
     * @param keepPortable Keep portable flag.
     * @return Unwrapped object.
     */
    @SuppressWarnings("IfMayBeConditional")
    public Object unwrapPortableIfNeeded(Object o, boolean keepPortable) {
        return cacheObjCtx.unwrapPortableIfNeeded(o, keepPortable);
    }

    /**
     * @return Cache object context.
     */
    public CacheObjectContext cacheObjectContext() {
        return cacheObjCtx;
    }

    /**
     * @param obj Object.
     * @return Cache object.
     */
    @Nullable public CacheObject toCacheObject(@Nullable Object obj) {
        return cacheObjects().toCacheObject(cacheObjCtx, obj, true);
    }

    /**
     * @param obj Object.
     * @return Cache key object.
     */
    public KeyCacheObject toCacheKeyObject(Object obj) {
        return cacheObjects().toCacheKeyObject(cacheObjCtx, obj, true);
    }

    /**
     * @param bytes Bytes.
     * @return Cache key object.
     * @throws IgniteCheckedException If failed.
     */
    public KeyCacheObject toCacheKeyObject(byte[] bytes) throws IgniteCheckedException {
        Object obj = ctx.cacheObjects().unmarshal(cacheObjCtx, bytes, deploy().localLoader());

        return cacheObjects().toCacheKeyObject(cacheObjCtx, obj, false);
    }

    /**
     * @param type Type.
     * @param bytes Bytes.
     * @param clsLdrId Class loader ID.
     * @return Cache object.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public CacheObject unswapCacheObject(byte type, byte[] bytes, @Nullable IgniteUuid clsLdrId)
        throws IgniteCheckedException
    {
        if (ctx.config().isPeerClassLoadingEnabled() && type != CacheObject.TYPE_BYTE_ARR) {
            ClassLoader ldr = clsLdrId != null ? deploy().getClassLoader(clsLdrId) : deploy().localLoader();

            if (ldr == null)
                return null;

            return ctx.cacheObjects().toCacheObject(cacheObjCtx,
                ctx.cacheObjects().unmarshal(cacheObjCtx, bytes, ldr),
                false);
        }

        return ctx.cacheObjects().toCacheObject(cacheObjCtx, type, bytes);
    }

    /**
     * @param valPtr Value pointer.
     * @param tmp If {@code true} can return temporary instance which is valid while entry lock is held.
     * @return Cache object.
     * @throws IgniteCheckedException If failed.
     */
    public CacheObject fromOffheap(long valPtr, boolean tmp) throws IgniteCheckedException {
        assert config().getMemoryMode() == OFFHEAP_TIERED || config().getMemoryMode() == OFFHEAP_VALUES;
        assert valPtr != 0;

        return ctx.cacheObjects().toCacheObject(this, valPtr, tmp);
    }

    /**
     * @param map Map.
     * @param key Key.
     * @param val Value.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects flag.
     * @param deserializePortable Deserialize portable flag.
     * @param cpy Copy flag.
     */
    @SuppressWarnings("unchecked")
    public <K1, V1> void addResult(Map<K1, V1> map,
        KeyCacheObject key,
        CacheObject val,
        boolean skipVals,
        boolean keepCacheObjects,
        boolean deserializePortable,
        boolean cpy) {
        assert key != null;
        assert val != null;

        if (!keepCacheObjects) {
            Object key0 = key.value(cacheObjCtx, false);
            Object val0 = skipVals ? true : val.value(cacheObjCtx, cpy);

            if (deserializePortable) {
                key0 = unwrapPortableIfNeeded(key0, false);

                if (!skipVals)
                    val0 = unwrapPortableIfNeeded(val0, false);
            }

            assert key0 != null : key;
            assert val0 != null : val;

            map.put((K1)key0, (V1)val0);
        }
        else
            map.put((K1)key, (V1)(skipVals ? true : val));
    }

    /**
     * Nulling references to potentially leak-prone objects.
     */
    public void cleanup() {
        cache = null;
        cacheCfg = null;
        evictMgr = null;
        qryMgr = null;
        dataStructuresMgr = null;
        cacheObjCtx = null;

        mgrs.clear();
    }

    /**
     * Print memory statistics of all cache managers.
     *
     * NOTE: this method is for testing and profiling purposes only.
     */
    public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Cache memory stats [grid=" + ctx.gridName() + ", cache=" + name() + ']');

        cache().printMemoryStats();

        Collection<GridCacheManager> printed = new LinkedList<>();

        for (GridCacheManager mgr : managers()) {
            mgr.printMemoryStats();

            printed.add(mgr);
        }

        if (isNear())
            for (GridCacheManager mgr : near().dht().context().managers())
                if (!printed.contains(mgr))
                    mgr.printMemoryStats();
    }

    /**
     * @param keys Keys.
     * @return Co
     */
    public Collection<KeyCacheObject> cacheKeysView(Collection<?> keys) {
        return F.viewReadOnly(keys, new C1<Object, KeyCacheObject>() {
            @Override public KeyCacheObject apply(Object key) {
                if (key == null)
                    throw new NullPointerException("Null key.");

                return toCacheKeyObject(key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, gridName());
        U.writeString(out, namex());
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        IgniteBiTuple<String, String> t = stash.get();

        t.set1(U.readString(in));
        t.set2(U.readString(in));
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            IgniteBiTuple<String, String> t = stash.get();

            IgniteKernal grid = IgnitionEx.gridx(t.get1());

            GridCacheAdapter<K, V> cache = grid.internalCache(t.get2());

            if (cache == null)
                throw new IllegalStateException("Failed to find cache for name: " + t.get2());

            return cache.context();
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridCacheContext: " + name();
    }
}
