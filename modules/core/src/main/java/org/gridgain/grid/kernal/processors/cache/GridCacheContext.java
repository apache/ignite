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
import org.apache.ignite.portables.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.cloner.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.managers.discovery.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.managers.swapspace.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.colocated.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.gridgain.grid.kernal.processors.cache.jta.*;
import org.gridgain.grid.kernal.processors.cache.local.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.query.continuous.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.grid.kernal.processors.closure.*;
import org.gridgain.grid.kernal.processors.offheap.*;
import org.gridgain.grid.kernal.processors.portable.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.apache.ignite.plugin.security.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import javax.cache.configuration.*;
import javax.cache.expiry.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCacheMemoryMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

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
    private GridCacheAffinityManager<K, V> affMgr;

    /** Event manager. */
    private GridCacheEventManager<K, V> evtMgr;

    /** Query manager. */
    private GridCacheQueryManager<K, V> qryMgr;

    /** Continuous query manager. */
    private GridCacheContinuousQueryManager<K, V> contQryMgr;

    /** Swap manager. */
    private GridCacheSwapManager<K, V> swapMgr;

    /** Evictions manager. */
    private GridCacheEvictionManager<K, V> evictMgr;

    /** Data structures manager. */
    private GridCacheDataStructuresManager<K, V> dataStructuresMgr;

    /** Eager TTL manager. */
    private GridCacheTtlManager<K, V> ttlMgr;

    /** Store manager. */
    private GridCacheStoreManager<K, V> storeMgr;

    /** Replication manager. */
    private GridCacheDrManager<K, V> drMgr;

    /** JTA manager. */
    private GridCacheJtaManagerAdapter<K, V> jtaMgr;

    /** Managers. */
    private List<GridCacheManager<K, V>> mgrs = new LinkedList<>();

    /** Cache gateway. */
    private GridCacheGateway<K, V> gate;

    /** Grid cache. */
    private GridCacheAdapter<K, V> cache;

    /** No-value filter array. */
    private IgnitePredicate<GridCacheEntry<K, V>>[] noValArr;

    /** Has-value filter array. */
    private IgnitePredicate<GridCacheEntry<K, V>>[] hasValArr;

    /** No-peek-value filter array. */
    private IgnitePredicate<GridCacheEntry<K, V>>[] noPeekArr;

    /** Has-peek-value filter array. */
    private IgnitePredicate<GridCacheEntry<K, V>>[] hasPeekArr;

    /** No-op filter array. */
    private IgnitePredicate<GridCacheEntry<K, V>>[] trueArr;

    /** Cached local rich node. */
    private ClusterNode locNode;

    /**
     * Thread local projection. If it's set it means that method call was initiated
     * by child projection of initial cache.
     */
    private ThreadLocal<GridCacheProjectionImpl<K, V>> prjPerCall = new ThreadLocal<>();

    /** Thread local forced flags that affect any projection in the same thread. */
    private ThreadLocal<GridCacheFlag[]> forcedFlags = new ThreadLocal<>();

    /** Constant array to avoid recreation. */
    private static final GridCacheFlag[] FLAG_LOCAL_READ = new GridCacheFlag[]{LOCAL, READ};

    /** Local flag array. */
    private static final GridCacheFlag[] FLAG_LOCAL = new GridCacheFlag[]{LOCAL};

    /** Cache name. */
    private String cacheName;

    /** Cache ID. */
    private int cacheId;

    /** System cache flag. */
    private boolean sys;

    /** Default expiry policy. */
    private ExpiryPolicy expiryPlc;

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

        /*
         * Managers in starting order!
         * ===========================
         */

        GridCacheEventManager<K, V> evtMgr,
        GridCacheSwapManager<K, V> swapMgr,
        GridCacheStoreManager<K, V> storeMgr,
        GridCacheEvictionManager<K, V> evictMgr,
        GridCacheQueryManager<K, V> qryMgr,
        GridCacheContinuousQueryManager<K, V> contQryMgr,
        GridCacheAffinityManager<K, V> affMgr,
        GridCacheDataStructuresManager<K, V> dataStructuresMgr,
        GridCacheTtlManager<K, V> ttlMgr,
        GridCacheDrManager<K, V> drMgr,
        GridCacheJtaManagerAdapter<K, V> jtaMgr) {
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

        noValArr = new IgnitePredicate[]{F.cacheNoGetValue()};
        hasValArr = new IgnitePredicate[]{F.cacheHasGetValue()};
        noPeekArr = new IgnitePredicate[]{F.cacheNoPeekValue()};
        hasPeekArr = new IgnitePredicate[]{F.cacheHasPeekValue()};
        trueArr = new IgnitePredicate[]{F.alwaysTrue()};

        // Create unsafe memory only if writing values
        unsafeMemory = cacheCfg.getMemoryMode() == OFFHEAP_VALUES ?
            new GridUnsafeMemory(cacheCfg.getOffHeapMaxMemory()) : null;

        gate = new GridCacheGateway<>(this);

        cacheName = cacheCfg.getName();

        if (cacheName != null) {
            int hash = cacheName.hashCode();

            if (hash == 0)
                hash = 1;

            cacheId = hash;
        }
        else
            cacheId = 1;

        sys = CU.UTILITY_CACHE_NAME.equals(cacheName);

        Factory<ExpiryPolicy> factory = cacheCfg.getExpiryPolicyFactory();

        expiryPlc = factory != null ? factory.create() : null;

        if (expiryPlc instanceof EternalExpiryPolicy)
            expiryPlc = null;
    }

    /**
     * @return Cache default {@link ExpiryPolicy}.
     */
    @Nullable public ExpiryPolicy expiry() {
        return expiryPlc;
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
        return cacheCfg.getCacheMode() == GridCacheMode.REPLICATED;
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
    public void incrementPublicSize(GridCacheMapEntry<K, V> e) {
        assert deferredDelete();
        assert e != null;
        assert !e.isInternal();

        cache.map().incrementSize(e);

        if (isDht() || isColocated() || isDhtAtomic()) {
            GridDhtLocalPartition<K, V> part = topology().localPartition(e.partition(), -1, false);

            if (part != null)
                part.incrementPublicSize();
        }
    }

    /**
     * @param e Entry.
     */
    public void decrementPublicSize(GridCacheMapEntry<K, V> e) {
        assert deferredDelete();
        assert e != null;
        assert !e.isInternal();

        cache.map().decrementSize(e);

        if (isDht() || isColocated() || isDhtAtomic()) {
            GridDhtLocalPartition<K, V> part = topology().localPartition(e.partition(), -1, false);

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
    public GridEx grid() {
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
    public IgniteTxKey<K> txKey(K key) {
        return new IgniteTxKey<>(key, cacheId);
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
     * @return {@code True} if preload is enabled.
     */
    public boolean preloadEnabled() {
        return cacheCfg.getPreloadMode() != NONE;
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
    public GridDhtPartitionTopology<K, V> topology() {
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
    public IgniteMarshaller marshaller() {
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
    public IgniteTxManager<K, V> tm() {
         return sharedCtx.tm();
    }

    /**
     * @return Lock order manager.
     */
    public GridCacheVersionManager<K, V> versions() {
        return sharedCtx.versions();
    }

    /**
     * @return Lock manager.
     */
    public GridCacheMvccManager<K, V> mvcc() {
        return sharedCtx.mvcc();
    }

    /**
     * @return Event manager.
     */
    public GridCacheEventManager<K, V> events() {
        return evtMgr;
    }

    /**
     * @return Cache affinity manager.
     */
    public GridCacheAffinityManager<K, V> affinity() {
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
    public GridCacheContinuousQueryManager<K, V> continuousQueries() {
        return contQryMgr;
    }

    /**
     * @return Swap manager.
     */
    public GridCacheSwapManager<K, V> swap() {
        return swapMgr;
    }

    /**
     * @return Store manager.
     */
    public GridCacheStoreManager<K, V> store() {
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
    public GridCacheIoManager<K, V> io() {
        return sharedCtx.io();
    }

    /**
     * @return Eviction manager.
     */
    public GridCacheEvictionManager<K, V> evicts() {
        return evictMgr;
    }

    /**
     * @return Data structures manager.
     */
    public GridCacheDataStructuresManager<K, V> dataStructures() {
        return dataStructuresMgr;
    }

    /**
     * @return DR manager.
     */
    public GridCacheDrManager<K, V> dr() {
        return drMgr;
    }

    /**
     * @return TTL manager.
     */
    public GridCacheTtlManager<K, V> ttl() {
        return ttlMgr;
    }

    /**
     * @return JTA manager.
     */
    public GridCacheJtaManagerAdapter<K, V> jta() {
        return jtaMgr;
    }
    /**
     * @return No get-value filter.
     */
    public IgnitePredicate<GridCacheEntry<K, V>>[] noGetArray() {
        return noValArr;
    }

    /**
     * @return Has get-value filer.
     */
    public IgnitePredicate<GridCacheEntry<K, V>>[] hasGetArray() {
        return hasValArr;
    }

    /**
     * @return No get-value filter.
     */
    public IgnitePredicate<GridCacheEntry<K, V>>[] noPeekArray() {
        return noPeekArr;
    }

    /**
     * @return Has get-value filer.
     */
    public IgnitePredicate<GridCacheEntry<K, V>>[] hasPeekArray() {
        return hasPeekArr;
    }

    /**
     * @param val Value to check.
     * @return Predicate array that checks for value.
     */
    @SuppressWarnings({"unchecked"})
    public IgnitePredicate<GridCacheEntry<K, V>>[] equalsPeekArray(V val) {
        assert val != null;

        return new IgnitePredicate[]{F.cacheContainsPeek(val)};
    }

    /**
     * @return Empty filter.
     */
    public IgnitePredicate<GridCacheEntry<K, V>> truex() {
        return F.alwaysTrue();
    }

    /**
     * @return No-op array.
     */
    public IgnitePredicate<GridCacheEntry<K, V>>[] trueArray() {
        return trueArr;
    }

    /**
     * @return Empty cache version array.
     */
    public GridCacheVersion[] emptyVersion() {
        return EMPTY_VERSION;
    }

    /**
     * @param p Single predicate.
     * @return Array containing single predicate.
     */
    @SuppressWarnings({"unchecked"})
    public IgnitePredicate<GridCacheEntry<K, V>>[] vararg(IgnitePredicate<GridCacheEntry<K, V>> p) {
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
    @SuppressWarnings({"ErrorNotRethrown"})
    public <K1, V1> boolean isAll(GridCacheEntryEx<K1, V1> e,
        @Nullable IgnitePredicate<GridCacheEntry<K1, V1>>[] p) throws IgniteCheckedException {
        return F.isEmpty(p) || isAll(e.wrap(false), p);
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
        GridCacheFlag[] oldFlags = forceFlags(FLAG_LOCAL_READ);

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
     * Forces LOCAL flag.
     *
     * @return Previously forced flags.
     */
    @Nullable public GridCacheFlag[] forceLocal() {
        return forceFlags(FLAG_LOCAL);
    }

    /**
     * Forces LOCAL and READ flags.
     *
     * @return Forced flags that were set prior to method call.
     */
    @Nullable public GridCacheFlag[] forceLocalRead() {
        return forceFlags(FLAG_LOCAL_READ);
    }

    /**
     * Force projection flags for the current thread. These flags will affect all
     * projections (even without flags) used within the current thread.
     *
     * @param flags Flags to force.
     * @return Forced flags that were set prior to method call.
     */
    @Nullable public GridCacheFlag[] forceFlags(@Nullable GridCacheFlag[] flags) {
        GridCacheFlag[] oldFlags = forcedFlags.get();

        forcedFlags.set(F.isEmpty(flags) ? null : flags);

        return oldFlags;
    }

    /**
     * Gets forced flags for current thread.
     *
     * @return Forced flags.
     */
    public GridCacheFlag[] forcedFlags() {
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
        if (obj == null)
            return obj;

        GridCacheCloner c = cacheCfg.getCloner();

        if (c != null)
            return c.cloneValue(obj);

        return X.cloneObject(obj, false, true);
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
    public boolean hasFlag(GridCacheFlag flag) {
        assert flag != null;

        if (nearContext())
            return dht().near().context().hasFlag(flag);

        GridCacheProjectionImpl<K, V> prj = prjPerCall.get();

        GridCacheFlag[] forced = forcedFlags.get();

        return (prj != null && prj.flags().contains(flag)) || (forced != null && U.containsObjectArray(forced, flag));
    }

    /**
     * Checks whether any of the given flags is set.
     *
     * @param flags Flags to check.
     * @return {@code true} if any of the given flags is set.
     */
    public boolean hasAnyFlags(GridCacheFlag[] flags) {
        assert !F.isEmpty(flags);

        if (nearContext())
            return dht().near().context().hasAnyFlags(flags);

        GridCacheProjectionImpl<K, V> prj = prjPerCall.get();

        if (prj == null && F.isEmpty(forcedFlags.get()))
            return false;

        for (GridCacheFlag f : flags)
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
    public boolean hasAnyFlags(Collection<GridCacheFlag> flags) {
        assert !F.isEmpty(flags);

        if (nearContext())
            return dht().near().context().hasAnyFlags(flags);

        GridCacheProjectionImpl<K, V> prj = prjPerCall.get();

        if (prj == null && F.isEmpty(forcedFlags.get()))
            return false;

        for (GridCacheFlag f : flags)
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
    public void denyOnFlag(GridCacheFlag flag) {
        assert flag != null;

        if (hasFlag(flag))
            throw new GridCacheFlagException(flag);
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
    public void denyOnFlags(GridCacheFlag[] flags) {
        assert !F.isEmpty(flags);

        if (hasAnyFlags(flags))
            throw new GridCacheFlagException(flags);
    }

    /**
     * @param flags Flags.
     */
    public void denyOnFlags(Collection<GridCacheFlag> flags) {
        assert !F.isEmpty(flags);

        if (hasAnyFlags(flags))
            throw new GridCacheFlagException(flags);
    }

    /**
     * Clones cached object depending on whether or not {@link GridCacheFlag#CLONE} flag
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
    public IgniteFuture<V> wrapClone(IgniteFuture<V> f) {
        if (!hasFlag(CLONE))
            return f;

        return f.chain(new CX1<IgniteFuture<V>, V>() {
            @Override public V applyx(IgniteFuture<V> f) throws IgniteCheckedException {
                return cloneValue(f.get());
            }
        });
    }

    /**
     * @param f Target future.
     * @return Wrapped future that is aware of cloning behaviour.
     */
    public IgniteFuture<Map<K, V>> wrapCloneMap(IgniteFuture<Map<K, V>> f) {
        if (!hasFlag(CLONE))
            return f;

        return f.chain(new CX1<IgniteFuture<Map<K, V>>, Map<K, V>>() {
            @Override public Map<K, V> applyx(IgniteFuture<Map<K, V>> f) throws IgniteCheckedException {
                Map<K, V> map = new GridLeanMap<>();

                for (Map.Entry<K, V> e : f.get().entrySet())
                    map.put(e.getKey(), cloneValue(e.getValue()));

                return map;
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
        final GridCacheFlag[] flags = forcedFlags();

        if (prj == null && F.isEmpty(flags))
            return r;

        return new GPR() {
            @Override public void run() {
                GridCacheProjectionImpl<K, V> oldPrj = projectionPerCall();

                projectionPerCall(prj);

                GridCacheFlag[] oldFlags = forceFlags(flags);

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
        final GridCacheFlag[] flags = forcedFlags();

        if (prj == null && F.isEmpty(flags))
            return r;

        return new GPC<T>() {
            @Override public T call() throws Exception {
                GridCacheProjectionImpl<K, V> oldPrj = projectionPerCall();

                projectionPerCall(prj);

                GridCacheFlag[] oldFlags = forceFlags(flags);

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
     * @return {@code True} if values should be always unmarshalled.
     */
    public boolean isUnmarshalValues() {
        return cacheCfg.isQueryIndexEnabled() || !cacheCfg.isStoreValueBytes();
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
    public boolean dhtMap(UUID nearNodeId, long topVer, GridDhtCacheEntry<K, V> entry, IgniteLogger log,
        Map<ClusterNode, List<GridDhtCacheEntry<K, V>>> dhtMap,
        Map<ClusterNode, List<GridDhtCacheEntry<K, V>>> nearMap) throws GridCacheEntryRemovedException {
        assert topVer != -1;

        Collection<ClusterNode> dhtNodes = dht().topology().nodes(entry.partition(), topVer);

        if (log.isDebugEnabled())
            log.debug("Mapping entry to DHT nodes [nodes=" + U.nodeIds(dhtNodes) + ", entry=" + entry + ']');

        Collection<UUID> readers = entry.readers();

        Collection<ClusterNode> nearNodes = null;

        if (!F.isEmpty(readers)) {
            nearNodes = discovery().nodes(readers, F0.notEqualTo(nearNodeId));

            if (log.isDebugEnabled())
                log.debug("Mapping entry to near nodes [nodes=" + U.nodeIds(nearNodes) + ", entry=" + entry + ']');
        }
        else if (log.isDebugEnabled())
            log.debug("Entry has no near readers: " + entry);

        Collection<ClusterNode> dhtRemoteNodes = F.view(dhtNodes, F.remoteNodes(nodeId())); // Exclude local node.

        boolean ret = map(entry, dhtRemoteNodes, dhtMap);

        if (nearNodes != null && !nearNodes.isEmpty())
            ret |= map(entry, F.view(nearNodes, F.notIn(dhtNodes)), nearMap);

        return ret;
    }

    /**
     * @param entry Entry.
     * @param nodes Nodes.
     * @param map Map.
     * @return {@code True} if mapped.
     */
    private boolean map(GridDhtCacheEntry<K, V> entry, Iterable<ClusterNode> nodes,
        Map<ClusterNode, List<GridDhtCacheEntry<K, V>>> map) {
        boolean ret = false;

        if (nodes != null) {
            for (ClusterNode n : nodes) {
                List<GridDhtCacheEntry<K, V>> entries = map.get(n);

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
     * @return Data center ID.
     */
    public byte dataCenterId() {
        return dr().dataCenterId();
    }

    /**
     * @param entry Entry.
     * @param ver Version.
     */
    public void onDeferredDelete(GridCacheEntryEx<K, V> entry, GridCacheVersion ver) {
        assert entry != null;
        assert !Thread.holdsLock(entry);
        assert ver != null;
        assert deferredDelete();

        cache.onDeferredDelete(entry, ver);
    }

    /**
     * @param interceptorRes Result of {@link GridCacheInterceptor#onBeforeRemove} callback.
     * @return {@code True} if interceptor cancels remove.
     */
    public boolean cancelRemove(@Nullable IgniteBiTuple<Boolean, ?> interceptorRes) {
        if (interceptorRes != null) {
            if (interceptorRes.get1() == null) {
                U.warn(log, "GridCacheInterceptor must not return null as cancellation flag value from " +
                    "'onBeforeRemove' method.");

                return false;
            }
            else
                return interceptorRes.get1();
        }
        else {
            U.warn(log, "GridCacheInterceptor must not return null from 'onBeforeRemove' method.");

            return false;
        }
    }

    /**
     * @return Portable processor.
     */
    public GridPortableProcessor portable() {
        return kernalContext().portable();
    }

    /**
     * @return Portable enabled flag.
     */
    public boolean portableEnabled() {
        return cacheCfg.isPortableEnabled();
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
        if (!offheapTiered() || !portableEnabled())
            return (T)obj;

        return (T)portable().unwrapTemporary(obj);
    }

    /**
     * @param bytes Object marshalled with portable marshaller.
     * @return Object marshalled with grid marshaller.
     * @throws IgniteCheckedException If failed.
     */
    public byte[] convertPortableBytes(byte[] bytes) throws IgniteCheckedException {
        assert portableEnabled() && offheapTiered();

        return marshaller().marshal(portable().unmarshal(bytes, 0));
    }

    /**
     * @param obj Object.
     * @return Portable object.
     * @throws PortableException In case of error.
     */
    @Nullable public Object marshalToPortable(@Nullable Object obj) throws PortableException {
        assert portableEnabled();

        if (obj == null)
            return null;

        if (obj instanceof PortableObject || obj instanceof GridCacheInternal)
            return obj;

        GridPortableProcessor proc = kernalContext().portable();

        assert proc != null;

        return proc.marshalToPortable(obj);
    }

    /**
     * Unwraps collection.
     *
     * @param col Collection to unwrap.
     * @param keepPortable Keep portable flag.
     * @return Unwrapped collection.
     */
    public Collection<Object> unwrapPortablesIfNeeded(Collection<Object> col, boolean keepPortable) {
        if (keepPortable || !config().isPortableEnabled())
            return col;

        if (col instanceof ArrayList)
            return unwrapPortables((ArrayList<Object>)col);

        Collection<Object> col0 = new ArrayList<>(col.size());

        for (Object obj : col)
            col0.add(unwrapPortable(obj));

        return col0;
    }

    /**
     * Unwraps map.
     *
     * @param map Map to unwrap.
     * @param keepPortable Keep portable flag.
     * @return Unwrapped collection.
     */
    public Map<Object, Object> unwrapPortablesIfNeeded(Map<Object, Object> map, boolean keepPortable) {
        if (keepPortable || !config().isPortableEnabled())
            return map;

        Map<Object, Object> map0 = U.newHashMap(map.size());

        for (Map.Entry<Object, Object> e : map.entrySet())
            map0.put(unwrapPortable(e.getKey()), unwrapPortable(e.getValue()));

        return map0;
    }

    /**
     * Unwraps array list.
     *
     * @param col List to unwrap.
     * @return Unwrapped list.
     */
    private Collection<Object> unwrapPortables(ArrayList<Object> col) {
        int size = col.size();

        for (int i = 0; i < size; i++) {
            Object o = col.get(i);

            Object unwrapped = unwrapPortable(o);

            if (o != unwrapped)
                col.set(i, unwrapped);
        }

        return col;
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
        assert !portableEnabled() || o == null || U.isPortableOrCollectionType(o.getClass());

        if (o == null)
            return null;

        if (keepPortable || !portableEnabled())
            return o;

        return unwrapPortable(o);
    }

    /**
     * @param o Object to unwrap.
     * @return Unwrapped object.
     */
    private Object unwrapPortable(Object o) {
        if (o instanceof Map.Entry) {
            Map.Entry entry = (Map.Entry)o;

            Object key = entry.getKey();

            boolean unwrapped = false;

            if (key instanceof PortableObject) {
                key = ((PortableObject)key).deserialize();

                unwrapped = true;
            }

            Object val = entry.getValue();

            if (val instanceof PortableObject) {
                val = ((PortableObject)val).deserialize();

                unwrapped = true;
            }

            return unwrapped ? F.t(key, val) : o;
        }
        else {
            if (o instanceof Collection)
                return unwrapPortablesIfNeeded((Collection<Object>)o, false);
            else if (o instanceof Map)
                return unwrapPortablesIfNeeded((Map<Object, Object>)o, false);
            else if (o instanceof PortableObject)
                return ((PortableObject)o).deserialize();
        }

        return o;
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

            GridKernal grid = GridGainEx.gridx(t.get1());

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
