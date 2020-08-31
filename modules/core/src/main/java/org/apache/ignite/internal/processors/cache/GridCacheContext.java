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

import java.io.Closeable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManager;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.datastructures.CacheDataStructuresManager;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheAdapter.GlobalRemoveAllJob;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTransactionalCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtColocatedCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTransactionalCache;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrManager;
import org.apache.ignite.internal.processors.cache.jta.CacheJtaManagerAdapter;
import org.apache.ignite.internal.processors.cache.local.GridLocalCache;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager;
import org.apache.ignite.internal.processors.cache.store.CacheStoreManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheManager;
import org.apache.ignite.internal.processors.plugin.CachePluginManager;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAddQueryEntityOperation;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.GPC;
import org.apache.ignite.internal.util.typedef.internal.GPR;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_TRIGGERING_CACHE_INTERCEPTOR_ON_CONFLICT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_READ_LOAD_BALANCING;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

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
            return new IgniteBiTuple<>();
        }
    };

    /** Empty cache version array. */
    private static final GridCacheVersion[] EMPTY_VERSION = new GridCacheVersion[0];

    /** Kernal context. */
    private GridKernalContext ctx;

    /** Cache shared context. */
    private GridCacheSharedContext<K, V> sharedCtx;

    /** Cache group. */
    private CacheGroupContext grp;

    /** Logger. */
    private IgniteLogger log;

    /** Cache configuration. */
    private volatile CacheConfiguration cacheCfg;

    /** Affinity manager. */
    private GridCacheAffinityManager affMgr;

    /** Event manager. */
    private GridCacheEventManager evtMgr;

    /** Query manager. */
    private GridCacheQueryManager<K, V> qryMgr;

    /** Continuous query manager. */
    private CacheContinuousQueryManager contQryMgr;

    /** Evictions manager. */
    private CacheEvictionManager evictMgr;

    /** Data structures manager. */
    private CacheDataStructuresManager dataStructuresMgr;

    /** Eager TTL manager. */
    private GridCacheTtlManager ttlMgr;

    /** Store manager. */
    private CacheStoreManager storeMgr;

    /** Compression manager. */
    private CacheCompressionManager compressMgr;

    /** Replication manager. */
    private GridCacheDrManager drMgr;

    /** Conflict resolver manager. */
    private CacheConflictResolutionManager rslvrMgr;

    /** Cache plugin manager. */
    private CachePluginManager pluginMgr;

    /** Managers. */
    private List<GridCacheManager<K, V>> mgrs = new LinkedList<>();

    /** Cache gateway. */
    private GridCacheGateway<K, V> gate;

    /** Grid cache. */
    private GridCacheAdapter<K, V> cache;

    /** Cached local rich node. */
    private ClusterNode locNode;

    /**
     * Thread local operation context. If it's set it means that method call was initiated
     * by child cache of initial cache.
     */
    private ThreadLocal<CacheOperationContext> opCtxPerCall = new ThreadLocal<>();

    /** Cache name. */
    private String cacheName;

    /** Cache ID. */
    private int cacheId;

    /** Cache ID. */
    private Integer cacheIdBoxed;

    /** Cache type. */
    private CacheType cacheType;

    /** IO policy. */
    private byte plc;

    /** Default expiry policy. */
    private ExpiryPolicy expiryPlc;

    /** Cache weak query iterator holder. */
    private CacheWeakQueryIteratorsHolder<Map.Entry<K, V>> itHolder;

    /** Affinity node. */
    private volatile boolean affNode;

    /** Conflict resolver. */
    private CacheVersionConflictResolver conflictRslvr;

    /** */
    private CacheObjectContext cacheObjCtx;

    /** */
    private CountDownLatch startLatch = new CountDownLatch(1);

    /** Topology version when cache was started on local node. */
    private volatile AffinityTopologyVersion locStartTopVer;

    /** Dynamic cache deployment ID. */
    private volatile IgniteUuid dynamicDeploymentId;

    /** Updates allowed flag. */
    private boolean updatesAllowed;

    /** Deployment enabled flag for this specific cache */
    private boolean depEnabled;

    /** */
    private boolean deferredDel;

    /** Whether {@link EventType#EVT_CACHE_REBALANCE_STARTED} was sent (used only for REPLICATED cache). */
    private volatile boolean rebalanceStartedEvtSent;

    /** Whether {@link EventType#EVT_CACHE_REBALANCE_STOPPED} was sent (used only for REPLICATED cache). */
    private volatile boolean rebalanceStoppedEvtSent;

    /** Statistics enabled flag. */
    private volatile boolean statisticsEnabled;

    /** Whether to enable read load balancing. */
    private final boolean readLoadBalancingEnabled = IgniteSystemProperties.getBoolean(IGNITE_READ_LOAD_BALANCING, true);

    /** Flag indicating whether data can be read from backup. */
    private boolean readFromBackup = CacheConfiguration.DFLT_READ_FROM_BACKUP;

    /** Local node's MAC address. */
    private volatile String locMacs;

    /** Recovery mode flag. */
    private volatile boolean recoveryMode;

    /** */
    private final boolean disableTriggeringCacheInterceptorOnConflict =
        Boolean.parseBoolean(System.getProperty(IGNITE_DISABLE_TRIGGERING_CACHE_INTERCEPTOR_ON_CONFLICT, "false"));

    /** Last remove all job future. */
    private AtomicReference<IgniteInternalFuture<Boolean>> lastRmvAllJobFut = new AtomicReference<>();

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
     * @param grp Cache group.
     * @param cacheType Cache type.
     * @param locStartTopVer Topology version when cache was started on local node.
     * @param affNode {@code True} if local node is affinity node.
     * @param updatesAllowed Updates allowed flag.
     * @param evtMgr Cache event manager.
     * @param storeMgr Store manager.
     * @param evictMgr Cache eviction manager.
     * @param qryMgr Cache query manager.
     * @param contQryMgr Continuous query manager.
     * @param affMgr Affinity manager.
     * @param dataStructuresMgr Cache dataStructures manager.
     * @param ttlMgr TTL manager.
     * @param drMgr Data center replication manager.
     * @param rslvrMgr Conflict resolution manager.
     * @param pluginMgr Cache plugin manager.
     */
    @SuppressWarnings({"unchecked"})
    public GridCacheContext(
        GridKernalContext ctx,
        GridCacheSharedContext sharedCtx,
        CacheConfiguration cacheCfg,
        CacheGroupContext grp,
        CacheType cacheType,
        AffinityTopologyVersion locStartTopVer,
        IgniteUuid deploymentId,
        boolean affNode,
        boolean updatesAllowed,
        boolean statisticsEnabled,
        boolean recoveryMode,

        /*
         * Managers in starting order!
         * ===========================
         */

        CacheCompressionManager compressMgr,
        GridCacheEventManager evtMgr,
        CacheStoreManager storeMgr,
        CacheEvictionManager evictMgr,
        GridCacheQueryManager<K, V> qryMgr,
        CacheContinuousQueryManager contQryMgr,
        CacheDataStructuresManager dataStructuresMgr,
        GridCacheTtlManager ttlMgr,
        GridCacheDrManager drMgr,
        CacheConflictResolutionManager<K, V> rslvrMgr,
        CachePluginManager pluginMgr,
        GridCacheAffinityManager affMgr,
        PlatformCacheManager platformMgr
    ) {
        assert ctx != null;
        assert sharedCtx != null;
        assert cacheCfg != null;
        assert locStartTopVer != null : cacheCfg.getName();

        assert compressMgr != null;
        assert grp != null;
        assert evtMgr != null;
        assert storeMgr != null;
        assert evictMgr != null;
        assert qryMgr != null;
        assert contQryMgr != null;
        assert affMgr != null;
        assert dataStructuresMgr != null;
        assert ttlMgr != null;
        assert rslvrMgr != null;
        assert pluginMgr != null;

        this.ctx = ctx;
        this.sharedCtx = sharedCtx;
        this.cacheCfg = cacheCfg;
        this.grp = grp;
        this.cacheType = cacheType;
        this.locStartTopVer = locStartTopVer;
        this.affNode = affNode;
        this.updatesAllowed = updatesAllowed;
        this.depEnabled = ctx.deploy().enabled() && !cacheObjects().isBinaryEnabled(cacheCfg);

        /*
         * Managers in starting order!
         * ===========================
         */
        this.compressMgr = add(compressMgr);
        this.evtMgr = add(evtMgr);
        this.storeMgr = add(storeMgr);
        this.evictMgr = add(evictMgr);
        this.qryMgr = add(qryMgr);
        this.contQryMgr = add(contQryMgr);
        this.dataStructuresMgr = add(dataStructuresMgr);
        this.ttlMgr = add(ttlMgr);
        this.drMgr = add(drMgr);
        this.rslvrMgr = add(rslvrMgr);
        this.pluginMgr = add(pluginMgr);
        this.affMgr = add(affMgr);
        add(platformMgr);

        log = ctx.log(getClass());

        gate = new GridCacheGateway<>(this);

        cacheName = cacheCfg.getName();

        cacheId = CU.cacheId(cacheName);

        cacheIdBoxed = cacheId;

        plc = cacheType.ioPolicy();

        Factory<ExpiryPolicy> factory = cacheCfg.getExpiryPolicyFactory();

        expiryPlc = factory != null ? factory.create() : null;

        if (expiryPlc instanceof EternalExpiryPolicy)
            expiryPlc = null;

        itHolder = new CacheWeakQueryIteratorsHolder(log);

        readFromBackup = cacheCfg.isReadFromBackup();

        this.dynamicDeploymentId = deploymentId;
        this.recoveryMode = recoveryMode;

        statisticsEnabled(statisticsEnabled);

        assert kernalContext().recoveryMode() == recoveryMode;

        if (!recoveryMode) {
            locMacs = localNode().attribute(ATTR_MACS);

            assert locMacs != null;
        }
    }

    /**
     * Called when cache was restored during recovery and node has joined to topology.
     *
     * @param topVer Cache topology join version.
     * @param clusterWideDesc Cluster-wide cache descriptor received during exchange.
     */
    public void finishRecovery(AffinityTopologyVersion topVer, DynamicCacheDescriptor clusterWideDesc) {
        assert recoveryMode : this;

        recoveryMode = false;

        locStartTopVer = topVer;

        locMacs = localNode().attribute(ATTR_MACS);

        assert locMacs != null;

        this.statisticsEnabled = clusterWideDesc.cacheConfiguration().isStatisticsEnabled();
        this.dynamicDeploymentId = clusterWideDesc.deploymentId();
    }

    /**
     * @return {@code True} if cache is in recovery mode.
     */
    public boolean isRecoveryMode() {
        return recoveryMode;
    }

    /**
     * @return Cache group ID.
     */
    public int groupId() {
        return grp.groupId();
    }

    /**
     * @return Cache group.
     */
    public CacheGroupContext group() {
        return grp;
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
        conflictRslvr = rslvrMgr.conflictResolver();
    }

    /**
     * @return {@code True} if local node is affinity node.
     */
    public boolean affinityNode() {
        return affNode;
    }

    /**
     * @return {@code true} If this is a replicated cache and we are on a data node.
     */
    public boolean isReplicatedAffinityNode() {
        return isReplicated() && affinityNode();
    }

    /**
     * @throws IgniteCheckedException If failed to wait.
     */
    public void awaitStarted() throws IgniteCheckedException {
        U.await(startLatch);

        GridCachePreloader prldr = preloader();

        if (prldr != null)
            prldr.startFuture().get();
    }

    /**
     * @return Started flag.
     */
    public boolean started() {
        if (startLatch.getCount() != 0)
            return false;

        GridCachePreloader prldr = preloader();

        return prldr == null || prldr.startFuture().isDone();
    }

    /**
     *
     */
    public void onStarted() {
        startLatch.countDown();
    }

    /**
     * @return Topology version when cache was started on local node.
     */
    public AffinityTopologyVersion startTopologyVersion() {
        assert locStartTopVer != null : name();

        return locStartTopVer;
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
     * @return Cache ID.
     */
    public Integer cacheIdBoxed() {
        return cacheIdBoxed;
    }

    /**
     * @return {@code True} if should use system transactions which are isolated from user transactions.
     */
    public boolean systemTx() {
        return cacheType == CacheType.UTILITY ||
            ((cacheType == CacheType.INTERNAL || cacheType == CacheType.DATA_STRUCTURES) && transactional());
    }

    /**
     * @return {@code True} if cache created by user.
     */
    public boolean userCache() {
        return cacheType.userCache();
    }

    /**
     * @return {@code True} if cache created to store data structures.
     */
    public boolean dataStructuresCache() {
        return cacheType == CacheType.DATA_STRUCTURES;
    }

    /**
     * @return IO policy for the given cache.
     */
    public byte ioPolicy() {
        return plc;
    }

    /**
     * @param cache Cache.
     */
    public void cache(GridCacheAdapter<K, V> cache) {
        this.cache = cache;

        deferredDel = cache.isDht() || cache.isDhtAtomic() || cache.isColocated() ||
            (cache.isNear() && cache.configuration().getAtomicityMode() == ATOMIC);
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
        return config().getCacheMode() == CacheMode.REPLICATED;
    }

    /**
     * @return {@code True} if cache is partitioned cache.
     */
    public boolean isPartitioned() {
        return config().getCacheMode() == CacheMode.PARTITIONED;
    }

    /**
     * @return {@code True} in case replication is enabled.
     */
    public boolean isDrEnabled() {
        return dr().enabled();
    }

    /**
     * @return {@code True} in case cache supports query.
     */
    public boolean isQueryEnabled() {
        return !F.isEmpty(config().getQueryEntities());
    }

    /**
     * @return {@code True} if entries should not be deleted from cache immediately.
     */
    public boolean deferredDelete() {
        return deferredDel;
    }

    /**
     * @param e Entry.
     */
    public void incrementPublicSize(GridCacheMapEntry e) {
        assert deferredDelete();
        assert e != null;
        assert !e.isInternal() : e;

        cache.incrementSize(e);
    }

    /**
     * @param e Entry.
     */
    public void decrementPublicSize(GridCacheMapEntry e) {
        assert deferredDelete();
        assert e != null;
        assert !e.isInternal() : e;

        cache.decrementSize(e);
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
     * @return Ignite instance name.
     */
    public String igniteInstanceName() {
        return ctx.igniteInstanceName();
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return cacheName;
    }

    /**
     * @return Data region.
     */
    public DataRegion dataRegion() {
        return grp.dataRegion();
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
     * @throws SecurityException If security check failed.
     */
    public void checkSecurity(SecurityPermission op) throws SecurityException {
        if (CU.isSystemCache(name()))
            return;

        ctx.security().authorize(name(), op);
    }

    /**
     * @return Preloader.
     */
    public GridCachePreloader preloader() {
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
        return grp.rebalanceEnabled();
    }

    /**
     * @return {@code True} if atomic.
     */
    public boolean atomic() {
        return config().getAtomicityMode() == ATOMIC;
    }

    /**
     * @return {@code True} if transactional.
     */
    public boolean transactional() {
        CacheConfiguration cfg = config();

        return cfg.getAtomicityMode() == TRANSACTIONAL || cfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT;
    }

    /**
     * @return {@code True} if transactional snapshot.
     */
    public boolean transactionalSnapshot() {
        return config().getAtomicityMode() == TRANSACTIONAL_SNAPSHOT;
    }

    /**
     * @return {@code True} if cache interceptor should be skipped in case of conflicts.
     */
    public boolean disableTriggeringCacheInterceptorOnConflict() {
        return disableTriggeringCacheInterceptorOnConflict;
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
        return grp.topology();
    }

    /**
     * @return DHT cache.
     */
    public GridDhtCacheAdapter dhtCache() {
        GridCacheAdapter<K, V> cache = this.cache;

        if (cache == null)
            throw new IllegalStateException("Cache stopped: " + cacheName);

        return isNear() ? ((GridNearCacheAdapter<K, V>)cache).dht() : dht();
    }

    /**
     * @return Topology version future.
     */
    public GridDhtTopologyFuture topologyVersionFuture() {
        GridCacheAdapter<K, V> cache = this.cache;

        if (cache == null)
            throw new IllegalStateException("Cache stopped: " + cacheName);

        assert cache.isNear() || cache.isDht() || cache.isColocated() || cache.isDhtAtomic() : cache;

        return topology(cache).topologyVersionFuture();
    }

    /**
     * @param cache Cache.
     * @return Partition topology.
     */
    private GridDhtPartitionTopology topology(GridCacheAdapter<K, V> cache) {
        return cache.isNear() ? ((GridNearCacheAdapter<K, V>)cache).dht().topology() :
            ((GridDhtCacheAdapter<K, V>)cache).topology();
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
     * @return Grid deployment manager.
     */
    public GridDeploymentManager gridDeploy() {
        return ctx.deploy();
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
     * @throws IllegalStateException If this cache context was cleaned up.
     */
    public CacheConfiguration config() {
        CacheConfiguration res = cacheCfg;

        if (res == null)
            throw new IllegalStateException((new CacheStoppedException(name())));

        return res;
    }

    /**
     * @return {@code True} If store writes should be performed from dht transactions. This happens if both
     *      {@code writeBehindEnabled} and {@code writeBehindPreferPrimary} cache configuration properties
     *      are set to {@code true} or the store is local.
     */
    public boolean writeToStoreFromDht() {
        return store().isLocal() || config().isWriteBehindEnabled();
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
     * @return Store manager.
     */
    public CacheStoreManager store() {
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
    public CacheEvictionManager evicts() {
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
     * @return Offheap manager.
     */
    public IgniteCacheOffheapManager offheap() {
        return grp.offheap();
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
        return sharedCtx.jta();
    }

    /**
     * @return Cache plugin manager.
     */
    public CachePluginManager plugin() {
        return pluginMgr;
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
    public CacheEntryPredicate noVal() {
        return new CacheEntrySerializablePredicate(new CacheEntryPredicateNoValue());
    }

    /**
     * @return Has value filter.
     */
    public CacheEntryPredicate hasVal() {
        return new CacheEntrySerializablePredicate(new CacheEntryPredicateHasValue());
    }

    /**
     * @param val Value to check.
     * @return Predicate that checks for value.
     */
    public CacheEntryPredicate equalsVal(V val) {
        return new CacheEntryPredicateContainsValue(toCacheObject(val));
    }

    /**
     * @return Empty cache version array.
     */
    public GridCacheVersion[] emptyVersion() {
        return EMPTY_VERSION;
    }

    /**
     * @return Compression manager.
     */
    public CacheCompressionManager compress() {
        return compressMgr;
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
        return p == null ? CU.<K, V>empty() : new IgnitePredicate[] {p};
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
    public <E> boolean isAll(E e, @Nullable IgnitePredicate<? super E>[] p) throws IgniteCheckedException {
        if (F.isEmpty(p))
            return true;

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
     * Sets thread local cache operation context.
     *
     * @param opCtx Operation context.
     */
    public void operationContextPerCall(@Nullable CacheOperationContext opCtx) {
        if (nearContext())
            dht().near().context().opCtxPerCall.set(opCtx);
        else
            opCtxPerCall.set(opCtx);
    }

    /**
     * Gets thread local cache operation context.
     *
     * @return Operation context per call.
     */
    public CacheOperationContext operationContextPerCall() {
        return nearContext() ? dht().near().context().opCtxPerCall.get() : opCtxPerCall.get();
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

        return subjectIdPerCall(subjId, operationContextPerCall());
    }

    /**
     * Gets subject ID per call.
     *
     * @param subjId Optional already existing subject ID.
     * @param opCtx Optional thread local operation context.
     * @return Subject ID per call.
     */
    public UUID subjectIdPerCall(@Nullable UUID subjId, @Nullable CacheOperationContext opCtx) {
        if (opCtx != null)
            subjId = opCtx.subjectId();

        if (subjId == null)
            subjId = ctx.localNodeId();

        return subjId;
    }

    /**
     * @return {@code true} if the skip store flag is set.
     */
    public boolean skipStore() {
        if (nearContext())
            return dht().near().context().skipStore();

        CacheOperationContext opCtx = opCtxPerCall.get();

        return (opCtx != null && opCtx.skipStore());
    }

    /**
     * @return {@code True} if need check near cache context.
     */
    private boolean nearContext() {
        return isDht() || (isDhtAtomic() && dht().near() != null);
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

        // Have to get operation context per call used by calling thread to use it in a new thread.
        final CacheOperationContext opCtx = operationContextPerCall();

        if (opCtx == null)
            return r;

        return new GPR() {
            @Override public void run() {
                CacheOperationContext old = operationContextPerCall();

                operationContextPerCall(opCtx);

                try {
                    r.run();
                }
                finally {
                    operationContextPerCall(old);
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

        // Have to get operation context per call used by calling thread to use it in a new thread.
        final CacheOperationContext opCtx = operationContextPerCall();

        if (opCtx == null)
            return r;

        return new GPC<T>() {
            @Override public T call() throws Exception {
                CacheOperationContext old = operationContextPerCall();

                operationContextPerCall(opCtx);

                try {
                    return r.call();
                }
                finally {
                    operationContextPerCall(old);
                }
            }
        };
    }

    /**
     * @return {@code True} if deployment is enabled.
     */
    public boolean deploymentEnabled() {
        return depEnabled;
    }

    /**
     * @return {@code True} if store read-through mode is enabled.
     */
    public boolean readThrough() {
        return config().isReadThrough() && !skipStore();
    }

    /**
     * @return {@code True} if store and read-through mode are enabled in configuration.
     */
    public boolean readThroughConfigured() {
        return store().configured() && config().isReadThrough();
    }

    /**
     * @return {@code True} if {@link CacheConfiguration#isLoadPreviousValue()} flag is set.
     */
    public boolean loadPreviousValue() {
        return config().isLoadPreviousValue();
    }

    /**
     * @return {@code True} if store write-through is enabled.
     */
    public boolean writeThrough() {
        return config().isWriteThrough() && !skipStore();
    }

    /**
     * @return {@code True} if invalidation is enabled.
     */
    public boolean isInvalidate() {
        return config().isInvalidate();
    }

    /**
     * @return {@code True} if synchronous commit is enabled.
     */
    public boolean syncCommit() {
        return config().getWriteSynchronizationMode() == FULL_SYNC;
    }

    /**
     * @return {@code True} if synchronous rollback is enabled.
     */
    public boolean syncRollback() {
        return config().getWriteSynchronizationMode() == FULL_SYNC;
    }

    /**
     * @return {@code True} if only primary node should be updated synchronously.
     */
    public boolean syncPrimary() {
        return config().getWriteSynchronizationMode() == PRIMARY_SYNC;
    }

    /**
     * @param nearNodeId Near node ID.
     * @param topVer Topology version.
     * @param entry Entry.
     * @param log Log.
     * @param dhtMap Dht mappings.
     * @param nearMap Near mappings.
     * @throws GridCacheEntryRemovedException If reader for entry is removed.
     */
    public void dhtMap(
        UUID nearNodeId,
        AffinityTopologyVersion topVer,
        GridDhtCacheEntry entry,
        GridCacheVersion explicitLockVer,
        IgniteLogger log,
        Map<ClusterNode, List<GridDhtCacheEntry>> dhtMap,
        @Nullable Map<ClusterNode, List<GridDhtCacheEntry>> nearMap
    ) throws GridCacheEntryRemovedException {
        assert !AffinityTopologyVersion.NONE.equals(topVer);

        Collection<ClusterNode> dhtNodes = dht().topology().nodes(entry.partition(), topVer);

        if (log.isDebugEnabled())
            log.debug("Mapping entry to DHT nodes [nodes=" + U.nodeIds(dhtNodes) + ", entry=" + entry + ']');

        Collection<ClusterNode> dhtRemoteNodes = F.view(dhtNodes, F.remoteNodes(nodeId())); // Exclude local node.

        map(entry, dhtRemoteNodes, dhtMap);

        Collection<ClusterNode> nearRemoteNodes = null;

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

            if (nearNodes != null && !nearNodes.isEmpty()) {
                nearRemoteNodes = F.view(nearNodes, F.notIn(dhtNodes));

                map(entry, nearRemoteNodes, nearMap);
            }
        }

        if (explicitLockVer != null) {
            Collection<ClusterNode> dhtNodeIds = new ArrayList<>(dhtRemoteNodes);
            Collection<ClusterNode> nearNodeIds = F.isEmpty(nearRemoteNodes) ? null : new ArrayList<>(nearRemoteNodes);

            entry.mappings(explicitLockVer, dhtNodeIds, nearNodeIds);
        }
    }

    /**
     * @param entry Entry.
     * @param log Log.
     * @param dhtMap Dht mappings.
     * @param nearMap Near mappings.
     * @throws GridCacheEntryRemovedException If reader for entry is removed.
     */
    public void dhtMap(
        GridDhtCacheEntry entry,
        GridCacheVersion explicitLockVer,
        IgniteLogger log,
        Map<ClusterNode, List<GridDhtCacheEntry>> dhtMap,
        Map<ClusterNode, List<GridDhtCacheEntry>> nearMap
    ) throws GridCacheEntryRemovedException {
        assert explicitLockVer != null;

        GridCacheMvccCandidate cand = entry.candidate(explicitLockVer);

        if (cand != null) {
            Collection<ClusterNode> dhtNodes = cand.mappedDhtNodes();

            if (log.isDebugEnabled())
                log.debug("Mapping explicit lock to DHT nodes [nodes=" + U.nodeIds(dhtNodes) + ", entry=" + entry + ']');

            Collection<ClusterNode> nearNodes = cand.mappedNearNodes();

            map(entry, dhtNodes, dhtMap);

            if (nearNodes != null && !nearNodes.isEmpty())
                map(entry, nearNodes, nearMap);
        }
    }

    /**
     * @param entry Entry.
     * @param nodes Nodes.
     * @param map Map.
     */
    private void map(GridDhtCacheEntry entry, Iterable<ClusterNode> nodes,
        Map<ClusterNode, List<GridDhtCacheEntry>> map) {
        if (nodes != null) {
            for (ClusterNode n : nodes) {
                List<GridDhtCacheEntry> entries = map.get(n);

                if (entries == null)
                    map.put(n, entries = new LinkedList<>());

                entries.add(entry);
            }
        }
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

        GridCacheVersionConflictContext<K, V> ctx = conflictRslvr.resolve(cacheObjCtx, oldEntry, newEntry,
            atomicVerComp);

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
        assert !entry.lockedByCurrentThread() : entry;
        assert ver != null;
        assert deferredDelete() : cache;

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
     * @return Binary processor.
     */
    public IgniteCacheObjectProcessor cacheObjects() {
        return kernalContext().cacheObjects();
    }

    /**
     * @return {@code True} if {@link BinaryMarshaller is configured}.
     */
    public boolean binaryMarshaller() {
        return marshaller() instanceof BinaryMarshaller;
    }

    /**
     * @return Keep binary flag.
     */
    public boolean keepBinary() {
        CacheOperationContext opCtx = operationContextPerCall();

        return opCtx != null && opCtx.isKeepBinary();
    }

    /**
     * @return {@code True} if the value for the cache object has to be copied because
     * of {@link CacheConfiguration#isCopyOnRead()}.
     */
    public boolean needValueCopy() {
        return affNode && config().isCopyOnRead();
    }

    /**
     * Converts temporary offheap object to heap-based.
     *
     * @param obj Object.
     * @return Heap-based object.
     */
    @Nullable public <T> T unwrapTemporary(@Nullable Object obj) {
        return (T)cacheObjects().unwrapTemporary(this, obj);
    }

    /**
     * Unwraps collection.
     *
     * @param col Collection to unwrap.
     * @param keepBinary Keep binary flag.
     * @return Unwrapped collection.
     */
    public Collection<Object> unwrapBinariesIfNeeded(Collection<Object> col, boolean keepBinary) {
        return CacheObjectUtils.unwrapBinariesIfNeeded(cacheObjCtx, col, keepBinary);
    }

    /**
     * Unwraps object for binary.
     *
     * @param o Object to unwrap.
     * @param keepBinary Keep binary flag.
     * @return Unwrapped object.
     */
    public Object unwrapBinaryIfNeeded(Object o, boolean keepBinary) {
        return unwrapBinaryIfNeeded(o, keepBinary, true);
    }

    /**
     * Unwraps object for binary.
     *
     * @param o Object to unwrap.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy value flag.
     * @return Unwrapped object.
     */
    public Object unwrapBinaryIfNeeded(Object o, boolean keepBinary, boolean cpy) {
        return cacheObjCtx.unwrapBinaryIfNeeded(o, keepBinary, cpy);
    }

    /**
     * @param resMap Invoke results map.
     * @param keepBinary Keep binary flag.
     * @return Unwrapped results.
     */
    public Map unwrapInvokeResult(@Nullable Map<Object, EntryProcessorResult> resMap, final boolean keepBinary) {
        return F.viewReadOnly(resMap, new C1<EntryProcessorResult, EntryProcessorResult>() {
            @Override public EntryProcessorResult apply(EntryProcessorResult res) {
                if (res instanceof CacheInvokeResult) {
                    CacheInvokeResult invokeRes = (CacheInvokeResult)res;

                    if (invokeRes.result() != null)
                        res = CacheInvokeResult.fromResult(unwrapBinaryIfNeeded(invokeRes.result(),
                            keepBinary, false));
                }

                return res;
            }
        });
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
        assert validObjectForCache(obj) : obj;

        return cacheObjects().toCacheObject(cacheObjCtx, obj, true, grp.isTopologyLocked());
    }

    /**
     * @param obj Object.
     * @return Cache key object.
     */
    public KeyCacheObject toCacheKeyObject(Object obj) {
        assert validObjectForCache(obj) : obj;

        return cacheObjects().toCacheKeyObject(cacheObjCtx, this, obj, true);
    }

    /**
     * @param obj Object.
     * @return {@code False} if objects is not expected for cache.
     */
    private boolean validObjectForCache(Object obj) {
        return obj == null ||
            !CU.isUtilityCache(cacheName) ||
            ctx.marshallerContext().isSystemType(obj.getClass().getName());
    }

    /**
     * @param bytes Bytes.
     * @return Cache key object.
     * @throws IgniteCheckedException If failed.
     */
    public KeyCacheObject toCacheKeyObject(byte[] bytes) throws IgniteCheckedException {
        Object obj = ctx.cacheObjects().unmarshal(cacheObjCtx, bytes, deploy().localLoader());

        return cacheObjects().toCacheKeyObject(cacheObjCtx, this, obj, false);
    }

    /**
     * Performs validation of provided key and value against configured constraints.
     *
     * @param key Key.
     * @param val Value.
     * @throws IgniteCheckedException, If validation fails.
     */
    public void validateKeyAndValue(KeyCacheObject key, CacheObject val) throws IgniteCheckedException {
        // No validation for removal.
        if (val == null)
            return;

        if (!isQueryEnabled())
            return;

        try {
            ctx.query().validateKeyAndValue(cacheObjCtx, key, val);
        }
        catch (RuntimeException e) {
            throw U.cast(e);
        }
    }

    /**
     * @param map Map.
     * @param key Key.
     * @param val Value.
     * @param skipVals Skip values flag.
     * @param keepCacheObjects Keep cache objects flag.
     * @param deserializeBinary Deserialize binary flag.
     * @param cpy Copy flag.
     * @param ver GridCacheVersion.
     */
    public <K1, V1> void addResult(Map<K1, V1> map,
        KeyCacheObject key,
        CacheObject val,
        boolean skipVals,
        boolean keepCacheObjects,
        boolean deserializeBinary,
        boolean cpy,
        final GridCacheVersion ver,
        final long expireTime,
        final long ttl) {
        // Creates EntryGetResult
        addResult(map, key, val, skipVals, keepCacheObjects, deserializeBinary, cpy, null,
            ver, expireTime, ttl, ver != null);
    }

    /**
     * @param map Map.
     * @param key Key.
     * @param getRes EntryGetResult.
     * @param skipVals Skip values.
     * @param keepCacheObjects Keep CacheObject.
     * @param deserializeBinary Deserialize binary flag.
     * @param cpy Copy flag.
     * @param needVer Need version flag.
     */
    public <K1, V1> void addResult(Map<K1, V1> map,
        KeyCacheObject key,
        EntryGetResult getRes,
        boolean skipVals,
        boolean keepCacheObjects,
        boolean deserializeBinary,
        boolean cpy,
        boolean needVer) {
        // Uses getRes as result.
        addResult(map, key, getRes.<CacheObject>value(), skipVals, keepCacheObjects, deserializeBinary, cpy, getRes,
            null, 0, 0, needVer);
    }

    /**
     * @param map Map.
     * @param key Key.
     * @param val Value.
     * @param skipVals Skip values.
     * @param keepCacheObjects Keep CacheObject.
     * @param deserializeBinary Deserialize binary.
     * @param cpy Copy flag.
     * @param getRes EntryGetResult.
     * @param ver Version.
     * @param expireTime Entry expire time.
     * @param ttl Entry TTL.
     * @param needVer Need version flag.
     */
    public <K1, V1> void addResult(Map<K1, V1> map,
        KeyCacheObject key,
        CacheObject val,
        boolean skipVals,
        boolean keepCacheObjects,
        boolean deserializeBinary,
        boolean cpy,
        @Nullable EntryGetResult getRes,
        final GridCacheVersion ver,
        final long expireTime,
        final long ttl,
        boolean needVer) {
        assert key != null;
        assert val != null || skipVals;

        if (!keepCacheObjects) {
            Object key0 = unwrapBinaryIfNeeded(key, !deserializeBinary, cpy);

            Object val0 = skipVals ? true : unwrapBinaryIfNeeded(val, !deserializeBinary, cpy);

            assert key0 != null : key;
            assert val0 != null : val;

            V1 v = createValue(ver, expireTime, ttl, val0, getRes, needVer);

            map.put((K1)key0, v);
        }
        else {
            Object val0 = skipVals ? true : val;

            V1 v = createValue(ver, expireTime, ttl, val0, getRes, needVer);

            map.put((K1)key, v);
        }
    }

    /**
     * Creates new EntryGetResult or uses existing one.
     *
     * @param ver Version.
     * @param expireTime Entry expire time.
     * @param ttl Entry TTL.
     * @param val Value.
     * @param getRes EntryGetResult
     * @param needVer Need version flag.
     * @return EntryGetResult or value.
     */
    private <V1> V1 createValue(final GridCacheVersion ver,
        final long expireTime,
        final long ttl,
        final Object val,
        @Nullable final EntryGetResult getRes,
        final boolean needVer) {
        final V1 v;

        if (!needVer)
            v = (V1) val;
        else if (getRes == null) {
            v = expireTime != 0 || ttl != 0
                ? (V1)new EntryGetWithTtlResult(val, ver, false, expireTime, ttl)
                : (V1)new EntryGetResult(val, ver, false);
        }
        else {
            getRes.value(val);

            v = (V1)getRes;
        }

        return v;
    }

    /**
     * @return Updates allowed.
     */
    public boolean updatesAllowed() {
        return updatesAllowed;
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

        if (expiryPlc instanceof Closeable)
            U.closeQuiet((Closeable)expiryPlc);

        mgrs.clear();
    }

    /**
     * Print memory statistics of all cache managers.
     *
     * NOTE: this method is for testing and profiling purposes only.
     */
    public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Cache memory stats [igniteInstanceName=" + ctx.igniteInstanceName() +
            ", cache=" + name() + ']');

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
     * @return Read-only collection of KeyCacheObject instances.
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

    /**
     * Checks if local reads are allowed for the given partition and reserves the partition when needed. If this
     * method returns {@code true}, then {@link #releaseForFastLocalGet(int, AffinityTopologyVersion)} method
     * must be called after the read is completed.
     *
     * @param part Partition.
     * @param topVer Topology version.
     * @return {@code True} if cache 'get' operation is allowed to get entry locally.
     */
    public boolean reserveForFastLocalGet(int part, AffinityTopologyVersion topVer) {
        boolean result = affinityNode() && rebalanceEnabled() && checkAndReservePartition(part, topVer);

        // When persistence is enabled, only reading from partitions with OWNING state is allowed.
        assert !result || !group().persistenceEnabled() ||
            topology().partitionState(localNodeId(), part) == OWNING :
            "result=" + result + ", persistenceEnabled=" + group().persistenceEnabled() +
                ", partitionState=" + topology().partitionState(localNodeId(), part) +
                ", replicated=" + isReplicated() + ", part=" + part;

        return result;
    }

    /**
     * Releases the partition that was reserved by a call to
     * {@link #reserveForFastLocalGet(int, AffinityTopologyVersion)}.
     *
     * @param part Partition to release.
     * @param topVer Topology version.
     */
    public void releaseForFastLocalGet(int part, AffinityTopologyVersion topVer) {
        assert affinityNode();

        if (!isReplicated() || group().persistenceEnabled()) {
            GridDhtLocalPartition locPart = topology().localPartition(part, topVer, false);

            assert locPart != null && locPart.state() == OWNING : "partition evicted after reserveForFastLocalGet " +
                "[part=" + part + ", locPart=" + locPart + ", topVer=" + topVer + ']';

            locPart.release();
        }
    }

    /**
     * Checks if it is possible to directly read data memory without entry creation (this
     * is optimization to avoid unnecessary blocking synchronization on cache entry).
     *
     * @param expiryPlc Optional expiry policy for read operation.
     * @param readers {@code True} if need update near cache readers.
     * @return {@code True} if it is possible to directly read offheap instead of using {@link GridCacheEntryEx#innerGet}.
     */
    public boolean readNoEntry(@Nullable IgniteCacheExpiryPolicy expiryPlc, boolean readers) {
        return mvccEnabled() || (!config().isOnheapCacheEnabled() && !readers && expiryPlc == null);
    }

    /**
     * @return {@code True} if mvcc is enabled for cache.
     */
    public boolean mvccEnabled() {
        return grp.mvccEnabled();
    }

    /**
     * @param part Partition.
     * @param topVer Topology version.
     * @return {@code True} if partition is available locally.
     */
    private boolean checkAndReservePartition(int part, AffinityTopologyVersion topVer) {
        assert affinityNode();

        GridDhtPartitionTopology top = topology();

        if (isReplicated() && !group().persistenceEnabled()) {
            boolean rebFinished = top.rebalanceFinished(topVer);

            if (rebFinished)
                return true;

            GridDhtLocalPartition locPart = top.localPartition(part, topVer, false, false);

            // No need to reserve a partition for REPLICATED cache because this partition cannot be evicted.
            return locPart != null && locPart.state() == OWNING;
        }
        else {
            GridDhtLocalPartition locPart = top.localPartition(part, topVer, false, false);

            if (locPart != null && locPart.reserve()) {
                boolean canRead = true;

                try {
                    canRead = locPart.state() == OWNING;

                    return canRead;
                }
                finally {
                    if (!canRead)
                        locPart.release();
                }
            }
            else
                return false;
        }
    }

    /**
     * @param type Event type.
     * @return {@code True} if event should be recorded.
     */
    public boolean recordEvent(int type) {
        if (isReplicated()) {
            if (type == EVT_CACHE_REBALANCE_STARTED) {
                if (!rebalanceStartedEvtSent) {
                    rebalanceStartedEvtSent = true;

                    return true;
                }
                else
                    return false;
            }
            else if (type == EVT_CACHE_REBALANCE_STOPPED) {
                if (!rebalanceStoppedEvtSent) {
                    rebalanceStoppedEvtSent = true;

                    return true;
                }
                else
                    return false;
            }
        }

        return true;
    }

    /**
     * Determines an affinity node to send get request to.
     *
     * @param affNodes All affinity nodes.
     * @param canRemap Flag indicating that 'get' should be done on a locked topology version.
     * @param partId Partition ID.
     * @param forcePrimary Force primary flag.
     * @return Affinity node to get key from or {@code null} if there is no suitable alive node.
     */
    @Nullable public ClusterNode selectAffinityNodeBalanced(
        List<ClusterNode> affNodes,
        Set<ClusterNode> invalidNodes,
        int partId,
        boolean canRemap,
        boolean forcePrimary
    ) {
        if (!readLoadBalancingEnabled) {
            if (!canRemap && !forcePrimary) {
                // Find next available node if we can not wait next topology version.
                for (ClusterNode node : affNodes) {
                    if (ctx.discovery().alive(node) && !invalidNodes.contains(node))
                        return node;
                }

                return null;
            }
            else {
                ClusterNode first = affNodes.get(0);

                return !invalidNodes.contains(first) ? first : null;
            }
        }

        if (!readFromBackup || forcePrimary) {
            ClusterNode first = affNodes.get(0);

            return !invalidNodes.contains(first) ? first : null;
        }

        assert locMacs != null;

        int r = ThreadLocalRandom.current().nextInt(affNodes.size());

        ClusterNode n0 = null;

        for (ClusterNode node : affNodes) {
            if ((canRemap || discovery().alive(node)) && !invalidNodes.contains(node)) {
                if (locMacs.equals(node.attribute(ATTR_MACS)))
                    return node;

                if (r >= 0 || n0 == null)
                    n0 = node;
            }

            r--;
        }

        return n0;
    }

    /**
     * Prepare affinity field for builder (if possible).
     *
     * @param builder Builder.
     */
    public void prepareAffinityField(BinaryObjectBuilder builder) {
        assert binaryMarshaller();
        assert builder instanceof BinaryObjectBuilderImpl;

        BinaryObjectBuilderImpl builder0 = (BinaryObjectBuilderImpl)builder;

        if (!cacheObjCtx.customAffinityMapper()) {
            CacheDefaultBinaryAffinityKeyMapper mapper =
                (CacheDefaultBinaryAffinityKeyMapper)cacheObjCtx.defaultAffMapper();

            BinaryField field = mapper.affinityKeyField(builder0.typeId());

            if (field != null) {
                String fieldName = field.name();

                builder0.affinityFieldName(fieldName);
            }
        }
    }

    /**
     * @return Statistics enabled flag.
     */
    public boolean statisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * @param statisticsEnabled Statistics enabled flag.
     */
    public void statisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;

        if (isNear())
            near().dht().context().statisticsEnabled = statisticsEnabled;
    }

    /**
     * @param tx Transaction.
     * @return {@code True} if it is need to notify continuous query listeners.
     */
    public boolean hasContinuousQueryListeners(@Nullable IgniteInternalTx tx) {
        return grp.sharedGroup() ? grp.hasContinuousQueryCaches() :
            contQryMgr.notifyContinuousQueries(tx) && !F.isEmpty(contQryMgr.updateListeners(false, false));
    }

    /**
     * Apply changes from {@link SchemaAddQueryEntityOperation}.
     *
     * @param op Add query entity schema operation.
     */
    public void onSchemaAddQueryEntity(SchemaAddQueryEntityOperation op) {
        onSchemaAddQueryEntity(op.entities(), op.schemaName(), op.isSqlEscape(),
                op.queryParallelism());
    }

    /**
     * Apply changes on enable indexing.
     *
     * @param entities New query entities.
     * @param sqlSchema Sql schema name.
     * @param isSqlEscape Sql escape flag.
     * @param qryParallelism Query parallelism parameter.
     */
    public void onSchemaAddQueryEntity(
            Collection<QueryEntity> entities,
            String sqlSchema,
            boolean isSqlEscape,
            int qryParallelism
    ) {
        CacheConfiguration oldCfg = cacheCfg;

        if (oldCfg != null)
            cacheCfg = GridCacheUtils.patchCacheConfiguration(oldCfg, entities, sqlSchema, isSqlEscape, qryParallelism);

        if (qryMgr != null)
            qryMgr.enable();
    }

    /**
     * Returns future that assigned to last performing {@link GlobalRemoveAllJob}.
     */
    public AtomicReference<IgniteInternalFuture<Boolean>> lastRemoveAllJobFut() {
        return lastRmvAllJobFut;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, igniteInstanceName());
        U.writeString(out, name());
    }

    /** {@inheritDoc} */
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

            IgniteKernal grid = IgnitionEx.localIgnite();

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
