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
import org.apache.ignite.cache.affinity.fair.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.cache.datastructures.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.jta.*;
import org.apache.ignite.internal.processors.cache.local.*;
import org.apache.ignite.internal.processors.cache.local.atomic.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.cache.query.continuous.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.spi.*;
import org.jetbrains.annotations.*;

import javax.cache.configuration.*;
import javax.cache.integration.*;
import javax.management.*;
import java.util.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.configuration.CacheConfiguration.*;
import static org.apache.ignite.configuration.DeploymentMode.*;
import static org.apache.ignite.internal.IgniteComponentType.*;
import static org.apache.ignite.internal.IgniteNodeAttributes.*;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Cache processor.
 */
public class GridCacheProcessor extends GridProcessorAdapter {
    /** Shared cache context. */
    private GridCacheSharedContext<?, ?> sharedCtx;

    /** */
    private final Map<String, GridCacheAdapter<?, ?>> caches;

    /** Map of proxies. */
    private final Map<String, GridCache<?, ?>> proxies;

    /** Map of proxies. */
    private final Map<String, IgniteCacheProxy<?, ?>> jCacheProxies;

    /** Map of public proxies, i.e. proxies which could be returned to the user. */
    private final Map<String, GridCache<?, ?>> publicProxies;

    /** Map of preload finish futures grouped by preload order. */
    private final NavigableMap<Integer, IgniteInternalFuture<?>> preloadFuts;

    /** Maximum detected rebalance order. */
    private int maxRebalanceOrder;

    /** System cache names. */
    private final Set<String> sysCaches;

    /** Caches stop sequence. */
    private final Deque<GridCacheAdapter<?, ?>> stopSeq;

    /** Transaction interface implementation. */
    private IgniteTransactionsImpl transactions;

    /**
     * @param ctx Kernal context.
     */
    public GridCacheProcessor(GridKernalContext ctx) {
        super(ctx);

        caches = new LinkedHashMap<>();
        proxies = new HashMap<>();
        publicProxies = new HashMap<>();
        jCacheProxies = new HashMap<>();
        preloadFuts = new TreeMap<>();

        sysCaches = new HashSet<>();
        stopSeq = new LinkedList<>();
    }

    /**
     * @param cfg Initializes cache configuration with proper defaults.
     * @throws IgniteCheckedException If configuration is not valid.
     */
    @SuppressWarnings("unchecked")
    private void initialize(CacheConfiguration cfg, CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        if (cfg.getCacheMode() == null)
            cfg.setCacheMode(DFLT_CACHE_MODE);

        if (cfg.getMemoryMode() == null)
            cfg.setMemoryMode(DFLT_MEMORY_MODE);

        if (cfg.getAffinity() == null) {
            if (cfg.getCacheMode() == PARTITIONED) {
                CacheRendezvousAffinityFunction aff = new CacheRendezvousAffinityFunction();

                aff.setHashIdResolver(new CacheAffinityNodeAddressHashResolver());

                cfg.setAffinity(aff);
            }
            else if (cfg.getCacheMode() == REPLICATED) {
                CacheRendezvousAffinityFunction aff = new CacheRendezvousAffinityFunction(false, 512);

                aff.setHashIdResolver(new CacheAffinityNodeAddressHashResolver());

                cfg.setAffinity(aff);

                cfg.setBackups(Integer.MAX_VALUE);
            }
            else
                cfg.setAffinity(new LocalAffinityFunction());
        }
        else {
            if (cfg.getCacheMode() == PARTITIONED) {
                if (cfg.getAffinity() instanceof CacheRendezvousAffinityFunction) {
                    CacheRendezvousAffinityFunction aff = (CacheRendezvousAffinityFunction)cfg.getAffinity();

                    if (aff.getHashIdResolver() == null)
                        aff.setHashIdResolver(new CacheAffinityNodeAddressHashResolver());
                }
            }
        }

        if (cfg.getCacheMode() == REPLICATED)
            cfg.setBackups(Integer.MAX_VALUE);

        if (cfg.getAffinityMapper() == null)
            cfg.setAffinityMapper(cacheObjCtx.defaultAffMapper());

        ctx.igfsHelper().preProcessCacheConfiguration(cfg);

        if (cfg.getRebalanceMode() == null)
            cfg.setRebalanceMode(ASYNC);

        if (cfg.getAtomicityMode() == null)
            cfg.setAtomicityMode(ATOMIC);

        if (cfg.getCacheMode() == PARTITIONED || cfg.getCacheMode() == REPLICATED) {
            if (cfg.getDistributionMode() == null)
                cfg.setDistributionMode(PARTITIONED_ONLY);

            if (cfg.getDistributionMode() == PARTITIONED_ONLY ||
                cfg.getDistributionMode() == CLIENT_ONLY) {
                if (cfg.getNearEvictionPolicy() != null)
                    U.quietAndWarn(log, "Ignoring near eviction policy since near cache is disabled.");
            }

            assert cfg.getDistributionMode() != null;
        }
        else
            assert cfg.getCacheMode() == LOCAL;

        if (cfg.getWriteSynchronizationMode() == null)
            cfg.setWriteSynchronizationMode(PRIMARY_SYNC);

        assert cfg.getWriteSynchronizationMode() != null;

        if (cfg.getAtomicityMode() == ATOMIC) {
            if (cfg.getAtomicWriteOrderMode() == null) {
                cfg.setAtomicWriteOrderMode(cfg.getWriteSynchronizationMode() == FULL_SYNC ?
                    CacheAtomicWriteOrderMode.CLOCK :
                    CacheAtomicWriteOrderMode.PRIMARY);
            }
            else if (cfg.getWriteSynchronizationMode() != FULL_SYNC &&
                cfg.getAtomicWriteOrderMode() == CacheAtomicWriteOrderMode.CLOCK) {
                cfg.setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.PRIMARY);

                U.warn(log, "Automatically set write order mode to PRIMARY for better performance " +
                    "[writeSynchronizationMode=" + cfg.getWriteSynchronizationMode() + ", " +
                    "cacheName=" + cfg.getName() + ']');
            }
        }

        if (cfg.getCacheStoreFactory() == null) {
            Factory<CacheLoader> ldrFactory = cfg.getCacheLoaderFactory();

            CacheLoader ldr = null;

            if (ldrFactory != null)
                ldr = ldrFactory.create();

            Factory<CacheWriter> writerFactory = cfg.getCacheWriterFactory();

            CacheWriter writer = null;

            if (cfg.isWriteThrough() && writerFactory != null)
                writer = writerFactory.create();

            if (ldr != null || writer != null) {
                final GridCacheLoaderWriterStore store = new GridCacheLoaderWriterStore(ldr, writer);

                cfg.setCacheStoreFactory(new Factory<CacheStore<? super Object, ? super Object>>() {
                    @Override public CacheStore<? super Object, ? super Object> create() {
                        return store;
                    }
                });
            }
        }
        else {
            if (cfg.getCacheLoaderFactory() != null)
                throw new IgniteCheckedException("Cannot set both cache loaded factory and cache store factory " +
                    "for cache: " + cfg.getName());

            if (cfg.getCacheWriterFactory() != null)
                throw new IgniteCheckedException("Cannot set both cache writer factory and cache store factory " +
                    "for cache: " + cfg.getName());
        }
    }

    /**
     * @param cfg Configuration to check for possible performance issues.
     * @param hasStore {@code True} if store is configured.
     */
    private void suggestOptimizations(CacheConfiguration cfg, boolean hasStore) {
        GridPerformanceSuggestions perf = ctx.performance();

        String msg = "Disable eviction policy (remove from configuration)";

        if (cfg.getEvictionPolicy() != null) {
            perf.add(msg, false);

            perf.add("Disable synchronized evictions (set 'evictSynchronized' to false)", !cfg.isEvictSynchronized());
        }
        else
            perf.add(msg, true);

        if (cfg.getCacheMode() == PARTITIONED) {
            perf.add("Disable near cache (set 'partitionDistributionMode' to PARTITIONED_ONLY or CLIENT_ONLY)",
                cfg.getDistributionMode() != NEAR_PARTITIONED &&
                    cfg.getDistributionMode() != NEAR_ONLY);

            if (cfg.getAffinity() != null)
                perf.add("Decrease number of backups (set 'keyBackups' to 0)", cfg.getBackups() == 0);
        }

        // Suppress warning if at least one ATOMIC cache found.
        perf.add("Enable ATOMIC mode if not using transactions (set 'atomicityMode' to ATOMIC)",
            cfg.getAtomicityMode() == ATOMIC);

        // Suppress warning if at least one non-FULL_SYNC mode found.
        perf.add("Disable fully synchronous writes (set 'writeSynchronizationMode' to PRIMARY_SYNC or FULL_ASYNC)",
            cfg.getWriteSynchronizationMode() != FULL_SYNC);

        // Suppress warning if at least one swap is disabled.
        perf.add("Disable swap store (set 'swapEnabled' to false)", !cfg.isSwapEnabled());

        if (hasStore && cfg.isWriteThrough())
            perf.add("Enable write-behind to persistent store (set 'writeBehindEnabled' to true)",
                cfg.isWriteBehindEnabled());

        perf.add("Disable query index (set 'queryIndexEnabled' to false)", !cfg.isQueryIndexEnabled());
    }

    /**
     * @param c Grid configuration.
     * @param cc Configuration to validate.
     * @param cfgStore Cache store.
     * @throws IgniteCheckedException If failed.
     */
    private void validate(IgniteConfiguration c,
        CacheConfiguration cc,
        @Nullable CacheStore cfgStore) throws IgniteCheckedException {
        if (cc.getCacheMode() == REPLICATED) {
            if (cc.getAffinity() instanceof CachePartitionFairAffinity)
                throw new IgniteCheckedException("REPLICATED cache can not be started with CachePartitionFairAffinity" +
                    " [cacheName=" + cc.getName() + ']');

            if (cc.getAffinity() instanceof CacheRendezvousAffinityFunction) {
                CacheRendezvousAffinityFunction aff = (CacheRendezvousAffinityFunction)cc.getAffinity();

                if (aff.isExcludeNeighbors())
                    throw new IgniteCheckedException("For REPLICATED cache flag 'excludeNeighbors' in " +
                        "CacheRendezvousAffinityFunction cannot be set [cacheName=" + cc.getName() + ']');
            }

            if (cc.getDistributionMode() == NEAR_PARTITIONED) {
                U.warn(log, "NEAR_PARTITIONED distribution mode cannot be used with REPLICATED cache, " +
                    "will be changed to PARTITIONED_ONLY [cacheName=" + cc.getName() + ']');

                cc.setDistributionMode(PARTITIONED_ONLY);
            }
        }

        if (cc.getCacheMode() == LOCAL && !cc.getAffinity().getClass().equals(LocalAffinityFunction.class))
            U.warn(log, "CacheAffinityFunction configuration parameter will be ignored for local cache [cacheName=" +
                cc.getName() + ']');

        if (cc.getRebalanceMode() != CacheRebalanceMode.NONE) {
            assertParameter(cc.getRebalanceThreadPoolSize() > 0, "rebalanceThreadPoolSize > 0");
            assertParameter(cc.getRebalanceBatchSize() > 0, "rebalanceBatchSize > 0");
        }

        if (cc.getCacheMode() == PARTITIONED || cc.getCacheMode() == REPLICATED) {
            if (cc.getAtomicityMode() == ATOMIC && cc.getWriteSynchronizationMode() == FULL_ASYNC)
                U.warn(log, "Cache write synchronization mode is set to FULL_ASYNC. All single-key 'put' and " +
                    "'remove' operations will return 'null', all 'putx' and 'removex' operations will return" +
                    " 'true' [cacheName=" + cc.getName() + ']');
        }

        DeploymentMode depMode = c.getDeploymentMode();

        if (c.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED) &&
            !CU.isSystemCache(cc.getName()))
            throw new IgniteCheckedException("Cannot start cache in PRIVATE or ISOLATED deployment mode: " +
                ctx.config().getDeploymentMode());

        if (!c.getTransactionConfiguration().isTxSerializableEnabled() &&
            c.getTransactionConfiguration().getDefaultTxIsolation() == SERIALIZABLE)
            U.warn(log,
                "Serializable transactions are disabled while default transaction isolation is SERIALIZABLE " +
                    "(most likely misconfiguration - either update 'isTxSerializableEnabled' or " +
                    "'defaultTxIsolationLevel' properties) for cache: " + cc.getName(),
                "Serializable transactions are disabled while default transaction isolation is SERIALIZABLE " +
                    "for cache: " + cc.getName());

        if (cc.isWriteBehindEnabled()) {
            if (cfgStore == null)
                throw new IgniteCheckedException("Cannot enable write-behind (writer or store is not provided) " +
                    "for cache: " + cc.getName());

            assertParameter(cc.getWriteBehindBatchSize() > 0, "writeBehindBatchSize > 0");
            assertParameter(cc.getWriteBehindFlushSize() >= 0, "writeBehindFlushSize >= 0");
            assertParameter(cc.getWriteBehindFlushFrequency() >= 0, "writeBehindFlushFrequency >= 0");
            assertParameter(cc.getWriteBehindFlushThreadCount() > 0, "writeBehindFlushThreadCount > 0");

            if (cc.getWriteBehindFlushSize() == 0 && cc.getWriteBehindFlushFrequency() == 0)
                throw new IgniteCheckedException("Cannot set both 'writeBehindFlushFrequency' and " +
                    "'writeBehindFlushSize' parameters to 0 for cache: " + cc.getName());
        }

        if (cc.isReadThrough() && cfgStore == null)
            throw new IgniteCheckedException("Cannot enable read-through (loader or store is not provided) " +
                "for cache: " + cc.getName());

        if (cc.isWriteThrough() && cfgStore == null)
            throw new IgniteCheckedException("Cannot enable write-through (writer or store is not provided) " +
                "for cache: " + cc.getName());

        long delay = cc.getRebalanceDelay();

        if (delay != 0) {
            if (cc.getCacheMode() != PARTITIONED)
                U.warn(log, "Rebalance delay is supported only for partitioned caches (will ignore): " + cc.getName(),
                    "Will ignore rebalance delay for cache: " + cc.getName());
            else if (cc.getRebalanceMode() == SYNC) {
                if (delay < 0) {
                    U.warn(log, "Ignoring SYNC rebalance mode with manual rebalance start (node will not wait for " +
                        "rebalancing to be finished): " + cc.getName(),
                        "Node will not wait for rebalance in SYNC mode: " + cc.getName());
                }
                else {
                    U.warn(log,
                        "Using SYNC rebalance mode with rebalance delay (node will wait until rebalancing is " +
                            "initiated for " + delay + "ms) for cache: " + cc.getName(),
                        "Node will wait until rebalancing is initiated for " + delay + "ms for cache: " + cc.getName());
                }
            }
        }

        ctx.igfsHelper().validateCacheConfiguration(cc);

        switch (cc.getMemoryMode()) {
            case OFFHEAP_VALUES: {
                if (cc.getOffHeapMaxMemory() < 0)
                    cc.setOffHeapMaxMemory(0); // Set to unlimited.

                break;
            }

            case OFFHEAP_TIERED: {
                if (cc.getOffHeapMaxMemory() < 0)
                    cc.setOffHeapMaxMemory(0); // Set to unlimited.

                break;
            }

            case ONHEAP_TIERED:
                if (!systemCache(cc.getName()) && cc.getEvictionPolicy() == null && cc.getOffHeapMaxMemory() >= 0)
                    U.quietAndWarn(log, "Eviction policy not enabled with ONHEAP_TIERED mode for cache " +
                        "(entries will not be moved to off-heap store): " + cc.getName());

                break;

            default:
                throw new IllegalStateException("Unknown memory mode: " + cc.getMemoryMode());
        }

        if (cc.getMemoryMode() == CacheMemoryMode.OFFHEAP_VALUES) {
            if (cc.isQueryIndexEnabled())
                throw new IgniteCheckedException("Cannot have query indexing enabled while values are stored off-heap. " +
                    "You must either disable query indexing or disable off-heap values only flag for cache: " +
                    cc.getName());
        }

        boolean igfsCache = CU.isIgfsCache(c, cc.getName());
        boolean utilityCache = CU.isUtilityCache(cc.getName());

        if (!igfsCache && !utilityCache && !cc.isQueryIndexEnabled())
            U.warn(log, "Query indexing is disabled (queries will not work) for cache: '" + cc.getName() + "'. " +
                "To enable change GridCacheConfiguration.isQueryIndexEnabled() property.",
                "Query indexing is disabled (queries will not work) for cache: " + cc.getName());

        if (cc.getAtomicityMode() == ATOMIC)
            assertParameter(cc.getTransactionManagerLookupClassName() == null,
                "transaction manager can not be used with ATOMIC cache");
    }

    /**
     * @param ctx Context.
     * @return DHT managers.
     */
    private List<GridCacheManager> dhtManagers(GridCacheContext ctx) {
        return F.asList(ctx.store(), ctx.events(), ctx.swap(), ctx.evicts(), ctx.queries(), ctx.continuousQueries(),
            ctx.dr());
    }

    /**
     * @param ctx Context.
     * @return Managers present in both, DHT and Near caches.
     */
    @SuppressWarnings("IfMayBeConditional")
    private Collection<GridCacheManager> dhtExcludes(GridCacheContext ctx) {
        if (ctx.config().getCacheMode() == LOCAL || !isNearEnabled(ctx))
            return Collections.emptyList();
        else
            return F.asList((GridCacheManager)ctx.queries(), ctx.continuousQueries(), ctx.store());
    }

    /**
     * @param cfg Configuration.
     * @param objs Extra components.
     * @throws IgniteCheckedException If failed to inject.
     */
    private void prepare(CacheConfiguration cfg, Object... objs) throws IgniteCheckedException {
        prepare(cfg, cfg.getEvictionPolicy(), false);
        prepare(cfg, cfg.getNearEvictionPolicy(), true);
        prepare(cfg, cfg.getAffinity(), false);
        prepare(cfg, cfg.getAffinityMapper(), false);
        prepare(cfg, cfg.getEvictionFilter(), false);
        prepare(cfg, cfg.getInterceptor(), false);

        for (Object obj : objs)
            prepare(cfg, obj, false);
    }

    /**
     * @param cfg Cache configuration.
     * @param rsrc Resource.
     * @param near Near flag.
     * @throws IgniteCheckedException If failed.
     */
    private void prepare(CacheConfiguration cfg, @Nullable Object rsrc, boolean near) throws IgniteCheckedException {
        if (rsrc != null) {
            ctx.resource().injectGeneric(rsrc);

            ctx.resource().injectCacheName(rsrc, cfg.getName());

            registerMbean(rsrc, cfg.getName(), near);
        }
    }

    /**
     * @param cctx Cache context.
     */
    private void cleanup(GridCacheContext cctx) {
        CacheConfiguration cfg = cctx.config();

        cleanup(cfg, cfg.getEvictionPolicy(), false);
        cleanup(cfg, cfg.getNearEvictionPolicy(), true);
        cleanup(cfg, cfg.getAffinity(), false);
        cleanup(cfg, cfg.getAffinityMapper(), false);
        cleanup(cfg, cctx.jta().tmLookup(), false);
        cleanup(cfg, cctx.store().configuredStore(), false);

        cctx.cleanup();
    }

    /**
     * @param cfg Cache configuration.
     * @param rsrc Resource.
     * @param near Near flag.
     */
    private void cleanup(CacheConfiguration cfg, @Nullable Object rsrc, boolean near) {
        if (rsrc != null) {
            unregisterMbean(rsrc, cfg.getName(), near);

            try {
                ctx.resource().cleanupGeneric(rsrc);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to cleanup resource: " + rsrc, e);
            }
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked"})
    @Override public void start() throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        DeploymentMode depMode = ctx.config().getDeploymentMode();

        if (!F.isEmpty(ctx.config().getCacheConfiguration())) {
            if (depMode != CONTINUOUS && depMode != SHARED)
                U.warn(log, "Deployment mode for cache is not CONTINUOUS or SHARED " +
                    "(it is recommended that you change deployment mode and restart): " + depMode,
                    "Deployment mode for cache is not CONTINUOUS or SHARED.");
        }

        maxRebalanceOrder = validatePreloadOrder(ctx.config().getCacheConfiguration());

        // Internal caches which should not be returned to user.
        FileSystemConfiguration[] igfsCfgs = ctx.grid().configuration().getFileSystemConfiguration();

        if (igfsCfgs != null) {
            for (FileSystemConfiguration igfsCfg : igfsCfgs) {
                sysCaches.add(igfsCfg.getMetaCacheName());
                sysCaches.add(igfsCfg.getDataCacheName());
            }
        }

        if (IgniteComponentType.HADOOP.inClassPath())
            sysCaches.add(CU.SYS_CACHE_HADOOP_MR);

        sysCaches.add(CU.MARSH_CACHE_NAME);
        sysCaches.add(CU.UTILITY_CACHE_NAME);
        sysCaches.add(CU.ATOMICS_CACHE_NAME);

        CacheConfiguration[] cfgs = ctx.config().getCacheConfiguration();

        sharedCtx = createSharedContext(ctx);

        ctx.performance().add("Disable serializable transactions (set 'txSerializableEnabled' to false)",
            !ctx.config().getTransactionConfiguration().isTxSerializableEnabled());

        Collection<GridCacheAdapter<?, ?>> startSeq = new ArrayList<>(cfgs.length);

        IdentityHashMap<CacheStore, ThreadLocal> sesHolders = new IdentityHashMap<>();

        for (int i = 0; i < cfgs.length; i++) {
            CacheConfiguration<?, ?> cfg = new CacheConfiguration(cfgs[i]);

            CacheObjectContext cacheObjCtx = ctx.cacheObjects().contextForCache(null, cfg.getName());

            // Initialize defaults.
            initialize(cfg, cacheObjCtx);

            CacheStore cfgStore = cfg.getCacheStoreFactory() != null ? cfg.getCacheStoreFactory().create() : null;

            validate(ctx.config(), cfg, cfgStore);

            CacheJtaManagerAdapter jta = JTA.create(cfg.getTransactionManagerLookupClassName() == null);

            jta.createTmLookup(cfg);

            // Skip suggestions for system caches.
            if (!sysCaches.contains(cfg.getName()))
                suggestOptimizations(cfg, cfgStore != null);

            List<Object> toPrepare = new ArrayList<>();

            toPrepare.add(jta.tmLookup());
            toPrepare.add(cfgStore);
            toPrepare.add(cfg.getAffinityMapper());

            if (cfg.getAffinityMapper() != cacheObjCtx.defaultAffMapper())
                toPrepare.add(cacheObjCtx.defaultAffMapper());

            if (cfgStore instanceof GridCacheLoaderWriterStore) {
                toPrepare.add(((GridCacheLoaderWriterStore)cfgStore).loader());
                toPrepare.add(((GridCacheLoaderWriterStore)cfgStore).writer());
            }

            prepare(cfg, toPrepare.toArray(new Object[toPrepare.size()]));

            U.startLifecycleAware(lifecycleAwares(cfg, jta.tmLookup(), cfgStore));

            cfgs[i] = cfg; // Replace original configuration value.

            GridCacheAffinityManager affMgr = new GridCacheAffinityManager();
            GridCacheEventManager evtMgr = new GridCacheEventManager();
            GridCacheSwapManager swapMgr = new GridCacheSwapManager(cfg.getCacheMode() == LOCAL || !GridCacheUtils.isNearEnabled(cfg));
            GridCacheEvictionManager evictMgr = new GridCacheEvictionManager();
            GridCacheQueryManager qryMgr = queryManager(cfg);
            CacheContinuousQueryManager contQryMgr = new CacheContinuousQueryManager();
            CacheDataStructuresManager dataStructuresMgr = new CacheDataStructuresManager();
            GridCacheTtlManager ttlMgr = new GridCacheTtlManager();
            GridCacheDrManager drMgr = ctx.createComponent(GridCacheDrManager.class);

            GridCacheStoreManager storeMgr = new GridCacheStoreManager(ctx, sesHolders, cfgStore, cfg);

            GridCacheContext<?, ?> cacheCtx = new GridCacheContext(
                ctx,
                sharedCtx,
                cfg,

                /*
                 * Managers in starting order!
                 * ===========================
                 */
                evtMgr,
                swapMgr,
                storeMgr,
                evictMgr,
                qryMgr,
                contQryMgr,
                affMgr,
                dataStructuresMgr,
                ttlMgr,
                drMgr,
                jta);

            cacheCtx.cacheObjectContext(cacheObjCtx);

            GridCacheAdapter cache = null;

            switch (cfg.getCacheMode()) {
                case LOCAL: {
                    switch (cfg.getAtomicityMode()) {
                        case TRANSACTIONAL: {
                            cache = new GridLocalCache(cacheCtx);

                            break;
                        }
                        case ATOMIC: {
                            cache = new GridLocalAtomicCache(cacheCtx);

                            break;
                        }

                        default: {
                            assert false : "Invalid cache atomicity mode: " + cfg.getAtomicityMode();
                        }
                    }

                    break;
                }
                case PARTITIONED:
                case REPLICATED: {
                    if (GridCacheUtils.isNearEnabled(cfg)) {
                        switch (cfg.getAtomicityMode()) {
                            case TRANSACTIONAL: {
                                cache = new GridNearTransactionalCache(cacheCtx);

                                break;
                            }
                            case ATOMIC: {
                                cache = new GridNearAtomicCache(cacheCtx);

                                break;
                            }

                            default: {
                                assert false : "Invalid cache atomicity mode: " + cfg.getAtomicityMode();
                            }
                        }
                    }
                    else {
                        switch (cfg.getAtomicityMode()) {
                            case TRANSACTIONAL: {
                                cache = GridCacheUtils.isAffinityNode(cfg) ? new GridDhtColocatedCache(cacheCtx) :
                                    new GridDhtColocatedCache(cacheCtx, new GridNoStorageCacheMap(cacheCtx));

                                break;
                            }
                            case ATOMIC: {
                                cache = GridCacheUtils.isAffinityNode(cfg) ? new GridDhtAtomicCache(cacheCtx) :
                                    new GridDhtAtomicCache(cacheCtx, new GridNoStorageCacheMap(cacheCtx));

                                break;
                            }

                            default: {
                                assert false : "Invalid cache atomicity mode: " + cfg.getAtomicityMode();
                            }
                        }
                    }

                    break;
                }

                default: {
                    assert false : "Invalid cache mode: " + cfg.getCacheMode();
                }
            }

            cacheCtx.cache(cache);

            if (caches.containsKey(cfg.getName())) {
                String cacheName = cfg.getName();

                if (cacheName != null)
                    throw new IgniteCheckedException("Duplicate cache name found (check configuration and " +
                        "assign unique name to each cache): " + cacheName);
                else
                    throw new IgniteCheckedException("Default cache has already been configured (check configuration and " +
                        "assign unique name to each cache).");
            }

            caches.put(cfg.getName(), cache);

            if (sysCaches.contains(cfg.getName()))
                stopSeq.addLast(cache);
            else
                stopSeq.addFirst(cache);

            startSeq.add(cache);

            /*
             * Create DHT cache.
             * ================
             */
            if (cfg.getCacheMode() != LOCAL && GridCacheUtils.isNearEnabled(cfg)) {
                /*
                 * Specifically don't create the following managers
                 * here and reuse the one from Near cache:
                 * 1. GridCacheVersionManager
                 * 2. GridCacheIoManager
                 * 3. GridCacheDeploymentManager
                 * 4. GridCacheQueryManager (note, that we start it for DHT cache though).
                 * 5. CacheContinuousQueryManager (note, that we start it for DHT cache though).
                 * 6. GridCacheDgcManager
                 * 7. GridCacheTtlManager.
                 * ===============================================
                 */
                swapMgr = new GridCacheSwapManager(true);
                evictMgr = new GridCacheEvictionManager();
                evtMgr = new GridCacheEventManager();
                drMgr = ctx.createComponent(GridCacheDrManager.class);

                cacheCtx = new GridCacheContext(
                    ctx,
                    sharedCtx,
                    cfg,

                    /*
                     * Managers in starting order!
                     * ===========================
                     */
                    evtMgr,
                    swapMgr,
                    storeMgr,
                    evictMgr,
                    qryMgr,
                    contQryMgr,
                    affMgr,
                    dataStructuresMgr,
                    ttlMgr,
                    drMgr,
                    jta);

                cacheCtx.cacheObjectContext(cacheObjCtx);

                GridDhtCacheAdapter dht = null;

                switch (cfg.getAtomicityMode()) {
                    case TRANSACTIONAL: {
                        assert cache instanceof GridNearTransactionalCache;

                        GridNearTransactionalCache near = (GridNearTransactionalCache)cache;

                        GridDhtCache dhtCache = !GridCacheUtils.isAffinityNode(cfg) ?
                            new GridDhtCache(cacheCtx, new GridNoStorageCacheMap(cacheCtx)) :
                            new GridDhtCache(cacheCtx);

                        dhtCache.near(near);

                        near.dht(dhtCache);

                        dht = dhtCache;

                        break;
                    }
                    case ATOMIC: {
                        assert cache instanceof GridNearAtomicCache;

                        GridNearAtomicCache near = (GridNearAtomicCache)cache;

                        GridDhtAtomicCache dhtCache = GridCacheUtils.isAffinityNode(cfg) ? new GridDhtAtomicCache(cacheCtx) :
                            new GridDhtAtomicCache(cacheCtx, new GridNoStorageCacheMap(cacheCtx));

                        dhtCache.near(near);

                        near.dht(dhtCache);

                        dht = dhtCache;

                        break;
                    }

                    default: {
                        assert false : "Invalid cache atomicity mode: " + cfg.getAtomicityMode();
                    }
                }

                cacheCtx.cache(dht);
            }

            sharedCtx.addCacheContext(cache.context());
        }

        // Start shared managers.
        for (GridCacheSharedManager mgr : sharedCtx.managers())
            mgr.start(sharedCtx);

        for (GridCacheAdapter<?, ?> cache : startSeq) {
            GridCacheContext<?, ?> cacheCtx = cache.context();

            CacheConfiguration cfg = cacheCtx.config();

            // Start managers.
            for (GridCacheManager mgr : F.view(cacheCtx.managers(), F.notContains(dhtExcludes(cacheCtx))))
                mgr.start(cacheCtx);

            cacheCtx.initConflictResolver();

            if (cfg.getCacheMode() != LOCAL && GridCacheUtils.isNearEnabled(cfg)) {
                GridCacheContext<?, ?> dhtCtx = cacheCtx.near().dht().context();

                // Start DHT managers.
                for (GridCacheManager mgr : dhtManagers(dhtCtx))
                    mgr.start(dhtCtx);

                dhtCtx.initConflictResolver();

                // Start DHT cache.
                dhtCtx.cache().start();

                if (log.isDebugEnabled())
                    log.debug("Started DHT cache: " + dhtCtx.cache().name());
            }

            cacheCtx.cache().start();

            if (log.isInfoEnabled())
                log.info("Started cache [name=" + cfg.getName() + ", mode=" + cfg.getCacheMode() + ']');
        }

        for (Map.Entry<String, GridCacheAdapter<?, ?>> e : caches.entrySet()) {
            GridCacheAdapter cache = e.getValue();

            proxies.put(e.getKey(), new GridCacheProxyImpl(cache.context(), cache, null));

            jCacheProxies.put(e.getKey(), new IgniteCacheProxy(cache.context(), cache, null, false));
        }

        // Internal caches which should not be returned to user.
        for (Map.Entry<String, GridCacheAdapter<?, ?>> e : caches.entrySet()) {
            GridCacheAdapter cache = e.getValue();

            if (!sysCaches.contains(e.getKey()))
                publicProxies.put(e.getKey(), new GridCacheProxyImpl(cache.context(), cache, null));
        }

        transactions = new IgniteTransactionsImpl(sharedCtx);

        if (!(ctx.isDaemon() || F.isEmpty(ctx.config().getCacheConfiguration()))) {
            GridCacheAttributes[] attrVals = new GridCacheAttributes[ctx.config().getCacheConfiguration().length];

            Map<String, String> interceptors = new HashMap<>();

            int i = 0;

            for (CacheConfiguration cfg : ctx.config().getCacheConfiguration()) {
                assert caches.containsKey(cfg.getName()) : cfg.getName();

                GridCacheContext ctx = caches.get(cfg.getName()).context();

                attrVals[i++] = new GridCacheAttributes(cfg, ctx.store().configuredStore());

                if (cfg.getInterceptor() != null)
                    interceptors.put(cfg.getName(), cfg.getInterceptor().getClass().getName());
            }

            ctx.addNodeAttribute(ATTR_CACHE, attrVals);

            ctx.addNodeAttribute(ATTR_TX_CONFIG, ctx.config().getTransactionConfiguration());

            if (!interceptors.isEmpty())
                ctx.addNodeAttribute(ATTR_CACHE_INTERCEPTORS, interceptors);
        }

        marshallerCache().context().preloader().syncFuture().listen(new CI1<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> f) {
                ctx.marshallerContext().onMarshallerCacheReady(ctx);
            }
        });

        if (log.isDebugEnabled())
            log.debug("Started cache processor.");
    }

    /**
     * Creates shared context.
     *
     * @param kernalCtx Kernal context.
     * @return Shared context.
     */
    @SuppressWarnings("unchecked")
    private GridCacheSharedContext createSharedContext(GridKernalContext kernalCtx) {
        IgniteTxManager tm = new IgniteTxManager();
        GridCacheMvccManager mvccMgr = new GridCacheMvccManager();
        GridCacheVersionManager verMgr = new GridCacheVersionManager();
        GridCacheDeploymentManager depMgr = new GridCacheDeploymentManager();
        GridCachePartitionExchangeManager exchMgr = new GridCachePartitionExchangeManager();
        GridCacheIoManager ioMgr = new GridCacheIoManager();

        return new GridCacheSharedContext(
            kernalCtx,
            tm,
            verMgr,
            mvccMgr,
            depMgr,
            exchMgr,
            ioMgr
        );
    }

    /**
     * Checks that preload-order-dependant caches has SYNC or ASYNC preloading mode.
     *
     * @param cfgs Caches.
     * @return Maximum detected preload order.
     * @throws IgniteCheckedException If validation failed.
     */
    private int validatePreloadOrder(CacheConfiguration[] cfgs) throws IgniteCheckedException {
        int maxOrder = 0;

        for (CacheConfiguration cfg : cfgs) {
            int rebalanceOrder = cfg.getRebalanceOrder();

            if (rebalanceOrder > 0) {
                if (cfg.getCacheMode() == LOCAL)
                    throw new IgniteCheckedException("Rebalance order set for local cache (fix configuration and restart the " +
                        "node): " + cfg.getName());

                if (cfg.getRebalanceMode() == CacheRebalanceMode.NONE)
                    throw new IgniteCheckedException("Only caches with SYNC or ASYNC rebalance mode can be set as rebalance " +
                        "dependency for other caches [cacheName=" + cfg.getName() +
                        ", rebalanceMode=" + cfg.getRebalanceMode() + ", rebalanceOrder=" + cfg.getRebalanceOrder() + ']');

                maxOrder = Math.max(maxOrder, rebalanceOrder);
            }
            else if (rebalanceOrder < 0)
                throw new IgniteCheckedException("Rebalance order cannot be negative for cache (fix configuration and restart " +
                    "the node) [cacheName=" + cfg.getName() + ", rebalanceOrder=" + rebalanceOrder + ']');
        }

        return maxOrder;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteSpiNodeValidationResult validateNode(ClusterNode node) {
        return validateHashIdResolvers(node);
    }

    /**
     * @param node Joining node.
     * @return Validation result or {@code null} in case of success.
     */
    @Nullable private IgniteSpiNodeValidationResult validateHashIdResolvers(ClusterNode node) {
        for (GridCacheAdapter cache : ctx.cache().internalCaches()) {
            CacheConfiguration cfg = cache.configuration();

            if (cfg.getAffinity() instanceof CacheRendezvousAffinityFunction) {
                CacheRendezvousAffinityFunction aff = (CacheRendezvousAffinityFunction)cfg.getAffinity();

                CacheAffinityNodeHashResolver hashIdRslvr = aff.getHashIdResolver();

                assert hashIdRslvr != null;

                Object nodeHashObj = hashIdRslvr.resolve(node);

                for (ClusterNode topNode : ctx.discovery().allNodes()) {
                    Object topNodeHashObj = hashIdRslvr.resolve(topNode);

                    if (nodeHashObj.hashCode() == topNodeHashObj.hashCode()) {
                        String errMsg = "Failed to add node to topology because it has the same hash code for " +
                            "partitioned affinity as one of existing nodes [cacheName=" + cache.name() +
                            ", hashIdResolverClass=" + hashIdRslvr.getClass().getName() +
                            ", existingNodeId=" + topNode.id() + ']';

                        String sndMsg = "Failed to add node to topology because it has the same hash code for " +
                            "partitioned affinity as one of existing nodes [cacheName=" + cache.name() +
                            ", hashIdResolverClass=" + hashIdRslvr.getClass().getName() + ", existingNodeId=" +
                            topNode.id() + ']';

                        return new IgniteSpiNodeValidationResult(topNode.id(), errMsg, sndMsg);
                    }
                }
            }
        }

        return null;
    }

    /**
     * Gets cache interceptor class name from node attributes.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @return Interceptor class name.
     */
    @Nullable private String interceptor(ClusterNode node, @Nullable String cacheName) {
        Map<String, String> map = node.attribute(ATTR_CACHE_INTERCEPTORS);

        return map != null ? map.get(cacheName) : null;
    }

    /**
     * Checks that remote caches has configuration compatible with the local.
     *
     * @param rmt Node.
     * @throws IgniteCheckedException If check failed.
     */
    private void checkCache(ClusterNode rmt) throws IgniteCheckedException {
        checkTransactionConfiguration(rmt);

        GridCacheAttributes[] rmtAttrs = U.cacheAttributes(rmt);
        GridCacheAttributes[] locAttrs = U.cacheAttributes(ctx.discovery().localNode());

        // If remote or local node does not have cache configured, do nothing
        if (F.isEmpty(rmtAttrs) || F.isEmpty(locAttrs))
            return;

        DeploymentMode locDepMode = ctx.config().getDeploymentMode();
        DeploymentMode rmtDepMode = rmt.attribute(IgniteNodeAttributes.ATTR_DEPLOYMENT_MODE);

        for (GridCacheAttributes rmtAttr : rmtAttrs) {
            for (GridCacheAttributes locAttr : locAttrs) {
                if (F.eq(rmtAttr.cacheName(), locAttr.cacheName())) {
                    CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cacheMode", "Cache mode",
                        locAttr.cacheMode(), rmtAttr.cacheMode(), true);

                    if (rmtAttr.cacheMode() != LOCAL) {
                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "interceptor", "Cache Interceptor",
                            interceptor(ctx.discovery().localNode(), rmtAttr.cacheName()),
                            interceptor(rmt, rmtAttr.cacheName()), true);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "atomicityMode",
                            "Cache atomicity mode", locAttr.atomicityMode(), rmtAttr.atomicityMode(), true);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cacheRebalanceMode",
                            "Cache rebalance mode", locAttr.cacheRebalanceMode(), rmtAttr.cacheRebalanceMode(), true);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cacheAffinity", "Cache affinity",
                            locAttr.cacheAffinityClassName(), rmtAttr.cacheAffinityClassName(), true);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cacheAffinityMapper",
                            "Cache affinity mapper", locAttr.cacheAffinityMapperClassName(),
                            rmtAttr.cacheAffinityMapperClassName(), true);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "affinityPartitionsCount",
                            "Affinity partitions count", locAttr.affinityPartitionsCount(),
                            rmtAttr.affinityPartitionsCount(), true);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "evictionFilter", "Eviction filter",
                            locAttr.evictionFilterClassName(), rmtAttr.evictionFilterClassName(), true);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "evictionPolicy", "Eviction policy",
                            locAttr.evictionPolicyClassName(), rmtAttr.evictionPolicyClassName(), true);

                        if (!skipStoreConsistencyCheck(locAttr, rmtAttr)) {
                            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "store", "Cache store",
                                locAttr.storeClassName(), rmtAttr.storeClassName(), true);

                            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "readThrough",
                                "Read through enabled", locAttr.readThrough(), locAttr.readThrough(), true);

                            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "writeThrough",
                                "Write through enabled", locAttr.writeThrough(), locAttr.writeThrough(), true);

                            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "loadPreviousValue",
                                "Load previous value enabled", locAttr.loadPreviousValue(),
                                locAttr.loadPreviousValue(), true);
                        }

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "transactionManagerLookup",
                            "Transaction manager lookup", locAttr.transactionManagerLookupClassName(),
                            rmtAttr.transactionManagerLookupClassName(), false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "defaultLockTimeout",
                            "Default lock timeout", locAttr.defaultLockTimeout(), rmtAttr.defaultLockTimeout(), false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "defaultQueryTimeout",
                            "Default query timeout", locAttr.defaultQueryTimeout(), rmtAttr.defaultQueryTimeout(),
                            false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "defaultTimeToLive",
                            "Default time to live", locAttr.defaultTimeToLive(), rmtAttr.defaultTimeToLive(), false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "rebalanceBatchSize",
                            "Rebalance batch size", locAttr.rebalanceBatchSize(), rmtAttr.rebalanceBatchSize(), false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "swapEnabled",
                            "Swap enabled", locAttr.swapEnabled(), rmtAttr.swapEnabled(), false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "writeSynchronizationMode",
                            "Write synchronization mode", locAttr.writeSynchronization(), rmtAttr.writeSynchronization(),
                            true);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "writeBehindBatchSize",
                            "Write behind batch size", locAttr.writeBehindBatchSize(), rmtAttr.writeBehindBatchSize(),
                            false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "writeBehindEnabled",
                            "Write behind enabled", locAttr.writeBehindEnabled(), rmtAttr.writeBehindEnabled(), false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "writeBehindFlushFrequency",
                            "Write behind flush frequency", locAttr.writeBehindFlushFrequency(),
                            rmtAttr.writeBehindFlushFrequency(), false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "writeBehindFlushSize",
                            "Write behind flush size", locAttr.writeBehindFlushSize(), rmtAttr.writeBehindFlushSize(),
                            false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "writeBehindFlushThreadCount",
                            "Write behind flush thread count", locAttr.writeBehindFlushThreadCount(),
                            rmtAttr.writeBehindFlushThreadCount(), false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "evictMaxOverflowRatio",
                            "Eviction max overflow ratio", locAttr.evictMaxOverflowRatio(),
                            rmtAttr.evictMaxOverflowRatio(), true);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "indexingSpiName", "IndexingSpiName",
                            locAttr.indexingSpiName(), rmtAttr.indexingSpiName(), true);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "queryIndexEnabled",
                            "Query index enabled", locAttr.queryIndexEnabled(), rmtAttr.queryIndexEnabled(), true);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "queryIndexEnabled",
                            "Query index enabled", locAttr.queryIndexEnabled(), rmtAttr.queryIndexEnabled(), true);

                        if (locAttr.cacheMode() == PARTITIONED) {
                            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "evictSynchronized",
                                "Eviction synchronized", locAttr.evictSynchronized(), rmtAttr.evictSynchronized(),
                                true);

                            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "evictNearSynchronized",
                                "Eviction near synchronized", locAttr.evictNearSynchronized(),
                                rmtAttr.evictNearSynchronized(), true);

                            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "nearEvictionPolicy",
                                "Near eviction policy", locAttr.nearEvictionPolicyClassName(),
                                rmtAttr.nearEvictionPolicyClassName(), false);

                            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "affinityIncludeNeighbors",
                                "Affinity include neighbors", locAttr.affinityIncludeNeighbors(),
                                rmtAttr.affinityIncludeNeighbors(), true);

                            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "affinityKeyBackups",
                                "Affinity key backups", locAttr.affinityKeyBackups(),
                                rmtAttr.affinityKeyBackups(), true);

                            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cacheAffinity.hashIdResolver",
                                "Partitioned cache affinity hash ID resolver class",
                                locAttr.affinityHashIdResolverClassName(), rmtAttr.affinityHashIdResolverClassName(),
                                true);
                        }
                    }
                }

                CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "deploymentMode", "Deployment mode",
                    locDepMode, rmtDepMode, true);
            }
        }
    }

    /**
     * @param rmt Remote node to check.
     * @throws IgniteCheckedException If check failed.
     */
    private void checkTransactionConfiguration(ClusterNode rmt) throws IgniteCheckedException {
        TransactionConfiguration txCfg = rmt.attribute(ATTR_TX_CONFIG);

        if (txCfg != null) {
            TransactionConfiguration locTxCfg = ctx.config().getTransactionConfiguration();

            if (locTxCfg.isTxSerializableEnabled() != txCfg.isTxSerializableEnabled())
                throw new IgniteCheckedException("Serializable transactions enabled mismatch " +
                    "(fix txSerializableEnabled property or set -D" + IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true " +
                    "system property) [rmtNodeId=" + rmt.id() +
                    ", locTxSerializableEnabled=" + locTxCfg.isTxSerializableEnabled() +
                    ", rmtTxSerializableEnabled=" + txCfg.isTxSerializableEnabled() + ']');
        }
    }

    /**
     * Checks if store check should be skipped for given nodes.
     *
     * @param locAttr Local node attributes.
     * @param rmtAttr Remote node attributes.
     * @return {@code True} if store check should be skipped.
     */
    private boolean skipStoreConsistencyCheck(GridCacheAttributes locAttr, GridCacheAttributes rmtAttr) {
        return
            // In atomic mode skip check if either local or remote node is client.
            locAttr.atomicityMode() == ATOMIC &&
                (locAttr.partitionedTaxonomy() == CLIENT_ONLY || locAttr.partitionedTaxonomy() == NEAR_ONLY ||
                rmtAttr.partitionedTaxonomy() == CLIENT_ONLY || rmtAttr.partitionedTaxonomy() == NEAR_ONLY);
    }

    /**
     * @param cache Cache.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private void onKernalStart(GridCacheAdapter<?, ?> cache) throws IgniteCheckedException {
        GridCacheContext<?, ?> ctx = cache.context();

        // Start DHT cache as well.
        if (isNearEnabled(ctx)) {
            GridDhtCacheAdapter dht = ctx.near().dht();

            GridCacheContext<?, ?> dhtCtx = dht.context();

            for (GridCacheManager mgr : dhtManagers(dhtCtx))
                mgr.onKernalStart();

            dht.onKernalStart();

            if (log.isDebugEnabled())
                log.debug("Executed onKernalStart() callback for DHT cache: " + dht.name());
        }

        for (GridCacheManager mgr : F.view(ctx.managers(), F0.notContains(dhtExcludes(ctx))))
            mgr.onKernalStart();

        cache.onKernalStart();

        if (log.isDebugEnabled())
            log.debug("Executed onKernalStart() callback for cache [name=" + cache.name() + ", mode=" +
                cache.configuration().getCacheMode() + ']');
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onKernalStart() throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        if (!getBoolean(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK)) {
            for (ClusterNode n : ctx.discovery().remoteNodes())
                checkCache(n);
        }

        for (Map.Entry<String, GridCacheAdapter<?, ?>> e : caches.entrySet()) {
            GridCacheAdapter cache = e.getValue();

            if (maxRebalanceOrder > 0) {
                CacheConfiguration cfg = cache.configuration();

                int order = cfg.getRebalanceOrder();

                if (order > 0 && order != maxRebalanceOrder && cfg.getCacheMode() != LOCAL) {
                    GridCompoundFuture<Object, Object> fut = (GridCompoundFuture<Object, Object>)preloadFuts
                        .get(order);

                    if (fut == null) {
                        fut = new GridCompoundFuture<>();

                        preloadFuts.put(order, fut);
                    }

                    fut.add(cache.preloader().syncFuture());
                }
            }
        }

        for (IgniteInternalFuture<?> fut : preloadFuts.values())
            ((GridCompoundFuture<Object, Object>)fut).markInitialized();

        for (GridCacheSharedManager<?, ?> mgr : sharedCtx.managers())
            mgr.onKernalStart();

        for (GridCacheAdapter<?, ?> cache : caches.values())
            onKernalStart(cache);

        // Wait for caches in SYNC preload mode.
        for (GridCacheAdapter<?, ?> cache : caches.values()) {
            CacheConfiguration cfg = cache.configuration();

            if (cfg.getRebalanceMode() == SYNC) {
                if (cfg.getCacheMode() == REPLICATED ||
                    (cfg.getCacheMode() == PARTITIONED && cfg.getRebalanceDelay() >= 0))
                    cache.preloader().syncFuture().get();
            }
        }

        ctx.cacheObjects().onCacheProcessorStarted();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onKernalStop(boolean cancel) {
        if (ctx.config().isDaemon())
            return;

        for (GridCacheAdapter<?, ?> cache : stopSeq) {
            GridCacheContext ctx = cache.context();

            if (isNearEnabled(ctx)) {
                GridDhtCacheAdapter dht = ctx.near().dht();

                if (dht != null) {
                    GridCacheContext<?, ?> dhtCtx = dht.context();

                    for (GridCacheManager mgr : dhtManagers(dhtCtx))
                        mgr.onKernalStop(cancel);

                    dht.onKernalStop();
                }
            }

            List<GridCacheManager> mgrs = ctx.managers();

            Collection<GridCacheManager> excludes = dhtExcludes(ctx);

            // Reverse order.
            for (ListIterator<GridCacheManager> it = mgrs.listIterator(mgrs.size()); it.hasPrevious(); ) {
                GridCacheManager mgr = it.previous();

                if (!excludes.contains(mgr))
                    mgr.onKernalStop(cancel);
            }

            cache.onKernalStop();
        }

        List<? extends GridCacheSharedManager<?, ?>> sharedMgrs = sharedCtx.managers();

        for (ListIterator<? extends GridCacheSharedManager<?, ?>> it = sharedMgrs.listIterator(sharedMgrs.size());
            it.hasPrevious();) {
            GridCacheSharedManager<?, ?> mgr = it.previous();

            mgr.onKernalStop(cancel);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        for (GridCacheAdapter<?, ?> cache : stopSeq) {
            cache.stop();

            GridCacheContext ctx = cache.context();

            if (isNearEnabled(ctx)) {
                GridDhtCacheAdapter dht = ctx.near().dht();

                // Check whether dht cache has been started.
                if (dht != null) {
                    dht.stop();

                    GridCacheContext<?, ?> dhtCtx = dht.context();

                    List<GridCacheManager> dhtMgrs = dhtManagers(dhtCtx);

                    for (ListIterator<GridCacheManager> it = dhtMgrs.listIterator(dhtMgrs.size()); it.hasPrevious();) {
                        GridCacheManager mgr = it.previous();

                        mgr.stop(cancel);
                    }
                }
            }

            List<GridCacheManager> mgrs = ctx.managers();

            Collection<GridCacheManager> excludes = dhtExcludes(ctx);

            // Reverse order.
            for (ListIterator<GridCacheManager> it = mgrs.listIterator(mgrs.size()); it.hasPrevious();) {
                GridCacheManager mgr = it.previous();

                if (!excludes.contains(mgr))
                    mgr.stop(cancel);
            }

            U.stopLifecycleAware(log, lifecycleAwares(cache.configuration(), ctx.jta().tmLookup(),
                ctx.store().configuredStore()));

            if (log.isInfoEnabled())
                log.info("Stopped cache: " + cache.name());

            cleanup(ctx);
        }

        List<? extends GridCacheSharedManager<?, ?>> mgrs = sharedCtx.managers();

        for (ListIterator<? extends GridCacheSharedManager<?, ?>> it = mgrs.listIterator(mgrs.size()); it.hasPrevious();) {
            GridCacheSharedManager<?, ?> mgr = it.previous();

            mgr.stop(cancel);
        }

        sharedCtx.cleanup();

        if (log.isDebugEnabled())
            log.debug("Stopped cache processor.");
    }

    /**
     * Gets preload finish future for preload-ordered cache with given order. I.e. will get compound preload future
     * with maximum order less than {@code order}.
     *
     * @param order Cache order.
     * @return Compound preload future or {@code null} if order is minimal order found.
     */
    @Nullable public IgniteInternalFuture<?> orderedPreloadFuture(int order) {
        Map.Entry<Integer, IgniteInternalFuture<?>> entry = preloadFuts.lowerEntry(order);

        return entry == null ? null : entry.getValue();
    }

    /**
     * @param spaceName Space name.
     * @param keyBytes Key bytes.
     */
    @SuppressWarnings( {"unchecked"})
    public void onEvictFromSwap(String spaceName, byte[] keyBytes) {
        assert spaceName != null;
        assert keyBytes != null;

        /*
         * NOTE: this method should not have any synchronization because
         * it is called from synchronization block within Swap SPI.
         */

        GridCacheAdapter cache = caches.get(CU.cacheNameForSwapSpaceName(spaceName));

        assert cache != null : "Failed to resolve cache name for swap space name: " + spaceName;

        GridCacheContext cctx = cache.configuration().getCacheMode() == PARTITIONED ?
            ((GridNearCacheAdapter<?, ?>)cache).dht().context() : cache.context();

        if (spaceName.equals(CU.swapSpaceName(cctx))) {
            GridCacheQueryManager qryMgr = cctx.queries();

            if (qryMgr != null) {
                try {
                    KeyCacheObject key = cctx.toCacheKeyObject(keyBytes);

                    qryMgr.remove(key.value(cctx.cacheObjectContext(), false));
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to unmarshal key evicted from swap [swapSpaceName=" + spaceName + ']', e);
                }
            }
        }
    }

    /**
     * @param cfg Cache configuration.
     * @return Query manager.
     */
    private GridCacheQueryManager queryManager(CacheConfiguration cfg) {
        return cfg.getCacheMode() == LOCAL ? new GridCacheLocalQueryManager() : new GridCacheDistributedQueryManager();
    }

    /**
     * @return Last data version.
     */
    public long lastDataVersion() {
        long max = 0;

        for (GridCacheAdapter<?, ?> cache : caches.values()) {
            GridCacheContext<?, ?> ctx = cache.context();

            if (ctx.versions().last().order() > max)
                max = ctx.versions().last().order();

            if (ctx.isNear()) {
                ctx = ctx.near().dht().context();

                if (ctx.versions().last().order() > max)
                    max = ctx.versions().last().order();
            }
        }

        assert caches.isEmpty() || max > 0;

        return max;
    }

    /**
     * @param <K> type of keys.
     * @param <V> type of values.
     * @return Default cache.
     */
    public <K, V> GridCache<K, V> cache() {
        return cache(null);
    }

    /**
     * @param name Cache name.
     * @param <K> type of keys.
     * @param <V> type of values.
     * @return Cache instance for given name.
     */
    @SuppressWarnings("unchecked")
    public <K, V> GridCache<K, V> cache(@Nullable String name) {
        if (log.isDebugEnabled())
            log.debug("Getting cache for name: " + name);

        return (GridCache<K, V>)proxies.get(name);
    }

    /**
     * @return All configured cache instances.
     */
    public Collection<GridCache<?, ?>> caches() {
        return proxies.values();
    }

    /**
     * @param <K> type of keys.
     * @param <V> type of values.
     * @return Default cache.
     */
    public <K, V> GridCache<K, V> publicCache() {
        return publicCache(null);
    }

    /**
     * @return Marshaller system cache.
     */
    public GridCacheAdapter<Integer, String> marshallerCache() {
        return internalCache(CU.MARSH_CACHE_NAME);
    }

    /**
     * Gets utility cache.
     *
     * @param keyCls Key class.
     * @param valCls Value class.
     * @return Projection over utility cache.
     */
    public <K extends GridCacheUtilityKey, V> GridCacheProjectionEx<K, V> utilityCache(Class<K> keyCls, Class<V> valCls) {
        return (GridCacheProjectionEx<K, V>)cache(CU.UTILITY_CACHE_NAME).projection(keyCls, valCls);
    }

    /**
     * Gets utility cache.
     *
     * @return Utility cache.
     */
    public <K, V> GridCacheAdapter<K, V> utilityCache() {
        return internalCache(CU.UTILITY_CACHE_NAME);
    }

    /**
     * Gets utility cache for atomic data structures.
     *
     * @return Utility cache for atomic data structures.
     */
    public <K, V> GridCache<K, V> atomicsCache() {
        return cache(CU.ATOMICS_CACHE_NAME);
    }

    /**
     * @param name Cache name.
     * @param <K> type of keys.
     * @param <V> type of values.
     * @return Cache instance for given name.
     */
    @SuppressWarnings("unchecked")
    public <K, V> GridCache<K, V> publicCache(@Nullable String name) {
        if (log.isDebugEnabled())
            log.debug("Getting public cache for name: " + name);

        if (sysCaches.contains(name))
            throw new IllegalStateException("Failed to get cache because it is system cache: " + name);

        GridCache<K, V> cache = (GridCache<K, V>)publicProxies.get(name);

        if (cache == null)
            throw new IllegalArgumentException("Cache is not configured: " + name);

        return cache;
    }

    /**
     * @param name Cache name.
     * @param <K> type of keys.
     * @param <V> type of values.
     * @return Cache instance for given name.
     */
    @SuppressWarnings("unchecked")
    public <K, V> IgniteCache<K, V> publicJCache(@Nullable String name) {
        if (log.isDebugEnabled())
            log.debug("Getting public cache for name: " + name);

        if (sysCaches.contains(name))
            throw new IllegalStateException("Failed to get cache because it is system cache: " + name);

        IgniteCache<K,V> cache = (IgniteCache<K, V>)jCacheProxies.get(name);

        if (cache == null)
            throw new IllegalArgumentException("Cache is not configured: " + name);

        return cache;
    }

    /**
     * @param name Cache name.
     * @return Cache instance for given name.
     */
    @SuppressWarnings("unchecked")
    public <K, V> IgniteCacheProxy<K, V> jcache(@Nullable String name) {
        IgniteCacheProxy<K, V> cache = (IgniteCacheProxy<K, V>)jCacheProxies.get(name);

        if (cache == null)
            throw new IllegalArgumentException("Cache is not configured: " + name);

        return cache;
    }

    /**
     * @return All configured public cache instances.
     */
    public Collection<GridCache<?, ?>> publicCaches() {
        return publicProxies.values();
    }

    /**
     * @param <K> type of keys.
     * @param <V> type of values.
     * @return Default cache.
     */
    public <K, V> GridCacheAdapter<K, V> internalCache() {
        return internalCache(null);
    }

    /**
     * @param name Cache name.
     * @param <K> type of keys.
     * @param <V> type of values.
     * @return Cache instance for given name.
     */
    @SuppressWarnings("unchecked")
    public <K, V> GridCacheAdapter<K, V> internalCache(@Nullable String name) {
        if (log.isDebugEnabled())
            log.debug("Getting internal cache adapter: " + name);

        return (GridCacheAdapter<K, V>)caches.get(name);
    }

    /**
     * Cancel all user operations.
     */
    public void cancelUserOperations() {
        for (GridCacheAdapter<?, ?> cache : caches.values())
            cache.ctx.mvcc().cancelClientFutures();
    }

    /**
     * @return All internal cache instances.
     */
    public Collection<GridCacheAdapter<?, ?>> internalCaches() {
        return caches.values();
    }

    /**
     * @param name Cache name.
     * @return {@code True} if specified cache is system, {@code false} otherwise.
     */
    public boolean systemCache(@Nullable String name) {
        return sysCaches.contains(name);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");

        for (GridCacheAdapter c : caches.values()) {
            X.println(">>> Cache memory stats [grid=" + ctx.gridName() + ", cache=" + c.name() + ']');

            c.context().printMemoryStats();
        }
    }

    /**
     * Callback invoked by deployment manager for whenever a class loader
     * gets undeployed.
     *
     * @param ldr Class loader.
     */
    public void onUndeployed(ClassLoader ldr) {
        if (!ctx.isStopping()) {
            for (GridCacheAdapter<?, ?> cache : caches.values()) {
                // Do not notify system caches.
                if (!cache.context().system() && !CU.isAtomicsCache(cache.context().name()))
                    cache.onUndeploy(ldr);
            }
        }
    }

    /**
     * @return Shared context.
     */
    public <K, V> GridCacheSharedContext<K, V> context() {
        return (GridCacheSharedContext<K, V>)sharedCtx;
    }

    /**
     * @return Transactions interface implementation.
     */
    public IgniteTransactionsEx transactions() {
        return transactions;
    }

    /**
     * Registers MBean for cache components.
     *
     * @param o Cache component.
     * @param cacheName Cache name.
     * @param near Near flag.
     * @throws IgniteCheckedException If registration failed.
     */
    @SuppressWarnings("unchecked")
    private void registerMbean(Object o, @Nullable String cacheName, boolean near)
        throws IgniteCheckedException {
        assert o != null;

        MBeanServer srvr = ctx.config().getMBeanServer();

        assert srvr != null;

        cacheName = U.maskName(cacheName);

        cacheName = near ? cacheName + "-near" : cacheName;

        for (Class<?> itf : o.getClass().getInterfaces()) {
            if (itf.getName().endsWith("MBean")) {
                try {
                    U.registerCacheMBean(srvr, ctx.gridName(), cacheName, o.getClass().getName(), o,
                        (Class<Object>)itf);
                }
                catch (JMException e) {
                    throw new IgniteCheckedException("Failed to register MBean for component: " + o, e);
                }

                break;
            }
        }
    }

    /**
     * Unregisters MBean for cache components.
     *
     * @param o Cache component.
     * @param cacheName Cache name.
     * @param near Near flag.
     */
    private void unregisterMbean(Object o, @Nullable String cacheName, boolean near) {
        assert o != null;

        MBeanServer srvr = ctx.config().getMBeanServer();

        assert srvr != null;

        cacheName = U.maskName(cacheName);

        cacheName = near ? cacheName + "-near" : cacheName;

        for (Class<?> itf : o.getClass().getInterfaces()) {
            if (itf.getName().endsWith("MBean")) {
                try {
                    srvr.unregisterMBean(U.makeCacheMBeanName(ctx.gridName(), cacheName, o.getClass().getName()));
                }
                catch (JMException e) {
                    U.error(log, "Failed to unregister MBean for component: " + o, e);
                }

                break;
            }
        }
    }

    /**
     * @param ccfg Cache configuration.
     * @param objs Extra components.
     * @return Components provided in cache configuration which can implement {@link LifecycleAware} interface.
     */
    private Iterable<Object> lifecycleAwares(CacheConfiguration ccfg, Object...objs) {
        Collection<Object> ret = new ArrayList<>(7 + objs.length);

        ret.add(ccfg.getAffinity());
        ret.add(ccfg.getAffinityMapper());
        ret.add(ccfg.getEvictionFilter());
        ret.add(ccfg.getEvictionPolicy());
        ret.add(ccfg.getNearEvictionPolicy());
        ret.add(ccfg.getInterceptor());

        Collections.addAll(ret, objs);

        return ret;
    }

    /**
     *
     */
    private static class LocalAffinityFunction implements CacheAffinityFunction {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(CacheAffinityFunctionContext affCtx) {
            ClusterNode locNode = null;

            for (ClusterNode n : affCtx.currentTopologySnapshot()) {
                if (n.isLocal()) {
                    locNode = n;

                    break;
                }
            }

            if (locNode == null)
                throw new IgniteException("Local node is not included into affinity nodes for 'LOCAL' cache");

            List<List<ClusterNode>> res = new ArrayList<>(partitions());

            for (int part = 0; part < partitions(); part++)
                res.add(Collections.singletonList(locNode));

            return Collections.unmodifiableList(res);
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // No-op.
        }
    }
}

