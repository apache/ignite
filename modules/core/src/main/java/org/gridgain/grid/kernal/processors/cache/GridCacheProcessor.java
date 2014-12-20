/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.spi.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.cache.affinity.fair.*;
import org.gridgain.grid.cache.affinity.rendezvous.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.colocated.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.gridgain.grid.kernal.processors.cache.jta.*;
import org.gridgain.grid.kernal.processors.cache.local.*;
import org.gridgain.grid.kernal.processors.cache.local.atomic.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.query.continuous.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.management.*;
import java.util.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.configuration.IgniteDeploymentMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheConfiguration.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.kernal.GridComponentType.*;
import static org.gridgain.grid.kernal.GridNodeAttributes.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;

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

    /** Map of public proxies, i.e. proxies which could be returned to the user. */
    private final Map<String, GridCache<?, ?>> publicProxies;

    /** Map of preload finish futures grouped by preload order. */
    private final NavigableMap<Integer, IgniteFuture<?>> preloadFuts;

    /** Maximum detected preload order. */
    private int maxPreloadOrder;

    /** System cache names. */
    private final Set<String> sysCaches;

    /** Caches stop sequence. */
    private final Deque<GridCacheAdapter<?, ?>> stopSeq;

    /** MBean server. */
    private final MBeanServer mBeanSrv;

    /** Cache MBeans. */
    private final Collection<ObjectName> cacheMBeans = new LinkedList<>();

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
        preloadFuts = new TreeMap<>();

        sysCaches = new HashSet<>();
        stopSeq = new LinkedList<>();

        mBeanSrv = ctx.config().getMBeanServer();
    }

    /**
     * @param cfg Initializes cache configuration with proper defaults.
     */
    @SuppressWarnings("unchecked")
    private void initialize(GridCacheConfiguration cfg) {
        if (cfg.getCacheMode() == null)
            cfg.setCacheMode(DFLT_CACHE_MODE);

        if (cfg.getMemoryMode() == null)
            cfg.setMemoryMode(DFLT_MEMORY_MODE);

        if (cfg.getAffinity() == null) {
            if (cfg.getCacheMode() == PARTITIONED) {
                GridCacheConsistentHashAffinityFunction aff = new GridCacheConsistentHashAffinityFunction();

                aff.setHashIdResolver(new GridCacheAffinityNodeAddressHashResolver());

                cfg.setAffinity(aff);
            }
            else if (cfg.getCacheMode() == REPLICATED) {
                GridCacheConsistentHashAffinityFunction aff = new GridCacheConsistentHashAffinityFunction(false, 512);

                aff.setHashIdResolver(new GridCacheAffinityNodeAddressHashResolver());

                cfg.setAffinity(aff);

                cfg.setBackups(Integer.MAX_VALUE);
            }
            else
                cfg.setAffinity(new LocalAffinityFunction());
        }
        else {
            if (cfg.getCacheMode() == PARTITIONED) {
                if (cfg.getAffinity() instanceof GridCacheConsistentHashAffinityFunction) {
                    GridCacheConsistentHashAffinityFunction aff = (GridCacheConsistentHashAffinityFunction)cfg.getAffinity();

                    if (aff.getHashIdResolver() == null)
                        aff.setHashIdResolver(new GridCacheAffinityNodeAddressHashResolver());
                }
            }
        }

        if (cfg.getCacheMode() == REPLICATED)
            cfg.setBackups(Integer.MAX_VALUE);

        if (cfg.getAffinityMapper() == null)
            cfg.setAffinityMapper(new GridCacheDefaultAffinityKeyMapper());

        ctx.ggfsHelper().preProcessCacheConfiguration(cfg);

        if (cfg.getPreloadMode() == null)
            cfg.setPreloadMode(ASYNC);

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
                    GridCacheAtomicWriteOrderMode.CLOCK :
                    GridCacheAtomicWriteOrderMode.PRIMARY);
            }
            else if (cfg.getWriteSynchronizationMode() != FULL_SYNC &&
                cfg.getAtomicWriteOrderMode() == GridCacheAtomicWriteOrderMode.CLOCK) {
                cfg.setAtomicWriteOrderMode(GridCacheAtomicWriteOrderMode.PRIMARY);

                U.warn(log, "Automatically set write order mode to PRIMARY for better performance " +
                    "[writeSynchronizationMode=" + cfg.getWriteSynchronizationMode() + ", " +
                    "cacheName=" + cfg.getName() + ']');
            }
        }
    }

    /**
     * @param cfg Configuration to check for possible performance issues.
     */
    private void suggestOptimizations(GridCacheConfiguration cfg) {
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

        if (cfg.getStore() != null)
            perf.add("Enable write-behind to persistent store (set 'writeBehindEnabled' to true)",
                cfg.isWriteBehindEnabled());

        perf.add("Disable query index (set 'queryIndexEnabled' to false)", !cfg.isQueryIndexEnabled());
    }

    /**
     * @param c Grid configuration.
     * @param cc Configuration to validate.
     * @throws IgniteCheckedException If failed.
     */
    private void validate(IgniteConfiguration c, GridCacheConfiguration cc) throws IgniteCheckedException {
        if (cc.getCacheMode() == REPLICATED) {
            if (cc.getAffinity() instanceof GridCachePartitionFairAffinity)
                throw new IgniteCheckedException("REPLICATED cache can not be started with GridCachePartitionFairAffinity" +
                    " [cacheName=" + cc.getName() + ']');

            if (cc.getAffinity() instanceof GridCacheConsistentHashAffinityFunction) {
                GridCacheConsistentHashAffinityFunction aff = (GridCacheConsistentHashAffinityFunction)cc.getAffinity();

                if (aff.isExcludeNeighbors())
                    throw new IgniteCheckedException("For REPLICATED cache flag 'excludeNeighbors' in " +
                        "GridCacheConsistentHashAffinityFunction cannot be set [cacheName=" + cc.getName() + ']');
            }

            if (cc.getAffinity() instanceof GridCacheRendezvousAffinityFunction) {
                GridCacheRendezvousAffinityFunction aff = (GridCacheRendezvousAffinityFunction)cc.getAffinity();

                if (aff.isExcludeNeighbors())
                    throw new IgniteCheckedException("For REPLICATED cache flag 'excludeNeighbors' in " +
                        "GridCacheRendezvousAffinityFunction cannot be set [cacheName=" + cc.getName() + ']');
            }

            if (cc.getDistributionMode() == NEAR_PARTITIONED) {
                U.warn(log, "NEAR_PARTITIONED distribution mode cannot be used with REPLICATED cache, " +
                    "will be changed to PARTITIONED_ONLY [cacheName=" + cc.getName() + ']');

                cc.setDistributionMode(PARTITIONED_ONLY);
            }
        }

        if (cc.getCacheMode() == LOCAL && !cc.getAffinity().getClass().equals(LocalAffinityFunction.class))
            U.warn(log, "GridCacheAffinityFunction configuration parameter will be ignored for local cache [cacheName=" +
                cc.getName() + ']');

        if (cc.getPreloadMode() != GridCachePreloadMode.NONE) {
            assertParameter(cc.getPreloadThreadPoolSize() > 0, "preloadThreadPoolSize > 0");
            assertParameter(cc.getPreloadBatchSize() > 0, "preloadBatchSize > 0");
        }

        if (cc.getCacheMode() == PARTITIONED || cc.getCacheMode() == REPLICATED) {
            if (cc.getAtomicityMode() == ATOMIC && cc.getWriteSynchronizationMode() == FULL_ASYNC)
                U.warn(log, "Cache write synchronization mode is set to FULL_ASYNC. All single-key 'put' and " +
                    "'remove' operations will return 'null', all 'putx' and 'removex' operations will return" +
                    " 'true' [cacheName=" + cc.getName() + ']');
        }

        IgniteDeploymentMode depMode = c.getDeploymentMode();

        if (c.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED) &&
            !CU.isSystemCache(cc.getName()))
            throw new IgniteCheckedException("Cannot start cache in PRIVATE or ISOLATED deployment mode: " +
                ctx.config().getDeploymentMode());

        if (!c.getTransactionsConfiguration().isTxSerializableEnabled() &&
            c.getTransactionsConfiguration().getDefaultTxIsolation() == SERIALIZABLE)
            U.warn(log,
                "Serializable transactions are disabled while default transaction isolation is SERIALIZABLE " +
                    "(most likely misconfiguration - either update 'isTxSerializableEnabled' or " +
                    "'defaultTxIsolationLevel' properties) for cache: " + cc.getName(),
                "Serializable transactions are disabled while default transaction isolation is SERIALIZABLE " +
                    "for cache: " + cc.getName());

        if (cc.isWriteBehindEnabled()) {
            if (cc.getStore() == null)
                throw new IgniteCheckedException("Cannot enable write-behind cache (cache store is not provided) for cache: " +
                    cc.getName());

            assertParameter(cc.getWriteBehindBatchSize() > 0, "writeBehindBatchSize > 0");
            assertParameter(cc.getWriteBehindFlushSize() >= 0, "writeBehindFlushSize >= 0");
            assertParameter(cc.getWriteBehindFlushFrequency() >= 0, "writeBehindFlushFrequency >= 0");
            assertParameter(cc.getWriteBehindFlushThreadCount() > 0, "writeBehindFlushThreadCount > 0");

            if (cc.getWriteBehindFlushSize() == 0 && cc.getWriteBehindFlushFrequency() == 0)
                throw new IgniteCheckedException("Cannot set both 'writeBehindFlushFrequency' and " +
                    "'writeBehindFlushSize' parameters to 0 for cache: " + cc.getName());
        }

        long delay = cc.getPreloadPartitionedDelay();

        if (delay != 0) {
            if (cc.getCacheMode() != PARTITIONED)
                U.warn(log, "Preload delay is supported only for partitioned caches (will ignore): " + cc.getName(),
                    "Will ignore preload delay for cache: " + cc.getName());
            else if (cc.getPreloadMode() == SYNC) {
                if (delay < 0) {
                    U.warn(log, "Ignoring SYNC preload mode with manual preload start (node will not wait for " +
                        "preloading to be finished): " + cc.getName(),
                        "Node will not wait for preload in SYNC mode: " + cc.getName());
                }
                else {
                    U.warn(log,
                        "Using SYNC preload mode with preload delay (node will wait until preloading is " +
                            "initiated for " + delay + "ms) for cache: " + cc.getName(),
                        "Node will wait until preloading is initiated for " + delay + "ms for cache: " + cc.getName());
                }
            }
        }

        ctx.ggfsHelper().validateCacheConfiguration(cc);

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

        if (cc.getMemoryMode() == GridCacheMemoryMode.OFFHEAP_VALUES) {
            if (cc.isQueryIndexEnabled())
                throw new IgniteCheckedException("Cannot have query indexing enabled while values are stored off-heap. " +
                    "You must either disable query indexing or disable off-heap values only flag for cache: " +
                    cc.getName());
        }

        boolean ggfsCache = CU.isGgfsCache(c, cc.getName());
        boolean utilityCache = CU.isUtilityCache(cc.getName());

        if (!ggfsCache && !utilityCache && !cc.isQueryIndexEnabled())
            U.warn(log, "Query indexing is disabled (queries will not work) for cache: '" + cc.getName() + "'. " +
                "To enable change GridCacheConfiguration.isQueryIndexEnabled() property.",
                "Query indexing is disabled (queries will not work) for cache: " + cc.getName());

        if (cc.getAtomicityMode() == ATOMIC)
            assertParameter(cc.getTransactionManagerLookupClassName() == null,
                "transaction manager can not be used with ATOMIC cache");

        if (cc.isPortableEnabled() && !ctx.isEnterprise())
            throw new IgniteCheckedException("Portable mode for cache is supported only in Enterprise edition " +
                "(set 'portableEnabled' property to 'false') [cacheName=" + cc.getName() + ']');
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
    private void prepare(GridCacheConfiguration cfg, Object... objs) throws IgniteCheckedException {
        prepare(cfg, cfg.getEvictionPolicy(), false);
        prepare(cfg, cfg.getNearEvictionPolicy(), true);
        prepare(cfg, cfg.getAffinity(), false);
        prepare(cfg, cfg.getAffinityMapper(), false);
        prepare(cfg, cfg.getCloner(), false);
        prepare(cfg, cfg.getStore(), false);
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
    private void prepare(GridCacheConfiguration cfg, @Nullable Object rsrc, boolean near) throws IgniteCheckedException {
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
        GridCacheConfiguration cfg = cctx.config();

        cleanup(cfg, cfg.getEvictionPolicy(), false);
        cleanup(cfg, cfg.getNearEvictionPolicy(), true);
        cleanup(cfg, cfg.getAffinity(), false);
        cleanup(cfg, cfg.getAffinityMapper(), false);
        cleanup(cfg, cctx.jta().tmLookup(), false);
        cleanup(cfg, cfg.getCloner(), false);
        cleanup(cfg, cfg.getStore(), false);

        cctx.cleanup();
    }

    /**
     * @param cfg Cache configuration.
     * @param rsrc Resource.
     * @param near Near flag.
     */
    private void cleanup(GridCacheConfiguration cfg, @Nullable Object rsrc, boolean near) {
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

        IgniteDeploymentMode depMode = ctx.config().getDeploymentMode();

        if (!F.isEmpty(ctx.config().getCacheConfiguration())) {
            if (depMode != CONTINUOUS && depMode != SHARED)
                U.warn(log, "Deployment mode for cache is not CONTINUOUS or SHARED " +
                    "(it is recommended that you change deployment mode and restart): " + depMode,
                    "Deployment mode for cache is not CONTINUOUS or SHARED.");
        }

        maxPreloadOrder = validatePreloadOrder(ctx.config().getCacheConfiguration());

        // Internal caches which should not be returned to user.
        IgniteFsConfiguration[] ggfsCfgs = ctx.grid().configuration().getGgfsConfiguration();

        if (ggfsCfgs != null) {
            for (IgniteFsConfiguration ggfsCfg : ggfsCfgs) {
                sysCaches.add(ggfsCfg.getMetaCacheName());
                sysCaches.add(ggfsCfg.getDataCacheName());
            }
        }

        if (GridComponentType.HADOOP.inClassPath())
            sysCaches.add(CU.SYS_CACHE_HADOOP_MR);

        sysCaches.add(CU.UTILITY_CACHE_NAME);

        GridCacheConfiguration[] cfgs = ctx.config().getCacheConfiguration();

        sharedCtx = createSharedContext(ctx);

        ctx.performance().add("Disable serializable transactions (set 'txSerializableEnabled' to false)",
            !ctx.config().getTransactionsConfiguration().isTxSerializableEnabled());

        Collection<GridCacheAdapter<?, ?>> startSeq = new ArrayList<>(cfgs.length);

        for (int i = 0; i < cfgs.length; i++) {
            GridCacheConfiguration cfg = new GridCacheConfiguration(cfgs[i]);

            // Initialize defaults.
            initialize(cfg);

            // Skip suggestions for system caches.
            if (!sysCaches.contains(cfg.getName()))
                suggestOptimizations(cfg);

            validate(ctx.config(), cfg);

            GridCacheJtaManagerAdapter jta = JTA.create(cfg.getTransactionManagerLookupClassName() == null);

            jta.createTmLookup(cfg);

            prepare(cfg, jta.tmLookup());

            U.startLifecycleAware(lifecycleAwares(cfg, jta.tmLookup()));

            cfgs[i] = cfg; // Replace original configuration value.

            GridCacheAffinityManager affMgr = new GridCacheAffinityManager();
            GridCacheEventManager evtMgr = new GridCacheEventManager();
            GridCacheSwapManager swapMgr = new GridCacheSwapManager(cfg.getCacheMode() == LOCAL || !isNearEnabled(cfg));
            GridCacheEvictionManager evictMgr = new GridCacheEvictionManager();
            GridCacheQueryManager qryMgr = queryManager(cfg);
            GridCacheContinuousQueryManager contQryMgr = new GridCacheContinuousQueryManager();
            GridCacheDataStructuresManager dataStructuresMgr = new GridCacheDataStructuresManager();
            GridCacheTtlManager ttlMgr = new GridCacheTtlManager();
            GridCacheDrManager drMgr = ctx.createComponent(GridCacheDrManager.class);

            GridCacheStore store = cacheStore(ctx.gridName(), cfg);

            GridCacheStoreManager storeMgr = new GridCacheStoreManager(store);

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
                    if (isNearEnabled(cfg)) {
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
                                cache = isAffinityNode(cfg) ? new GridDhtColocatedCache(cacheCtx) :
                                    new GridDhtColocatedCache(cacheCtx, new GridNoStorageCacheMap(cacheCtx));

                                break;
                            }
                            case ATOMIC: {
                                cache = isAffinityNode(cfg) ? new GridDhtAtomicCache(cacheCtx) :
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
            if (cfg.getCacheMode() != LOCAL && isNearEnabled(cfg)) {
                /*
                 * Specifically don't create the following managers
                 * here and reuse the one from Near cache:
                 * 1. GridCacheVersionManager
                 * 2. GridCacheIoManager
                 * 3. GridCacheDeploymentManager
                 * 4. GridCacheQueryManager (note, that we start it for DHT cache though).
                 * 5. GridCacheContinuousQueryManager (note, that we start it for DHT cache though).
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

                GridDhtCacheAdapter dht = null;

                switch (cfg.getAtomicityMode()) {
                    case TRANSACTIONAL: {
                        assert cache instanceof GridNearTransactionalCache;

                        GridNearTransactionalCache near = (GridNearTransactionalCache)cache;

                        GridDhtCache dhtCache = !isAffinityNode(cfg) ?
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

                        GridDhtAtomicCache dhtCache = isAffinityNode(cfg) ? new GridDhtAtomicCache(cacheCtx) :
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

            GridCacheConfiguration cfg = cacheCtx.config();

            // Start managers.
            for (GridCacheManager mgr : F.view(cacheCtx.managers(), F.notContains(dhtExcludes(cacheCtx))))
                mgr.start(cacheCtx);

            if (cfg.getCacheMode() != LOCAL && isNearEnabled(cfg)) {
                GridCacheContext<?, ?> dhtCtx = cacheCtx.near().dht().context();

                // Start DHT managers.
                for (GridCacheManager mgr : dhtManagers(dhtCtx))
                    mgr.start(dhtCtx);

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
        }

        for (GridCacheAdapter<?, ?> cache : caches.values()) {
            try {
                ObjectName mb = U.registerCacheMBean(mBeanSrv, ctx.gridName(), cache.name(), "Cache",
                    new GridCacheMBeanAdapter(cache.context()), GridCacheMBean.class);

                cacheMBeans.add(mb);

                if (log.isDebugEnabled())
                    log.debug("Registered cache MBean: " + mb);
            }
            catch (JMException ex) {
                U.error(log, "Failed to register cache MBean.", ex);
            }
        }

        // Internal caches which should not be returned to user.
        for (Map.Entry<String, GridCacheAdapter<?, ?>> e : caches.entrySet()) {
            GridCacheAdapter cache = e.getValue();

            if (!sysCaches.contains(e.getKey()))
                publicProxies.put(e.getKey(), new GridCacheProxyImpl(cache.context(), cache, null));
        }

        transactions = new IgniteTransactionsImpl(sharedCtx);

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
        GridCacheTxManager tm = new GridCacheTxManager();
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

    /** {@inheritDoc} */
    @Override public void addAttributes(Map<String, Object> attrs) throws IgniteCheckedException {
        if (ctx.isDaemon() || F.isEmpty(ctx.config().getCacheConfiguration()))
            return;

        GridCacheAttributes[] attrVals = new GridCacheAttributes[ctx.config().getCacheConfiguration().length];

        Map<String, Boolean> attrPortable = new HashMap<>();

        Map<String, String> interceptors = new HashMap<>();

        int i = 0;

        for (GridCacheConfiguration cfg : ctx.config().getCacheConfiguration()) {
            attrVals[i++] = new GridCacheAttributes(cfg);

            attrPortable.put(CU.mask(cfg.getName()), cfg.isPortableEnabled());

            if (cfg.getInterceptor() != null)
                interceptors.put(cfg.getName(), cfg.getInterceptor().getClass().getName());
        }

        attrs.put(ATTR_CACHE, attrVals);

        attrs.put(ATTR_CACHE_PORTABLE, attrPortable);

        if (!interceptors.isEmpty())
            attrs.put(ATTR_CACHE_INTERCEPTORS, interceptors);
    }

    /**
     * Checks that preload-order-dependant caches has SYNC or ASYNC preloading mode.
     *
     * @param cfgs Caches.
     * @return Maximum detected preload order.
     * @throws IgniteCheckedException If validation failed.
     */
    private int validatePreloadOrder(GridCacheConfiguration[] cfgs) throws IgniteCheckedException {
        int maxOrder = 0;

        for (GridCacheConfiguration cfg : cfgs) {
            int preloadOrder = cfg.getPreloadOrder();

            if (preloadOrder > 0) {
                if (cfg.getCacheMode() == LOCAL)
                    throw new IgniteCheckedException("Preload order set for local cache (fix configuration and restart the " +
                        "node): " + cfg.getName());

                if (cfg.getPreloadMode() == GridCachePreloadMode.NONE)
                    throw new IgniteCheckedException("Only caches with SYNC or ASYNC preload mode can be set as preload " +
                        "dependency for other caches [cacheName=" + cfg.getName() +
                        ", preloadMode=" + cfg.getPreloadMode() + ", preloadOrder=" + cfg.getPreloadOrder() + ']');

                maxOrder = Math.max(maxOrder, preloadOrder);
            }
            else if (preloadOrder < 0)
                throw new IgniteCheckedException("Preload order cannot be negative for cache (fix configuration and restart " +
                    "the node) [cacheName=" + cfg.getName() + ", preloadOrder=" + preloadOrder + ']');
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
            GridCacheConfiguration cfg = cache.configuration();

            if (cfg.getAffinity() instanceof GridCacheConsistentHashAffinityFunction) {
                GridCacheConsistentHashAffinityFunction aff = (GridCacheConsistentHashAffinityFunction)cfg.getAffinity();

                GridCacheAffinityNodeHashResolver hashIdRslvr = aff.getHashIdResolver();

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
        GridCacheAttributes[] rmtAttrs = U.cacheAttributes(rmt);
        GridCacheAttributes[] locAttrs = U.cacheAttributes(ctx.discovery().localNode());

        // If remote or local node does not have cache configured, do nothing
        if (F.isEmpty(rmtAttrs) || F.isEmpty(locAttrs))
            return;

        IgniteDeploymentMode locDepMode = ctx.config().getDeploymentMode();
        IgniteDeploymentMode rmtDepMode = rmt.attribute(GridNodeAttributes.ATTR_DEPLOYMENT_MODE);

        // TODO GG-9141 Check tx configuration consistency.

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

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cachePreloadMode",
                            "Cache preload mode", locAttr.cachePreloadMode(), rmtAttr.cachePreloadMode(), true);

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

                        if (!skipStoreConsistencyCheck(locAttr, rmtAttr))
                            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "store", "Cache store",
                                locAttr.storeClassName(), rmtAttr.storeClassName(), true);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cloner", "Cache cloner",
                            locAttr.clonerClassName(), rmtAttr.clonerClassName(), false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "transactionManagerLookup",
                            "Transaction manager lookup", locAttr.transactionManagerLookupClassName(),
                            rmtAttr.transactionManagerLookupClassName(), false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "atomicSequenceReserveSize",
                            "Atomic sequence reserve size", locAttr.sequenceReserveSize(),
                            rmtAttr.sequenceReserveSize(), false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "defaultLockTimeout",
                            "Default lock timeout", locAttr.defaultLockTimeout(), rmtAttr.defaultLockTimeout(), false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "defaultQueryTimeout",
                            "Default query timeout", locAttr.defaultQueryTimeout(), rmtAttr.defaultQueryTimeout(),
                            false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "defaultTimeToLive",
                            "Default time to live", locAttr.defaultTimeToLive(), rmtAttr.defaultTimeToLive(), false);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "preloadBatchSize",
                            "Preload batch size", locAttr.preloadBatchSize(), rmtAttr.preloadBatchSize(), false);

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

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "storeValueBytes",
                            "Store value bytes", locAttr.storeValueBytes(), rmtAttr.storeValueBytes(), true);

                        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "queryIndexEnabled",
                            "Query index enabled", locAttr.queryIndexEnabled(), rmtAttr.queryIndexEnabled(), true);

                        Boolean locPortableEnabled = U.portableEnabled(ctx.discovery().localNode(), locAttr.cacheName());
                        Boolean rmtPortableEnabled = U.portableEnabled(rmt, locAttr.cacheName());

                        if (locPortableEnabled != null && rmtPortableEnabled != null)
                            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "portableEnabled",
                                "Portables enabled", locPortableEnabled, rmtPortableEnabled, true);

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

                            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "affinityReplicas",
                                "Affinity replicas", locAttr.affinityReplicas(),
                                rmtAttr.affinityReplicas(), true);

                            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "affinityReplicaCountAttrName",
                                "Affinity replica count attribute name", locAttr.affinityReplicaCountAttrName(),
                                rmtAttr.affinityReplicaCountAttrName(), true);

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

        if (!getBoolean(GG_SKIP_CONFIGURATION_CONSISTENCY_CHECK)) {
            for (ClusterNode n : ctx.discovery().remoteNodes())
                checkCache(n);
        }

        for (Map.Entry<String, GridCacheAdapter<?, ?>> e : caches.entrySet()) {
            GridCacheAdapter cache = e.getValue();

            if (maxPreloadOrder > 0) {
                GridCacheConfiguration cfg = cache.configuration();

                int order = cfg.getPreloadOrder();

                if (order > 0 && order != maxPreloadOrder && cfg.getCacheMode() != LOCAL) {
                    GridCompoundFuture<Object, Object> fut = (GridCompoundFuture<Object, Object>)preloadFuts
                        .get(order);

                    if (fut == null) {
                        fut = new GridCompoundFuture<>(ctx);

                        preloadFuts.put(order, fut);
                    }

                    fut.add(cache.preloader().syncFuture());
                }
            }
        }

        for (IgniteFuture<?> fut : preloadFuts.values())
            ((GridCompoundFuture<Object, Object>)fut).markInitialized();

        for (GridCacheSharedManager<?, ?> mgr : sharedCtx.managers())
            mgr.onKernalStart();

        for (GridCacheAdapter<?, ?> cache : caches.values())
            onKernalStart(cache);

        // Wait for caches in SYNC preload mode.
        for (GridCacheAdapter<?, ?> cache : caches.values()) {
            GridCacheConfiguration cfg = cache.configuration();

            if (cfg.getPreloadMode() == SYNC) {
                if (cfg.getCacheMode() == REPLICATED ||
                    (cfg.getCacheMode() == PARTITIONED && cfg.getPreloadPartitionedDelay() >= 0))
                    cache.preloader().syncFuture().get();
            }
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onKernalStop(boolean cancel) {
        if (ctx.config().isDaemon())
            return;

        if (!F.isEmpty(cacheMBeans)) {
            for (ObjectName mb : cacheMBeans) {
                try {
                    mBeanSrv.unregisterMBean(mb);

                    if (log.isDebugEnabled())
                        log.debug("Unregistered cache MBean: " + mb);
                }
                catch (JMException e) {
                    U.error(log, "Failed to unregister cache MBean: " + mb, e);
                }
            }
        }

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

            U.stopLifecycleAware(log, lifecycleAwares(cache.configuration(), ctx.jta().tmLookup()));

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
    @Nullable public IgniteFuture<?> orderedPreloadFuture(int order) {
        Map.Entry<Integer, IgniteFuture<?>> entry = preloadFuts.lowerEntry(order);

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
                    Object key = cctx.marshaller().unmarshal(keyBytes, cctx.shared().deploy().globalLoader());

                    qryMgr.remove(key, keyBytes);
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
    private GridCacheQueryManager queryManager(GridCacheConfiguration cfg) {
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
    public <K, V> GridCache<K, V> utilityCache() {
        return cache(CU.UTILITY_CACHE_NAME);
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

        GridCacheAdapter<K, V> cache = (GridCacheAdapter<K, V>)caches.get(name);

        if (cache == null)
            throw new IllegalArgumentException("Cache is not configured: " + name);

        return new IgniteCacheProxy<>(cache);
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
     * @param leftNodeId Left node ID.
     * @param ldr Class loader.
     */
    public void onUndeployed(@Nullable UUID leftNodeId, ClassLoader ldr) {
        if (!ctx.isStopping())
            for (GridCacheAdapter<?, ?> cache : caches.values())
                cache.onUndeploy(leftNodeId, ldr);
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
     * Creates a wrapped cache store if write-behind cache is configured.
     *
     * @param gridName Grid name.
     * @param cfg Cache configuration.
     * @return Instance if {@link GridCacheWriteBehindStore} if write-behind store is configured,
     *         or user-defined cache store.
     */
    @SuppressWarnings({"unchecked"})
    private GridCacheStore cacheStore(String gridName, GridCacheConfiguration cfg) {
        if (cfg.getStore() == null || !cfg.isWriteBehindEnabled())
            return cfg.getStore();

        GridCacheWriteBehindStore store = new GridCacheWriteBehindStore(gridName, cfg.getName(), log,
            cfg.getStore());

        store.setFlushSize(cfg.getWriteBehindFlushSize());
        store.setFlushThreadCount(cfg.getWriteBehindFlushThreadCount());
        store.setFlushFrequency(cfg.getWriteBehindFlushFrequency());
        store.setBatchSize(cfg.getWriteBehindBatchSize());

        return store;
    }

    /**
     * @param ccfg Cache configuration.
     * @param objs Extra components.
     * @return Components provided in cache configuration which can implement {@link LifecycleAware} interface.
     */
    private Iterable<Object> lifecycleAwares(GridCacheConfiguration ccfg, Object...objs) {
        Collection<Object> ret = new ArrayList<>(7 + objs.length);

        ret.add(ccfg.getAffinity());
        ret.add(ccfg.getAffinityMapper());
        ret.add(ccfg.getCloner());
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
    private static class LocalAffinityFunction implements GridCacheAffinityFunction {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(GridCacheAffinityFunctionContext affCtx) {
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

