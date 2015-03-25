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
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.affinity.*;
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
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.jdk.*;
import org.apache.ignite.spi.*;
import org.jetbrains.annotations.*;

import javax.cache.configuration.*;
import javax.cache.integration.*;
import javax.management.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.configuration.CacheConfiguration.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.configuration.DeploymentMode.*;
import static org.apache.ignite.internal.IgniteComponentType.*;
import static org.apache.ignite.internal.IgniteNodeAttributes.*;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Cache processor.
 */
@SuppressWarnings("unchecked")
public class GridCacheProcessor extends GridProcessorAdapter {
    /** Null cache name. */
    private static final String NULL_NAME = U.id8(UUID.randomUUID());

    /** Shared cache context. */
    private GridCacheSharedContext<?, ?> sharedCtx;

    /** */
    private final Map<String, GridCacheAdapter<?, ?>> caches;

    /** Map of proxies. */
    private final Map<String, IgniteCacheProxy<?, ?>> jCacheProxies;

    /** Map of preload finish futures grouped by preload order. */
    private final NavigableMap<Integer, IgniteInternalFuture<?>> preloadFuts;

    /** Maximum detected rebalance order. */
    private int maxRebalanceOrder;

    /** System cache names. */
    private final Set<String> sysCaches;

    /** Caches stop sequence. */
    private final Deque<String> stopSeq;

    /** Transaction interface implementation. */
    private IgniteTransactionsImpl transactions;

    /** Pending cache starts. */
    private ConcurrentMap<String, IgniteInternalFuture> pendingFuts = new ConcurrentHashMap<>();

    /** Dynamic caches. */
    private ConcurrentMap<String, DynamicCacheDescriptor> registeredCaches = new ConcurrentHashMap<>();

    /** */
    private IdentityHashMap<CacheStore, ThreadLocal> sesHolders = new IdentityHashMap<>();

    /** Must use JDK marshaller since it is used by discovery to fire custom events. */
    private Marshaller marshaller = new JdkMarshaller();

    /**
     * @param ctx Kernal context.
     */
    public GridCacheProcessor(GridKernalContext ctx) {
        super(ctx);

        caches = new ConcurrentHashMap<>();
        jCacheProxies = new ConcurrentHashMap<>();
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

        if (cfg.getNodeFilter() == null)
            cfg.setNodeFilter(CacheConfiguration.SERVER_NODES);

        if (cfg.getAffinity() == null) {
            if (cfg.getCacheMode() == PARTITIONED) {
                RendezvousAffinityFunction aff = new RendezvousAffinityFunction();

                aff.setHashIdResolver(new AffinityNodeAddressHashResolver());

                cfg.setAffinity(aff);
            }
            else if (cfg.getCacheMode() == REPLICATED) {
                RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false, 512);

                aff.setHashIdResolver(new AffinityNodeAddressHashResolver());

                cfg.setAffinity(aff);

                cfg.setBackups(Integer.MAX_VALUE);
            }
            else
                cfg.setAffinity(new LocalAffinityFunction());
        }
        else {
            if (cfg.getCacheMode() == PARTITIONED) {
                if (cfg.getAffinity() instanceof RendezvousAffinityFunction) {
                    RendezvousAffinityFunction aff = (RendezvousAffinityFunction)cfg.getAffinity();

                    if (aff.getHashIdResolver() == null)
                        aff.setHashIdResolver(new AffinityNodeAddressHashResolver());
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
            Factory<CacheWriter> writerFactory = cfg.isWriteThrough() ? cfg.getCacheWriterFactory() : null;

            if (ldrFactory != null || writerFactory != null)
                cfg.setCacheStoreFactory(new GridCacheLoaderWriterStoreFactory(ldrFactory, writerFactory));
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
            perf.add("Disable near cache (set 'nearConfiguration' to null)", cfg.getNearConfiguration() == null);

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
            if (cc.getAffinity() instanceof FairAffinityFunction)
                throw new IgniteCheckedException("REPLICATED cache can not be started with FairAffinityFunction" +
                    " [cacheName=" + cc.getName() + ']');

            if (cc.getAffinity() instanceof RendezvousAffinityFunction) {
                RendezvousAffinityFunction aff = (RendezvousAffinityFunction)cc.getAffinity();

                if (aff.isExcludeNeighbors())
                    throw new IgniteCheckedException("For REPLICATED cache flag 'excludeNeighbors' in " +
                        "RendezvousAffinityFunction cannot be set [cacheName=" + cc.getName() + ']');
            }

            if (cc.getNearConfiguration() != null &&
                ctx.discovery().cacheAffinityNode(ctx.discovery().localNode(), cc.getName())) {
                U.warn(log, "Near cache cannot be used with REPLICATED cache, " +
                    "will be ignored [cacheName=" + cc.getName() + ']');

                cc.setNearConfiguration(null);
            }
        }

        if (cc.getCacheMode() == LOCAL && !cc.getAffinity().getClass().equals(LocalAffinityFunction.class))
            U.warn(log, "AffinityFunction configuration parameter will be ignored for local cache [cacheName=" +
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
            if (GridQueryProcessor.isEnabled(cc))
                throw new IgniteCheckedException("Cannot have query indexing enabled while values are stored off-heap. " +
                    "You must either disable query indexing or disable off-heap values only flag for cache: " +
                    cc.getName());
        }

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
    private void prepare(CacheConfiguration cfg, Collection<Object> objs) throws IgniteCheckedException {
        prepare(cfg, cfg.getEvictionPolicy(), false);
        prepare(cfg, cfg.getAffinity(), false);
        prepare(cfg, cfg.getAffinityMapper(), false);
        prepare(cfg, cfg.getEvictionFilter(), false);
        prepare(cfg, cfg.getInterceptor(), false);

        NearCacheConfiguration nearCfg = cfg.getNearConfiguration();

        if (nearCfg != null)
            prepare(cfg, nearCfg.getNearEvictionPolicy(), true);

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
        cleanup(cfg, cfg.getAffinity(), false);
        cleanup(cfg, cfg.getAffinityMapper(), false);
        cleanup(cfg, cctx.jta().tmLookup(), false);
        cleanup(cfg, cctx.store().configuredStore(), false);

        NearCacheConfiguration nearCfg = cfg.getNearConfiguration();

        if (nearCfg != null)
            cleanup(cfg, nearCfg.getNearEvictionPolicy(), true);

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

        ctx.discovery().setCustomEventListener(new GridPlainInClosure<Serializable>() {
            @Override public void apply(Serializable evt) {
                if (evt instanceof DynamicCacheChangeBatch)
                    onCacheChangeRequested((DynamicCacheChangeBatch)evt);
            }
        });

        // Internal caches which should not be returned to user.
        FileSystemConfiguration[] igfsCfgs = ctx.grid().configuration().getFileSystemConfiguration();

        if (igfsCfgs != null) {
            for (FileSystemConfiguration igfsCfg : igfsCfgs) {
                sysCaches.add(maskNull(igfsCfg.getMetaCacheName()));
                sysCaches.add(maskNull(igfsCfg.getDataCacheName()));
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

        for (int i = 0; i < cfgs.length; i++) {
            checkSerializable(cfgs[i]);

            CacheConfiguration<?, ?> cfg = new CacheConfiguration(cfgs[i]);

            CacheObjectContext cacheObjCtx = ctx.cacheObjects().contextForCache(null, cfg.getName(), cfg);

            // Initialize defaults.
            initialize(cfg, cacheObjCtx);

            cfgs[i] = cfg; // Replace original configuration value.

            String masked = maskNull(cfg.getName());

            if (registeredCaches.containsKey(masked)) {
                String cacheName = cfg.getName();

                if (cacheName != null)
                    throw new IgniteCheckedException("Duplicate cache name found (check configuration and " +
                        "assign unique name to each cache): " + cacheName);
                else
                    throw new IgniteCheckedException("Default cache has already been configured (check configuration and " +
                        "assign unique name to each cache).");
            }

            DynamicCacheDescriptor desc = new DynamicCacheDescriptor(cfg, IgniteUuid.randomUuid());

            desc.locallyConfigured(true);
            desc.staticallyConfigured(true);

            registeredCaches.put(masked, desc);

            ctx.discovery().setCacheFilter(
                cfg.getName(),
                cfg.getNodeFilter(),
                cfg.getNearConfiguration() != null,
                cfg.getCacheMode() == LOCAL);

            if (sysCaches.contains(maskNull(cfg.getName())))
                stopSeq.addLast(cfg.getName());
            else
                stopSeq.addFirst(cfg.getName());
        }

        // Start shared managers.
        for (GridCacheSharedManager mgr : sharedCtx.managers())
            mgr.start(sharedCtx);

        transactions = new IgniteTransactionsImpl(sharedCtx);

        if (log.isDebugEnabled())
            log.debug("Started cache processor.");
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onKernalStart() throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        ClusterNode locNode = ctx.discovery().localNode();

        if (!getBoolean(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK)) {
            for (ClusterNode n : ctx.discovery().remoteNodes()) {
                checkTransactionConfiguration(n);

                DeploymentMode locDepMode = ctx.config().getDeploymentMode();
                DeploymentMode rmtDepMode = n.attribute(IgniteNodeAttributes.ATTR_DEPLOYMENT_MODE);

                CU.checkAttributeMismatch(log, null, n.id(), "deploymentMode", "Deployment mode",
                    locDepMode, rmtDepMode, true);

                for (DynamicCacheDescriptor desc : registeredCaches.values()) {
                    CacheConfiguration rmtCfg = desc.remoteConfiguration(n.id());

                    if (rmtCfg != null)
                        checkCache(desc.cacheConfiguration(), rmtCfg, n);
                }
            }
        }

        // Start dynamic caches received from collect discovery data.
        for (DynamicCacheDescriptor desc : registeredCaches.values()) {
            boolean started = desc.onStart();

            assert started : "Failed to change started flag for locally configured cache: " + desc;

            desc.clearRemoteConfigurations();

            CacheConfiguration ccfg = desc.cacheConfiguration();

            IgnitePredicate filter = ccfg.getNodeFilter();

            if (filter.apply(locNode)) {
                CacheObjectContext cacheObjCtx = ctx.cacheObjects().contextForCache(null, ccfg.getName(), ccfg);

                GridCacheContext ctx = createCache(ccfg, cacheObjCtx);

                ctx.dynamicDeploymentId(desc.deploymentId());

                sharedCtx.addCacheContext(ctx);

                GridCacheAdapter cache = ctx.cache();

                String name = ccfg.getName();

                caches.put(maskNull(name), cache);

                startCache(cache);

                jCacheProxies.put(maskNull(name), new IgniteCacheProxy(ctx, cache, null, false));
            }
        }

        marshallerCache().context().preloader().syncFuture().listen(new CI1<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> f) {
                ctx.marshallerContext().onMarshallerCacheReady(ctx);
            }
        });

        // Must call onKernalStart on shared managers after creation of fetched caches.
        for (GridCacheSharedManager<?, ?> mgr : sharedCtx.managers())
            mgr.onKernalStart();

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
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        for (String cacheName : stopSeq) {
            GridCacheAdapter<?, ?> cache = caches.remove(maskNull(cacheName));

            if (cache != null)
                stopCache(cache, cancel);
        }

        for (GridCacheAdapter<?, ?> cache : caches.values())
            stopCache(cache, cancel);

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
     * Blocks all available gateways
     */
    public void blockGateways() {
        for (IgniteCacheProxy<?, ?> proxy : jCacheProxies.values())
            proxy.gate().onStopped();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onKernalStop(boolean cancel) {
        if (ctx.config().isDaemon())
            return;

        for (String cacheName : stopSeq) {
            GridCacheAdapter<?, ?> cache = caches.get(maskNull(cacheName));

            if (cache != null)
                onKernalStop(cache, cancel);
        }

        for (Map.Entry<String, GridCacheAdapter<?, ?>> entry : caches.entrySet()) {
            if (!stopSeq.contains(entry.getKey()))
                onKernalStop(entry.getValue(), cancel);
        }

        List<? extends GridCacheSharedManager<?, ?>> sharedMgrs = sharedCtx.managers();

        for (ListIterator<? extends GridCacheSharedManager<?, ?>> it = sharedMgrs.listIterator(sharedMgrs.size());
            it.hasPrevious();) {
            GridCacheSharedManager<?, ?> mgr = it.previous();

            mgr.onKernalStop(cancel);
        }
    }

    /**
     * @param cache Cache to start.
     * @throws IgniteCheckedException If failed to start cache.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
    private void startCache(GridCacheAdapter<?, ?> cache) throws IgniteCheckedException {
        GridCacheContext<?, ?> cacheCtx = cache.context();

        ctx.query().onCacheStart(cacheCtx);
        ctx.continuous().onCacheStart(cacheCtx);

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

        cacheCtx.onStarted();

        if (log.isInfoEnabled())
            log.info("Started cache [name=" + cfg.getName() + ", mode=" + cfg.getCacheMode() + ']');
    }

    /**
     * @param cache Cache to stop.
     * @param cancel Cancel flag.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
    private void stopCache(GridCacheAdapter<?, ?> cache, boolean cancel) {
        GridCacheContext ctx = cache.context();

        sharedCtx.removeCacheContext(ctx);

        cache.stop();

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

        ctx.kernalContext().query().onCacheStop(ctx);
        ctx.kernalContext().continuous().onCacheStop(ctx);

        U.stopLifecycleAware(log, lifecycleAwares(cache.configuration(), ctx.jta().tmLookup(),
            ctx.store().configuredStore()));

        if (log.isInfoEnabled())
            log.info("Stopped cache: " + cache.name());

        cleanup(ctx);
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

        if (ctx.events().isRecordable(EventType.EVT_CACHE_STARTED))
            ctx.events().addEvent(EventType.EVT_CACHE_STARTED);

        if (log.isDebugEnabled())
            log.debug("Executed onKernalStart() callback for cache [name=" + cache.name() + ", mode=" +
                cache.configuration().getCacheMode() + ']');
    }

    /**
     * @param cache Cache to stop.
     * @param cancel Cancel flag.
     */
    @SuppressWarnings("unchecked")
    private void onKernalStop(GridCacheAdapter<?, ?> cache, boolean cancel) {
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

        if (ctx.events().isRecordable(EventType.EVT_CACHE_STOPPED))
            ctx.events().addEvent(EventType.EVT_CACHE_STOPPED);
    }

    /**
     * @param cfg Cache configuration to use to create cache.
     * @return Cache context.
     * @throws IgniteCheckedException If failed to create cache.
     */
    @SuppressWarnings({"unchecked"})
    private GridCacheContext createCache(CacheConfiguration<?, ?> cfg, CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        assert cfg != null;

        CacheStore cfgStore = cfg.getCacheStoreFactory() != null ? cfg.getCacheStoreFactory().create() : null;

        validate(ctx.config(), cfg, cfgStore);

        CacheJtaManagerAdapter jta = JTA.create(cfg.getTransactionManagerLookupClassName() == null);

        jta.createTmLookup(cfg);

        // Skip suggestions for system caches.
        if (!sysCaches.contains(maskNull(cfg.getName())))
            suggestOptimizations(cfg, cfgStore != null);

        Collection<Object> toPrepare = new ArrayList<>();

        toPrepare.add(jta.tmLookup());

        if (cfgStore instanceof GridCacheLoaderWriterStore) {
            toPrepare.add(((GridCacheLoaderWriterStore)cfgStore).loader());
            toPrepare.add(((GridCacheLoaderWriterStore)cfgStore).writer());
        }
        else
            toPrepare.add(cfgStore);

        prepare(cfg, toPrepare);

        U.startLifecycleAware(lifecycleAwares(cfg, jta.tmLookup(), cfgStore));

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
            ctx.discovery().cacheAffinityNode(ctx.discovery().localNode(), cfg.getName()),

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
                            cache = cacheCtx.affinityNode() ?
                                new GridDhtColocatedCache(cacheCtx) :
                                new GridDhtColocatedCache(cacheCtx, new GridNoStorageCacheMap(cacheCtx));

                            break;
                        }
                        case ATOMIC: {
                            cache = cacheCtx.affinityNode() ?
                                new GridDhtAtomicCache(cacheCtx) :
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

        GridCacheContext<?, ?> ret = cacheCtx;

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
                ctx.discovery().cacheAffinityNode(ctx.discovery().localNode(), cfg.getName()),

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

                    GridDhtCache dhtCache = cacheCtx.affinityNode() ?
                        new GridDhtCache(cacheCtx) :
                        new GridDhtCache(cacheCtx, new GridNoStorageCacheMap(cacheCtx));

                    dhtCache.near(near);

                    near.dht(dhtCache);

                    dht = dhtCache;

                    break;
                }
                case ATOMIC: {
                    assert cache instanceof GridNearAtomicCache;

                    GridNearAtomicCache near = (GridNearAtomicCache)cache;

                    GridDhtAtomicCache dhtCache = cacheCtx.affinityNode() ?
                        new GridDhtAtomicCache(cacheCtx) :
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

        return ret;
    }

    /**
     * Gets a collection of currentlty started caches.
     *
     * @return Collection of started cache names.
     */
    public Collection<String> cacheNames() {
        return F.viewReadOnly(registeredCaches.keySet(),
            new IgniteClosure<String, String>() {
                @Override public String apply(String s) {
                    return unmaskNull(s);
                }
            });
    }

    /**
     * Gets cache mode.
     *
     * @param cacheName Cache name to check.
     * @return Cache mode.
     */
    public CacheMode cacheMode(String cacheName) {
        DynamicCacheDescriptor desc = registeredCaches.get(maskNull(cacheName));

        return desc != null ? desc.cacheConfiguration().getCacheMode() : null;
    }

    /**
     * @param req Request to check.
     * @return {@code True} if change request was registered to apply.
     */
    @SuppressWarnings("IfMayBeConditional")
    public boolean dynamicCacheRegistered(DynamicCacheChangeRequest req) {
        DynamicCacheDescriptor desc = registeredCaches.get(maskNull(req.cacheName()));

        if (desc != null) {
            if (desc.deploymentId().equals(req.deploymentId())) {
                if (req.start())
                    return !desc.cancelled();
                else
                    return desc.cancelled();
            }

            // If client requested cache start
            if (req.initiatingNodeId() != null)
                return true;
        }

        return false;
    }

    /**
     * @param reqs Requests to start.
     * @throws IgniteCheckedException If failed to start cache.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public void prepareCachesStart(
        Collection<DynamicCacheChangeRequest> reqs,
        AffinityTopologyVersion topVer
    ) throws IgniteCheckedException {
        for (DynamicCacheChangeRequest req : reqs) {
            assert req.start();

            prepareCacheStart(
                req.startCacheConfiguration(),
                req.nearCacheConfiguration(),
                req.clientStartOnly(),
                req.initiatingNodeId(),
                req.deploymentId(),
                topVer
            );
        }

        // Start statically configured caches received from remote nodes during exchange.
        for (DynamicCacheDescriptor desc : registeredCaches.values()) {
            if (desc.staticallyConfigured() && !desc.locallyConfigured()) {
                if (desc.onStart()) {
                    prepareCacheStart(
                        desc.cacheConfiguration(),
                        null,
                        false,
                        null,
                        desc.deploymentId(),
                        topVer
                    );
                }
            }
        }
    }

    /**
     * @param cfg Start configuration.
     * @param nearCfg Near configuration.
     * @param clientStartOnly Client only start request.
     * @param initiatingNodeId Initiating node ID.
     * @param deploymentId Deployment ID.
     */
    private void prepareCacheStart(
        CacheConfiguration cfg,
        NearCacheConfiguration nearCfg,
        boolean clientStartOnly,
        UUID initiatingNodeId,
        IgniteUuid deploymentId,
        AffinityTopologyVersion topVer
    ) throws IgniteCheckedException {
        CacheConfiguration ccfg = new CacheConfiguration(cfg);

        IgnitePredicate nodeFilter = ccfg.getNodeFilter();

        ClusterNode locNode = ctx.discovery().localNode();

        boolean affNodeStart = !clientStartOnly && nodeFilter.apply(locNode);
        boolean clientNodeStart = locNode.id().equals(initiatingNodeId);

        if (sharedCtx.cacheContext(CU.cacheId(cfg.getName())) != null)
            return;

        if (affNodeStart || clientNodeStart) {
            if (clientNodeStart && !affNodeStart) {
                if (nearCfg != null)
                    ccfg.setNearConfiguration(nearCfg);
            }

            CacheObjectContext cacheObjCtx = ctx.cacheObjects().contextForCache(null, ccfg.getName(), ccfg);

            GridCacheContext cacheCtx = createCache(ccfg, cacheObjCtx);

            cacheCtx.startTopologyVersion(topVer);

            cacheCtx.dynamicDeploymentId(deploymentId);

            sharedCtx.addCacheContext(cacheCtx);

            caches.put(maskNull(cacheCtx.name()), cacheCtx.cache());

            startCache(cacheCtx.cache());
            onKernalStart(cacheCtx.cache());
        }
    }

    /**
     * @param req Stop request.
     */
    public void blockGateway(DynamicCacheChangeRequest req) {
        assert req.stop();

        // Break the proxy before exchange future is done.
        IgniteCacheProxy<?, ?> proxy = jCacheProxies.get(maskNull(req.cacheName()));

        if (proxy != null)
            proxy.gate().block();
    }

    /**
     * @param req Request.
     */
    private void stopGateway(DynamicCacheChangeRequest req) {
        assert req.stop();

        // Break the proxy before exchange future is done.
        IgniteCacheProxy<?, ?> proxy = jCacheProxies.remove(maskNull(req.cacheName()));

        if (proxy != null)
            proxy.gate().onStopped();
    }

    /**
     * @param req Stop request.
     */
    public void prepareCacheStop(DynamicCacheChangeRequest req) {
        assert req.stop();

        GridCacheAdapter<?, ?> cache = caches.remove(maskNull(req.cacheName()));

        if (cache != null) {
            GridCacheContext<?, ?> ctx = cache.context();

            sharedCtx.removeCacheContext(ctx);

            assert req.deploymentId().equals(ctx.dynamicDeploymentId()) : "Different deployment IDs [req=" + req +
                ", ctxDepId=" + ctx.dynamicDeploymentId() + ']';

            onKernalStop(cache, true);
            stopCache(cache, true);
        }
    }

    /**
     * Callback invoked when first exchange future for dynamic cache is completed.
     *
     * @param topVer Completed topology version.
     * @param reqs Change requests.
     */
    @SuppressWarnings("unchecked")
    public void onExchangeDone(
        AffinityTopologyVersion topVer,
        Collection<DynamicCacheChangeRequest> reqs,
        Throwable err
    ) {
        for (GridCacheAdapter<?, ?> cache : caches.values()) {
            GridCacheContext<?, ?> cacheCtx = cache.context();

            if (F.eq(cacheCtx.startTopologyVersion(), topVer)) {
                cacheCtx.preloader().onInitialExchangeComplete(err);

                String masked = maskNull(cacheCtx.name());

                jCacheProxies.put(masked, new IgniteCacheProxy(cache.context(), cache, null, false));
            }
        }

        if (!F.isEmpty(reqs) && err == null) {
            for (DynamicCacheChangeRequest req : reqs) {
                String masked = maskNull(req.cacheName());

                if (req.stop()) {
                    stopGateway(req);

                    prepareCacheStop(req);

                    DynamicCacheDescriptor desc = registeredCaches.get(masked);

                    if (desc != null && desc.cancelled() && desc.deploymentId().equals(req.deploymentId()))
                        registeredCaches.remove(masked, desc);
                }

                completeStartFuture(req);
            }
        }
    }

    /**
     * @param req Request to complete future for.
     */
    public void completeStartFuture(DynamicCacheChangeRequest req) {
        DynamicCacheStartFuture fut = (DynamicCacheStartFuture)pendingFuts.get(maskNull(req.cacheName()));

        assert req.deploymentId() != null;
        assert fut == null || fut.deploymentId != null;

        if (fut != null && fut.deploymentId().equals(req.deploymentId()) &&
            F.eq(req.initiatingNodeId(), ctx.localNodeId()))
            fut.onDone();
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

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return DiscoveryDataExchangeType.CACHE_PROC;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object collectDiscoveryData(UUID nodeId) {
        // Collect dynamically started caches to a single object.
        Collection<DynamicCacheChangeRequest> reqs = new ArrayList<>(registeredCaches.size());

        for (DynamicCacheDescriptor desc : registeredCaches.values()) {
            if (!desc.cancelled()) {
                DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(desc.cacheConfiguration().getName(), null);

                req.startCacheConfiguration(desc.cacheConfiguration());

                req.deploymentId(desc.deploymentId());

                reqs.add(req);
            }
        }

        DynamicCacheChangeBatch req = new DynamicCacheChangeBatch(reqs);

        req.clientNodes(ctx.discovery().clientNodesMap());

        return req;
    }

    /** {@inheritDoc} */
    @Override public void onDiscoveryDataReceived(UUID joiningNodeId, UUID rmtNodeId, Object data) {
        if (data instanceof DynamicCacheChangeBatch) {
            DynamicCacheChangeBatch batch = (DynamicCacheChangeBatch)data;

            for (DynamicCacheChangeRequest req : batch.requests()) {
                DynamicCacheDescriptor existing = registeredCaches.get(maskNull(req.cacheName()));

                if (req.start() && !req.clientStartOnly()) {
                    CacheConfiguration ccfg = req.startCacheConfiguration();

                    if (existing != null) {
                        if (existing.locallyConfigured()) {
                            existing.deploymentId(req.deploymentId());

                            existing.addRemoteConfiguration(rmtNodeId, req.startCacheConfiguration());

                            ctx.discovery().setCacheFilter(
                                req.cacheName(),
                                ccfg.getNodeFilter(),
                                ccfg.getNearConfiguration() != null,
                                ccfg.getCacheMode() == LOCAL);
                        }
                    }
                    else {
                        DynamicCacheDescriptor desc = new DynamicCacheDescriptor(
                            ccfg,
                            req.deploymentId());

                        // Received statically configured cache.
                        if (req.initiatingNodeId() == null)
                            desc.staticallyConfigured(true);

                        registeredCaches.put(maskNull(req.cacheName()), desc);

                        ctx.discovery().setCacheFilter(
                            req.cacheName(),
                            ccfg.getNodeFilter(),
                            ccfg.getNearConfiguration() != null,
                            ccfg.getCacheMode() == LOCAL);
                    }
                }
            }

            if (!F.isEmpty(batch.clientNodes())) {
                for (Map.Entry<String, Map<UUID, Boolean>> entry : batch.clientNodes().entrySet()) {
                    String cacheName = entry.getKey();

                    for (Map.Entry<UUID, Boolean> tup : entry.getValue().entrySet())
                        ctx.discovery().addClientNode(cacheName, tup.getKey(), tup.getValue());
                }
            }
        }
    }

    /**
     * Dynamically starts cache.
     *
     * @param ccfg Cache configuration.
     * @return Future that will be completed when cache is deployed.
     */
    @SuppressWarnings("IfMayBeConditional")
    public IgniteInternalFuture<?> dynamicStartCache(
        @Nullable CacheConfiguration ccfg,
        String cacheName,
        @Nullable NearCacheConfiguration nearCfg,
        boolean failIfExists
    ) {
        assert ccfg != null || nearCfg != null;

        DynamicCacheDescriptor desc = registeredCaches.get(maskNull(cacheName));

        DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(cacheName, ctx.localNodeId());

        req.failIfExists(failIfExists);

        if (ccfg != null) {
            if (desc != null && !desc.cancelled()) {
                if (failIfExists)
                    return new GridFinishedFuture<>(new CacheExistsException("Failed to start cache " +
                            "(a cache with the same name is already started): " + cacheName));
                else {
                    CacheConfiguration descCfg = desc.cacheConfiguration();

                    // Check if we were asked to start a near cache.
                    if (nearCfg != null) {
                        if (descCfg.getNodeFilter().apply(ctx.discovery().localNode())) {
                            // If we are on a data node and near cache was enabled, return success, else - fail.
                            if (descCfg.getNearConfiguration() != null)
                                return new GridFinishedFuture<>();
                            else
                                return new GridFinishedFuture<>(new IgniteCheckedException("Failed to start near " +
                                        "cache (local node is an affinity node for cache): " + cacheName));
                        }
                        else
                            // If local node has near cache, return success.
                            req.clientStartOnly(true);
                    }
                    else
                        return new GridFinishedFuture<>();

                    req.deploymentId(desc.deploymentId());

                    req.startCacheConfiguration(descCfg);
                }
            }
            else {
                req.deploymentId(IgniteUuid.randomUuid());

                try {
                    CacheConfiguration cfg = new CacheConfiguration(ccfg);

                    CacheObjectContext cacheObjCtx = ctx.cacheObjects().contextForCache(null, cfg.getName(), cfg);

                    initialize(cfg, cacheObjCtx);

                    req.startCacheConfiguration(cfg);
                } catch (IgniteCheckedException e) {
                    return new GridFinishedFuture(e);
                }
            }
        }
        else {
            req.clientStartOnly(true);

            if (desc != null && !desc.cancelled())
                ccfg = desc.cacheConfiguration();

            if (ccfg == null)
                return new GridFinishedFuture<>(new CacheExistsException("Failed to start near cache " +
                    "(a cache with the given name is not started): " + cacheName));

            if (ccfg.getNodeFilter().apply(ctx.discovery().localNode())) {
                if (ccfg.getNearConfiguration() != null)
                    return new GridFinishedFuture<>();
                else
                    return new GridFinishedFuture<>(new IgniteCheckedException("Failed to start near cache " +
                        "(local node is an affinity node for cache): " + cacheName));
            }

            req.deploymentId(desc.deploymentId());
            req.startCacheConfiguration(ccfg);
        }

        if (nearCfg != null)
            req.nearCacheConfiguration(nearCfg);

        return F.first(initiateCacheChanges(F.asList(req)));
    }

    /**
     * @param cacheName Cache name to stop.
     * @return Future that will be completed when cache is stopped.
     */
    public IgniteInternalFuture<?> dynamicStopCache(String cacheName) {
        DynamicCacheChangeRequest t = new DynamicCacheChangeRequest(cacheName, ctx.localNodeId(), true);

        return F.first(initiateCacheChanges(F.asList(t)));
    }

    /**
     * @param reqs Requests.
     * @return Collection of futures.
     */
    public Collection<DynamicCacheStartFuture> initiateCacheChanges(Collection<DynamicCacheChangeRequest> reqs) {
        Collection<DynamicCacheStartFuture> res = new ArrayList<>(reqs.size());

        Collection<DynamicCacheChangeRequest> sendReqs = new ArrayList<>(reqs.size());

        for (DynamicCacheChangeRequest req : reqs) {
            DynamicCacheStartFuture fut = new DynamicCacheStartFuture(req.cacheName(), req.deploymentId(), req);

            try {
                if (req.stop()) {
                    DynamicCacheDescriptor desc = registeredCaches.get(maskNull(req.cacheName()));

                    if (desc == null)
                        // No-op.
                        fut.onDone();
                    else {
                        IgniteUuid dynamicDeploymentId = desc.deploymentId();

                        assert dynamicDeploymentId != null;

                        // Save deployment ID to avoid concurrent stops.
                        req.deploymentId(dynamicDeploymentId);
                        fut.deploymentId = dynamicDeploymentId;
                    }
                }

                if (fut.isDone())
                    continue;

                DynamicCacheStartFuture old = (DynamicCacheStartFuture)pendingFuts.putIfAbsent(
                    maskNull(req.cacheName()), fut);

                if (old != null) {
                    if (req.start() && !req.clientStartOnly()) {
                        fut.onDone(new CacheExistsException("Failed to start cache " +
                            "(a cache with the same name is already being started or stopped): " + req.cacheName()));
                    }
                    else {
                        fut = old;

                        continue;
                    }
                }

                if (fut.isDone())
                    continue;

                sendReqs.add(req);
            }
            catch (Exception e) {
                fut.onDone(e);
            }
            finally {
                res.add(fut);
            }
        }

        if (!sendReqs.isEmpty())
            ctx.discovery().sendCustomEvent(new DynamicCacheChangeBatch(sendReqs));

        return res;
    }

    /**
     * Callback invoked from discovery thread when cache deployment request is received.
     *
     * @param batch Change request batch.
     */
    private void onCacheChangeRequested(DynamicCacheChangeBatch batch) {
        for (DynamicCacheChangeRequest req : batch.requests()) {
            DynamicCacheDescriptor desc = registeredCaches.get(maskNull(req.cacheName()));

            if (req.start()) {
                CacheConfiguration ccfg = req.startCacheConfiguration();

                DynamicCacheStartFuture startFut = (DynamicCacheStartFuture)pendingFuts.get(
                    maskNull(ccfg.getName()));

                // Check if cache with the same name was concurrently started form different node.
                if (desc != null) {
                    if (!req.clientStartOnly() && req.failIfExists()) {
                        // If local node initiated start, fail the start future.
                        if (startFut != null && startFut.deploymentId().equals(req.deploymentId())) {
                            startFut.onDone(new CacheExistsException("Failed to start cache " +
                                "(a cache with the same name is already started): " + ccfg.getName()));
                        }

                        return;
                    }

                    req.clientStartOnly(true);
                }
                else {
                    if (req.clientStartOnly()) {
                        if (startFut != null && startFut.deploymentId().equals(req.deploymentId())) {
                            startFut.onDone(new IgniteCheckedException("Failed to start client cache " +
                                "(a cache with the given name is not started): " + ccfg.getName()));
                        }

                        return;
                    }
                }

                if (!req.clientStartOnly() && desc == null) {
                    DynamicCacheDescriptor startDesc = new DynamicCacheDescriptor(ccfg, req.deploymentId());

                    DynamicCacheDescriptor old = registeredCaches.put(maskNull(ccfg.getName()), startDesc);

                    assert old == null :
                        "Dynamic cache map was concurrently modified [new=" + startDesc + ", old=" + old + ']';

                    ctx.discovery().setCacheFilter(
                        ccfg.getName(),
                        ccfg.getNodeFilter(),
                        ccfg.getNearConfiguration() != null,
                        ccfg.getCacheMode() == LOCAL);
                }

                ctx.discovery().addClientNode(req.cacheName(), req.initiatingNodeId(), req.nearCacheConfiguration() != null);
            }
            else {
                if (desc == null) {
                    // If local node initiated start, fail the start future.
                    DynamicCacheStartFuture changeFut = (DynamicCacheStartFuture)pendingFuts.get(maskNull(req.cacheName()));

                    if (changeFut != null && changeFut.deploymentId().equals(req.deploymentId())) {
                        // No-op.
                        changeFut.onDone();
                    }

                    return;
                }

                desc.onCancelled();

                ctx.discovery().removeCacheFilter(req.cacheName());
            }
        }
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
        for (DynamicCacheDescriptor desc : registeredCaches.values()) {
            CacheConfiguration cfg = desc.cacheConfiguration();

            if (cfg.getAffinity() instanceof RendezvousAffinityFunction) {
                RendezvousAffinityFunction aff = (RendezvousAffinityFunction)cfg.getAffinity();

                AffinityNodeHashResolver hashIdRslvr = aff.getHashIdResolver();

                assert hashIdRslvr != null;

                Object nodeHashObj = hashIdRslvr.resolve(node);

                for (ClusterNode topNode : ctx.discovery().allNodes()) {
                    Object topNodeHashObj = hashIdRslvr.resolve(topNode);

                    if (nodeHashObj.hashCode() == topNodeHashObj.hashCode()) {
                        String errMsg = "Failed to add node to topology because it has the same hash code for " +
                            "partitioned affinity as one of existing nodes [cacheName=" + cfg.getName() +
                            ", hashIdResolverClass=" + hashIdRslvr.getClass().getName() +
                            ", existingNodeId=" + topNode.id() + ']';

                        String sndMsg = "Failed to add node to topology because it has the same hash code for " +
                            "partitioned affinity as one of existing nodes [cacheName=" + cfg.getName() +
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
     * Checks that remote caches has configuration compatible with the local.
     *
     * @param rmtNode Remote node.
     * @throws IgniteCheckedException If check failed.
     */
    private void checkCache(CacheConfiguration locCfg, CacheConfiguration rmtCfg, ClusterNode rmtNode)
        throws IgniteCheckedException {
        ClusterNode locNode = ctx.discovery().localNode();

        UUID rmt = rmtNode.id();

        GridCacheAttributes rmtAttr = new GridCacheAttributes(rmtCfg);
        GridCacheAttributes locAttr = new GridCacheAttributes(locCfg);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cacheMode", "Cache mode",
            locAttr.cacheMode(), rmtAttr.cacheMode(), true);

        if (rmtAttr.cacheMode() != LOCAL) {
            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "interceptor", "Cache Interceptor",
                locAttr.interceptorClassName(), rmtAttr.interceptorClassName(), true);

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "atomicityMode",
                "Cache atomicity mode", locAttr.atomicityMode(), rmtAttr.atomicityMode(), true);

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cachePreloadMode",
                "Cache preload mode", locAttr.cacheRebalanceMode(), rmtAttr.cacheRebalanceMode(), true);

            if (locCfg.getAtomicityMode() == TRANSACTIONAL ||
                (rmtCfg.getNodeFilter().apply(rmtNode) && locCfg.getNodeFilter().apply(locNode)))
                CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "storeFactory", "Store factory",
                    locAttr.storeFactoryClassName(), rmtAttr.storeFactoryClassName(), true);

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

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "transactionManagerLookup",
                "Transaction manager lookup", locAttr.transactionManagerLookupClassName(),
                rmtAttr.transactionManagerLookupClassName(), false);

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "defaultLockTimeout",
                "Default lock timeout", locAttr.defaultLockTimeout(), rmtAttr.defaultLockTimeout(), false);

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "defaultTimeToLive",
                "Default time to live", locAttr.defaultTimeToLive(), rmtAttr.defaultTimeToLive(), false);

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "preloadBatchSize",
                "Preload batch size", locAttr.rebalanceBatchSize(), rmtAttr.rebalanceBatchSize(), false);

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

            if (locAttr.cacheMode() == PARTITIONED) {
                CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "evictSynchronized",
                    "Eviction synchronized", locAttr.evictSynchronized(), rmtAttr.evictSynchronized(),
                    true);

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
     * @param cfg Cache configuration.
     * @param rmtNode Remote node.
     * @param locNode Local node.
     * @return {@code True} if store check should be skipped.
     */
    private boolean checkStoreConsistency(CacheConfiguration cfg, ClusterNode rmtNode, ClusterNode locNode) {
        return
            // In atomic mode skip check if either local or remote node is client.
            cfg.getAtomicityMode() == ATOMIC &&
                (!ctx.discovery().cacheAffinityNode(rmtNode, cfg.getName()) ||
                 !ctx.discovery().cacheAffinityNode(locNode, cfg.getName()));
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
     * @param valBytes Value bytes.
     */
    @SuppressWarnings( {"unchecked"})
    public void onEvictFromSwap(String spaceName, byte[] keyBytes, byte[] valBytes) {
        assert spaceName != null;
        assert keyBytes != null;
        assert valBytes != null;

        /*
         * NOTE: this method should not have any synchronization because
         * it is called from synchronization block within Swap SPI.
         */

        GridCacheAdapter cache = caches.get(maskNull(CU.cacheNameForSwapSpaceName(spaceName)));

        assert cache != null : "Failed to resolve cache name for swap space name: " + spaceName;

        GridCacheContext cctx = cache.configuration().getCacheMode() == PARTITIONED ?
            ((GridNearCacheAdapter<?, ?>)cache).dht().context() : cache.context();

        if (spaceName.equals(CU.swapSpaceName(cctx))) {
            GridCacheQueryManager qryMgr = cctx.queries();

            if (qryMgr != null) {
                try {
                    KeyCacheObject key = cctx.toCacheKeyObject(keyBytes);

                    GridCacheSwapEntry swapEntry = GridCacheSwapEntryImpl.unmarshal(valBytes);

                    CacheObject val = swapEntry.value();

                    if (val == null)
                        val = cctx.cacheObjects().toCacheObject(cctx.cacheObjectContext(), swapEntry.type(),
                            swapEntry.valueBytes());

                    assert val != null;

                    qryMgr.remove(key.value(cctx.cacheObjectContext(), false),
                        val.value(cctx.cacheObjectContext(), false));
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

        IgniteCacheProxy<K, V> jcache = (IgniteCacheProxy<K, V>)jCacheProxies.get(maskNull(name));

        return jcache == null ? null : jcache.legacyProxy();
    }

    /**
     * @return All configured cache instances.
     */
    public Collection<GridCache<?, ?>> caches() {
        return F.viewReadOnly(jCacheProxies.values(), new IgniteClosure<IgniteCacheProxy<?, ?>, GridCache<?, ?>>() {
            @Override public GridCache<?, ?> apply(IgniteCacheProxy<?, ?> entries) {
                return entries.legacyProxy();
            }
        });
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

        IgniteCacheProxy<K, V> jcache = (IgniteCacheProxy<K, V>)jCacheProxies.get(maskNull(name));

        if (jcache == null)
            throw new IllegalArgumentException("Cache is not configured: " + name);

        return jcache.legacyProxy();
    }

    /**
     * @param cacheName Cache name.
     * @param <K> type of keys.
     * @param <V> type of values.
     * @return Cache instance for given name.
     */
    @SuppressWarnings("unchecked")
    public <K, V> IgniteCache<K, V> publicJCache(@Nullable String cacheName) {
        if (log.isDebugEnabled())
            log.debug("Getting public cache for name: " + cacheName);

        if (sysCaches.contains(maskNull(cacheName)))
            throw new IllegalStateException("Failed to get cache because it is a system cache: " + cacheName);

        try {
            String masked = maskNull(cacheName);

            IgniteCache<K,V> cache = (IgniteCache<K, V>)jCacheProxies.get(masked);

            if (cache == null) {
                DynamicCacheDescriptor desc = registeredCaches.get(masked);

                if (desc == null || desc.cancelled())
                    throw new IllegalArgumentException("Cache is not started: " + cacheName);

                DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(cacheName, ctx.localNodeId());

                req.cacheName(cacheName);

                req.deploymentId(desc.deploymentId());

                CacheConfiguration cfg = new CacheConfiguration(desc.cacheConfiguration());

                cfg.setNearConfiguration(null);

                req.startCacheConfiguration(cfg);

                req.clientStartOnly(true);

                F.first(initiateCacheChanges(F.asList(req))).get();

                cache = (IgniteCache<K, V>)jCacheProxies.get(masked);

                if (cache == null)
                    throw new IllegalArgumentException("Cache is not started: " + cacheName);
            }

            return cache;
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
    }

    /**
     * @param name Cache name.
     * @return Cache instance for given name.
     */
    @SuppressWarnings("unchecked")
    public <K, V> IgniteCacheProxy<K, V> jcache(@Nullable String name) {
        IgniteCacheProxy<K, V> cache = (IgniteCacheProxy<K, V>)jCacheProxies.get(maskNull(name));

        if (cache == null)
            throw new IllegalArgumentException("Cache is not configured: " + name);

        return cache;
    }

    /**
     * @return All configured public cache instances.
     */
    public Collection<IgniteCacheProxy<?, ?>> publicCaches() {
        Collection<IgniteCacheProxy<?, ?>> res = new ArrayList<>(jCacheProxies.size());

        for (Map.Entry<String, IgniteCacheProxy<?, ?>> entry : jCacheProxies.entrySet()) {
            if (!sysCaches.contains(entry.getKey()))
                res.add(entry.getValue());
        }

        return res;
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

        return (GridCacheAdapter<K, V>)caches.get(maskNull(name));
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
        return sysCaches.contains(maskNull(name));
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
        ret.add(ccfg.getInterceptor());

        NearCacheConfiguration nearCfg = ccfg.getNearConfiguration();

        if (nearCfg != null)
            ret.add(nearCfg.getNearEvictionPolicy());

        Collections.addAll(ret, objs);

        return ret;
    }

    /**
     * @param val Object to check.
     */
    private void checkSerializable(CacheConfiguration val) throws IgniteCheckedException {
        if (val == null)
            return;

        try {
            marshaller.unmarshal(marshaller.marshal(val), val.getClass().getClassLoader());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Failed to validate cache configuration " +
                "(make sure all objects in cache configuration are serializable): " + val.getName(), e);
        }
    }

    /**
     * @param name Name to mask.
     * @return Masked name.
     */
    private static String maskNull(String name) {
        return name == null ? NULL_NAME : name;
    }

    /**
     * @param name Name to unmask.
     * @return Unmasked name.
     */
    @SuppressWarnings("StringEquality")
    private static String unmaskNull(String name) {
        // Intentional identity equality.
        return name == NULL_NAME ? null : name;
    }

    /**
     *
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    private class DynamicCacheStartFuture extends GridFutureAdapter<Object> {
        /** Start ID. */
        @GridToStringInclude
        private IgniteUuid deploymentId;

        /** Cache name. */
        private String cacheName;

        /** Change request. */
        @GridToStringInclude
        private DynamicCacheChangeRequest req;

        /**
         */
        private DynamicCacheStartFuture(String cacheName, IgniteUuid deploymentId, DynamicCacheChangeRequest req) {
            this.deploymentId = deploymentId;
            this.cacheName = cacheName;
            this.req = req;
        }

        /**
         * @return Start ID.
         */
        public IgniteUuid deploymentId() {
            return deploymentId;
        }

        /**
         * @return Request.
         */
        public DynamicCacheChangeRequest request() {
            return req;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Object res, @Nullable Throwable err) {
            // Make sure to remove future before completion.
            pendingFuts.remove(maskNull(cacheName), this);

            return super.onDone(res, err);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DynamicCacheStartFuture.class, this);
        }
    }

    /**
     *
     */
    private static class LocalAffinityFunction implements AffinityFunction {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
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

