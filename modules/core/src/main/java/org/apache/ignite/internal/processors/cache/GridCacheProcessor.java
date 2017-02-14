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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.management.JMException;
import javax.management.MBeanServer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.AffinityNodeAddressHashResolver;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.suggestions.GridPerformanceSuggestions;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.IgniteTransactionsEx;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.datastructures.CacheDataStructuresManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtColocatedCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearAtomicCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTransactionalCache;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrManager;
import org.apache.ignite.internal.processors.cache.jta.CacheJtaManagerAdapter;
import org.apache.ignite.internal.processors.cache.local.GridLocalCache;
import org.apache.ignite.internal.processors.cache.local.atomic.GridLocalAtomicCache;
import org.apache.ignite.internal.processors.cache.query.GridCacheDistributedQueryManager;
import org.apache.ignite.internal.processors.cache.query.GridCacheLocalQueryManager;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager;
import org.apache.ignite.internal.processors.cache.store.CacheStoreManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTransactionsImpl;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.processors.plugin.CachePluginManager;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.configuration.CacheConfiguration.DFLT_CACHE_MODE;
import static org.apache.ignite.configuration.CacheConfiguration.DFLT_MEMORY_MODE;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.configuration.DeploymentMode.ISOLATED;
import static org.apache.ignite.configuration.DeploymentMode.PRIVATE;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.internal.IgniteComponentType.JTA;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CONSISTENCY_CHECK_SKIPPED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_TX_CONFIG;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isNearEnabled;

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

    /** Caches stopped from onKernalStop callback. */
    private final Map<String, GridCacheAdapter> stoppedCaches = new ConcurrentHashMap<>();

    /** Map of proxies. */
    private final Map<String, IgniteCacheProxy<?, ?>> jCacheProxies;

    /** Caches stop sequence. */
    private final Deque<String> stopSeq;

    /** Transaction interface implementation. */
    private IgniteTransactionsImpl transactions;

    /** Pending cache starts. */
    private ConcurrentMap<String, IgniteInternalFuture> pendingFuts = new ConcurrentHashMap<>();

    /** Template configuration add futures. */
    private ConcurrentMap<String, IgniteInternalFuture> pendingTemplateFuts = new ConcurrentHashMap<>();

    /** Dynamic caches. */
    private ConcurrentMap<String, DynamicCacheDescriptor> registeredCaches = new ConcurrentHashMap<>();

    /** Cache templates. */
    private ConcurrentMap<String, DynamicCacheDescriptor> registeredTemplates = new ConcurrentHashMap<>();

    /** */
    private IdentityHashMap<CacheStore, ThreadLocal> sesHolders = new IdentityHashMap<>();

    /** Must use JDK marsh since it is used by discovery to fire custom events. */
    private final Marshaller marsh;

    /** Count down latch for caches. */
    private final CountDownLatch cacheStartedLatch = new CountDownLatch(1);

    /** */
    private Map<String, DynamicCacheDescriptor> cachesOnDisconnect;

    /** */
    private Map<UUID, DynamicCacheChangeBatch> clientReconnectReqs;

    /**
     * @param ctx Kernal context.
     */
    public GridCacheProcessor(GridKernalContext ctx) {
        super(ctx);

        caches = new ConcurrentHashMap<>();
        jCacheProxies = new ConcurrentHashMap<>();
        stopSeq = new LinkedList<>();

        marsh = MarshallerUtils.jdkMarshaller(ctx.gridName());
    }

    /**
     * @param cfg Initializes cache configuration with proper defaults.
     * @param cacheObjCtx Cache object context.
     * @throws IgniteCheckedException If configuration is not valid.
     */
    private void initialize(CacheConfiguration cfg, CacheObjectContext cacheObjCtx)
        throws IgniteCheckedException {
        if (cfg.getCacheMode() == null)
            cfg.setCacheMode(DFLT_CACHE_MODE);

        if (cfg.getMemoryMode() == null)
            cfg.setMemoryMode(DFLT_MEMORY_MODE);

        if (cfg.getNodeFilter() == null)
            cfg.setNodeFilter(CacheConfiguration.ALL_NODES);

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
            if (cfg.getCacheMode() != LOCAL) {
                if (cfg.getAffinity() instanceof RendezvousAffinityFunction) {
                    RendezvousAffinityFunction aff = (RendezvousAffinityFunction)cfg.getAffinity();

                    if (aff.getHashIdResolver() == null)
                        aff.setHashIdResolver(new AffinityNodeAddressHashResolver());
                }
            }
            else if (cfg.getCacheMode() == LOCAL && !(cfg.getAffinity() instanceof LocalAffinityFunction)) {
                cfg.setAffinity(new LocalAffinityFunction());

                U.warn(log, "AffinityFunction configuration parameter will be ignored for local cache" +
                    " [cacheName=" + U.maskName(cfg.getName()) + ']');
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
            cfg.setAtomicityMode(CacheConfiguration.DFLT_CACHE_ATOMICITY_MODE);

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
                    "cacheName=" + U.maskName(cfg.getName()) + ']');
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
                    "for cache: " + U.maskName(cfg.getName()));

            if (cfg.getCacheWriterFactory() != null)
                throw new IgniteCheckedException("Cannot set both cache writer factory and cache store factory " +
                    "for cache: " + U.maskName(cfg.getName()));
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
                perf.add("Decrease number of backups (set 'backups' to 0)", cfg.getBackups() == 0);
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
     * @param c Ignite configuration.
     * @param cc Configuration to validate.
     * @param cacheType Cache type.
     * @param cfgStore Cache store.
     * @throws IgniteCheckedException If failed.
     */
    private void validate(IgniteConfiguration c,
        CacheConfiguration cc,
        CacheType cacheType,
        @Nullable CacheStore cfgStore) throws IgniteCheckedException {
        if (cc.getCacheMode() == REPLICATED) {
            if (cc.getNearConfiguration() != null &&
                ctx.discovery().cacheAffinityNode(ctx.discovery().localNode(), cc.getName())) {
                U.warn(log, "Near cache cannot be used with REPLICATED cache, " +
                    "will be ignored [cacheName=" + U.maskName(cc.getName()) + ']');

                cc.setNearConfiguration(null);
            }
        }

        if (cc.getCacheMode() == LOCAL && !cc.getAffinity().getClass().equals(LocalAffinityFunction.class))
            U.warn(log, "AffinityFunction configuration parameter will be ignored for local cache [cacheName=" +
                U.maskName(cc.getName()) + ']');

        if (cc.getAffinity().partitions() > CacheConfiguration.MAX_PARTITIONS_COUNT)
            throw new IgniteCheckedException("Cannot have more than " + CacheConfiguration.MAX_PARTITIONS_COUNT +
                " partitions [cacheName=" + cc.getName() + ", partitions=" + cc.getAffinity().partitions() + ']');

        if (cc.getRebalanceMode() != CacheRebalanceMode.NONE)
            assertParameter(cc.getRebalanceBatchSize() > 0, "rebalanceBatchSize > 0");

        if (cc.getCacheMode() == PARTITIONED || cc.getCacheMode() == REPLICATED) {
            if (cc.getAtomicityMode() == ATOMIC && cc.getWriteSynchronizationMode() == FULL_ASYNC)
                U.warn(log, "Cache write synchronization mode is set to FULL_ASYNC. All single-key 'put' and " +
                    "'remove' operations will return 'null', all 'putx' and 'removex' operations will return" +
                    " 'true' [cacheName=" + U.maskName(cc.getName()) + ']');
        }

        DeploymentMode depMode = c.getDeploymentMode();

        if (c.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED) &&
            !CU.isSystemCache(cc.getName()) && !(c.getMarshaller() instanceof BinaryMarshaller))
            throw new IgniteCheckedException("Cache can be started in PRIVATE or ISOLATED deployment mode only when" +
                " BinaryMarshaller is used [depMode=" + ctx.config().getDeploymentMode() + ", marshaller=" +
                c.getMarshaller().getClass().getName() + ']');

        if (cc.isWriteBehindEnabled()) {
            if (cfgStore == null)
                throw new IgniteCheckedException("Cannot enable write-behind (writer or store is not provided) " +
                    "for cache: " + U.maskName(cc.getName()));

            assertParameter(cc.getWriteBehindBatchSize() > 0, "writeBehindBatchSize > 0");
            assertParameter(cc.getWriteBehindFlushSize() >= 0, "writeBehindFlushSize >= 0");
            assertParameter(cc.getWriteBehindFlushFrequency() >= 0, "writeBehindFlushFrequency >= 0");
            assertParameter(cc.getWriteBehindFlushThreadCount() > 0, "writeBehindFlushThreadCount > 0");

            if (cc.getWriteBehindFlushSize() == 0 && cc.getWriteBehindFlushFrequency() == 0)
                throw new IgniteCheckedException("Cannot set both 'writeBehindFlushFrequency' and " +
                    "'writeBehindFlushSize' parameters to 0 for cache: " + U.maskName(cc.getName()));
        }

        if (cc.isReadThrough() && cfgStore == null)
            throw new IgniteCheckedException("Cannot enable read-through (loader or store is not provided) " +
                "for cache: " + U.maskName(cc.getName()));

        if (cc.isWriteThrough() && cfgStore == null)
            throw new IgniteCheckedException("Cannot enable write-through (writer or store is not provided) " +
                "for cache: " + U.maskName(cc.getName()));

        long delay = cc.getRebalanceDelay();

        if (delay != 0) {
            if (cc.getCacheMode() != PARTITIONED)
                U.warn(log, "Rebalance delay is supported only for partitioned caches (will ignore): " + (cc.getName()),
                    "Will ignore rebalance delay for cache: " + U.maskName(cc.getName()));
            else if (cc.getRebalanceMode() == SYNC) {
                if (delay < 0) {
                    U.warn(log, "Ignoring SYNC rebalance mode with manual rebalance start (node will not wait for " +
                        "rebalancing to be finished): " + U.maskName(cc.getName()),
                        "Node will not wait for rebalance in SYNC mode: " + U.maskName(cc.getName()));
                }
                else {
                    U.warn(log,
                        "Using SYNC rebalance mode with rebalance delay (node will wait until rebalancing is " +
                            "initiated for " + delay + "ms) for cache: " + U.maskName(cc.getName()),
                            "Node will wait until rebalancing is initiated for " + delay + "ms for cache: " + U.maskName(cc.getName()));
                }
            }
        }

        ctx.igfsHelper().validateCacheConfiguration(cc);

        switch (cc.getMemoryMode()) {
            case OFFHEAP_VALUES: {
                if (cacheType.userCache() && cc.getEvictionPolicy() == null && cc.getOffHeapMaxMemory() >= 0)
                    U.quietAndWarn(log, "Off heap maximum memory configuration property will be ignored for the " +
                        "cache working in OFFHEAP_VALUES mode (memory usage will be unlimited): " +
                        U.maskName(cc.getName()) + ". Consider configuring eviction policy or switching to " +
                        "OFFHEAP_TIERED mode or.");

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
                if (cacheType.userCache() && cc.getEvictionPolicy() == null && cc.getOffHeapMaxMemory() >= 0)
                    U.quietAndWarn(log, "Eviction policy not enabled with ONHEAP_TIERED mode for cache " +
                        "(entries will not be moved to off-heap store): " + U.maskName(cc.getName()));

                break;

            default:
                throw new IllegalStateException("Unknown memory mode: " + cc.getMemoryMode());
        }

        if (cc.getMemoryMode() == CacheMemoryMode.OFFHEAP_VALUES) {
            if (GridQueryProcessor.isEnabled(cc))
                throw new IgniteCheckedException("Cannot have query indexing enabled while values are stored off-heap. " +
                    "You must either disable query indexing or disable off-heap values only flag for cache: " +
                    U.maskName(cc.getName()));
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
            return F.asList(ctx.queries(), ctx.continuousQueries(), ctx.store());
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
        prepare(cfg, cfg.getTopologyValidator(), false);

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
        cleanup(cfg, cfg.getEvictionFilter(), false);
        cleanup(cfg, cfg.getInterceptor(), false);
        cleanup(cfg, cfg.getTopologyValidator(), false);
        cleanup(cfg, cctx.store().configuredStore(), false);

        if (!CU.isUtilityCache(cfg.getName()) && !CU.isSystemCache(cfg.getName())) {
            unregisterMbean(cctx.cache().localMxBean(), cfg.getName(), false);
            unregisterMbean(cctx.cache().clusterMxBean(), cfg.getName(), false);
        }

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
    @SuppressWarnings({"unchecked"})
    @Override public void start() throws IgniteCheckedException {
        DeploymentMode depMode = ctx.config().getDeploymentMode();

        if (!F.isEmpty(ctx.config().getCacheConfiguration())) {
            if (depMode != CONTINUOUS && depMode != SHARED)
                U.warn(log, "Deployment mode for cache is not CONTINUOUS or SHARED " +
                    "(it is recommended that you change deployment mode and restart): " + depMode,
                    "Deployment mode for cache is not CONTINUOUS or SHARED.");
        }

        Set<String> internalCaches = internalCachesNames();

        CacheConfiguration[] cfgs = ctx.config().getCacheConfiguration();

        sharedCtx = createSharedContext(ctx, CU.startStoreSessionListeners(ctx,
            ctx.config().getCacheStoreSessionListenerFactories()));

        for (int i = 0; i < cfgs.length; i++) {
            if (ctx.config().isDaemon() && !CU.isMarshallerCache(cfgs[i].getName()))
                continue;

            cloneCheckSerializable(cfgs[i]);

            CacheConfiguration<?, ?> cfg = new CacheConfiguration(cfgs[i]);

            CacheObjectContext cacheObjCtx = ctx.cacheObjects().contextForCache(cfg);

            // Initialize defaults.
            initialize(cfg, cacheObjCtx);

            cfgs[i] = cfg; // Replace original configuration value.

            String masked = maskNull(cfg.getName());

            if (registeredCaches.containsKey(masked)) {
                String cacheName = cfg.getName();

                if (cacheName != null)
                    throw new IgniteCheckedException("Duplicate cache name found (check configuration and " +
                        "assign unique name to each cache): " + U.maskName(cacheName));
                else
                    throw new IgniteCheckedException("Default cache has already been configured (check configuration and " +
                        "assign unique name to each cache).");
            }

            CacheType cacheType;

            if (CU.isUtilityCache(cfg.getName()))
                cacheType = CacheType.UTILITY;
            else if (CU.isMarshallerCache(cfg.getName()))
                cacheType = CacheType.MARSHALLER;
            else if (internalCaches.contains(maskNull(cfg.getName())))
                cacheType = CacheType.INTERNAL;
            else
                cacheType = CacheType.USER;

            boolean template = cfg.getName() != null && cfg.getName().endsWith("*");

            DynamicCacheDescriptor desc = new DynamicCacheDescriptor(ctx, cfg, cacheType, template,
                IgniteUuid.randomUuid());

            desc.locallyConfigured(true);
            desc.staticallyConfigured(true);
            desc.receivedFrom(ctx.localNodeId());

            if (!template) {
                registeredCaches.put(masked, desc);

                ctx.discovery().setCacheFilter(
                    cfg.getName(),
                    cfg.getNodeFilter(),
                    cfg.getNearConfiguration() != null && cfg.getCacheMode() == PARTITIONED,
                    cfg.getCacheMode());

                ctx.discovery().addClientNode(cfg.getName(),
                    ctx.localNodeId(),
                    cfg.getNearConfiguration() != null);

                if (!cacheType.userCache())
                    stopSeq.addLast(cfg.getName());
                else
                    stopSeq.addFirst(cfg.getName());
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Use cache configuration as template: " + cfg);

                registeredTemplates.put(masked, desc);
            }

            if (cfg.getName() == null) { // Use cache configuration with null name as template.
                DynamicCacheDescriptor desc0 =
                    new DynamicCacheDescriptor(ctx, cfg, cacheType, true, IgniteUuid.randomUuid());

                desc0.locallyConfigured(true);
                desc0.staticallyConfigured(true);

                registeredTemplates.put(masked, desc0);
            }
        }

        // Start shared managers.
        for (GridCacheSharedManager mgr : sharedCtx.managers())
            mgr.start(sharedCtx);

        transactions = new IgniteTransactionsImpl(sharedCtx);

        if (log.isDebugEnabled())
            log.debug("Started cache processor.");
    }

    /**
     * @return Internal caches names.
     */
    private Set<String> internalCachesNames() {
        // Internal caches which should not be returned to user.
        Set<String> internalCaches = new HashSet<>();

        FileSystemConfiguration[] igfsCfgs = ctx.grid().configuration().getFileSystemConfiguration();

        if (igfsCfgs != null) {
            for (FileSystemConfiguration igfsCfg : igfsCfgs) {
                internalCaches.add(maskNull(igfsCfg.getMetaCacheName()));
                internalCaches.add(maskNull(igfsCfg.getDataCacheName()));
            }
        }

        if (IgniteComponentType.HADOOP.inClassPath())
            internalCaches.add(CU.SYS_CACHE_HADOOP_MR);

        internalCaches.add(CU.ATOMICS_CACHE_NAME);

        return internalCaches;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onKernalStart() throws IgniteCheckedException {
        ClusterNode locNode = ctx.discovery().localNode();

        try {
            if (!ctx.config().isDaemon() && !getBoolean(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK)) {
                for (ClusterNode n : ctx.discovery().remoteNodes()) {
                    if (n.attribute(ATTR_CONSISTENCY_CHECK_SKIPPED))
                        continue;

                    checkTransactionConfiguration(n);

                    DeploymentMode locDepMode = ctx.config().getDeploymentMode();
                    DeploymentMode rmtDepMode = n.attribute(IgniteNodeAttributes.ATTR_DEPLOYMENT_MODE);

                    CU.checkAttributeMismatch(log, null, n.id(), "deploymentMode", "Deployment mode",
                        locDepMode, rmtDepMode, true);

                    for (DynamicCacheDescriptor desc : registeredCaches.values()) {
                        CacheConfiguration rmtCfg = desc.remoteConfiguration(n.id());

                        if (rmtCfg != null) {
                            CacheConfiguration locCfg = desc.cacheConfiguration();

                            checkCache(locCfg, rmtCfg, n);

                            // Check plugin cache configurations.
                            CachePluginManager pluginMgr = desc.pluginManager();

                            pluginMgr.validateRemotes(rmtCfg, n);
                        }
                    }
                }
            }

            // Start dynamic caches received from collect discovery data.
            for (DynamicCacheDescriptor desc : registeredCaches.values()) {
                if (ctx.config().isDaemon() && !CU.isMarshallerCache(desc.cacheConfiguration().getName()))
                    continue;

                desc.clearRemoteConfigurations();

                CacheConfiguration ccfg = desc.cacheConfiguration();

                IgnitePredicate filter = ccfg.getNodeFilter();

                boolean loc = desc.locallyConfigured();

                if (loc || (desc.receivedOnDiscovery() && CU.affinityNode(locNode, filter))) {
                    boolean started = desc.onStart();

                    assert started : "Failed to change started flag for locally configured cache: " + desc;

                    CacheObjectContext cacheObjCtx = ctx.cacheObjects().contextForCache(ccfg);

                    CachePluginManager pluginMgr = desc.pluginManager();

                    GridCacheContext ctx = createCache(
                        ccfg, pluginMgr, desc.cacheType(), cacheObjCtx, desc.updatesAllowed());

                    ctx.dynamicDeploymentId(desc.deploymentId());

                    sharedCtx.addCacheContext(ctx);

                    GridCacheAdapter cache = ctx.cache();

                    String name = ccfg.getName();

                    caches.put(maskNull(name), cache);

                    startCache(cache);

                    jCacheProxies.put(maskNull(name), new IgniteCacheProxy(ctx, cache, null, false));
                }
            }
        }
        finally {
            cacheStartedLatch.countDown();
        }

        // Must call onKernalStart on shared managers after creation of fetched caches.
        for (GridCacheSharedManager<?, ?> mgr : sharedCtx.managers())
            mgr.onKernalStart(false);

        for (GridCacheAdapter<?, ?> cache : caches.values())
            onKernalStart(cache);

        ctx.marshallerContext().onMarshallerCacheStarted(ctx);

        if (!ctx.config().isDaemon())
            ctx.cacheObjects().onUtilityCacheStarted();

        ctx.service().onUtilityCacheStarted();

        // Wait for caches in SYNC preload mode.
        for (DynamicCacheDescriptor desc : registeredCaches.values()) {
            CacheConfiguration cfg = desc.cacheConfiguration();

            IgnitePredicate filter = cfg.getNodeFilter();

            if (desc.locallyConfigured() || desc.receivedOnDiscovery() && CU.affinityNode(locNode, filter)) {
                GridCacheAdapter cache = caches.get(maskNull(cfg.getName()));

                if (cache != null) {
                    if (cfg.getRebalanceMode() == SYNC) {
                        CacheMode cacheMode = cfg.getCacheMode();

                        if (cacheMode == REPLICATED || (cacheMode == PARTITIONED && cfg.getRebalanceDelay() >= 0))
                            cache.preloader().syncFuture().get();
                    }
                }
            }
        }

        assert caches.containsKey(CU.MARSH_CACHE_NAME) : "Marshaller cache should be started";
        assert ctx.config().isDaemon() || caches.containsKey(CU.UTILITY_CACHE_NAME) : "Utility cache should be started";

        if (!ctx.clientNode() && !ctx.isDaemon())
            addRemovedItemsCleanupTask(Long.getLong(IGNITE_CACHE_REMOVED_ENTRIES_TTL, 10_000));

    }

    /**
     * @param timeout Cleanup timeout.
     */
    private void addRemovedItemsCleanupTask(long timeout) {
        ctx.timeout().addTimeoutObject(new RemovedItemsCleanupTask(timeout));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        for (String cacheName : stopSeq) {
            GridCacheAdapter<?, ?> cache = stoppedCaches.remove(maskNull(cacheName));

            if (cache != null)
                stopCache(cache, cancel);
        }

        for (GridCacheAdapter<?, ?> cache : stoppedCaches.values()) {
            if (cache == stoppedCaches.remove(maskNull(cache.name())))
                stopCache(cache, cancel);
        }

        List<? extends GridCacheSharedManager<?, ?>> mgrs = sharedCtx.managers();

        for (ListIterator<? extends GridCacheSharedManager<?, ?>> it = mgrs.listIterator(mgrs.size()); it.hasPrevious();) {
            GridCacheSharedManager<?, ?> mgr = it.previous();

            mgr.stop(cancel);
        }

        CU.stopStoreSessionListeners(ctx, sharedCtx.storeSessionListeners());

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
        cacheStartedLatch.countDown();

        GridCachePartitionExchangeManager<Object, Object> exch = context().exchange();

        // Stop exchange manager first so that we call onKernalStop on all caches.
        // No new caches should be added after this point.
        exch.onKernalStop(cancel);

        for (GridCacheAdapter<?, ?> cache : caches.values()) {
            GridCacheAffinityManager aff = cache.context().affinity();

            if (aff != null)
                aff.cancelFutures();
        }

        for (String cacheName : stopSeq) {
            GridCacheAdapter<?, ?> cache = caches.remove(maskNull(cacheName));

            if (cache != null) {
                stoppedCaches.put(maskNull(cacheName), cache);

                onKernalStop(cache, cancel);
            }
        }

        for (Map.Entry<String, GridCacheAdapter<?, ?>> entry : caches.entrySet()) {
            GridCacheAdapter<?, ?> cache = entry.getValue();

            if (cache == caches.remove(entry.getKey())) {
                stoppedCaches.put(entry.getKey(), cache);

                onKernalStop(entry.getValue(), cancel);
            }
        }

        cancelFutures();

        List<? extends GridCacheSharedManager<?, ?>> sharedMgrs = sharedCtx.managers();

        for (ListIterator<? extends GridCacheSharedManager<?, ?>> it = sharedMgrs.listIterator(sharedMgrs.size());
            it.hasPrevious();) {
            GridCacheSharedManager<?, ?> mgr = it.previous();

            if (mgr != exch)
                mgr.onKernalStop(cancel);
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        cachesOnDisconnect = new HashMap<>(registeredCaches);

        IgniteClientDisconnectedCheckedException err = new IgniteClientDisconnectedCheckedException(
            ctx.cluster().clientReconnectFuture(),
            "Failed to execute dynamic cache change request, client node disconnected.");

        for (IgniteInternalFuture fut : pendingFuts.values())
            ((GridFutureAdapter)fut).onDone(err);

        for (IgniteInternalFuture fut : pendingTemplateFuts.values())
            ((GridFutureAdapter)fut).onDone(err);

        for (GridCacheAdapter cache : caches.values()) {
            GridCacheContext cctx = cache.context();

            cctx.gate().onDisconnected(reconnectFut);

            List<GridCacheManager> mgrs = cache.context().managers();

            for (ListIterator<GridCacheManager> it = mgrs.listIterator(mgrs.size()); it.hasPrevious();) {
                GridCacheManager mgr = it.previous();

                mgr.onDisconnected(reconnectFut);
            }
        }

        sharedCtx.onDisconnected(reconnectFut);

        registeredCaches.clear();

        registeredTemplates.clear();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) throws IgniteCheckedException {
        List<GridCacheAdapter> reconnected = new ArrayList<>(caches.size());

        GridCompoundFuture<?, ?> stopFut = null;

        for (final GridCacheAdapter cache : caches.values()) {
            String name = cache.name();

            boolean stopped;

            boolean sysCache = CU.isMarshallerCache(name) || CU.isUtilityCache(name) || CU.isAtomicsCache(name);

            if (!sysCache) {
                DynamicCacheDescriptor oldDesc = cachesOnDisconnect.get(maskNull(name));

                assert oldDesc != null : "No descriptor for cache: " + name;

                DynamicCacheDescriptor newDesc = registeredCaches.get(maskNull(name));

                stopped = newDesc == null || !oldDesc.deploymentId().equals(newDesc.deploymentId());
            }
            else
                stopped = false;

            if (stopped) {
                cache.context().gate().reconnected(true);

                sharedCtx.removeCacheContext(cache.ctx);

                caches.remove(maskNull(cache.name()));
                jCacheProxies.remove(maskNull(cache.name()));

                IgniteInternalFuture<?> fut = ctx.closure().runLocalSafe(new Runnable() {
                    @Override public void run() {
                        onKernalStop(cache, true);
                        stopCache(cache, true);
                    }
                });

                if (stopFut == null)
                    stopFut = new GridCompoundFuture<>();

                stopFut.add((IgniteInternalFuture)fut);
            }
            else {
                cache.onReconnected();

                reconnected.add(cache);
            }
        }

        if (clientReconnectReqs != null) {
            for (Map.Entry<UUID, DynamicCacheChangeBatch> e : clientReconnectReqs.entrySet())
                processClientReconnectData(e.getKey(), e.getValue());

            clientReconnectReqs = null;
        }

        sharedCtx.onReconnected();

        for (GridCacheAdapter cache : reconnected)
            cache.context().gate().reconnected(false);

        cachesOnDisconnect = null;

        if (stopFut != null)
            stopFut.markInitialized();

        return stopFut;
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

        // Intentionally compare Boolean references using '!=' below to check if the flag has been explicitly set.
        if (cfg.isStoreKeepBinary() && cfg.isStoreKeepBinary() != CacheConfiguration.DFLT_STORE_KEEP_BINARY
            && !(ctx.config().getMarshaller() instanceof BinaryMarshaller))
            U.warn(log, "CacheConfiguration.isStoreKeepBinary() configuration property will be ignored because " +
                "BinaryMarshaller is not used");

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
            log.info("Started cache [name=" + U.maskName(cfg.getName()) + ", mode=" + cfg.getCacheMode() + ']');
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

        U.stopLifecycleAware(log, lifecycleAwares(cache.configuration(), ctx.store().configuredStore()));

        if (log.isInfoEnabled())
            log.info("Stopped cache: " + cache.name());

        cleanup(ctx);
    }

    /**
     * @throws IgniteCheckedException If failed to wait.
     */
    public void awaitStarted() throws IgniteCheckedException {
        U.await(cacheStartedLatch);
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
     * @param pluginMgr Cache plugin manager.
     * @param cacheType Cache type.
     * @param cacheObjCtx Cache object context.
     * @param updatesAllowed Updates allowed flag.
     * @return Cache context.
     * @throws IgniteCheckedException If failed to create cache.
     */
    private GridCacheContext createCache(CacheConfiguration<?, ?> cfg,
        @Nullable CachePluginManager pluginMgr,
        CacheType cacheType,
        CacheObjectContext cacheObjCtx,
        boolean updatesAllowed)
        throws IgniteCheckedException
    {
        assert cfg != null;

        if (cfg.getCacheStoreFactory() instanceof GridCacheLoaderWriterStoreFactory) {
            GridCacheLoaderWriterStoreFactory factory = (GridCacheLoaderWriterStoreFactory)cfg.getCacheStoreFactory();

            prepare(cfg, factory.loaderFactory(), false);
            prepare(cfg, factory.writerFactory(), false);
        }
        else
            prepare(cfg, cfg.getCacheStoreFactory(), false);

        CacheStore cfgStore = cfg.getCacheStoreFactory() != null ? cfg.getCacheStoreFactory().create() : null;

        validate(ctx.config(), cfg, cacheType, cfgStore);

        if (pluginMgr == null)
            pluginMgr = new CachePluginManager(ctx, cfg);

        pluginMgr.validate();

        sharedCtx.jta().registerCache(cfg);

        // Skip suggestions for internal caches.
        if (cacheType.userCache())
            suggestOptimizations(cfg, cfgStore != null);

        Collection<Object> toPrepare = new ArrayList<>();

        if (cfgStore instanceof GridCacheLoaderWriterStore) {
            toPrepare.add(((GridCacheLoaderWriterStore)cfgStore).loader());
            toPrepare.add(((GridCacheLoaderWriterStore)cfgStore).writer());
        }
        else
            toPrepare.add(cfgStore);

        prepare(cfg, toPrepare);

        U.startLifecycleAware(lifecycleAwares(cfg, cfgStore));

        boolean affNode = CU.affinityNode(ctx.discovery().localNode(), cfg.getNodeFilter());

        GridCacheAffinityManager affMgr = new GridCacheAffinityManager();
        GridCacheEventManager evtMgr = new GridCacheEventManager();
        GridCacheSwapManager swapMgr = new GridCacheSwapManager(
            affNode && (cfg.getCacheMode() == LOCAL || !GridCacheUtils.isNearEnabled(cfg)));
        GridCacheEvictionManager evictMgr = new GridCacheEvictionManager();
        GridCacheQueryManager qryMgr = queryManager(cfg);
        CacheContinuousQueryManager contQryMgr = new CacheContinuousQueryManager();
        CacheDataStructuresManager dataStructuresMgr = new CacheDataStructuresManager();
        GridCacheTtlManager ttlMgr = new GridCacheTtlManager();

        CacheConflictResolutionManager rslvrMgr = pluginMgr.createComponent(CacheConflictResolutionManager.class);
        GridCacheDrManager drMgr = pluginMgr.createComponent(GridCacheDrManager.class);
        CacheStoreManager storeMgr = pluginMgr.createComponent(CacheStoreManager.class);

        storeMgr.initialize(cfgStore, sesHolders);

        GridCacheContext<?, ?> cacheCtx = new GridCacheContext(
            ctx,
            sharedCtx,
            cfg,
            cacheType,
            affNode,
            updatesAllowed,

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
            dataStructuresMgr,
            ttlMgr,
            drMgr,
            rslvrMgr,
            pluginMgr,
            affMgr
        );

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
            swapMgr = new GridCacheSwapManager(affNode);
            evictMgr = new GridCacheEvictionManager();
            evtMgr = new GridCacheEventManager();
            pluginMgr = new CachePluginManager(ctx, cfg);
            drMgr = pluginMgr.createComponent(GridCacheDrManager.class);

            cacheCtx = new GridCacheContext(
                ctx,
                sharedCtx,
                cfg,
                cacheType,
                affNode,
                true,

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
                dataStructuresMgr,
                ttlMgr,
                drMgr,
                rslvrMgr,
                pluginMgr,
                affMgr
            );

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

        if (!CU.isUtilityCache(cache.name()) && !CU.isSystemCache(cache.name())) {
            registerMbean(cache.localMxBean(), cache.name(), false);
            registerMbean(cache.clusterMxBean(), cache.name(), false);
        }

        return ret;
    }

    /**
     * Gets a collection of currently started caches.
     *
     * @return Collection of started cache names.
     */
    public Collection<String> cacheNames() {
        return F.viewReadOnly(registeredCaches.values(),
            new IgniteClosure<DynamicCacheDescriptor, String>() {
                @Override public String apply(DynamicCacheDescriptor desc) {
                    return desc.cacheConfiguration().getName();
                }
            },
            new IgnitePredicate<DynamicCacheDescriptor>() {
                @Override public boolean apply(DynamicCacheDescriptor desc) {
                    return desc.started();
                }
            });
    }

    /**
     * Gets public cache that can be used for query execution.
     * If cache isn't created on current node it will be started.
     *
     * @param start Start cache.
     * @param inclLoc Include local caches.
     * @return Cache or {@code null} if there is no suitable cache.
     */
    public IgniteCacheProxy<?, ?> getOrStartPublicCache(boolean start, boolean inclLoc) throws IgniteCheckedException {
        // Try to find started cache first.
        for (Map.Entry<String, GridCacheAdapter<?, ?>> e : caches.entrySet()) {
            CacheConfiguration ccfg = e.getValue().configuration();

            String cacheName = ccfg.getName();

            if ((inclLoc || ccfg.getCacheMode() != LOCAL) && GridQueryProcessor.isEnabled(ccfg))
                return publicJCache(cacheName);
        }

        if (start) {
            for (Map.Entry<String, DynamicCacheDescriptor> e : registeredCaches.entrySet()) {
                DynamicCacheDescriptor desc = e.getValue();

                CacheConfiguration ccfg = desc.cacheConfiguration();

                if (ccfg.getCacheMode() != LOCAL && GridQueryProcessor.isEnabled(ccfg)) {
                    dynamicStartCache(null, ccfg.getName(), null, false, true, true).get();

                    return publicJCache(ccfg.getName());
                }
            }
        }

        return null;
    }

    /**
     * Gets a collection of currently started public cache names.
     *
     * @return Collection of currently started public cache names
     */
    public Collection<String> publicCacheNames() {
        return F.viewReadOnly(registeredCaches.values(),
            new IgniteClosure<DynamicCacheDescriptor, String>() {
                @Override public String apply(DynamicCacheDescriptor desc) {
                    return desc.cacheConfiguration().getName();
                }
            },
            new IgnitePredicate<DynamicCacheDescriptor>() {
                @Override public boolean apply(DynamicCacheDescriptor desc) {
                    return desc.cacheType().userCache();
                }
            }
        );
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
     * @param req Cache start request.
     * @param topVer Topology version.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareCacheStart(DynamicCacheChangeRequest req, AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        assert req.start() : req;
        assert req.cacheType() != null : req;

        prepareCacheStart(
            req.startCacheConfiguration(),
            req.nearCacheConfiguration(),
            req.cacheType(),
            req.clientStartOnly(),
            req.initiatingNodeId(),
            req.deploymentId(),
            topVer
        );

        DynamicCacheDescriptor desc = registeredCaches.get(maskNull(req.cacheName()));

        if (desc != null)
            desc.onStart();
    }

    /**
     * Starts statically configured caches received from remote nodes during exchange.
     *
     * @param topVer Topology version.
     * @return Started caches descriptors.
     * @throws IgniteCheckedException If failed.
     */
    public Collection<DynamicCacheDescriptor> startReceivedCaches(AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        List<DynamicCacheDescriptor> started = null;

        for (DynamicCacheDescriptor desc : registeredCaches.values()) {
            if (!desc.started() && desc.staticallyConfigured() && !desc.locallyConfigured()) {
                if (desc.receivedFrom() != null) {
                    AffinityTopologyVersion startVer = desc.receivedFromStartVersion();

                    if (startVer == null || startVer.compareTo(topVer) > 0)
                        continue;
                }

                if (desc.onStart()) {
                    if (started == null)
                        started = new ArrayList<>();

                    started.add(desc);

                    prepareCacheStart(
                        desc.cacheConfiguration(),
                        null,
                        desc.cacheType(),
                        false,
                        null,
                        desc.deploymentId(),
                        topVer
                    );
                }
            }
        }

        return started;
    }

    /**
     * @param cfg Start configuration.
     * @param nearCfg Near configuration.
     * @param cacheType Cache type.
     * @param clientStartOnly Client only start request.
     * @param initiatingNodeId Initiating node ID.
     * @param deploymentId Deployment ID.
     * @param topVer Topology version.
     * @throws IgniteCheckedException If failed.
     */
    private void prepareCacheStart(
        CacheConfiguration cfg,
        NearCacheConfiguration nearCfg,
        CacheType cacheType,
        boolean clientStartOnly,
        UUID initiatingNodeId,
        IgniteUuid deploymentId,
        AffinityTopologyVersion topVer
    ) throws IgniteCheckedException {
        CacheConfiguration ccfg = new CacheConfiguration(cfg);

        IgnitePredicate nodeFilter = ccfg.getNodeFilter();

        ClusterNode locNode = ctx.discovery().localNode();

        boolean affNodeStart = !clientStartOnly && CU.affinityNode(locNode, nodeFilter);
        boolean clientNodeStart = locNode.id().equals(initiatingNodeId);

        if (sharedCtx.cacheContext(CU.cacheId(cfg.getName())) != null)
            return;

        if (affNodeStart || clientNodeStart) {
            if (clientNodeStart && !affNodeStart) {
                if (nearCfg != null)
                    ccfg.setNearConfiguration(nearCfg);
                else
                    ccfg.setNearConfiguration(null);
            }

            CacheObjectContext cacheObjCtx = ctx.cacheObjects().contextForCache(ccfg);

            GridCacheContext cacheCtx = createCache(ccfg, null, cacheType, cacheObjCtx, true);

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
        assert req.stop() || req.close();

        if (req.stop() || (req.close() && req.initiatingNodeId().equals(ctx.localNodeId()))) {
            // Break the proxy before exchange future is done.
            IgniteCacheProxy<?, ?> proxy = jCacheProxies.get(maskNull(req.cacheName()));

            if (proxy != null) {
                if (req.stop())
                    proxy.gate().stopped();
                else
                    proxy.closeProxy();
            }
        }
    }

    /**
     * @param req Request.
     */
    private void stopGateway(DynamicCacheChangeRequest req) {
        assert req.stop() : req;

        // Break the proxy before exchange future is done.
        IgniteCacheProxy<?, ?> proxy = jCacheProxies.remove(maskNull(req.cacheName()));

        if (proxy != null)
            proxy.gate().onStopped();
    }

    /**
     * @param req Stop request.
     */
    private void prepareCacheStop(DynamicCacheChangeRequest req) {
        assert req.stop() || req.close() : req;

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
     * @param err Error.
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
                if (cacheCtx.preloader() != null)
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
                }
                else if (req.close() && req.initiatingNodeId().equals(ctx.localNodeId())) {
                    IgniteCacheProxy<?, ?> proxy = jCacheProxies.remove(masked);

                    if (proxy != null) {
                        if (proxy.context().affinityNode()) {
                            GridCacheAdapter<?, ?> cache = caches.get(masked);

                            if (cache != null)
                                jCacheProxies.put(masked, new IgniteCacheProxy(cache.context(), cache, null, false));
                        }
                        else {
                            proxy.context().gate().onStopped();

                            prepareCacheStop(req);
                        }
                    }
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
     * @param storeSesLsnrs Store session listeners.
     * @return Shared context.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private GridCacheSharedContext createSharedContext(GridKernalContext kernalCtx,
        Collection<CacheStoreSessionListener> storeSesLsnrs) throws IgniteCheckedException {
        IgniteTxManager tm = new IgniteTxManager();
        GridCacheMvccManager mvccMgr = new GridCacheMvccManager();
        GridCacheVersionManager verMgr = new GridCacheVersionManager();
        GridCacheDeploymentManager depMgr = new GridCacheDeploymentManager();
        GridCachePartitionExchangeManager exchMgr = new GridCachePartitionExchangeManager();
        GridCacheIoManager ioMgr = new GridCacheIoManager();
        CacheAffinitySharedManager topMgr = new CacheAffinitySharedManager();
        GridCacheSharedTtlCleanupManager ttl = new GridCacheSharedTtlCleanupManager();

        CacheJtaManagerAdapter jta = JTA.createOptional();

        return new GridCacheSharedContext(
            kernalCtx,
            tm,
            verMgr,
            mvccMgr,
            depMgr,
            exchMgr,
            topMgr,
            ioMgr,
            ttl,
            jta,
            storeSesLsnrs
        );
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return DiscoveryDataExchangeType.CACHE_PROC;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Serializable collectDiscoveryData(UUID nodeId) {
        boolean reconnect = ctx.localNodeId().equals(nodeId) && cachesOnDisconnect != null;

        // Collect dynamically started caches to a single object.
        Collection<DynamicCacheChangeRequest> reqs;

        Map<String, Map<UUID, Boolean>> clientNodesMap;

        if (reconnect) {
            reqs = new ArrayList<>(caches.size());

            clientNodesMap = U.newHashMap(caches.size());

            for (GridCacheAdapter<?, ?> cache : caches.values()) {
                DynamicCacheDescriptor desc = cachesOnDisconnect.get(maskNull(cache.name()));

                if (desc == null)
                    continue;

                DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(cache.name(), null);

                req.startCacheConfiguration(desc.cacheConfiguration());

                req.cacheType(desc.cacheType());

                req.deploymentId(desc.deploymentId());

                req.receivedFrom(desc.receivedFrom());

                reqs.add(req);

                Boolean nearEnabled = cache.isNear();

                Map<UUID, Boolean> map = U.newHashMap(1);

                map.put(nodeId, nearEnabled);

                clientNodesMap.put(cache.name(), map);
            }
        }
        else {
            reqs = new ArrayList<>(registeredCaches.size() + registeredTemplates.size());

            for (DynamicCacheDescriptor desc : registeredCaches.values()) {
                DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(desc.cacheConfiguration().getName(), null);

                req.startCacheConfiguration(desc.cacheConfiguration());

                req.cacheType(desc.cacheType());

                req.deploymentId(desc.deploymentId());

                req.receivedFrom(desc.receivedFrom());

                reqs.add(req);
            }

            for (DynamicCacheDescriptor desc : registeredTemplates.values()) {
                DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(desc.cacheConfiguration().getName(), null);

                req.startCacheConfiguration(desc.cacheConfiguration());

                req.template(true);

                reqs.add(req);
            }

            clientNodesMap = ctx.discovery().clientNodesMap();
        }

        DynamicCacheChangeBatch batch = new DynamicCacheChangeBatch(reqs);

        batch.clientNodes(clientNodesMap);

        batch.clientReconnect(reconnect);

        // Reset random batch ID so that serialized batches with the same descriptors will be exactly the same.
        batch.id(null);

        return batch;
    }

    /** {@inheritDoc} */
    @Override public void onDiscoveryDataReceived(UUID joiningNodeId, UUID rmtNodeId, Serializable data) {
        if (data instanceof DynamicCacheChangeBatch) {
            DynamicCacheChangeBatch batch = (DynamicCacheChangeBatch)data;

            if (batch.clientReconnect()) {
                if (ctx.clientDisconnected()) {
                    if (clientReconnectReqs == null)
                        clientReconnectReqs = new LinkedHashMap<>();

                    clientReconnectReqs.put(joiningNodeId, batch);

                    return;
                }

                processClientReconnectData(joiningNodeId, batch);
            }
            else {
                for (DynamicCacheChangeRequest req : batch.requests()) {
                    initReceivedCacheConfiguration(req);

                    if (req.template()) {
                        CacheConfiguration ccfg = req.startCacheConfiguration();

                        assert ccfg != null : req;

                        DynamicCacheDescriptor existing = registeredTemplates.get(maskNull(req.cacheName()));

                        if (existing == null) {
                            DynamicCacheDescriptor desc = new DynamicCacheDescriptor(
                                ctx,
                                ccfg,
                                req.cacheType(),
                                true,
                                req.deploymentId());

                            registeredTemplates.put(maskNull(req.cacheName()), desc);
                        }

                        continue;
                    }

                    DynamicCacheDescriptor existing = registeredCaches.get(maskNull(req.cacheName()));

                    if (req.start() && !req.clientStartOnly()) {
                        CacheConfiguration ccfg = req.startCacheConfiguration();

                        if (existing != null) {
                            if (joiningNodeId.equals(ctx.localNodeId())) {
                                existing.receivedFrom(req.receivedFrom());

                                existing.deploymentId(req.deploymentId());
                            }

                            if (existing.locallyConfigured()) {
                                existing.addRemoteConfiguration(rmtNodeId, req.startCacheConfiguration());

                                ctx.discovery().setCacheFilter(
                                    req.cacheName(),
                                    ccfg.getNodeFilter(),
                                    ccfg.getNearConfiguration() != null,
                                    ccfg.getCacheMode());
                            }
                        }
                        else {
                            assert req.cacheType() != null : req;

                            DynamicCacheDescriptor desc = new DynamicCacheDescriptor(
                                ctx,
                                ccfg,
                                req.cacheType(),
                                false,
                                req.deploymentId());

                            // Received statically configured cache.
                            if (req.initiatingNodeId() == null)
                                desc.staticallyConfigured(true);

                            if (joiningNodeId.equals(ctx.localNodeId()))
                                desc.receivedOnDiscovery(true);

                            desc.receivedFrom(req.receivedFrom());

                            DynamicCacheDescriptor old = registeredCaches.put(maskNull(req.cacheName()), desc);

                            assert old == null : old;

                            ctx.discovery().setCacheFilter(
                                req.cacheName(),
                                ccfg.getNodeFilter(),
                                ccfg.getNearConfiguration() != null,
                                ccfg.getCacheMode());
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
    }

    /**
     * @param clientNodeId Client node ID.
     * @param batch Cache change batch.
     */
    private void processClientReconnectData(UUID clientNodeId, DynamicCacheChangeBatch batch) {
        assert batch.clientReconnect() : batch;

        for (DynamicCacheChangeRequest req : batch.requests()) {
            assert !req.template() : req;

            initReceivedCacheConfiguration(req);

            String name = req.cacheName();

            boolean sysCache = CU.isMarshallerCache(name) || CU.isUtilityCache(name) || CU.isAtomicsCache(name);

            if (!sysCache) {
                DynamicCacheDescriptor desc = registeredCaches.get(maskNull(req.cacheName()));

                if (desc != null && desc.deploymentId().equals(req.deploymentId())) {
                    Map<UUID, Boolean> nodes = batch.clientNodes().get(name);

                    assert nodes != null : req;
                    assert nodes.containsKey(clientNodeId) : nodes;

                    ctx.discovery().addClientNode(req.cacheName(), clientNodeId, nodes.get(clientNodeId));
                }
            }
            else
                ctx.discovery().addClientNode(req.cacheName(), clientNodeId, false);
        }
    }

    /**
     * Dynamically starts cache using template configuration.
     *
     * @param cacheName Cache name.
     * @return Future that will be completed when cache is deployed.
     */
    public IgniteInternalFuture<?> createFromTemplate(String cacheName) {
        try {
            CacheConfiguration cfg = createConfigFromTemplate(cacheName);

            return dynamicStartCache(cfg, cacheName, null, true, true, true);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Dynamically starts cache using template configuration.
     *
     * @param cacheName Cache name.
     * @param checkThreadTx If {@code true} checks that current thread does not have active transactions.
     * @return Future that will be completed when cache is deployed.
     */
    public IgniteInternalFuture<?> getOrCreateFromTemplate(String cacheName, boolean checkThreadTx) {
        try {
            if (publicJCache(cacheName, false, checkThreadTx) != null) // Cache with given name already started.
                return new GridFinishedFuture<>();

            CacheConfiguration cfg = createConfigFromTemplate(cacheName);

            return dynamicStartCache(cfg, cacheName, null, false, true, checkThreadTx);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     * @throws IgniteCheckedException If failed.
     */
    private CacheConfiguration createConfigFromTemplate(String cacheName) throws IgniteCheckedException {
        CacheConfiguration cfgTemplate = null;

        CacheConfiguration dfltCacheCfg = null;

        List<CacheConfiguration> wildcardNameCfgs = null;

        for (DynamicCacheDescriptor desc : registeredTemplates.values()) {
            assert desc.template();

            CacheConfiguration cfg = desc.cacheConfiguration();

            assert cfg != null;

            if (F.eq(cacheName, cfg.getName())) {
                cfgTemplate = cfg;

                break;
            }

            if (cfg.getName() != null) {
                if (cfg.getName().endsWith("*")) {
                    if (cfg.getName().length() > 1) {
                        if (wildcardNameCfgs == null)
                            wildcardNameCfgs = new ArrayList<>();

                        wildcardNameCfgs.add(cfg);
                    }
                    else
                        dfltCacheCfg = cfg; // Template with name '*'.
                }
            }
            else if (dfltCacheCfg == null)
                dfltCacheCfg = cfg;
        }

        if (cfgTemplate == null && cacheName != null && wildcardNameCfgs != null) {
            Collections.sort(wildcardNameCfgs, new Comparator<CacheConfiguration>() {
                @Override public int compare(CacheConfiguration cfg1, CacheConfiguration cfg2) {
                    Integer len1 = cfg1.getName() != null ? cfg1.getName().length() : 0;
                    Integer len2 = cfg2.getName() != null ? cfg2.getName().length() : 0;

                    return len2.compareTo(len1);
                }
            });

            for (CacheConfiguration cfg : wildcardNameCfgs) {
                if (cacheName.startsWith(cfg.getName().substring(0, cfg.getName().length() - 1))) {
                    cfgTemplate = cfg;

                    break;
                }
            }
        }

        if (cfgTemplate == null)
            cfgTemplate = dfltCacheCfg;

        if (cfgTemplate == null)
            cfgTemplate = new CacheConfiguration();
        else
            cfgTemplate = cloneCheckSerializable(cfgTemplate);

        CacheConfiguration cfg = new CacheConfiguration(cfgTemplate);

        cfg.setName(cacheName);

        return cfg;
    }

    /**
     * Dynamically starts cache.
     *
     * @param ccfg Cache configuration.
     * @param cacheName Cache name.
     * @param nearCfg Near cache configuration.
     * @param failIfExists Fail if exists flag.
     * @param failIfNotStarted If {@code true} fails if cache is not started.
     * @param checkThreadTx If {@code true} checks that current thread does not have active transactions.
     * @return Future that will be completed when cache is deployed.
     */
    @SuppressWarnings("IfMayBeConditional")
    public IgniteInternalFuture<?> dynamicStartCache(
        @Nullable CacheConfiguration ccfg,
        String cacheName,
        @Nullable NearCacheConfiguration nearCfg,
        boolean failIfExists,
        boolean failIfNotStarted,
        boolean checkThreadTx
    ) {
        return dynamicStartCache(ccfg,
            cacheName,
            nearCfg,
            CacheType.USER,
            failIfExists,
            failIfNotStarted,
            checkThreadTx);
    }

    /**
     * Dynamically starts cache.
     *
     * @param ccfg Cache configuration.
     * @param cacheName Cache name.
     * @param nearCfg Near cache configuration.
     * @param cacheType Cache type.
     * @param failIfExists Fail if exists flag.
     * @param failIfNotStarted If {@code true} fails if cache is not started.
     * @param checkThreadTx If {@code true} checks that current thread does not have active transactions.
     * @return Future that will be completed when cache is deployed.
     */
    @SuppressWarnings("IfMayBeConditional")
    public IgniteInternalFuture<?> dynamicStartCache(
        @Nullable CacheConfiguration ccfg,
        String cacheName,
        @Nullable NearCacheConfiguration nearCfg,
        CacheType cacheType,
        boolean failIfExists,
        boolean failIfNotStarted,
        boolean checkThreadTx
    ) {
        if (checkThreadTx)
            checkEmptyTransactions();

        try {
            DynamicCacheChangeRequest req = prepareCacheChangeRequest(
                ccfg,
                cacheName,
                nearCfg,
                cacheType,
                failIfExists,
                failIfNotStarted);

            if (req != null)
                return F.first(initiateCacheChanges(F.asList(req), failIfExists));
            else
                return new GridFinishedFuture<>();
        }
        catch (Exception e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * Dynamically starts multiple caches.
     *
     * @param ccfgList Collection of cache configuration.
     * @param failIfExists Fail if exists flag.
     * @param checkThreadTx If {@code true} checks that current thread does not have active transactions.
     * @return Future that will be completed when all caches are deployed.
     */
    public IgniteInternalFuture<?> dynamicStartCaches(
        Collection<CacheConfiguration> ccfgList,
        boolean failIfExists,
        boolean checkThreadTx
    ) {
        return dynamicStartCaches(ccfgList, CacheType.USER, failIfExists, checkThreadTx);
    }

    /**
     * Dynamically starts multiple caches.
     *
     * @param ccfgList Collection of cache configuration.
     * @param cacheType Cache type.
     * @param failIfExists Fail if exists flag.
     * @param checkThreadTx If {@code true} checks that current thread does not have active transactions.
     * @return Future that will be completed when all caches are deployed.
     */
    private IgniteInternalFuture<?> dynamicStartCaches(
        Collection<CacheConfiguration> ccfgList,
        CacheType cacheType,
        boolean failIfExists,
        boolean checkThreadTx
    ) {
        if (checkThreadTx)
            checkEmptyTransactions();

        List<DynamicCacheChangeRequest> reqList = new ArrayList<>(ccfgList.size());

        try {
            for (CacheConfiguration ccfg : ccfgList) {
                DynamicCacheChangeRequest req = prepareCacheChangeRequest(
                    ccfg,
                    ccfg.getName(),
                    null,
                    cacheType,
                    failIfExists,
                    true
                );

                if (req != null)
                    reqList.add(req);
            }
        }
        catch (Exception e) {
            return new GridFinishedFuture<>(e);
        }

        if (!reqList.isEmpty()) {
            GridCompoundFuture<?, ?> compoundFut = new GridCompoundFuture<>();

            for (DynamicCacheStartFuture fut : initiateCacheChanges(reqList, failIfExists))
                compoundFut.add((IgniteInternalFuture)fut);

            compoundFut.markInitialized();

            return compoundFut;
        }
        else
            return new GridFinishedFuture<>();
    }

    /**
     * @param cacheName Cache name to destroy.
     * @param checkThreadTx If {@code true} checks that current thread does not have active transactions.
     * @return Future that will be completed when cache is destroyed.
     */
    public IgniteInternalFuture<?> dynamicDestroyCache(String cacheName, boolean checkThreadTx) {
        if (checkThreadTx)
            checkEmptyTransactions();

        DynamicCacheChangeRequest t = new DynamicCacheChangeRequest(cacheName, ctx.localNodeId());

        t.stop(true);

        return F.first(initiateCacheChanges(F.asList(t), false));
    }

    /**
     * @param cacheNames Collection of cache names to destroy.
     * @param checkThreadTx If {@code true} checks that current thread does not have active transactions.
     * @return Future that will be completed when cache is destroyed.
     */
    public IgniteInternalFuture<?> dynamicDestroyCaches(Collection<String> cacheNames, boolean checkThreadTx) {
        if (checkThreadTx)
            checkEmptyTransactions();

        List<DynamicCacheChangeRequest> reqs = new ArrayList<>(cacheNames.size());

        for (String cacheName : cacheNames) {
            DynamicCacheChangeRequest t = new DynamicCacheChangeRequest(cacheName, ctx.localNodeId());

            t.stop(true);

            reqs.add(t);
        }

        GridCompoundFuture<?, ?> compoundFut = new GridCompoundFuture<>();

        for (DynamicCacheStartFuture fut : initiateCacheChanges(reqs, false))
            compoundFut.add((IgniteInternalFuture)fut);

        compoundFut.markInitialized();

        return compoundFut;
    }

    /**
     * @param cacheName Cache name to close.
     * @return Future that will be completed when cache is closed.
     */
    public IgniteInternalFuture<?> dynamicCloseCache(String cacheName) {
        IgniteCacheProxy<?, ?> proxy = jCacheProxies.get(maskNull(cacheName));

        if (proxy == null || proxy.proxyClosed())
            return new GridFinishedFuture<>(); // No-op.

        checkEmptyTransactions();

        DynamicCacheChangeRequest t = new DynamicCacheChangeRequest(cacheName, ctx.localNodeId());

        t.close(true);

        return F.first(initiateCacheChanges(F.asList(t), false));
    }

    /**
     * @param reqs Requests.
     * @param failIfExists Fail if exists flag.
     * @return Collection of futures.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private Collection<DynamicCacheStartFuture> initiateCacheChanges(Collection<DynamicCacheChangeRequest> reqs,
        boolean failIfExists) {
        Collection<DynamicCacheStartFuture> res = new ArrayList<>(reqs.size());

        Collection<DynamicCacheChangeRequest> sndReqs = new ArrayList<>(reqs.size());

        for (DynamicCacheChangeRequest req : reqs) {
            DynamicCacheStartFuture fut = new DynamicCacheStartFuture(req.cacheName(), req.deploymentId(), req);

            try {
                if (req.stop() || req.close()) {
                    DynamicCacheDescriptor desc = registeredCaches.get(maskNull(req.cacheName()));

                    if (desc == null)
                        // No-op.
                        fut.onDone();
                    else {
                        assert desc.cacheConfiguration() != null : desc;

                        if (req.close() && desc.cacheConfiguration().getCacheMode() == LOCAL) {
                            req.close(false);

                            req.stop(true);
                        }

                        IgniteUuid dynamicDeploymentId = desc.deploymentId();

                        assert dynamicDeploymentId != null : desc;

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
                    if (req.start()) {
                        if (!req.clientStartOnly()) {
                            if (failIfExists)
                                fut.onDone(new CacheExistsException("Failed to start cache " +
                                    "(a cache with the same name is already being started or stopped): " +
                                    req.cacheName()));
                            else {
                                fut = old;

                                continue;
                            }
                        }
                        else {
                            fut = old;

                            continue;
                        }
                    }
                    else {
                        fut = old;

                        continue;
                    }
                }

                if (fut.isDone())
                    continue;

                sndReqs.add(req);
            }
            catch (Exception e) {
                fut.onDone(e);
            }
            finally {
                res.add(fut);
            }
        }

        Exception err = null;

        if (!sndReqs.isEmpty()) {
            try {
                ctx.discovery().sendCustomEvent(new DynamicCacheChangeBatch(sndReqs));

                if (ctx.isStopping()) {
                    err = new IgniteCheckedException("Failed to execute dynamic cache change request, " +
                        "node is stopping.");
                }
                else if (ctx.clientDisconnected()) {
                    err = new IgniteClientDisconnectedCheckedException(ctx.cluster().clientReconnectFuture(),
                        "Failed to execute dynamic cache change request, client node disconnected.");
                }
            }
            catch (IgniteCheckedException e) {
                err = e;
            }
        }

        if (err != null) {
            for (DynamicCacheStartFuture fut : res)
                fut.onDone(err);
        }

        return res;
    }

    /**
     * @param type Event type.
     * @param node Event node.
     * @param topVer Topology version.
     */
    public void onDiscoveryEvent(int type, ClusterNode node, AffinityTopologyVersion topVer) {
        if (type == EVT_NODE_JOINED) {
            for (DynamicCacheDescriptor cacheDesc : registeredCaches.values()) {
                if (node.id().equals(cacheDesc.receivedFrom()))
                    cacheDesc.receivedFromStartVersion(topVer);
            }
        }

        sharedCtx.affinity().onDiscoveryEvent(type, node, topVer);
    }

    /**
     * Callback invoked from discovery thread when discovery custom message is received.
     *
     * @param msg Customer message.
     * @param topVer Current topology version.
     * @return {@code True} if minor topology version should be increased.
     */
    public boolean onCustomEvent(DiscoveryCustomMessage msg,
        AffinityTopologyVersion topVer) {
        if (msg instanceof CacheAffinityChangeMessage)
            return sharedCtx.affinity().onCustomEvent(((CacheAffinityChangeMessage)msg));

        return msg instanceof DynamicCacheChangeBatch && onCacheChangeRequested((DynamicCacheChangeBatch)msg, topVer);
    }

    /**
     * @param batch Change request batch.
     * @param topVer Current topology version.
     * @return {@code True} if minor topology version should be increased.
     */
    private boolean onCacheChangeRequested(DynamicCacheChangeBatch batch,
        AffinityTopologyVersion topVer) {
        AffinityTopologyVersion newTopVer = null;

        boolean incMinorTopVer = false;

        for (DynamicCacheChangeRequest req : batch.requests()) {
            initReceivedCacheConfiguration(req);

            if (req.template()) {
                CacheConfiguration ccfg = req.startCacheConfiguration();

                assert ccfg != null : req;

                DynamicCacheDescriptor desc = registeredTemplates.get(maskNull(req.cacheName()));

                if (desc == null) {
                    DynamicCacheDescriptor templateDesc =
                        new DynamicCacheDescriptor(ctx, ccfg, req.cacheType(), true, req.deploymentId());

                    DynamicCacheDescriptor old = registeredTemplates.put(maskNull(ccfg.getName()), templateDesc);

                    assert old == null :
                        "Dynamic cache map was concurrently modified [new=" + templateDesc + ", old=" + old + ']';
                }

                TemplateConfigurationFuture fut =
                    (TemplateConfigurationFuture)pendingTemplateFuts.get(maskNull(ccfg.getName()));

                if (fut != null && fut.deploymentId().equals(req.deploymentId()))
                    fut.onDone();

                continue;
            }

            DynamicCacheDescriptor desc = registeredCaches.get(maskNull(req.cacheName()));

            boolean needExchange = false;

            DynamicCacheStartFuture fut = null;

            if (ctx.localNodeId().equals(req.initiatingNodeId())) {
                fut = (DynamicCacheStartFuture)pendingFuts.get(maskNull(req.cacheName()));

                if (fut != null && !req.deploymentId().equals(fut.deploymentId()))
                    fut = null;
            }

            if (req.start()) {
                if (desc == null) {
                    if (req.clientStartOnly()) {
                        if (fut != null)
                            fut.onDone(new IgniteCheckedException("Failed to start client cache " +
                                "(a cache with the given name is not started): " + U.maskName(req.cacheName())));
                    }
                    else {
                        CacheConfiguration ccfg = req.startCacheConfiguration();

                        assert req.cacheType() != null : req;
                        assert F.eq(ccfg.getName(), req.cacheName()) : req;

                        DynamicCacheDescriptor startDesc =
                            new DynamicCacheDescriptor(ctx, ccfg, req.cacheType(), false, req.deploymentId());

                        if (newTopVer == null) {
                            newTopVer = new AffinityTopologyVersion(topVer.topologyVersion(),
                                topVer.minorTopologyVersion() + 1);
                        }

                        startDesc.startTopologyVersion(newTopVer);

                        DynamicCacheDescriptor old = registeredCaches.put(maskNull(ccfg.getName()), startDesc);

                        assert old == null :
                            "Dynamic cache map was concurrently modified [new=" + startDesc + ", old=" + old + ']';

                        ctx.discovery().setCacheFilter(
                            ccfg.getName(),
                            ccfg.getNodeFilter(),
                            ccfg.getNearConfiguration() != null,
                            ccfg.getCacheMode());

                        ctx.discovery().addClientNode(req.cacheName(),
                            req.initiatingNodeId(),
                            req.nearCacheConfiguration() != null);

                        needExchange = true;
                    }
                }
                else {
                    assert req.initiatingNodeId() != null : req;

                    // Cache already exists, exchange is needed only if client cache should be created.
                    ClusterNode node = ctx.discovery().node(req.initiatingNodeId());

                    boolean clientReq = node != null &&
                        !ctx.discovery().cacheAffinityNode(node, req.cacheName());

                    if (req.clientStartOnly()) {
                        needExchange = clientReq && ctx.discovery().addClientNode(req.cacheName(),
                            req.initiatingNodeId(),
                            req.nearCacheConfiguration() != null);
                    }
                    else {
                        if (req.failIfExists()) {
                            if (fut != null)
                                fut.onDone(new CacheExistsException("Failed to start cache " +
                                    "(a cache with the same name is already started): " + U.maskName(req.cacheName())));
                        }
                        else {
                            needExchange = clientReq && ctx.discovery().addClientNode(req.cacheName(),
                                req.initiatingNodeId(),
                                req.nearCacheConfiguration() != null);

                            if (needExchange)
                                req.clientStartOnly(true);
                        }
                    }
                }

                if (!needExchange && desc != null)
                    req.cacheFutureTopologyVersion(desc.startTopologyVersion());
            }
            else {
                assert req.stop() ^ req.close() : req;

                if (desc != null) {
                    if (req.stop()) {
                        DynamicCacheDescriptor old = registeredCaches.remove(maskNull(req.cacheName()));

                        assert old != null : "Dynamic cache map was concurrently modified [req=" + req + ']';

                        ctx.discovery().removeCacheFilter(req.cacheName());

                        needExchange = true;
                    }
                    else {
                        assert req.close() : req;

                        needExchange = ctx.discovery().onClientCacheClose(req.cacheName(), req.initiatingNodeId());
                    }
                }
            }

            req.exchangeNeeded(needExchange);

            incMinorTopVer |= needExchange;
        }

        return incMinorTopVer;
    }

    /**
     * @param req Cache change request.
     */
    private void initReceivedCacheConfiguration(DynamicCacheChangeRequest req) {
        if (req.startCacheConfiguration() != null) {
            CacheConfiguration ccfg = req.startCacheConfiguration();

            if (ccfg.isStoreKeepBinary() == null)
                ccfg.setStoreKeepBinary(CacheConfiguration.DFLT_STORE_KEEP_BINARY);
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
                        "node): " + U.maskName(cfg.getName()));

                if (cfg.getRebalanceMode() == CacheRebalanceMode.NONE)
                    throw new IgniteCheckedException("Only caches with SYNC or ASYNC rebalance mode can be set as rebalance " +
                        "dependency for other caches [cacheName=" + U.maskName(cfg.getName()) +
                        ", rebalanceMode=" + cfg.getRebalanceMode() + ", rebalanceOrder=" + cfg.getRebalanceOrder() + ']');

                maxOrder = Math.max(maxOrder, rebalanceOrder);
            }
            else if (rebalanceOrder < 0)
                throw new IgniteCheckedException("Rebalance order cannot be negative for cache (fix configuration and restart " +
                    "the node) [cacheName=" + U.maskName(cfg.getName()) + ", rebalanceOrder=" + rebalanceOrder + ']');
        }

        return maxOrder;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        return validateHashIdResolvers(node);
    }

    /**
     * @param node Joining node.
     * @return Validation result or {@code null} in case of success.
     */
    @Nullable private IgniteNodeValidationResult validateHashIdResolvers(ClusterNode node) {
        if (!node.isClient()) {
            for (DynamicCacheDescriptor desc : registeredCaches.values()) {
                CacheConfiguration cfg = desc.cacheConfiguration();

                if (cfg.getAffinity() instanceof RendezvousAffinityFunction) {
                    RendezvousAffinityFunction aff = (RendezvousAffinityFunction)cfg.getAffinity();

                    Object nodeHashObj = aff.resolveNodeHash(node);

                    for (ClusterNode topNode : ctx.discovery().allNodes()) {
                        Object topNodeHashObj = aff.resolveNodeHash(topNode);

                        if (nodeHashObj.hashCode() == topNodeHashObj.hashCode()) {
                            String hashIdRslvrName = "";

                            if (aff.getHashIdResolver() != null)
                                hashIdRslvrName = ", hashIdResolverClass=" +
                                    aff.getHashIdResolver().getClass().getName();

                            String errMsg = "Failed to add node to topology because it has the same hash code for " +
                                "partitioned affinity as one of existing nodes [cacheName=" +
                                U.maskName(cfg.getName()) + hashIdRslvrName + ", existingNodeId=" + topNode.id() + ']';

                            String sndMsg = "Failed to add node to topology because it has the same hash code for " +
                                "partitioned affinity as one of existing nodes [cacheName=" +
                                U.maskName(cfg.getName()) + hashIdRslvrName + ", existingNodeId=" + topNode.id() + ']';

                            return new IgniteNodeValidationResult(topNode.id(), errMsg, sndMsg);
                        }
                    }
                }
            }
        }

        return null;
    }

    /**
     * Checks that remote caches has configuration compatible with the local.
     *
     * @param locCfg Local configuration.
     * @param rmtCfg Remote configuration.
     * @param rmtNode Remote node.
     * @throws IgniteCheckedException If check failed.
     */
    private void checkCache(CacheConfiguration locCfg, CacheConfiguration rmtCfg, ClusterNode rmtNode) throws IgniteCheckedException {
        ClusterNode locNode = ctx.discovery().localNode();

        UUID rmt = rmtNode.id();

        GridCacheAttributes rmtAttr = new GridCacheAttributes(rmtCfg);
        GridCacheAttributes locAttr = new GridCacheAttributes(locCfg);

        boolean isLocAff = CU.affinityNode(locNode, locCfg.getNodeFilter());
        boolean isRmtAff = CU.affinityNode(rmtNode, rmtCfg.getNodeFilter());

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cacheMode", "Cache mode",
            locAttr.cacheMode(), rmtAttr.cacheMode(), true);

        if (rmtAttr.cacheMode() != LOCAL) {
            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "interceptor", "Cache Interceptor",
                locAttr.interceptorClassName(), rmtAttr.interceptorClassName(), true);

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "atomicityMode",
                "Cache atomicity mode", locAttr.atomicityMode(), rmtAttr.atomicityMode(), true);

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cachePreloadMode",
                "Cache preload mode", locAttr.cacheRebalanceMode(), rmtAttr.cacheRebalanceMode(), true);

            boolean checkStore = isLocAff && isRmtAff;

            if (checkStore)
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

                String locHashIdResolver = locAttr.affinityHashIdResolverClassName();
                String rmtHashIdResolver = rmtAttr.affinityHashIdResolverClassName();
                String defHashIdResolver = AffinityNodeAddressHashResolver.class.getName();

                if (!((locHashIdResolver == null && rmtHashIdResolver == null) ||
                    (locHashIdResolver == null && rmtHashIdResolver.equals(defHashIdResolver)) ||
                    (rmtHashIdResolver == null && locHashIdResolver.equals(defHashIdResolver)))) {

                    CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cacheAffinity.hashIdResolver",
                        "Partitioned cache affinity hash ID resolver class",
                        locHashIdResolver, rmtHashIdResolver, true);
                }

                if (locHashIdResolver == null &&
                    (rmtHashIdResolver != null && rmtHashIdResolver.equals(defHashIdResolver))) {
                    U.warn(log, "Set " + RendezvousAffinityFunction.class + " with " + defHashIdResolver +
                        " to CacheConfiguration to start node [cacheName=" + rmtAttr.cacheName() + "]");
                }
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
     * @param spaceName Space name.
     * @param keyBytes Key bytes.
     * @param valBytes Value bytes.
     */
    @SuppressWarnings({"unchecked"})
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

            if (qryMgr.enabled()) {
                try {
                    KeyCacheObject key = cctx.toCacheKeyObject(keyBytes);

                    CacheObject val = cctx.swap().unmarshalSwapEntryValue(valBytes);

                    assert val != null;

                    qryMgr.remove(key, val);
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
    public <K, V> IgniteInternalCache<K, V> cache() {
        return cache(null);
    }

    /**
     * @param name Cache name.
     * @param <K> type of keys.
     * @param <V> type of values.
     * @return Cache instance for given name.
     */
    @SuppressWarnings("unchecked")
    public <K, V> IgniteInternalCache<K, V> cache(@Nullable String name) {
        if (log.isDebugEnabled())
            log.debug("Getting cache for name: " + name);

        IgniteCacheProxy<K, V> jcache = (IgniteCacheProxy<K, V>)jCacheProxies.get(maskNull(name));

        return jcache == null ? null : jcache.internalProxy();
    }

    /**
     * @param name Cache name.
     * @return Cache instance for given name.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public <K, V> IgniteInternalCache<K, V> getOrStartCache(@Nullable String name) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Getting cache for name: " + name);

        String masked = maskNull(name);

        IgniteCacheProxy<?, ?> cache = jCacheProxies.get(masked);

        if (cache == null) {
            dynamicStartCache(null, name, null, false, true, true).get();

            cache = jCacheProxies.get(masked);
        }

        return cache == null ? null : (IgniteInternalCache<K, V>)cache.internalProxy();
    }

    /**
     * @return All configured cache instances.
     */
    public Collection<IgniteInternalCache<?, ?>> caches() {
        return F.viewReadOnly(jCacheProxies.values(), new IgniteClosure<IgniteCacheProxy<?, ?>,
            IgniteInternalCache<?, ?>>() {
            @Override public IgniteInternalCache<?, ?> apply(IgniteCacheProxy<?, ?> entries) {
                return entries.internalProxy();
            }
        });
    }

    /**
     * @return All configured cache instances.
     */
    public Collection<IgniteCacheProxy<?, ?>> jcaches() {
        return jCacheProxies.values();
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
     * @return Utility cache.
     */
    public <K, V> IgniteInternalCache<K, V> utilityCache() {
        return internalCacheEx(CU.UTILITY_CACHE_NAME);
    }

    /**
     * Gets utility cache for atomic data structures.
     *
     * @return Utility cache for atomic data structures.
     */
    public <K, V> IgniteInternalCache<K, V> atomicsCache() {
        return internalCacheEx(CU.ATOMICS_CACHE_NAME);
    }

    /**
     * @param name Cache name.
     * @return Cache.
     */
    private <K, V> IgniteInternalCache<K, V> internalCacheEx(String name) {
        if (ctx.discovery().localNode().isClient()) {
            IgniteCacheProxy<K, V> proxy = (IgniteCacheProxy<K, V>)jCacheProxies.get(name);

            assert proxy != null;

            return proxy.internalProxy();
        }

        return internalCache(name);
    }

    /**
     * @param name Cache name.
     * @param <K> type of keys.
     * @param <V> type of values.
     * @return Cache instance for given name.
     */
    @SuppressWarnings("unchecked")
    public <K, V> IgniteInternalCache<K, V> publicCache(@Nullable String name) {
        if (log.isDebugEnabled())
            log.debug("Getting public cache for name: " + name);

        DynamicCacheDescriptor desc = registeredCaches.get(maskNull(name));

        if (desc == null)
            throw new IllegalArgumentException("Cache is not started: " + name);

        if (!desc.cacheType().userCache())
            throw new IllegalStateException("Failed to get cache because it is a system cache: " + name);

        IgniteCacheProxy<K, V> jcache = (IgniteCacheProxy<K, V>)jCacheProxies.get(maskNull(name));

        if (jcache == null)
            throw new IllegalArgumentException("Cache is not started: " + name);

        return jcache.internalProxy();
    }

    /**
     * @param cacheName Cache name.
     * @param <K> type of keys.
     * @param <V> type of values.
     * @return Cache instance for given name.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> IgniteCacheProxy<K, V> publicJCache(@Nullable String cacheName) throws IgniteCheckedException {
        return publicJCache(cacheName, true, true);
    }

    /**
     * @param cacheName Cache name.
     * @param failIfNotStarted If {@code true} throws {@link IllegalArgumentException} if cache is not started,
     *        otherwise returns {@code null} in this case.
     * @param checkThreadTx If {@code true} checks that current thread does not have active transactions.
     * @return Cache instance for given name.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    @Nullable public <K, V> IgniteCacheProxy<K, V> publicJCache(@Nullable String cacheName,
        boolean failIfNotStarted,
        boolean checkThreadTx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Getting public cache for name: " + cacheName);

        String masked = maskNull(cacheName);

        IgniteCacheProxy<?, ?> cache = jCacheProxies.get(masked);

        DynamicCacheDescriptor desc = registeredCaches.get(masked);

        if (desc != null && !desc.cacheType().userCache())
            throw new IllegalStateException("Failed to get cache because it is a system cache: " + cacheName);

        if (cache == null) {
            dynamicStartCache(null, cacheName, null, false, failIfNotStarted, checkThreadTx).get();

            cache = jCacheProxies.get(masked);
        }

        return (IgniteCacheProxy<K, V>)cache;
    }

    /**
     * Get configuration for the given cache.
     *
     * @param name Cache name.
     * @return Cache configuration.
     */
    public CacheConfiguration cacheConfiguration(String name) {
        DynamicCacheDescriptor desc = registeredCaches.get(maskNull(name));

        if (desc == null)
            throw new IllegalStateException("Cache doesn't exist: " + name);
        else
            return desc.cacheConfiguration();
    }

    /**
     * @return Cache descriptors.
     */
    public Collection<DynamicCacheDescriptor> cacheDescriptors() {
        return registeredCaches.values();
    }

    /**
     * @param cacheId Cache ID.
     * @return Cache descriptor.
     */
    @Nullable public DynamicCacheDescriptor cacheDescriptor(int cacheId) {
        for (DynamicCacheDescriptor cacheDesc : registeredCaches.values()) {
            CacheConfiguration ccfg = cacheDesc.cacheConfiguration();

            assert ccfg != null : cacheDesc;

            if (CU.cacheId(ccfg.getName()) == cacheId)
                return cacheDesc;
        }

        return null;
    }

    /**
     * @param cacheCfg Cache configuration template.
     * @throws IgniteCheckedException If failed.
     */
    public void addCacheConfiguration(CacheConfiguration cacheCfg) throws IgniteCheckedException {
        String masked = maskNull(cacheCfg.getName());

        DynamicCacheDescriptor desc = registeredTemplates.get(masked);

        if (desc != null)
            return;

        DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(cacheCfg.getName(), ctx.localNodeId());

        CacheConfiguration cfg = new CacheConfiguration(cacheCfg);

        req.template(true);

        req.startCacheConfiguration(cfg);

        req.deploymentId(IgniteUuid.randomUuid());

        TemplateConfigurationFuture fut = new TemplateConfigurationFuture(req.cacheName(), req.deploymentId());

        TemplateConfigurationFuture old =
            (TemplateConfigurationFuture)pendingTemplateFuts.putIfAbsent(maskNull(cacheCfg.getName()), fut);

        if (old != null)
            fut = old;

        Exception err = null;

        try {
            ctx.discovery().sendCustomEvent(new DynamicCacheChangeBatch(Collections.singleton(req)));

            if (ctx.isStopping()) {
                err = new IgniteCheckedException("Failed to execute dynamic cache change request, " +
                    "node is stopping.");
            }
            else if (ctx.clientDisconnected()) {
                err = new IgniteClientDisconnectedCheckedException(ctx.cluster().clientReconnectFuture(),
                    "Failed to execute dynamic cache change request, client node disconnected.");
            }
        }
        catch (IgniteCheckedException e) {
            err = e;
        }

        if (err != null)
            fut.onDone(err);

        fut.get();
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
            if (entry.getValue().context().userCache())
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
    private void cancelFutures() {
        sharedCtx.mvcc().onStop();

        Exception err = new IgniteCheckedException("Operation has been cancelled (node is stopping).");

        for (IgniteInternalFuture fut : pendingFuts.values())
            ((GridFutureAdapter)fut).onDone(err);

        for (IgniteInternalFuture fut : pendingTemplateFuts.values())
            ((GridFutureAdapter)fut).onDone(err);
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
        DynamicCacheDescriptor desc = registeredCaches.get(maskNull(name));

        return desc != null && !desc.cacheType().userCache();
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
     * Callback invoked by deployment manager for whenever a class loader gets undeployed.
     *
     * @param ldr Class loader.
     */
    public void onUndeployed(ClassLoader ldr) {
        if (!ctx.isStopping()) {
            for (GridCacheAdapter<?, ?> cache : caches.values()) {
                // Do not notify system caches and caches for which deployment is disabled.
                if (cache.context().userCache() && cache.context().deploymentEnabled())
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
     * Starts client caches that do not exist yet.
     *
     * @throws IgniteCheckedException In case of error.
     */
    public void createMissingCaches() throws IgniteCheckedException {
        for (Map.Entry<String, DynamicCacheDescriptor> e : registeredCaches.entrySet()) {
            CacheConfiguration ccfg = e.getValue().cacheConfiguration();

            if (!caches.containsKey(maskNull(ccfg.getName())) && GridQueryProcessor.isEnabled(ccfg))
                dynamicStartCache(null, ccfg.getName(), null, false, true, true).get();
        }
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
            if (itf.getName().endsWith("MBean") || itf.getName().endsWith("MXBean")) {
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
            if (itf.getName().endsWith("MBean") || itf.getName().endsWith("MXBean")) {
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
    private Iterable<Object> lifecycleAwares(CacheConfiguration ccfg, Object... objs) {
        Collection<Object> ret = new ArrayList<>(7 + objs.length);

        ret.add(ccfg.getAffinity());
        ret.add(ccfg.getAffinityMapper());
        ret.add(ccfg.getEvictionFilter());
        ret.add(ccfg.getEvictionPolicy());
        ret.add(ccfg.getInterceptor());
        ret.add(ccfg.getTopologyValidator());

        NearCacheConfiguration nearCfg = ccfg.getNearConfiguration();

        if (nearCfg != null)
            ret.add(nearCfg.getNearEvictionPolicy());

        Collections.addAll(ret, objs);

        return ret;
    }

    /**
     * @throws IgniteException If transaction exist.
     */
    private void checkEmptyTransactions() throws IgniteException {
        if (transactions().tx() != null || sharedCtx.lockedTopologyVersion(null) != null)
            throw new IgniteException("Cannot start/stop cache within lock or transaction.");
    }

    /**
     * @param val Object to check.
     * @return Configuration copy.
     * @throws IgniteCheckedException If validation failed.
     */
    private CacheConfiguration cloneCheckSerializable(final CacheConfiguration val) throws IgniteCheckedException {
        if (val == null)
            return null;

        return withBinaryContext(new IgniteOutClosureX<CacheConfiguration>() {
            @Override public CacheConfiguration applyx() throws IgniteCheckedException {
                if (val.getCacheStoreFactory() != null) {
                    try {
                        ClassLoader ldr = ctx.config().getClassLoader();

                        if (ldr == null)
                            ldr = val.getCacheStoreFactory().getClass().getClassLoader();

                        U.unmarshal(marsh, U.marshal(marsh, val.getCacheStoreFactory()),
                            U.resolveClassLoader(ldr, ctx.config()));
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteCheckedException("Failed to validate cache configuration. " +
                            "Cache store factory is not serializable. Cache name: " + U.maskName(val.getName()), e);
                    }
                }

                try {
                    return U.unmarshal(marsh, U.marshal(marsh, val), U.resolveClassLoader(ctx.config()));
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteCheckedException("Failed to validate cache configuration " +
                        "(make sure all objects in cache configuration are serializable): " + U.maskName(val.getName()), e);
                }
            }
        });
    }

    /**
     * @param c Closure.
     * @return Closure result.
     * @throws IgniteCheckedException If failed.
     */
    private <T> T withBinaryContext(IgniteOutClosureX<T> c) throws IgniteCheckedException {
        IgniteCacheObjectProcessor objProc = ctx.cacheObjects();
        BinaryContext oldCtx = null;

        if (objProc instanceof CacheObjectBinaryProcessorImpl) {
            GridBinaryMarshaller binMarsh = ((CacheObjectBinaryProcessorImpl)objProc).marshaller();

            oldCtx = binMarsh == null ? null : binMarsh.pushContext();
        }

        try {
            return c.applyx();
        }
        finally {
            if (objProc instanceof CacheObjectBinaryProcessorImpl)
                GridBinaryMarshaller.popContext(oldCtx);
        }
    }

    /**
     * Prepares DynamicCacheChangeRequest for cache creation.
     *
     * @param ccfg Cache configuration
     * @param cacheName Cache name
     * @param nearCfg Near cache configuration
     * @param cacheType Cache type
     * @param failIfExists Fail if exists flag.
     * @param failIfNotStarted If {@code true} fails if cache is not started.
     * @return Request or {@code null} if cache already exists.
     * @throws IgniteCheckedException if some of pre-checks failed
     * @throws CacheExistsException if cache exists and failIfExists flag is {@code true}
     */
    private DynamicCacheChangeRequest prepareCacheChangeRequest(
        @Nullable CacheConfiguration ccfg,
        String cacheName,
        @Nullable NearCacheConfiguration nearCfg,
        CacheType cacheType,
        boolean failIfExists,
        boolean failIfNotStarted
    ) throws IgniteCheckedException {
        DynamicCacheDescriptor desc = registeredCaches.get(maskNull(cacheName));

        DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(cacheName, ctx.localNodeId());

        req.failIfExists(failIfExists);

        if (ccfg != null) {
            cloneCheckSerializable(ccfg);

            if (desc != null) {
                if (failIfExists) {
                    throw new CacheExistsException("Failed to start cache " +
                        "(a cache with the same name is already started): " + cacheName);
                }
                else {
                    CacheConfiguration descCfg = desc.cacheConfiguration();

                    // Check if we were asked to start a near cache.
                    if (nearCfg != null) {
                        if (CU.affinityNode(ctx.discovery().localNode(), descCfg.getNodeFilter())) {
                            // If we are on a data node and near cache was enabled, return success, else - fail.
                            if (descCfg.getNearConfiguration() != null)
                                return null;
                            else
                                throw new IgniteCheckedException("Failed to start near " +
                                    "cache (local node is an affinity node for cache): " + cacheName);
                        }
                        else
                            // If local node has near cache, return success.
                            req.clientStartOnly(true);
                    }
                    else
                        req.clientStartOnly(true);

                    req.deploymentId(desc.deploymentId());

                    req.startCacheConfiguration(descCfg);
                }
            }
            else {
                req.deploymentId(IgniteUuid.randomUuid());

                CacheConfiguration cfg = new CacheConfiguration(ccfg);

                CacheObjectContext cacheObjCtx = ctx.cacheObjects().contextForCache(cfg);

                initialize(cfg, cacheObjCtx);

                req.startCacheConfiguration(cfg);
            }
        }
        else {
            req.clientStartOnly(true);

            if (desc != null)
                ccfg = desc.cacheConfiguration();

            if (ccfg == null) {
                if (failIfNotStarted) {
                    throw new CacheExistsException("Failed to start client cache " +
                        "(a cache with the given name is not started): " + cacheName);
                }
                else
                    return null;
            }

            req.deploymentId(desc.deploymentId());
            req.startCacheConfiguration(ccfg);
        }

        // Fail cache with swap enabled creation on grid without swap space SPI.
        if (ccfg.isSwapEnabled())
            for (ClusterNode n : ctx.discovery().allNodes())
                if (!GridCacheUtils.clientNode(n) && !GridCacheUtils.isSwapEnabled(n)) {
                    throw new IgniteCheckedException("Failed to start cache " +
                        cacheName + " with swap enabled: Remote Node with ID " + n.id().toString().toUpperCase() +
                        " has not swap SPI configured");
                }

        if (nearCfg != null)
            req.nearCacheConfiguration(nearCfg);

        req.cacheType(cacheType);

        return req;
    }

    /**
     * @param obj Object to clone.
     * @return Object copy.
     * @throws IgniteCheckedException If failed.
     */
    public <T> T clone(final T obj) throws IgniteCheckedException {
        return withBinaryContext(new IgniteOutClosureX<T>() {
            @Override public T applyx() throws IgniteCheckedException {
                return U.unmarshal(marsh, U.marshal(marsh, obj), U.resolveClassLoader(ctx.config()));
            }
        });
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
         * @param cacheName Cache name.
         * @param deploymentId Deployment ID.
         * @param req Cache start request.
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
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    private class TemplateConfigurationFuture extends GridFutureAdapter<Object> {
        /** Start ID. */
        @GridToStringInclude
        private IgniteUuid deploymentId;

        /** Cache name. */
        private String cacheName;

        /**
         * @param cacheName Cache name.
         * @param deploymentId Deployment ID.
         */
        private TemplateConfigurationFuture(String cacheName, IgniteUuid deploymentId) {
            this.deploymentId = deploymentId;
            this.cacheName = cacheName;
        }

        /**
         * @return Start ID.
         */
        public IgniteUuid deploymentId() {
            return deploymentId;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Object res, @Nullable Throwable err) {
            // Make sure to remove future before completion.
            pendingTemplateFuts.remove(maskNull(cacheName), this);

            return super.onDone(res, err);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TemplateConfigurationFuture.class, this);
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

    /**
     *
     */
    private class RemovedItemsCleanupTask implements GridTimeoutObject {
        /** */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** */
        private final long endTime;

        /** */
        private final long timeout;

        /**
         * @param timeout Timeout.
         */
        RemovedItemsCleanupTask(long timeout) {
            this.timeout = timeout;
            this.endTime = U.currentTimeMillis() + timeout;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid timeoutId() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            ctx.closure().runLocalSafe(new Runnable() {
                @Override public void run() {
                    try {
                        for (GridCacheContext cacheCtx : sharedCtx.cacheContexts()) {
                            if (!cacheCtx.isLocal() && cacheCtx.affinityNode()) {
                                GridDhtPartitionTopology top = null;

                                try {
                                    top = cacheCtx.topology();
                                }
                                catch (IllegalStateException ignore) {
                                    // Cache stopped.
                                }

                                if (top != null) {
                                    for (GridDhtLocalPartition part : top.currentLocalPartitions())
                                        part.cleanupRemoveQueue();
                                }

                                if (ctx.isStopping())
                                    return;
                            }
                        }
                    }
                    catch (Exception e) {
                        U.error(log, "Failed to cleanup removed cache items: " + e, e);
                    }

                    if (ctx.isStopping())
                        return;

                    addRemovedItemsCleanupTask(timeout);
                }
            }, true);
        }
    }
}
