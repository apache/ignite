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
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
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
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.IgniteTransactionsEx;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.pagemem.snapshot.StartFullSnapshotAckDiscoveryMessage;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.database.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.database.MemoryPolicy;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeList;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.datastructures.CacheDataStructuresManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtColocatedCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearAtomicCache;
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
import org.apache.ignite.internal.processors.query.QuerySchema;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.schema.SchemaExchangeWorkerTask;
import org.apache.ignite.internal.processors.query.schema.SchemaNodeLeaveExchangeWorkerTask;
import org.apache.ignite.internal.processors.query.schema.message.SchemaAbstractDiscoveryMessage;
import org.apache.ignite.internal.processors.query.schema.message.SchemaProposeDiscoveryMessage;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.suggestions.GridPerformanceSuggestions;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
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
import org.apache.ignite.mxbean.IgniteMBeanAware;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.GridDiscoveryData;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.JoiningNodeDiscoveryData;
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
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.configuration.DeploymentMode.ISOLATED;
import static org.apache.ignite.configuration.DeploymentMode.PRIVATE;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CACHE_PROC;
import static org.apache.ignite.internal.IgniteComponentType.JTA;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CONSISTENCY_CHECK_SKIPPED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_TX_CONFIG;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isNearEnabled;

/**
 * Cache processor.
 */
@SuppressWarnings({"unchecked", "TypeMayBeWeakened", "deprecation"})
public class GridCacheProcessor extends GridProcessorAdapter {
    /** */
    private final boolean START_CLIENT_CACHES =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_START_CACHES_ON_JOIN, false);

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
    private ConcurrentMap<UUID, IgniteInternalFuture> pendingFuts = new ConcurrentHashMap<>();

    /** Template configuration add futures. */
    private ConcurrentMap<String, IgniteInternalFuture> pendingTemplateFuts = new ConcurrentHashMap<>();

    /** */
    private ClusterCachesInfo cachesInfo;

    /** */
    private IdentityHashMap<CacheStore, ThreadLocal> sesHolders = new IdentityHashMap<>();

    /** Must use JDK marsh since it is used by discovery to fire custom events. */
    private final Marshaller marsh;

    /** Count down latch for caches. */
    private final CountDownLatch cacheStartedLatch = new CountDownLatch(1);

    /** Internal cache names. */
    private final Set<String> internalCaches;

    /**
     * @param ctx Kernal context.
     */
    public GridCacheProcessor(GridKernalContext ctx) {
        super(ctx);

        caches = new ConcurrentHashMap<>();
        jCacheProxies = new ConcurrentHashMap<>();
        stopSeq = new LinkedList<>();
        internalCaches = new HashSet<>();

        marsh = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());
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

        if (cfg.getNodeFilter() == null)
            cfg.setNodeFilter(CacheConfiguration.ALL_NODES);

        if (cfg.getAffinity() == null) {
            if (cfg.getCacheMode() == PARTITIONED) {
                RendezvousAffinityFunction aff = new RendezvousAffinityFunction();

                cfg.setAffinity(aff);
            }
            else if (cfg.getCacheMode() == REPLICATED) {
                RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false, 512);

                cfg.setAffinity(aff);

                cfg.setBackups(Integer.MAX_VALUE);
            }
            else
                cfg.setAffinity(new LocalAffinityFunction());
        }
        else {
            if (cfg.getCacheMode() == LOCAL && !(cfg.getAffinity() instanceof LocalAffinityFunction)) {
                cfg.setAffinity(new LocalAffinityFunction());

                U.warn(log, "AffinityFunction configuration parameter will be ignored for local cache" +
                    " [cacheName=" + U.maskName(cfg.getName()) + ']');
            }
        }

        if (cfg.getCacheMode() == REPLICATED)
            cfg.setBackups(Integer.MAX_VALUE);

        if( cfg.getQueryParallelism() > 1 && cfg.getCacheMode() != PARTITIONED)
            throw new IgniteCheckedException("Segmented indices are supported for PARTITIONED mode only.");

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

        Collection<QueryEntity> entities = cfg.getQueryEntities();

        if (!F.isEmpty(entities)) {
            Collection<QueryEntity> normalEntities = new ArrayList<>(entities.size());

            for (QueryEntity entity : entities)
                normalEntities.add(QueryUtils.normalizeQueryEntity(entity, cfg.isSqlEscapeAll()));

            cfg.clearQueryEntities().setQueryEntities(normalEntities);
        }
    }

    /**
     * @param cfg Configuration to check for possible performance issues.
     * @param hasStore {@code True} if store is configured.
     */
    private void suggestOptimizations(CacheConfiguration cfg, boolean hasStore) {
        GridPerformanceSuggestions perf = ctx.performance();

        String msg = "Disable eviction policy (remove from configuration)";

        if (cfg.getEvictionPolicy() != null)
            perf.add(msg, false);
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

        if (hasStore && cfg.isWriteThrough())
            perf.add("Enable write-behind to persistent store (set 'writeBehindEnabled' to true)",
                cfg.isWriteBehindEnabled());
    }

    /**
     * Create exchange worker task for custom discovery message.
     *
     * @param msg Custom discovery message.
     * @return Task or {@code null} if message doesn't require any special processing.
     */
    public CachePartitionExchangeWorkerTask exchangeTaskForCustomDiscoveryMessage(DiscoveryCustomMessage msg) {
        if (msg instanceof SchemaAbstractDiscoveryMessage) {
            SchemaAbstractDiscoveryMessage msg0 = (SchemaAbstractDiscoveryMessage)msg;

            if (msg0.exchange())
                return new SchemaExchangeWorkerTask(msg0);
        }

        return null;
    }

    /**
     * Process custom exchange task.
     *
     * @param task Task.
     */
    public void processCustomExchangeTask(CachePartitionExchangeWorkerTask task) {
        if (task instanceof SchemaExchangeWorkerTask) {
            SchemaAbstractDiscoveryMessage msg = ((SchemaExchangeWorkerTask)task).message();

            if (msg instanceof SchemaProposeDiscoveryMessage) {
                SchemaProposeDiscoveryMessage msg0 = (SchemaProposeDiscoveryMessage)msg;

                ctx.query().onSchemaPropose(msg0);
            }
            else
                U.warn(log, "Unsupported schema discovery message: " + msg);
        }
        else if (task instanceof SchemaNodeLeaveExchangeWorkerTask) {
            SchemaNodeLeaveExchangeWorkerTask task0 = (SchemaNodeLeaveExchangeWorkerTask)task;

            ctx.query().onNodeLeave(task0.node());
        }
        else
            U.warn(log, "Unsupported custom exchange task: " + task);
    }

    /**
     * @param c Ignite Configuration.
     * @param cc Cache Configuration.
     * @return {@code true} if cache is starting on client node and this node is affinity node for the cache.
     */
    private boolean storesLocallyOnClient(IgniteConfiguration c, CacheConfiguration cc) {
        if (c.isClientMode() && c.getMemoryConfiguration() == null) {
            if (cc.getCacheMode() == LOCAL)
                return true;

            return ctx.discovery().cacheAffinityNode(ctx.discovery().localNode(), cc.getName());

        }
        else
            return false;
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
        assertParameter(cc.getName() != null && !cc.getName().isEmpty(), "name is null or empty");

        if (cc.getCacheMode() == REPLICATED) {
            if (cc.getNearConfiguration() != null &&
                ctx.discovery().cacheAffinityNode(ctx.discovery().localNode(), cc.getName())) {
                U.warn(log, "Near cache cannot be used with REPLICATED cache, " +
                    "will be ignored [cacheName=" + U.maskName(cc.getName()) + ']');

                cc.setNearConfiguration(null);
            }
        }

        if (storesLocallyOnClient(c, cc))
            throw new IgniteCheckedException("MemoryPolicy for client caches must be explicitly configured " +
                    "on client node startup. Use MemoryConfiguration to configure MemoryPolicy.");

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

        if (cc.getAffinity().partitions() > CacheConfiguration.MAX_PARTITIONS_COUNT)
            throw new IgniteCheckedException("Affinity function must return at most " +
                CacheConfiguration.MAX_PARTITIONS_COUNT + " partitions [actual=" + cc.getAffinity().partitions() +
                ", affFunction=" + cc.getAffinity() + ", cacheName=" + cc.getName() + ']');

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
                    U.warn(log, "Using SYNC rebalance mode with rebalance delay (node will wait until rebalancing is " +
                        "initiated for " + delay + "ms) for cache: " + U.maskName(cc.getName()),
                        "Node will wait until rebalancing is initiated for " + delay + "ms for cache: " + U.maskName(cc.getName()));
                }
            }
        }

        ctx.igfsHelper().validateCacheConfiguration(cc);

        if (cc.getAtomicityMode() == ATOMIC)
            assertParameter(cc.getTransactionManagerLookupClassName() == null,
                "transaction manager can not be used with ATOMIC cache");

        if (cc.getEvictionPolicy() != null && !cc.isOnheapCacheEnabled())
            throw new IgniteCheckedException("Onheap cache must be enabled if eviction policy is configured [cacheName="
                + U.maskName(cc.getName()) + "]");
    }

    /**
     * @param ctx Context.
     * @return DHT managers.
     */
    private List<GridCacheManager> dhtManagers(GridCacheContext ctx) {
        return F.asList(ctx.store(), ctx.events(), ctx.evicts(), ctx.queries(), ctx.continuousQueries(),
            ctx.dr(), ctx.offheap());
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
            return F.asList(ctx.queries(), ctx.continuousQueries(), ctx.store(), ctx.offheap());
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
    @Override public void start(boolean activeOnStart) throws IgniteCheckedException {
        cachesInfo = new ClusterCachesInfo(ctx);

        DeploymentMode depMode = ctx.config().getDeploymentMode();

        if (!F.isEmpty(ctx.config().getCacheConfiguration())) {
            if (depMode != CONTINUOUS && depMode != SHARED)
                U.warn(log, "Deployment mode for cache is not CONTINUOUS or SHARED " +
                    "(it is recommended that you change deployment mode and restart): " + depMode,
                    "Deployment mode for cache is not CONTINUOUS or SHARED.");
        }

        initializeInternalCacheNames();

        sharedCtx = createSharedContext(
            ctx, CU.startStoreSessionListeners(ctx, ctx.config().getCacheStoreSessionListenerFactories()));

        transactions = new IgniteTransactionsImpl(sharedCtx);

        // Start shared managers.
        for (GridCacheSharedManager mgr : sharedCtx.managers())
            mgr.start(sharedCtx);

        if (activeOnStart && !ctx.config().isDaemon()) {
            Map<String, CacheJoinNodeDiscoveryData.CacheInfo> caches = new HashMap<>();

            Map<String, CacheJoinNodeDiscoveryData.CacheInfo> templates = new HashMap<>();

            addCacheOnJoinFromConfig(caches, templates);

            addCacheOnJoinFromPersistentStore(caches, templates);

            CacheJoinNodeDiscoveryData discoData = new CacheJoinNodeDiscoveryData(IgniteUuid.randomUuid(),
                caches,
                templates,
                startAllCachesOnClientStart());

            cachesInfo.onStart(discoData);
        }
        else {
            cachesInfo.onStart(new CacheJoinNodeDiscoveryData(IgniteUuid.randomUuid(),
                Collections.<String, CacheJoinNodeDiscoveryData.CacheInfo>emptyMap(),
                Collections.<String, CacheJoinNodeDiscoveryData.CacheInfo>emptyMap(),
                false));
        }

        if (log.isDebugEnabled())
            log.debug("Started cache processor.");
    }

    /**
     * @param cfg Cache configuration.
     * @param caches Caches map.
     * @param templates Templates map.
     * @throws IgniteCheckedException If failed.
     */
    private void addCacheOnJoin(CacheConfiguration cfg,
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> caches,
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> templates) throws IgniteCheckedException {
        CU.validateCacheName(cfg.getName());

        cloneCheckSerializable(cfg);

        CacheObjectContext cacheObjCtx = ctx.cacheObjects().contextForCache(cfg);

        // Initialize defaults.
        initialize(cfg, cacheObjCtx);

        boolean template = cfg.getName().endsWith("*");

        if (!template) {
            if (caches.containsKey(cfg.getName())) {
                throw new IgniteCheckedException("Duplicate cache name found (check configuration and " +
                    "assign unique name to each cache): " + cfg.getName());
            }

            CacheType cacheType;

            if (CU.isUtilityCache(cfg.getName()))
                cacheType = CacheType.UTILITY;
            else if (internalCaches.contains(cfg.getName()))
                cacheType = CacheType.INTERNAL;
            else
                cacheType = CacheType.USER;

            if (cacheType != CacheType.USER && cfg.getMemoryPolicyName() == null)
                cfg.setMemoryPolicyName(sharedCtx.database().systemMemoryPolicyName());

            if (!cacheType.userCache())
                stopSeq.addLast(cfg.getName());
            else
                stopSeq.addFirst(cfg.getName());

            caches.put(cfg.getName(), new CacheJoinNodeDiscoveryData.CacheInfo(cfg, cacheType, (byte)0));
        }
        else
            templates.put(cfg.getName(), new CacheJoinNodeDiscoveryData.CacheInfo(cfg, CacheType.USER, (byte)0));
    }

    /**
     * @param caches Caches map.
     * @param templates Templates map.
     * @throws IgniteCheckedException If failed.
     */
    private void addCacheOnJoinFromConfig(
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> caches,
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> templates
    ) throws IgniteCheckedException {
        assert !ctx.config().isDaemon();

        CacheConfiguration[] cfgs = ctx.config().getCacheConfiguration();

        for (int i = 0; i < cfgs.length; i++) {
            CacheConfiguration<?, ?> cfg = new CacheConfiguration(cfgs[i]);

            cfgs[i] = cfg; // Replace original configuration value.

            addCacheOnJoin(cfg, caches, templates);
        }
    }

    /**
     * @param caches Caches map.
     * @param templates Templates map.
     * @throws IgniteCheckedException If failed.
     */
    private void addCacheOnJoinFromPersistentStore(
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> caches,
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> templates
    ) throws IgniteCheckedException {
        assert !ctx.config().isDaemon();

        if (sharedCtx.pageStore() != null && sharedCtx.database().persistenceEnabled()) {
            Set<String> savedCacheNames = sharedCtx.pageStore().savedCacheNames();

            savedCacheNames.removeAll(caches.keySet());

            savedCacheNames.removeAll(internalCaches);

            if (!F.isEmpty(savedCacheNames)) {
                if (log.isInfoEnabled())
                    log.info("Register persistent caches: " + savedCacheNames);

                for (String name : savedCacheNames) {
                    CacheConfiguration cfg = sharedCtx.pageStore().readConfiguration(name);

                    if (cfg != null)
                        addCacheOnJoin(cfg, caches, templates);
                }
            }
        }
    }

    /**
     * Initialize internal cache names
     */
    private void initializeInternalCacheNames() {
        FileSystemConfiguration[] igfsCfgs = ctx.grid().configuration().getFileSystemConfiguration();

        if (igfsCfgs != null) {
            for (FileSystemConfiguration igfsCfg : igfsCfgs) {
                internalCaches.add(igfsCfg.getMetaCacheConfiguration().getName());
                internalCaches.add(igfsCfg.getDataCacheConfiguration().getName());
            }
        }

        if (IgniteComponentType.HADOOP.inClassPath())
            internalCaches.add(CU.SYS_CACHE_HADOOP_MR);

        internalCaches.add(CU.ATOMICS_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onKernalStart(boolean activeOnStart) throws IgniteCheckedException {
        ClusterNode locNode = ctx.discovery().localNode();

        try {
            boolean checkConsistency =
                !ctx.config().isDaemon() && !getBoolean(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK);

            if (checkConsistency)
                checkConsistency();

            cachesInfo.onKernalStart(checkConsistency);

            boolean currStatus = ctx.state().active();

            // If we start as inactive node, and join to active cluster, we must register all caches
            // which were received on join.
            if (!ctx.isDaemon() && currStatus && !activeOnStart) {
                List<CacheConfiguration> tmpCacheCfg = new ArrayList<>();

                for (CacheConfiguration conf : ctx.config().getCacheConfiguration()) {
                    assert conf.getName() != null;

                    for (DynamicCacheDescriptor desc : cacheDescriptors()) {
                        CacheConfiguration c = desc.cacheConfiguration();
                        IgnitePredicate filter = c.getNodeFilter();

                        if (c.getName().equals(conf.getName()) &&
                            ((desc.receivedOnDiscovery() && CU.affinityNode(locNode, filter)) ||
                                CU.isSystemCache(c.getName()))) {

                            tmpCacheCfg.add(c);

                            break;
                        }
                    }
                }

                if (!tmpCacheCfg.isEmpty()) {
                    CacheConfiguration[] newCacheCfg = new CacheConfiguration[tmpCacheCfg.size()];

                    tmpCacheCfg.toArray(newCacheCfg);

                    ctx.config().setCacheConfiguration(newCacheCfg);
                }

                activeOnStart = currStatus;
            }

            if (activeOnStart && !ctx.clientNode() && !ctx.isDaemon())
                sharedCtx.database().lock();

            // Must start database before start first cache.
            sharedCtx.database().onKernalStart(false);

            ctx.query().onCacheKernalStart();

            for (GridCacheSharedManager<?, ?> mgr : sharedCtx.managers()) {
                if (sharedCtx.database() != mgr)
                    mgr.onKernalStart(false);
            }
        }
        finally {
            cacheStartedLatch.countDown();
        }

        // Escape if start active on start false
        if (!activeOnStart)
            return;

        if (!ctx.config().isDaemon())
            ctx.cacheObjects().onUtilityCacheStarted();

        ctx.service().onUtilityCacheStarted();

        final AffinityTopologyVersion startTopVer =
            new AffinityTopologyVersion(ctx.discovery().localJoinEvent().topologyVersion(), 0);

        final List<IgniteInternalFuture> syncFuts = new ArrayList<>(caches.size());

        sharedCtx.forAllCaches(new CIX1<GridCacheContext>() {
            @Override public void applyx(GridCacheContext cctx) throws IgniteCheckedException {
                CacheConfiguration cfg = cctx.config();

                if (cctx.affinityNode() &&
                    cfg.getRebalanceMode() == SYNC &&
                    startTopVer.equals(cctx.startTopologyVersion())) {
                    CacheMode cacheMode = cfg.getCacheMode();

                    if (cacheMode == REPLICATED || (cacheMode == PARTITIONED && cfg.getRebalanceDelay() >= 0))
                        // Need to wait outside to avoid a deadlock
                        syncFuts.add(cctx.preloader().syncFuture());
                }
            }
        });

        for (int i = 0, size = syncFuts.size(); i < size; i++)
            syncFuts.get(i).get();

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

    /**
     * @throws IgniteCheckedException if check failed.
     */
    private void checkConsistency() throws IgniteCheckedException {
        for (ClusterNode n : ctx.discovery().remoteNodes()) {
            if (n.attribute(ATTR_CONSISTENCY_CHECK_SKIPPED))
                continue;

            checkTransactionConfiguration(n);

            DeploymentMode locDepMode = ctx.config().getDeploymentMode();
            DeploymentMode rmtDepMode = n.attribute(IgniteNodeAttributes.ATTR_DEPLOYMENT_MODE);

            CU.checkAttributeMismatch(log, null, n.id(), "deploymentMode", "Deployment mode",
                locDepMode, rmtDepMode, true);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopCaches(cancel);

        List<? extends GridCacheSharedManager<?, ?>> mgrs = sharedCtx.managers();

        for (ListIterator<? extends GridCacheSharedManager<?, ?>> it = mgrs.listIterator(mgrs.size()); it.hasPrevious(); ) {
            GridCacheSharedManager<?, ?> mgr = it.previous();

            mgr.stop(cancel);
        }

        CU.stopStoreSessionListeners(ctx, sharedCtx.storeSessionListeners());

        sharedCtx.cleanup();

        if (log.isDebugEnabled())
            log.debug("Stopped cache processor.");
    }

    /**
     * @param cancel Cancel.
     */
    public void stopCaches(boolean cancel){
        for (String cacheName : stopSeq) {
            GridCacheAdapter<?, ?> cache = stoppedCaches.remove(cacheName);

            if (cache != null)
                stopCache(cache, cancel, false);
        }

        for (GridCacheAdapter<?, ?> cache : stoppedCaches.values()) {
            if (cache == stoppedCaches.remove(cache.name()))
                stopCache(cache, cancel, false);
        }

        cachesInfo.clearCaches();
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

        onKernalStopCaches(cancel);

        cancelFutures();

        List<? extends GridCacheSharedManager<?, ?>> sharedMgrs = sharedCtx.managers();

        for (ListIterator<? extends GridCacheSharedManager<?, ?>> it = sharedMgrs.listIterator(sharedMgrs.size());
            it.hasPrevious(); ) {
            GridCacheSharedManager<?, ?> mgr = it.previous();

            if (mgr != exch)
                mgr.onKernalStop(cancel);
        }
    }

    /**
     * @param cancel Cancel.
     */
    public void onKernalStopCaches(boolean cancel){
        for (GridCacheAdapter<?, ?> cache : caches.values()) {
            GridCacheAffinityManager aff = cache.context().affinity();

            if (aff != null)
                aff.cancelFutures();
        }

        for (String cacheName : stopSeq) {
            GridCacheAdapter<?, ?> cache = caches.remove(cacheName);

            if (cache != null) {
                stoppedCaches.put(cacheName, cache);

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
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
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

            for (ListIterator<GridCacheManager> it = mgrs.listIterator(mgrs.size()); it.hasPrevious(); ) {
                GridCacheManager mgr = it.previous();

                mgr.onDisconnected(reconnectFut);
            }
        }

        sharedCtx.onDisconnected(reconnectFut);

        cachesInfo.onDisconnect();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) throws IgniteCheckedException {
        List<GridCacheAdapter> reconnected = new ArrayList<>(caches.size());

        GridCompoundFuture<?, ?> stopFut = null;

        Set<String> stoppedCaches = cachesInfo.onReconnected();

        for (final GridCacheAdapter cache : caches.values()) {
            boolean stopped = stoppedCaches.contains(cache.name());

            if (stopped) {
                cache.context().gate().reconnected(true);

                sharedCtx.removeCacheContext(cache.ctx);

                caches.remove(cache.name());
                jCacheProxies.remove(cache.name());

                IgniteInternalFuture<?> fut = ctx.closure().runLocalSafe(new Runnable() {
                    @Override public void run() {
                        onKernalStop(cache, true);
                        stopCache(cache, true, false);
                    }
                });

                if (stopFut == null)
                    stopFut = new GridCompoundFuture<>();

                stopFut.add((IgniteInternalFuture)fut);
            }
            else {
                cache.onReconnected();

                reconnected.add(cache);

                if (cache.context().userCache()) {
                    // Re-create cache structures inside indexing in order to apply recent schema changes.
                    GridCacheContext cctx = cache.context();

                    DynamicCacheDescriptor desc = cacheDescriptor(cctx.name());

                    assert desc != null;

                    ctx.query().onCacheStop0(cctx.name());
                    ctx.query().onCacheStart0(cctx, desc.schema());
                }
            }
        }

        sharedCtx.onReconnected();

        for (GridCacheAdapter cache : reconnected)
            cache.context().gate().reconnected(false);

        if (stopFut != null)
            stopFut.markInitialized();

        return stopFut;
    }

    /**
     * @param cache Cache to start.
     * @param schema Cache schema.
     * @throws IgniteCheckedException If failed to start cache.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
    private void startCache(GridCacheAdapter<?, ?> cache, QuerySchema schema) throws IgniteCheckedException {
        GridCacheContext<?, ?> cacheCtx = cache.context();

        ctx.continuous().onCacheStart(cacheCtx);

        if (sharedCtx.pageStore() != null)
            sharedCtx.pageStore().initializeForCache(cacheCtx.config());

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

        ctx.query().onCacheStart(cacheCtx, schema);

        cacheCtx.onStarted();

        if (log.isInfoEnabled())
            log.info("Started cache [name=" + U.maskName(cfg.getName()) +
                ", memoryPolicyName=" + cfg.getMemoryPolicyName() + ", mode=" + cfg.getCacheMode() +
                ", atomicity=" + cfg.getAtomicityMode() + ']');
    }

    /**
     * @param cache Cache to stop.
     * @param cancel Cancel flag.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
    private void stopCache(GridCacheAdapter<?, ?> cache, boolean cancel, boolean destroy) {
        GridCacheContext ctx = cache.context();

        if (!cache.isNear() && ctx.shared().wal() != null) {
            try {
                ctx.shared().wal().fsync(null);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to flush write-ahead log on cache stop " +
                    "[cache=" + ctx.name() + "]", e);
            }
        }

        sharedCtx.removeCacheContext(ctx);

        cache.stop();

        ctx.kernalContext().query().onCacheStop(ctx);

        if (isNearEnabled(ctx)) {
            GridDhtCacheAdapter dht = ctx.near().dht();

            // Check whether dht cache has been started.
            if (dht != null) {
                dht.stop();

                GridCacheContext<?, ?> dhtCtx = dht.context();

                List<GridCacheManager> dhtMgrs = dhtManagers(dhtCtx);

                for (ListIterator<GridCacheManager> it = dhtMgrs.listIterator(dhtMgrs.size()); it.hasPrevious(); ) {
                    GridCacheManager mgr = it.previous();

                    mgr.stop(cancel, destroy);
                }
            }
        }

        List<GridCacheManager> mgrs = ctx.managers();

        Collection<GridCacheManager> excludes = dhtExcludes(ctx);

        // Reverse order.
        for (ListIterator<GridCacheManager> it = mgrs.listIterator(mgrs.size()); it.hasPrevious(); ) {
            GridCacheManager mgr = it.previous();

            if (!excludes.contains(mgr))
                mgr.stop(cancel, destroy);
        }

        ctx.kernalContext().continuous().onCacheStop(ctx);

        ctx.kernalContext().cache().context().database().onCacheStop(ctx);

        U.stopLifecycleAware(log, lifecycleAwares(cache.configuration(), ctx.store().configuredStore()));

        if (log.isInfoEnabled())
            log.info("Stopped cache: " + cache.name());

        if (sharedCtx.pageStore() != null) {
            try {
                sharedCtx.pageStore().shutdownForCache(ctx, destroy);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to gracefully clean page store resources for destroyed cache " +
                    "[cache=" + ctx.name() + "]", e);
            }
        }

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
     * @param desc Cache descriptor.
     * @param locStartTopVer Current topology version.
     * @param cacheObjCtx Cache object context.
     * @param affNode {@code True} if local node affinity node.
     * @param updatesAllowed Updates allowed flag.
     * @return Cache context.
     * @throws IgniteCheckedException If failed to create cache.
     */
    private GridCacheContext createCache(CacheConfiguration<?, ?> cfg,
        @Nullable CachePluginManager pluginMgr,
        DynamicCacheDescriptor desc,
        AffinityTopologyVersion locStartTopVer,
        CacheObjectContext cacheObjCtx,
        boolean affNode,
        boolean updatesAllowed)
        throws IgniteCheckedException {
        assert cfg != null;

        if (cfg.getCacheStoreFactory() instanceof GridCacheLoaderWriterStoreFactory) {
            GridCacheLoaderWriterStoreFactory factory = (GridCacheLoaderWriterStoreFactory)cfg.getCacheStoreFactory();

            prepare(cfg, factory.loaderFactory(), false);
            prepare(cfg, factory.writerFactory(), false);
        }
        else
            prepare(cfg, cfg.getCacheStoreFactory(), false);

        CacheStore cfgStore = cfg.getCacheStoreFactory() != null ? cfg.getCacheStoreFactory().create() : null;

        validate(ctx.config(), cfg, desc.cacheType(), cfgStore);

        if (pluginMgr == null)
            pluginMgr = new CachePluginManager(ctx, cfg);

        pluginMgr.validate();

        sharedCtx.jta().registerCache(cfg);

        // Skip suggestions for internal caches.
        if (desc.cacheType().userCache())
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

        boolean nearEnabled = GridCacheUtils.isNearEnabled(cfg);

        GridCacheAffinityManager affMgr = new GridCacheAffinityManager();
        GridCacheEventManager evtMgr = new GridCacheEventManager();
        CacheEvictionManager evictMgr = (nearEnabled || cfg.isOnheapCacheEnabled()) ? new GridCacheEvictionManager() : new CacheOffheapEvictionManager();
        GridCacheQueryManager qryMgr = queryManager(cfg);
        CacheContinuousQueryManager contQryMgr = new CacheContinuousQueryManager();
        CacheDataStructuresManager dataStructuresMgr = new CacheDataStructuresManager();
        GridCacheTtlManager ttlMgr = new GridCacheTtlManager();

        CacheConflictResolutionManager rslvrMgr = pluginMgr.createComponent(CacheConflictResolutionManager.class);
        GridCacheDrManager drMgr = pluginMgr.createComponent(GridCacheDrManager.class);
        CacheStoreManager storeMgr = pluginMgr.createComponent(CacheStoreManager.class);
        IgniteCacheOffheapManager offheapMgr = pluginMgr.createComponent(IgniteCacheOffheapManager.class);

        storeMgr.initialize(cfgStore, sesHolders);

        String memPlcName = cfg.getMemoryPolicyName();

        MemoryPolicy memPlc = sharedCtx.database().memoryPolicy(memPlcName);
        FreeList freeList = sharedCtx.database().freeList(memPlcName);
        ReuseList reuseList = sharedCtx.database().reuseList(memPlcName);

        GridCacheContext<?, ?> cacheCtx = new GridCacheContext(
            ctx,
            sharedCtx,
            cfg,
            desc.cacheType(),
            locStartTopVer,
            desc.receivedFrom(),
            affNode,
            updatesAllowed,
            memPlc,
            freeList,
            reuseList,

            /*
             * Managers in starting order!
             * ===========================
             */
            evtMgr,
            storeMgr,
            evictMgr,
            qryMgr,
            contQryMgr,
            dataStructuresMgr,
            ttlMgr,
            drMgr,
            offheapMgr,
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
                if (nearEnabled) {
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
        if (cfg.getCacheMode() != LOCAL && nearEnabled) {
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
            evictMgr = cfg.isOnheapCacheEnabled() ? new GridCacheEvictionManager() : new CacheOffheapEvictionManager();
            evtMgr = new GridCacheEventManager();
            pluginMgr = new CachePluginManager(ctx, cfg);
            drMgr = pluginMgr.createComponent(GridCacheDrManager.class);

            cacheCtx = new GridCacheContext(
                ctx,
                sharedCtx,
                cfg,
                desc.cacheType(),
                locStartTopVer,
                desc.receivedFrom(),
                affNode,
                true,
                memPlc,
                freeList,
                reuseList,

                /*
                 * Managers in starting order!
                 * ===========================
                 */
                evtMgr,
                storeMgr,
                evictMgr,
                qryMgr,
                contQryMgr,
                dataStructuresMgr,
                ttlMgr,
                drMgr,
                offheapMgr,
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
        return F.viewReadOnly(cacheDescriptors(), new IgniteClosure<DynamicCacheDescriptor, String>() {
            @Override public String apply(DynamicCacheDescriptor desc) {
                return desc.cacheConfiguration().getName();
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

            if ((inclLoc || ccfg.getCacheMode() != LOCAL) && QueryUtils.isEnabled(ccfg))
                return publicJCache(cacheName);
        }

        if (start) {
            for (Map.Entry<String, DynamicCacheDescriptor> e : cachesInfo.registeredCaches().entrySet()) {
                DynamicCacheDescriptor desc = e.getValue();

                CacheConfiguration ccfg = desc.cacheConfiguration();

                if (ccfg.getCacheMode() != LOCAL && QueryUtils.isEnabled(ccfg)) {
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
        return F.viewReadOnly(cacheDescriptors(),
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
        assert cacheName != null;

        DynamicCacheDescriptor desc = cacheDescriptor(cacheName);

        return desc != null ? desc.cacheConfiguration().getCacheMode() : null;
    }

    /**
     * @param cacheDesc Cache start request.
     * @param nearCfg Near cache configuration.
     * @param exchTopVer Current exchange version.
     * @throws IgniteCheckedException If failed.
     */
    void prepareCacheStart(DynamicCacheDescriptor cacheDesc,
        @Nullable NearCacheConfiguration nearCfg,
        AffinityTopologyVersion exchTopVer)
        throws IgniteCheckedException {
        prepareCacheStart(
            cacheDesc.cacheConfiguration(),
            nearCfg,
            cacheDesc,
            exchTopVer,
            cacheDesc.schema()
        );
    }

    /**
     * @param exchTopVer Current exchange version.
     * @throws IgniteCheckedException If failed.
     */
    public void startCachesOnLocalJoin(AffinityTopologyVersion exchTopVer) throws IgniteCheckedException {
        List<T2<DynamicCacheDescriptor, NearCacheConfiguration>> caches = cachesInfo.cachesToStartOnLocalJoin();

        if (!F.isEmpty(caches)) {
            for (T2<DynamicCacheDescriptor, NearCacheConfiguration> t : caches) {
                DynamicCacheDescriptor desc = t.get1();

                prepareCacheStart(
                    desc.cacheConfiguration(),
                    t.get2(),
                    desc,
                    exchTopVer,
                    desc.schema()
                );
            }
        }
    }

    /**
     * Starts statically configured caches received from remote nodes during exchange.
     *
     * @param nodeId Joining node ID.
     * @param exchTopVer Current exchange version.
     * @return Started caches descriptors.
     * @throws IgniteCheckedException If failed.
     */
    public Collection<DynamicCacheDescriptor> startReceivedCaches(UUID nodeId, AffinityTopologyVersion exchTopVer)
        throws IgniteCheckedException {
        List<DynamicCacheDescriptor> started = cachesInfo.cachesReceivedFromJoin(nodeId);

        if (started != null) {
            for (DynamicCacheDescriptor desc : started) {
                IgnitePredicate<ClusterNode> filter = desc.cacheConfiguration().getNodeFilter();

                if (CU.affinityNode(ctx.discovery().localNode(), filter)) {
                    prepareCacheStart(
                        desc.cacheConfiguration(),
                        null,
                        desc,
                        exchTopVer,
                        desc.schema()
                    );
                }
            }
        }

        return started != null ? started : Collections.<DynamicCacheDescriptor>emptyList();
    }

    /**
     * @param startCfg Start configuration.
     * @param reqNearCfg Near configuration if specified for client cache start request.
     * @param desc Cache descriptor.
     * @param exchTopVer Current exchange version.
     * @param schema Query schema.
     * @throws IgniteCheckedException If failed.
     */
    private void prepareCacheStart(
        CacheConfiguration startCfg,
        @Nullable NearCacheConfiguration reqNearCfg,
        DynamicCacheDescriptor desc,
        AffinityTopologyVersion exchTopVer,
        @Nullable QuerySchema schema
    ) throws IgniteCheckedException {
        assert !caches.containsKey(startCfg.getName()) : startCfg.getName();

        CacheConfiguration ccfg = new CacheConfiguration(startCfg);

        CacheObjectContext cacheObjCtx = ctx.cacheObjects().contextForCache(ccfg);

        boolean affNode;

        if (ccfg.getCacheMode() == LOCAL) {
            affNode = true;

            ccfg.setNearConfiguration(null);
        }
        else if (CU.affinityNode(ctx.discovery().localNode(), ccfg.getNodeFilter()))
            affNode = true;
        else {
            affNode = false;

            ccfg.setNearConfiguration(reqNearCfg);
        }

        GridCacheContext cacheCtx = createCache(ccfg,
            null,
            desc,
            exchTopVer,
            cacheObjCtx,
            affNode,
            true);

        cacheCtx.dynamicDeploymentId(desc.deploymentId());

        GridCacheAdapter cache = cacheCtx.cache();

        sharedCtx.addCacheContext(cacheCtx);

        caches.put(cacheCtx.name(), cache);

        startCache(cache, schema != null ? schema : new QuerySchema());

        onKernalStart(cache);
    }

    /**
     * @param req Stop request.
     */
    void blockGateway(DynamicCacheChangeRequest req) {
        assert req.stop() || req.close();

        if (req.stop() || (req.close() && req.initiatingNodeId().equals(ctx.localNodeId()))) {
            // Break the proxy before exchange future is done.
            IgniteCacheProxy<?, ?> proxy = jCacheProxies.get(req.cacheName());

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
        IgniteCacheProxy<?, ?> proxy = jCacheProxies.remove(req.cacheName());

        if (proxy != null)
            proxy.gate().onStopped();
    }

    /**
     * @param req Stop request.
     */
    private void prepareCacheStop(DynamicCacheChangeRequest req) {
        assert req.stop() || req.close() : req;

        GridCacheAdapter<?, ?> cache = caches.remove(req.cacheName());

        if (cache != null) {
            GridCacheContext<?, ?> ctx = cache.context();

            sharedCtx.removeCacheContext(ctx);

            onKernalStop(cache, req.destroy());

            stopCache(cache, true, req.destroy());
        }
    }

    /**
     * Callback invoked when first exchange future for dynamic cache is completed.
     *
     * @param topVer Completed topology version.
     * @param exchActions Change requests.
     * @param err Error.
     */
    @SuppressWarnings("unchecked")
    public void onExchangeDone(
        AffinityTopologyVersion topVer,
        @Nullable ExchangeActions exchActions,
        Throwable err
    ) {
        for (GridCacheAdapter<?, ?> cache : caches.values()) {
            GridCacheContext<?, ?> cacheCtx = cache.context();

            if (cacheCtx.startTopologyVersion().equals(topVer)) {
                jCacheProxies.put(cacheCtx.name(), new IgniteCacheProxy(cache.context(), cache, null, false));

                if (cacheCtx.preloader() != null)
                    cacheCtx.preloader().onInitialExchangeComplete(err);
            }
        }

        if (exchActions != null && err == null) {
            for (ExchangeActions.ActionData action : exchActions.cacheStopRequests()) {
                stopGateway(action.request());

                prepareCacheStop(action.request());
            }

            for (DynamicCacheChangeRequest req : exchActions.closeRequests(ctx.localNodeId())) {
                String cacheName = req.cacheName();

                IgniteCacheProxy<?, ?> proxy = jCacheProxies.get(cacheName);

                if (proxy != null) {
                    if (proxy.context().affinityNode()) {
                        GridCacheAdapter<?, ?> cache = caches.get(cacheName);

                        assert cache != null : cacheName;

                        jCacheProxies.put(cacheName, new IgniteCacheProxy(cache.context(), cache, null, false));
                    }
                    else {
                        jCacheProxies.remove(cacheName);

                        proxy.context().gate().onStopped();

                        prepareCacheStop(req);
                    }
                }
            }
        }
    }

    /**
     * @param cacheName Cache name.
     * @param deploymentId Future deployment ID.
     */
    void completeTemplateAddFuture(String cacheName, IgniteUuid deploymentId) {
        GridCacheProcessor.TemplateConfigurationFuture fut =
            (GridCacheProcessor.TemplateConfigurationFuture)pendingTemplateFuts.get(cacheName);

        if (fut != null && fut.deploymentId().equals(deploymentId))
            fut.onDone();
    }

    /**
     * @param req Request to complete future for.
     * @param err Error if any.
     */
    void completeCacheStartFuture(DynamicCacheChangeRequest req, boolean success, @Nullable Exception err) {
        if (req.initiatingNodeId().equals(ctx.localNodeId())) {
            DynamicCacheStartFuture fut = (DynamicCacheStartFuture)pendingFuts.get(req.requestId());

            if (fut != null)
                fut.onDone(success, err);
        }
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

        IgniteCacheDatabaseSharedManager dbMgr = ctx.plugins().createComponent(IgniteCacheDatabaseSharedManager.class);

        if (dbMgr == null)
            dbMgr = new IgniteCacheDatabaseSharedManager();

        IgnitePageStoreManager pageStoreMgr = ctx.plugins().createComponent(IgnitePageStoreManager.class);
        IgniteWriteAheadLogManager walMgr = ctx.plugins().createComponent(IgniteWriteAheadLogManager.class);

        GridCacheIoManager ioMgr = new GridCacheIoManager();
        CacheAffinitySharedManager topMgr = new CacheAffinitySharedManager();
        GridCacheSharedTtlCleanupManager ttl = new GridCacheSharedTtlCleanupManager();

        CacheJtaManagerAdapter jta = JTA.createOptional();

        return new GridCacheSharedContext(
            kernalCtx,
            tm,
            verMgr,
            mvccMgr,
            pageStoreMgr,
            walMgr,
            dbMgr,
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
        return CACHE_PROC;
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        cachesInfo.collectJoiningNodeData(dataBag);
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        cachesInfo.collectGridNodeData(dataBag);
    }

    /**
     * @return {@code True} if need locally start all existing caches on client node start.
     */
    private boolean startAllCachesOnClientStart() {
        return START_CLIENT_CACHES && ctx.clientNode();
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(JoiningNodeDiscoveryData data) {
        cachesInfo.onJoiningNodeDataReceived(data);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(GridDiscoveryData data) {
        cachesInfo.onGridDataReceived(data);
    }

    /**
     * Dynamically starts cache using template configuration.
     *
     * @param cacheName Cache name.
     * @return Future that will be completed when cache is deployed.
     */
    public IgniteInternalFuture<?> createFromTemplate(String cacheName) {
        try {
            CacheConfiguration cfg = getOrCreateConfigFromTemplate(cacheName);

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
        assert cacheName != null;

        try {
            if (publicJCache(cacheName, false, checkThreadTx) != null) // Cache with given name already started.
                return new GridFinishedFuture<>();

            CacheConfiguration cfg = getOrCreateConfigFromTemplate(cacheName);

            return dynamicStartCache(cfg, cacheName, null, false, true, checkThreadTx);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration, or {@code null} if no template with matching name found.
     * @throws IgniteCheckedException If failed.
     */
    public CacheConfiguration getConfigFromTemplate(String cacheName) throws IgniteCheckedException {
        CacheConfiguration cfgTemplate = null;

        CacheConfiguration dfltCacheCfg = null;

        List<CacheConfiguration> wildcardNameCfgs = null;

        for (DynamicCacheDescriptor desc : cachesInfo.registeredTemplates().values()) {
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
            return null;

        cfgTemplate = cloneCheckSerializable(cfgTemplate);

        CacheConfiguration cfg = new CacheConfiguration(cfgTemplate);

        cfg.setName(cacheName);

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     * @throws IgniteCheckedException If failed.
     */
    private CacheConfiguration getOrCreateConfigFromTemplate(String cacheName) throws IgniteCheckedException {
        CacheConfiguration cfg = getConfigFromTemplate(cacheName);

        if (cfg != null)
            return cfg;
        else
            return new CacheConfiguration(cacheName);
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
    public IgniteInternalFuture<Boolean> dynamicStartCache(
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
    public IgniteInternalFuture<Boolean> dynamicStartCache(
        @Nullable CacheConfiguration ccfg,
        String cacheName,
        @Nullable NearCacheConfiguration nearCfg,
        CacheType cacheType,
        boolean failIfExists,
        boolean failIfNotStarted,
        boolean checkThreadTx
    ) {
        assert cacheName != null;

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
    public IgniteInternalFuture<?> dynamicStartCaches(Collection<CacheConfiguration> ccfgList, boolean failIfExists,
        boolean checkThreadTx) {
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
    public IgniteInternalFuture<Boolean> dynamicDestroyCache(String cacheName, boolean checkThreadTx) {
        assert cacheName != null;

        if (checkThreadTx)
            checkEmptyTransactions();

        DynamicCacheChangeRequest req = DynamicCacheChangeRequest.stopRequest(ctx, cacheName, true);

        return F.first(initiateCacheChanges(F.asList(req), false));
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
            DynamicCacheChangeRequest req = DynamicCacheChangeRequest.stopRequest(ctx, cacheName, true);

            reqs.add(req);
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
    IgniteInternalFuture<?> dynamicCloseCache(String cacheName) {
        assert cacheName != null;

        IgniteCacheProxy<?, ?> proxy = jCacheProxies.get(cacheName);

        if (proxy == null || proxy.proxyClosed())
            return new GridFinishedFuture<>(); // No-op.

        checkEmptyTransactions();

        DynamicCacheChangeRequest t = DynamicCacheChangeRequest.closeRequest(ctx, cacheName);

        return F.first(initiateCacheChanges(F.asList(t), false));
    }

    /**
     * Resets cache state after the cache has been moved to recovery state.
     *
     * @param cacheNames Cache names.
     * @return Future that will be completed when state is changed for all caches.
     */
    public IgniteInternalFuture<?> resetCacheState(Collection<String> cacheNames) {
        checkEmptyTransactions();

        if (F.isEmpty(cacheNames))
            cacheNames = cachesInfo.registeredCaches().keySet();

        Collection<DynamicCacheChangeRequest> reqs = new ArrayList<>(cacheNames.size());

        for (String cacheName : cacheNames) {
            DynamicCacheDescriptor desc = cacheDescriptor(cacheName);

            if (desc == null) {
                U.warn(log, "Failed to find cache for reset lost partition request, cache does not exist: " + cacheName);

                continue;
            }

            DynamicCacheChangeRequest req = DynamicCacheChangeRequest.resetLostPartitions(ctx, cacheName);

            reqs.add(req);
        }

        GridCompoundFuture fut = new GridCompoundFuture();

        for (DynamicCacheStartFuture f : initiateCacheChanges(reqs, false))
            fut.add(f);

        fut.markInitialized();

        return fut;
    }

    /**
     *
     */
    public Collection<DynamicCacheChangeRequest> startAllCachesRequests() throws IgniteCheckedException {
        List<DynamicCacheChangeRequest> reqs = new ArrayList<>();

        if (!ctx.config().isDaemon() &&
            sharedCtx.pageStore() != null &&
            sharedCtx.database().persistenceEnabled()) {
            Set<String> savedCacheNames = sharedCtx.pageStore().savedCacheNames();

            for (String name : savedCacheNames) {
                CacheConfiguration cfg = sharedCtx.pageStore().readConfiguration(name);

                if (cfg != null)
                    reqs.add(createRequest(cfg, false));
            }

            for (CacheConfiguration cfg : ctx.config().getCacheConfiguration()) {
                if (!savedCacheNames.contains(cfg.getName()))
                    reqs.add(createRequest(cfg, true));
            }
        }
        else {
            for (CacheConfiguration cfg : ctx.config().getCacheConfiguration())
                reqs.add(createRequest(cfg, true));
        }

        return reqs;
    }

    /**
     *
     */
    public Collection<DynamicCacheChangeRequest> stopAllCachesRequests(){
        List<DynamicCacheChangeRequest> reqs = new ArrayList<>();

        for (String cacheName : cacheNames()) {
            DynamicCacheChangeRequest req = DynamicCacheChangeRequest.stopRequest(ctx, cacheName, false);

            reqs.add(req);
        }

        return reqs;
    }

    /**
     * @param cfg Cache configuration.
     */
    private DynamicCacheChangeRequest createRequest(
        CacheConfiguration cfg,
        boolean needInit
    ) throws IgniteCheckedException {
        assert cfg != null;
        assert cfg.getName() != null;

        cloneCheckSerializable(cfg);

        if (needInit){
            CacheObjectContext cacheObjCtx = ctx.cacheObjects().contextForCache(cfg);

            initialize(cfg, cacheObjCtx);
        }

        String cacheName = cfg.getName();

        DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(UUID.randomUUID(), cacheName, ctx.localNodeId());

        req.startCacheConfiguration(cfg);
        req.template(cfg.getName().endsWith("*"));
        req.nearCacheConfiguration(cfg.getNearConfiguration());
        req.deploymentId(IgniteUuid.randomUuid());
        req.schema(new QuerySchema(cfg.getQueryEntities()));

        if (CU.isUtilityCache(cacheName))
            req.cacheType(CacheType.UTILITY);
        else if (internalCaches.contains(cacheName))
            req.cacheType(CacheType.INTERNAL);
        else
            req.cacheType(CacheType.USER);

        return req;
    }

    /**
     * @param reqs Requests.
     * @param failIfExists Fail if exists flag.
     * @return Collection of futures.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private Collection<DynamicCacheStartFuture> initiateCacheChanges(
        Collection<DynamicCacheChangeRequest> reqs,
        boolean failIfExists
    ) {
        Collection<DynamicCacheStartFuture> res = new ArrayList<>(reqs.size());

        Collection<DynamicCacheChangeRequest> sndReqs = new ArrayList<>(reqs.size());

        for (DynamicCacheChangeRequest req : reqs) {
            DynamicCacheStartFuture fut = new DynamicCacheStartFuture(req.cacheName(), req);

            try {
                if (req.stop() || req.close()) {
                    DynamicCacheDescriptor desc = cacheDescriptor(req.cacheName());

                    if (desc == null)
                        // No-op.
                        fut.onDone(false);
                    else {
                        assert desc.cacheConfiguration() != null : desc;

                        if (req.close() && desc.cacheConfiguration().getCacheMode() == LOCAL) {
                            req.close(false);

                            req.stop(true);
                        }
                    }
                }

                if (fut.isDone())
                    continue;

                DynamicCacheStartFuture old = (DynamicCacheStartFuture)pendingFuts.putIfAbsent(
                    req.requestId(), fut);

                assert old == null;

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
        cachesInfo.onDiscoveryEvent(type, node, topVer);

        sharedCtx.affinity().onDiscoveryEvent(type, node, topVer);
    }

    /**
     * Callback invoked from discovery thread when discovery custom message is received.
     *
     * @param msg Customer message.
     * @param topVer Current topology version.
     * @return {@code True} if minor topology version should be increased.
     */
    public boolean onCustomEvent(DiscoveryCustomMessage msg, AffinityTopologyVersion topVer) {
        if (msg instanceof SchemaAbstractDiscoveryMessage) {
            ctx.query().onDiscovery((SchemaAbstractDiscoveryMessage)msg);

            return false;
        }

        if (msg instanceof CacheAffinityChangeMessage)
            return sharedCtx.affinity().onCustomEvent(((CacheAffinityChangeMessage)msg));

        if (msg instanceof StartFullSnapshotAckDiscoveryMessage &&
            ((StartFullSnapshotAckDiscoveryMessage)msg).error() == null)
            return true;

        if (msg instanceof DynamicCacheChangeBatch)
            return cachesInfo.onCacheChangeRequested((DynamicCacheChangeBatch)msg, topVer);

        return false;
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
            for (DynamicCacheDescriptor desc : cacheDescriptors()) {
                CacheConfiguration cfg = desc.cacheConfiguration();

                if (cfg.getAffinity() instanceof RendezvousAffinityFunction) {
                    RendezvousAffinityFunction aff = (RendezvousAffinityFunction)cfg.getAffinity();

                    Object nodeHashObj = aff.resolveNodeHash(node);

                    for (ClusterNode topNode : ctx.discovery().allNodes()) {
                        Object topNodeHashObj = aff.resolveNodeHash(topNode);

                        if (nodeHashObj.hashCode() == topNodeHashObj.hashCode()) {
                            String errMsg = "Failed to add node to topology because it has the same hash code for " +
                                "partitioned affinity as one of existing nodes [cacheName=" +
                                U.maskName(cfg.getName()) + ", existingNodeId=" + topNode.id() + ']';

                            String sndMsg = "Failed to add node to topology because it has the same hash code for " +
                                "partitioned affinity as one of existing nodes [cacheName=" +
                                U.maskName(cfg.getName()) + ", existingNodeId=" + topNode.id() + ']';

                            return new IgniteNodeValidationResult(topNode.id(), errMsg, sndMsg);
                        }
                    }
                }
            }
        }

        return null;
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
     * @param name Cache name.
     * @param <K> type of keys.
     * @param <V> type of values.
     * @return Cache instance for given name.
     */
    @SuppressWarnings("unchecked")
    public <K, V> IgniteInternalCache<K, V> cache(String name) {
        assert name != null;

        if (log.isDebugEnabled())
            log.debug("Getting cache for name: " + name);

        IgniteCacheProxy<K, V> jcache = (IgniteCacheProxy<K, V>)jCacheProxies.get(name);

        return jcache == null ? null : jcache.internalProxy();
    }

    /**
     * @param name Cache name.
     * @return Cache instance for given name.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public <K, V> IgniteInternalCache<K, V> getOrStartCache(String name) throws IgniteCheckedException {
        assert name != null;

        if (log.isDebugEnabled())
            log.debug("Getting cache for name: " + name);

        IgniteCacheProxy<?, ?> cache = jCacheProxies.get(name);

        if (cache == null) {
            dynamicStartCache(null, name, null, false, true, true).get();

            cache = jCacheProxies.get(name);
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
    public <K, V> IgniteInternalCache<K, V> publicCache(String name) {
        assert name != null;

        if (log.isDebugEnabled())
            log.debug("Getting public cache for name: " + name);

        DynamicCacheDescriptor desc = cacheDescriptor(name);

        if (desc == null)
            throw new IllegalArgumentException("Cache is not started: " + name);

        if (!desc.cacheType().userCache())
            throw new IllegalStateException("Failed to get cache because it is a system cache: " + name);

        IgniteCacheProxy<K, V> jcache = (IgniteCacheProxy<K, V>)jCacheProxies.get(name);

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
    public <K, V> IgniteCacheProxy<K, V> publicJCache(String cacheName) throws IgniteCheckedException {
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
    @Nullable public <K, V> IgniteCacheProxy<K, V> publicJCache(String cacheName,
        boolean failIfNotStarted,
        boolean checkThreadTx) throws IgniteCheckedException {
        assert cacheName != null;

        if (log.isDebugEnabled())
            log.debug("Getting public cache for name: " + cacheName);

        IgniteCacheProxy<?, ?> cache = jCacheProxies.get(cacheName);

        DynamicCacheDescriptor desc = cacheDescriptor(cacheName);

        if (desc != null && !desc.cacheType().userCache())
            throw new IllegalStateException("Failed to get cache because it is a system cache: " + cacheName);

        if (cache == null) {
            dynamicStartCache(null, cacheName, null, false, failIfNotStarted, checkThreadTx).get();

            cache = jCacheProxies.get(cacheName);
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
        assert name != null;

        DynamicCacheDescriptor desc = cacheDescriptor(name);

        if (desc == null)
            throw new IllegalStateException("Cache doesn't exist: " + name);
        else
            return desc.cacheConfiguration();
    }

    /**
     * Get registered cache descriptor.
     *
     * @param name Name.
     * @return Descriptor.
     */
    public DynamicCacheDescriptor cacheDescriptor(String name) {
        return cachesInfo.registeredCaches().get(name);
    }

    /**
     * @return Cache descriptors.
     */
    public Collection<DynamicCacheDescriptor> cacheDescriptors() {
        return cachesInfo.registeredCaches().values();
    }

    /**
     * @param cacheId Cache ID.
     * @return Cache descriptor.
     */
    @Nullable public DynamicCacheDescriptor cacheDescriptor(int cacheId) {
        for (DynamicCacheDescriptor cacheDesc : cacheDescriptors()) {
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
        assert cacheCfg.getName() != null;

        String name = cacheCfg.getName();

        DynamicCacheDescriptor desc = cachesInfo.registeredTemplates().get(name);

        if (desc != null)
            return;

        DynamicCacheChangeRequest req = DynamicCacheChangeRequest.addTemplateRequest(ctx, cacheCfg);

        TemplateConfigurationFuture fut = new TemplateConfigurationFuture(req.cacheName(), req.deploymentId());

        TemplateConfigurationFuture old =
            (TemplateConfigurationFuture)pendingTemplateFuts.putIfAbsent(cacheCfg.getName(), fut);

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
    public <K, V> IgniteCacheProxy<K, V> jcache(String name) {
        assert name != null;

        IgniteCacheProxy<K, V> cache = (IgniteCacheProxy<K, V>)jCacheProxies.get(name);

        if (cache == null)
            throw new IllegalArgumentException("Cache is not configured: " + name);

        return cache;
    }

    /**
     * @param name Cache name.
     * @return Cache proxy.
     */
    @Nullable public IgniteCacheProxy jcacheProxy(String name) {
        return jCacheProxies.get(name);
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
     * @param name Cache name.
     * @param <K> type of keys.
     * @param <V> type of values.
     * @return Cache instance for given name.
     */
    @SuppressWarnings("unchecked")
    public <K, V> GridCacheAdapter<K, V> internalCache(String name) {
        assert name != null;

        if (log.isDebugEnabled())
            log.debug("Getting internal cache adapter: " + name);

        return (GridCacheAdapter<K, V>)caches.get(name);
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
    public boolean systemCache(String name) {
        assert name != null;

        DynamicCacheDescriptor desc = cacheDescriptor(name);

        return desc != null && !desc.cacheType().userCache();
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");

        for (GridCacheAdapter c : caches.values()) {
            X.println(">>> Cache memory stats [igniteInstanceName=" + ctx.igniteInstanceName() +
                ", cache=" + c.name() + ']');

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
    public void createMissingQueryCaches() throws IgniteCheckedException {
        for (Map.Entry<String, DynamicCacheDescriptor> e : cachesInfo.registeredCaches().entrySet()) {
            DynamicCacheDescriptor desc = e.getValue();

            if (isMissingQueryCache(desc))
                dynamicStartCache(null, desc.cacheConfiguration().getName(), null, false, true, true).get();
        }
    }

    /**
     * Whether cache defined by provided descriptor is not yet started and has queries enabled.
     *
     * @param desc Descriptor.
     * @return {@code True} if this is missing query cache.
     */
    private boolean isMissingQueryCache(DynamicCacheDescriptor desc) {
        CacheConfiguration ccfg = desc.cacheConfiguration();

        return !caches.containsKey(ccfg.getName()) && QueryUtils.isEnabled(ccfg);
    }

    /**
     * Registers MBean for cache components.
     *
     * @param obj Cache component.
     * @param cacheName Cache name.
     * @param near Near flag.
     * @throws IgniteCheckedException If registration failed.
     */
    @SuppressWarnings("unchecked")
    private void registerMbean(Object obj, @Nullable String cacheName, boolean near)
        throws IgniteCheckedException {
        assert obj != null;

        MBeanServer srvr = ctx.config().getMBeanServer();

        assert srvr != null;

        cacheName = U.maskName(cacheName);

        cacheName = near ? cacheName + "-near" : cacheName;

        final Object mbeanImpl = (obj instanceof IgniteMBeanAware) ? ((IgniteMBeanAware)obj).getMBean() : obj;

        for (Class<?> itf : mbeanImpl.getClass().getInterfaces()) {
            if (itf.getName().endsWith("MBean") || itf.getName().endsWith("MXBean")) {
                try {
                    U.registerCacheMBean(srvr, ctx.igniteInstanceName(), cacheName, obj.getClass().getName(), mbeanImpl,
                        (Class<Object>)itf);
                }
                catch (JMException e) {
                    throw new IgniteCheckedException("Failed to register MBean for component: " + obj, e);
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

        boolean needToUnregister = o instanceof IgniteMBeanAware;

        if (!needToUnregister) {
            for (Class<?> itf : o.getClass().getInterfaces()) {
                if (itf.getName().endsWith("MBean") || itf.getName().endsWith("MXBean")) {
                    needToUnregister = true;

                    break;
                }
            }
        }

        if (needToUnregister) {
            try {
                srvr.unregisterMBean(U.makeCacheMBeanName(ctx.igniteInstanceName(), cacheName, o.getClass().getName()));
            }
            catch (JMException e) {
                U.error(log, "Failed to unregister MBean for component: " + o, e);
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
        DynamicCacheDescriptor desc = cacheDescriptor(cacheName);

        DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(UUID.randomUUID(), cacheName, ctx.localNodeId());

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
                    req.schema(desc.schema());
                }
            }
            else {
                req.deploymentId(IgniteUuid.randomUuid());

                CacheConfiguration cfg = new CacheConfiguration(ccfg);

                CacheObjectContext cacheObjCtx = ctx.cacheObjects().contextForCache(cfg);

                initialize(cfg, cacheObjCtx);

                req.startCacheConfiguration(cfg);
                req.schema(new QuerySchema(cfg.getQueryEntities()));
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
            req.schema(desc.schema());
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
     *
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    private class DynamicCacheStartFuture extends GridFutureAdapter<Boolean> {
        /** Cache name. */
        private String cacheName;

        /** Change request. */
        @GridToStringInclude
        private DynamicCacheChangeRequest req;

        /**
         * @param cacheName Cache name.
         * @param req Cache start request.
         */
        private DynamicCacheStartFuture(String cacheName, DynamicCacheChangeRequest req) {
            this.cacheName = cacheName;
            this.req = req;
        }

        /**
         * @return Request.
         */
        public DynamicCacheChangeRequest request() {
            return req;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Boolean res, @Nullable Throwable err) {
            // Make sure to remove future before completion.
            pendingFuts.remove(req.requestId(), this);

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
            pendingTemplateFuts.remove(cacheName, this);

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
