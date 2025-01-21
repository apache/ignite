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
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.GridCachePluginContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.managers.encryption.GroupKey;
import org.apache.ignite.internal.managers.encryption.GroupKeyEncrypted;
import org.apache.ignite.internal.managers.systemview.walker.CacheGroupViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.CacheViewWalker;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteClusterReadOnlyException;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.query.QuerySchema;
import org.apache.ignite.internal.processors.query.QuerySchemaPatch;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.lang.GridPlainCallable;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.systemview.view.CacheGroupView;
import org.apache.ignite.spi.systemview.view.CacheView;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ALLOW_MIXED_CACHE_GROUPS;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CACHE_PROC;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CLUSTER_READ_ONLY_MODE_ERROR_MSG_FORMAT;
import static org.apache.ignite.internal.processors.cache.GridLocalConfigManager.validateIncomingConfiguration;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNP_IN_PROGRESS_ERR_MSG;

/**
 * Logic related to cache discovery data processing.
 */
public class ClusterCachesInfo {
    /** */
    public static final String CACHES_VIEW = "caches";

    /** */
    public static final String CACHES_VIEW_DESC = "Caches";

    /** */
    public static final String CACHE_GRPS_VIEW = "cacheGroups";

    /** */
    public static final String CACHE_GRPS_VIEW_DESC = "Cache groups";

    /** Representation of null for restarting caches map */
    private static final IgniteUuid NULL_OBJECT = new IgniteUuid();

    /** */
    private final GridKernalContext ctx;

    /**
     * Map contains cache descriptors that were removed from {@link #registeredCaches} due to cache stop request.
     * Such descriptors will be removed from the map only after whole cache stop process is finished.
     */
    private final ConcurrentNavigableMap<AffinityTopologyVersion, Map<String, DynamicCacheDescriptor>>
        markedForDeletionCaches = new ConcurrentSkipListMap<>();

    /**
     * Map contains cache group descriptors that were removed from {@link #registeredCacheGrps} due to cache stop request.
     * Such descriptors will be removed from the map only after whole cache stop process is finished.
     * Affinity topology version equals the version which will be applied after a cache group is completely removed.
     */
    private final ConcurrentNavigableMap<AffinityTopologyVersion, Map<Integer, CacheGroupDescriptor>>
        markedForDeletionCacheGrps = new ConcurrentSkipListMap<>();

    /** Dynamic caches. */
    private final ConcurrentMap<String, DynamicCacheDescriptor> registeredCaches = new ConcurrentHashMap<>();

    /** Mapping of caches by cache ID */
    private final ConcurrentMap<Integer, DynamicCacheDescriptor> registeredCachesById = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<Integer, CacheGroupDescriptor> registeredCacheGrps = new ConcurrentHashMap<>();

    /** Cache templates. */
    private final ConcurrentMap<String, DynamicCacheDescriptor> registeredTemplates = new ConcurrentHashMap<>();

    /** Caches currently being restarted (with restarter id). */
    private final ConcurrentHashMap<String, IgniteUuid> restartingCaches = new ConcurrentHashMap<>();

    /** */
    private final IgniteLogger log;

    /** */
    private CachesOnDisconnect cachesOnDisconnect;

    /** Local cache info */
    private CacheJoinNodeDiscoveryData joinDiscoData;

    /** Cluster cache info */
    private GridData gridData;

    /** */
    private LocalJoinCachesContext locJoinCachesCtx;

    /** */
    private Map<String, T2<CacheConfiguration, NearCacheConfiguration>> locCfgsForActivation = Collections.emptyMap();

    /** */
    private Map<UUID, CacheClientReconnectDiscoveryData> clientReconnectReqs;

    /** {@code True} if joined cluster while cluster state change was in progress. */
    private boolean joinOnTransition;

    /** Flag that caches were already filtered out. */
    private final AtomicBoolean alreadyFiltered = new AtomicBoolean();

    /**
     * @param ctx Context.
     */
    public ClusterCachesInfo(GridKernalContext ctx) {
        this.ctx = ctx;

        ctx.systemView().registerView(CACHES_VIEW, CACHES_VIEW_DESC,
            new CacheViewWalker(),
            registeredCaches.values(),
            (desc) -> new CacheView(desc, ctx));

        ctx.systemView().registerView(CACHE_GRPS_VIEW, CACHE_GRPS_VIEW_DESC,
            new CacheGroupViewWalker(),
            registeredCacheGrps.values(),
            CacheGroupView::new);

        log = ctx.log(getClass());
    }

    /**
     * Filters all dynamic cache descriptors and groups that were not presented on node start
     * and were received with grid discovery data.
     *
     * @param localCachesOnStart Caches which were already presented on node start.
     */
    public void filterDynamicCacheDescriptors(Set<String> localCachesOnStart) {
        if (!alreadyFiltered.compareAndSet(false, true))
            return;

        filterRegisteredCachesAndCacheGroups(localCachesOnStart);

        List<T2<DynamicCacheDescriptor, NearCacheConfiguration>> locJoinStartCaches = locJoinCachesCtx.caches();

        filterLocalJoinStartCaches(locJoinStartCaches);

        List<DynamicCacheDescriptor> initCaches = locJoinCachesCtx.initCaches();

        filterInitCaches(initCaches);

        locJoinCachesCtx = new LocalJoinCachesContext(
            locJoinStartCaches,
            initCaches,
            registeredCacheGrps,
            registeredCaches
        );
    }

    /**
     * Filters from registered caches all caches that are not presented in node's local configuration.
     *
     * Then filters from registered cache groups all groups that became empty after registered caches were filtered.
     *
     * @param locCaches Caches from local node configuration (static configuration and persistent caches).
     */
    private void filterRegisteredCachesAndCacheGroups(Set<String> locCaches) {
        //filter registered caches
        Iterator<Map.Entry<String, DynamicCacheDescriptor>> cachesIter = registeredCaches.entrySet().iterator();

        while (cachesIter.hasNext()) {
            Map.Entry<String, DynamicCacheDescriptor> e = cachesIter.next();

            if (!locCaches.contains(e.getKey())) {
                cachesIter.remove();

                registeredCachesById.remove(e.getValue());

                ctx.discovery().removeCacheFilter(e.getKey());
            }
        }

        //filter registered cache groups
        Iterator<Map.Entry<Integer, CacheGroupDescriptor>> grpsIter = registeredCacheGrps.entrySet().iterator();

        while (grpsIter.hasNext()) {
            Map.Entry<Integer, CacheGroupDescriptor> e = grpsIter.next();

            boolean removeGrp = true;

            for (DynamicCacheDescriptor cacheDescr : registeredCaches.values()) {
                if (cacheDescr.groupId() == e.getKey()) {
                    removeGrp = false;

                    break;
                }
            }

            if (removeGrp) {
                grpsIter.remove();

                ctx.discovery().removeCacheGroup(e.getValue());
            }
        }
    }

    /**
     * Filters from local join context cache descriptors that should be started on join.
     *
     * @param locJoinStartCaches Collection to filter.
     */
    private void filterLocalJoinStartCaches(
        List<T2<DynamicCacheDescriptor, NearCacheConfiguration>> locJoinStartCaches) {

        locJoinStartCaches.removeIf(next -> !registeredCaches.containsKey(next.getKey().cacheName()));
    }

    /**
     * Filters from local join context cache descriptors that require init of query infrastructure without cache
     * start.
     *
     * @param initCaches Collection to filter.
     */
    private void filterInitCaches(List<DynamicCacheDescriptor> initCaches) {
        initCaches.removeIf(desc -> !registeredCaches.containsKey(desc.cacheName()));
    }

    /**
     * @param joinDiscoData Information about configured caches and templates.
     * @throws IgniteCheckedException If configuration validation failed.
     */
    public void onStart(CacheJoinNodeDiscoveryData joinDiscoData) throws IgniteCheckedException {
        this.joinDiscoData = joinDiscoData;

        Map<String, CacheConfiguration> grpCfgs = new HashMap<>();

        for (CacheJoinNodeDiscoveryData.CacheInfo info : joinDiscoData.caches().values()) {
            if (info.cacheData().config().getGroupName() == null)
                continue;

            CacheConfiguration ccfg = grpCfgs.get(info.cacheData().config().getGroupName());

            if (ccfg == null)
                grpCfgs.put(info.cacheData().config().getGroupName(), info.cacheData().config());
            else
                validateCacheGroupConfiguration(ccfg, info.cacheData().config());
        }

        String conflictErr = processJoiningNode(joinDiscoData, ctx.localNodeId(), true);

        if (conflictErr != null)
            throw new IgniteCheckedException("Failed to start configured cache. " + conflictErr);
    }

    /**
     * @param checkConsistency {@code True} if need check cache configurations consistency.
     * @throws IgniteCheckedException If failed.
     */
    public void onKernalStart(boolean checkConsistency) throws IgniteCheckedException {
        if (gridData != null && gridData.conflictErr != null)
            throw new IgniteCheckedException(gridData.conflictErr);

        if (gridData != null && gridData.joinDiscoData != null) {
            CacheJoinNodeDiscoveryData joinDiscoData = gridData.joinDiscoData;

            for (CacheJoinNodeDiscoveryData.CacheInfo locCacheInfo : joinDiscoData.caches().values()) {
                CacheConfiguration locCfg = locCacheInfo.cacheData().config();

                CacheData cacheData = gridData.gridData.caches().get(locCfg.getName());

                if (cacheData != null) {
                    if (!F.eq(cacheData.sql(), locCacheInfo.sql())) {
                        throw new IgniteCheckedException("Cache configuration mismatch (local cache was created " +
                            "via " + (locCacheInfo.sql() ? "CREATE TABLE" : "Ignite API") + ", while remote cache " +
                            "was created via " + (cacheData.sql() ? "CREATE TABLE" : "Ignite API") + "): " +
                            locCacheInfo.cacheData().config().getName());
                    }

                    if (checkConsistency) {
                        checkCache(locCacheInfo, cacheData, cacheData.receivedFrom());

                        ClusterNode rmt = ctx.discovery().node(cacheData.receivedFrom());

                        if (rmt == null) {
                            for (ClusterNode node : ctx.discovery().localJoin().discoCache().serverNodes()) {
                                if (!node.isLocal() && ctx.discovery().cacheAffinityNode(node, locCfg.getName())) {
                                    rmt = node;

                                    break;
                                }
                            }
                        }

                        if (rmt != null) {
                            for (PluginProvider p : ctx.plugins().allProviders()) {
                                CachePluginContext pluginCtx = new GridCachePluginContext(ctx, locCfg);

                                CachePluginProvider provider = p.createCacheProvider(pluginCtx);

                                if (provider != null)
                                    provider.validateRemote(locCfg, cacheData.cacheConfiguration(), rmt);
                            }
                        }
                    }
                }

                if (checkConsistency)
                    validateStartCacheConfiguration(locCfg);
            }
        }

        gridData = null;
    }

    /**
     * Checks that remote caches has configuration compatible with the local.
     *
     * @param locInfo Local configuration.
     * @param rmtData Remote configuration.
     * @param rmt Remote node.
     * @throws IgniteCheckedException If check failed.
     */
    @SuppressWarnings("unchecked")
    private void checkCache(CacheJoinNodeDiscoveryData.CacheInfo locInfo, CacheData rmtData, UUID rmt)
        throws IgniteCheckedException {
        GridCacheAttributes rmtAttr = new GridCacheAttributes(rmtData.cacheConfiguration(), rmtData.cacheConfigurationEnrichment());
        GridCacheAttributes locAttr =
            new GridCacheAttributes(locInfo.cacheData().config(), locInfo.cacheData().cacheConfigurationEnrichment());

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cacheMode", "Cache mode",
            locAttr.cacheMode(), rmtAttr.cacheMode(), true);

        CU.checkAttributeMismatch(log, rmtAttr.groupName(), rmt, "groupName", "Cache group name",
            locAttr.groupName(), rmtAttr.groupName(), true);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "interceptor", "Cache Interceptor",
            locAttr.interceptorClassName(), rmtAttr.interceptorClassName(), true);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "atomicityMode",
            "Cache atomicity mode", locAttr.atomicityMode(), rmtAttr.atomicityMode(), true);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cachePreloadMode",
            "Cache preload mode", locAttr.cacheRebalanceMode(), rmtAttr.cacheRebalanceMode(), true);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "topologyValidator",
            "Cache topology validator", locAttr.topologyValidatorClassName(), rmtAttr.topologyValidatorClassName(), true);

        ClusterNode rmtNode = ctx.discovery().node(rmt);

        if (CU.affinityNode(ctx.discovery().localNode(), locInfo.cacheData().config().getNodeFilter())
            && rmtNode != null && CU.affinityNode(rmtNode, rmtData.cacheConfiguration().getNodeFilter())) {
            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "storeFactory", "Store factory",
                locAttr.storeFactoryClassName(), rmtAttr.storeFactoryClassName(), true);
        }

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cacheAffinity", "Cache affinity",
            locAttr.cacheAffinityClassName(), rmtAttr.cacheAffinityClassName(), true);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cacheAffinityMapper",
            "Cache affinity mapper", locAttr.cacheAffinityMapperClassName(),
            rmtAttr.cacheAffinityMapperClassName(), true);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "affinityPartitionsCount",
            "Affinity partitions count", locAttr.affinityPartitionsCount(),
            rmtAttr.affinityPartitionsCount(), true);

        CU.validateKeyConfigiration(rmtAttr.groupName(), rmtAttr.cacheName(), rmt, rmtAttr.configuration().getKeyConfiguration(),
            locAttr.configuration().getKeyConfiguration(), log, true);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "evictionFilter", "Eviction filter",
            locAttr.evictionFilterClassName(), rmtAttr.evictionFilterClassName(), true);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "evictionPolicy", "Eviction policy",
            locAttr.evictionPolicyClassName(), rmtAttr.evictionPolicyClassName(), true);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "evictionPolicyFactory", "Eviction policy factory",
            locAttr.evictionPolicyFactoryClassName(), rmtAttr.evictionPolicyFactoryClassName(), true);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "transactionManagerLookup",
            "Transaction manager lookup", locAttr.transactionManagerLookupClassName(),
            rmtAttr.transactionManagerLookupClassName(), false);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "defaultLockTimeout",
            "Default lock timeout", locAttr.defaultLockTimeout(), rmtAttr.defaultLockTimeout(), false);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "preloadBatchSize",
            "Preload batch size", locAttr.rebalanceBatchSize(), rmtAttr.rebalanceBatchSize(), false);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "rebalanceDelay",
            "Rebalance delay", locAttr.rebalanceDelay(), rmtAttr.rebalanceDelay(), false);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "rebalanceBatchesPrefetchCount",
            "Rebalance batches prefetch count", locAttr.rebalanceBatchesPrefetchCount(),
            rmtAttr.rebalanceBatchesPrefetchCount(), false);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "rebalanceOrder",
            "Rebalance order", locAttr.rebalanceOrder(), rmtAttr.rebalanceOrder(), false);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "rebalanceThrottle",
            "Rebalance throttle", locAttr.rebalanceThrottle(), rmtAttr.rebalanceThrottle(), false);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "rebalanceTimeout",
            "Rebalance timeout", locAttr.rebalanceTimeout(), rmtAttr.rebalanceTimeout(), false);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "writeSynchronizationMode",
            "Write synchronization mode", locAttr.writeSynchronization(), rmtAttr.writeSynchronization(),
            true);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "writeBehindBatchSize",
            "Write behind batch size", locAttr.writeBehindBatchSize(), rmtAttr.writeBehindBatchSize(),
            false);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "writeBehindCoalescing",
            "Write behind coalescing", locAttr.writeBehindCoalescing(), rmtAttr.writeBehindCoalescing(),
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

        if (locAttr.cacheMode() == PARTITIONED) {
            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "nearEvictionPolicy",
                "Near eviction policy", locAttr.nearEvictionPolicyClassName(),
                rmtAttr.nearEvictionPolicyClassName(), false);

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "nearEvictionPolicyFactory",
                "Near eviction policy factory", locAttr.nearEvictionPolicyFactoryClassName(),
                rmtAttr.nearEvictionPolicyFactoryClassName(), false);

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "affinityIncludeNeighbors",
                "Affinity include neighbors", locAttr.affinityIncludeNeighbors(),
                rmtAttr.affinityIncludeNeighbors(), true);

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "affinityKeyBackups",
                "Affinity key backups", locAttr.affinityKeyBackups(),
                rmtAttr.affinityKeyBackups(), true);

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "qryParallelism",
                "Query parallelism", locAttr.qryParallelism(), rmtAttr.qryParallelism(), true);
        }

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "isEncryptionEnabled",
            "Cache encrypted", locAttr.isEncryptionEnabled(), rmtAttr.isEncryptionEnabled(), true);
    }

    /**
     * @param msg Message.
     * @param node Node sent message.
     */
    public void onClientCacheChange(ClientCacheChangeDiscoveryMessage msg, ClusterNode node) {
        Map<Integer, Boolean> startedCaches = msg.startedCaches();

        if (startedCaches != null) {
            for (Map.Entry<Integer, Boolean> e : startedCaches.entrySet()) {
                for (DynamicCacheDescriptor desc : registeredCaches.values()) {
                    if (e.getKey().equals(desc.cacheId())) {
                        ctx.discovery().addClientNode(desc.cacheName(), node.id(), e.getValue());

                        break;
                    }
                }
            }
        }

        Set<Integer> closedCaches = msg.closedCaches();

        if (closedCaches != null) {
            for (Integer cacheId : closedCaches) {
                for (DynamicCacheDescriptor desc : registeredCaches.values()) {
                    if (cacheId.equals(desc.cacheId())) {
                        ctx.discovery().onClientCacheClose(desc.cacheName(), node.id());

                        break;
                    }
                }
            }
        }
    }

    /**
     * Creates exchanges actions. Forms a list of caches and cache groups to be stopped
     * due to dynamic cache start failure.
     *
     * @param failMsg Dynamic change request fail message.
     * @param topVer Current topology version.
     */
    public void onCacheChangeRequested(DynamicCacheChangeFailureMessage failMsg, AffinityTopologyVersion topVer) {
        AffinityTopologyVersion actualTopVer = failMsg.exchangeId().topologyVersion();

        ExchangeActions exchangeActions = new ExchangeActions();

        List<T2<DynamicCacheChangeRequest, DynamicCacheDescriptor>> descs = new ArrayList<>(failMsg.cacheNames().size());

        for (String cacheName : failMsg.cacheNames()) {
            DynamicCacheDescriptor cacheDescr = null;

            // The required cache desriptor may have already been deleted in the following scenario:
            // 1 - DynamicCacheChangeBatch(start a new user cache with name "A")
            // 2 - At the same, the user initiates destroing the cache which should be started on the step 1.
            // 3 - The corresponding exchange (see step 1) results in an error, and so, the start should be rolled back.
            // 4 - When DynamicCacheChangeFailureMessage received the reqired cache descriptor is already deleted
            //     and therefore, we need to scan markedForDeletion collections in order to fins the right descriptor.

            // Find the "earliest" available descriptor.
            for (Map<String, DynamicCacheDescriptor> descriptors : markedForDeletionCaches.tailMap(actualTopVer).values()) {
                cacheDescr = descriptors.get(cacheName);

                if (cacheDescr != null)
                    break;
            }

            if (cacheDescr == null)
                cacheDescr = registeredCaches.get(cacheName);

            assert cacheDescr != null : "Dynamic cache descriptor is missing [cacheName=" + cacheName + "]";

            DynamicCacheChangeRequest req = DynamicCacheChangeRequest.stopRequest(ctx, cacheName, cacheDescr.sql(), true);

            descs.add(new T2<>(req, cacheDescr));
        }

        CacheChangeProcessResult res = new CacheChangeProcessResult();

        for (T2<DynamicCacheChangeRequest, DynamicCacheDescriptor> desc : descs) {
            DynamicCacheChangeRequest req = desc.get1();
            DynamicCacheDescriptor cacheDesc = desc.get2();

            processStopCacheRequest(exchangeActions, req, res, req.cacheName(), cacheDesc, actualTopVer, true);
        }

        failMsg.exchangeActions(exchangeActions);
    }

    /**
     * @param batch Cache change request.
     * @param topVer Current topology version.
     * @return {@code True} if minor topology version should be increased.
     */
    public boolean onCacheChangeRequested(DynamicCacheChangeBatch batch, AffinityTopologyVersion topVer) {
        DiscoveryDataClusterState state = ctx.state().clusterState();

        if (state.state().active() && !state.transition()) {
            Set<IgniteUuid> restartIds = new HashSet<>(F.viewReadOnly(
                batch.requests(), DynamicCacheChangeRequest::restartId, req -> req.start() && req.restartId() != null));

            assert restartIds.size() <= 1 : batch.requests();

            Collection<UUID> nodes = ctx.cache().context().snapshotMgr().cacheStartRequiredAliveNodes(F.first(restartIds));

            for (UUID nodeId : nodes) {
                ClusterNode node = ctx.discovery().node(nodeId);

                if (node != null && CU.baselineNode(node, state) && ctx.discovery().alive(node))
                    continue;

                ClusterTopologyCheckedException err =
                    new ClusterTopologyCheckedException("Required node has left the cluster [nodeId=" + nodeId + ']');

                for (DynamicCacheChangeRequest req : batch.requests())
                    ctx.cache().completeCacheStartFuture(req, false, err);

                return false;
            }

            ExchangeActions exchangeActions = new ExchangeActions();

            CacheChangeProcessResult res = processCacheChangeRequests(exchangeActions,
                batch.requests(),
                topVer,
                false);

            if (res.needExchange) {
                assert !exchangeActions.empty() : exchangeActions;

                batch.exchangeActions(exchangeActions);

                if (!nodes.isEmpty())
                    exchangeActions.cacheStartRequiredAliveNodes(nodes);
            }

            return res.needExchange;
        }
        else {
            IgniteCheckedException err = new IgniteCheckedException("Failed to start/stop cache, cluster state change " +
                "is in progress.");

            for (DynamicCacheChangeRequest req : batch.requests()) {
                if (req.template()) {
                    ctx.cache().completeTemplateAddFuture(req.startCacheConfiguration().getName(),
                        req.deploymentId());
                }
                else
                    ctx.cache().completeCacheStartFuture(req, false, err);
            }

            return false;
        }
    }

    /**
     * @param exchangeActions Exchange actions to update.
     * @param reqs Requests.
     * @param topVer Current topology version.
     * @param persistedCfgs {@code True} if process start of persisted caches during cluster activation.
     * @return Process result.
     */
    private CacheChangeProcessResult processCacheChangeRequests(
        ExchangeActions exchangeActions,
        Collection<DynamicCacheChangeRequest> reqs,
        AffinityTopologyVersion topVer,
        boolean persistedCfgs
    ) {
        CacheChangeProcessResult res = new CacheChangeProcessResult();

        final List<T2<DynamicCacheChangeRequest, AffinityTopologyVersion>> reqsToComplete = new ArrayList<>();

        for (DynamicCacheChangeRequest req : reqs)
            processCacheChangeRequest0(req, exchangeActions, topVer, persistedCfgs, res, reqsToComplete);

        if (!F.isEmpty(res.addedDescs)) {
            AffinityTopologyVersion startTopVer = res.needExchange ? topVer.nextMinorVersion() : topVer;

            for (DynamicCacheDescriptor desc : res.addedDescs) {
                assert desc.template() || res.needExchange;

                desc.startTopologyVersion(startTopVer);
            }
        }

        if (!F.isEmpty(reqsToComplete)) {
            ctx.closure().callLocalSafe(new GridPlainCallable<Void>() {
                @Override public Void call() {
                    for (T2<DynamicCacheChangeRequest, AffinityTopologyVersion> t : reqsToComplete) {
                        final DynamicCacheChangeRequest req = t.get1();
                        AffinityTopologyVersion waitTopVer = t.get2();

                        IgniteInternalFuture<?> fut = waitTopVer != null ?
                            ctx.cache().context().exchange().affinityReadyFuture(waitTopVer) : null;

                        if (fut == null || fut.isDone())
                            ctx.cache().completeCacheStartFuture(req, false, null);
                        else {
                            fut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
                                @Override public void apply(IgniteInternalFuture<?> fut) {
                                    ctx.cache().completeCacheStartFuture(req, false, null);
                                }
                            });
                        }
                    }

                    return null;
                }
            });
        }

        return res;
    }

    /**
     * @param req Cache change request.
     * @param exchangeActions Exchange actions to update.
     * @param topVer Current topology version.
     * @param persistedCfgs {@code True} if process start of persisted caches during cluster activation.
     * @param res Accumulator for cache change process results.
     * @param reqsToComplete Accumulator for cache change requests which should be completed after
     * ({@link org.apache.ignite.internal.processors.cache.GridCacheProcessor#pendingFuts}
     */
    private void processCacheChangeRequest0(
        DynamicCacheChangeRequest req,
        ExchangeActions exchangeActions,
        AffinityTopologyVersion topVer,
        boolean persistedCfgs,
        CacheChangeProcessResult res,
        List<T2<DynamicCacheChangeRequest, AffinityTopologyVersion>> reqsToComplete
    ) {
        String cacheName = req.cacheName();

        if (req.template()) {
            processTemplateAddRequest(persistedCfgs, res, req);

            return;
        }

        assert !req.clientStartOnly() : req;

        DynamicCacheDescriptor desc = cacheName != null ? registeredCaches.get(cacheName) : null;

        boolean needExchange = false;

        boolean clientCacheStart = false;

        AffinityTopologyVersion waitTopVer = null;

        if (req.start()) {
            boolean proceedFuther = true;

            if (restartingCaches.containsKey(cacheName) &&
                ((req.restartId() == null && restartingCaches.get(cacheName) != NULL_OBJECT)
                    || (req.restartId() != null && !req.restartId().equals(restartingCaches.get(cacheName))))) {

                if (req.failIfExists()) {
                    ctx.cache().completeCacheStartFuture(req, false,
                        new CacheExistsException("Failed to start cache (a cache is restarting): " + cacheName));
                }

                proceedFuther = false;
            }

            if (proceedFuther) {
                if (desc == null) { /* Starting a new cache.*/
                    if (!processStartNewCacheRequest(exchangeActions, topVer, persistedCfgs, res, req, cacheName))
                        return;

                    needExchange = true;
                }
                else {
                    clientCacheStart = processStartAlreadyStartedCacheRequest(topVer, persistedCfgs, req, cacheName, desc);

                    if (!clientCacheStart) {
                        if (desc.clientCacheStartVersion() != null)
                            waitTopVer = desc.clientCacheStartVersion();
                        else {
                            AffinityTopologyVersion nodeStartVer =
                                new AffinityTopologyVersion(ctx.discovery().localNode().order(), 0);

                            if (desc.startTopologyVersion() != null)
                                waitTopVer = desc.startTopologyVersion();
                            else
                                waitTopVer = desc.receivedFromStartVersion();

                            if (waitTopVer == null || nodeStartVer.compareTo(waitTopVer) > 0)
                                waitTopVer = nodeStartVer;
                        }
                    }
                }
            }
        }
        else if (req.resetLostPartitions()) {
            if (desc != null) {
                needExchange = true;

                exchangeActions.addCacheToResetLostPartitions(req, desc);
            }
        }
        else if (req.finalizePartitionCounters()) {
            needExchange = true;

            exchangeActions.addFinalizePartitionCounters(req);
        }
        else if (req.stop()) {
            if (desc != null) {
                if (req.sql() && !desc.sql()) {
                    ctx.cache().completeCacheStartFuture(req, false,
                        new IgniteCheckedException("Only cache created with CREATE TABLE may be removed with " +
                            "DROP TABLE [cacheName=" + cacheName + ']'));

                    return;
                }

                if (!processStopCacheRequest(exchangeActions, req, res, cacheName, desc, topVer.nextMinorVersion(), false))
                    return;

                needExchange = true;
            }
        }
        else
            assert false : req;

        if (!needExchange) {
            if (!clientCacheStart && ctx.localNodeId().equals(req.initiatingNodeId()))
                reqsToComplete.add(new T2<>(req, waitTopVer));
        }
        else
            res.needExchange = true;
    }

    /**
     * @param req Cache change request.
     * @param exchangeActions Exchange actions to update.
     * @param cacheName Cache name.
     * @param desc Dynamic cache descriptor.
     * @param topVer Topology version that will be applied after the corresponding partition map exchange.
     * @param checkForAlreadyDeleted {@code true} indicates that the given cache descriptor may have already been deleted.
     *                              This is possible in the case when the {@code DynamicCacheChangeFailureMessage}
     *                              is received after the {@code DynamicCacheChangeRequest},
     *                              which is responsible for stopping the cache.
     * @return {@code true} if stop request can be proceed.
     */
    private boolean processStopCacheRequest(
        ExchangeActions exchangeActions,
        DynamicCacheChangeRequest req,
        CacheChangeProcessResult res,
        String cacheName,
        DynamicCacheDescriptor desc,
        AffinityTopologyVersion topVer,
        boolean checkForAlreadyDeleted
    ) {
        if (ctx.cache().context().snapshotMgr().isSnapshotCreating()) {
            IgniteCheckedException err = new IgniteCheckedException(SNP_IN_PROGRESS_ERR_MSG);

            U.warn(log, err);

            res.errs.add(err);

            ctx.cache().completeCacheStartFuture(req, false, err);

            return false;
        }

        boolean alreadyRemovedDesc = false;
        DynamicCacheDescriptor old = null;

        if (checkForAlreadyDeleted) {
            // Find the "earliest" available descriptor.
            for (Map<String, DynamicCacheDescriptor> descriptors : markedForDeletionCaches.tailMap(topVer).values()) {
                old = descriptors.get(cacheName);

                if (old != null)
                    break;
            }

            alreadyRemovedDesc = old != null;
        }

        if (old == null) {
            old = registeredCaches.get(cacheName);

            markedForDeletionCaches
                .computeIfAbsent(topVer, map -> new ConcurrentHashMap<>())
                .put(cacheName, old);

            registeredCaches.remove(cacheName);

            ctx.discovery().removeCacheFilter(cacheName);

            alreadyRemovedDesc = false;
        }

        assert old != null && old == desc : "Dynamic cache map was concurrently modified [req=" + req + ']';

        registeredCachesById.remove(old.cacheId());

        if (req.restart()) {
            IgniteUuid restartId = req.restartId();

            restartingCaches.put(cacheName, restartId == null ? NULL_OBJECT : restartId);
        }

        exchangeActions.addCacheToStop(req, desc);

        boolean alreadyRemovedGrp = false;
        CacheGroupDescriptor grpDesc = null;

        if (checkForAlreadyDeleted) {
            // Find the "earliest" available descriptor.
            for (Map<Integer, CacheGroupDescriptor> descriptors : markedForDeletionCacheGrps.tailMap(topVer).values()) {
                grpDesc = descriptors.get(desc.groupId());

                if (grpDesc != null)
                    break;
            }

            alreadyRemovedGrp = grpDesc != null;
        }

        if (grpDesc == null) {
            grpDesc = registeredCacheGrps.get(desc.groupId());

            alreadyRemovedGrp = false;
        }

        assert grpDesc != null && grpDesc.groupId() == desc.groupId() : desc;

        if (!alreadyRemovedDesc)
            grpDesc.onCacheStopped(desc.cacheName(), desc.cacheId());

        if (!grpDesc.hasCaches()) {
            if (!alreadyRemovedGrp) {
                markedForDeletionCacheGrps
                    .computeIfAbsent(topVer, (map) -> new ConcurrentHashMap<>())
                    .put(grpDesc.groupId(), grpDesc);

                ctx.discovery().removeCacheGroup(grpDesc);
            }

            registeredCacheGrps.remove(grpDesc.groupId());

            exchangeActions.addCacheGroupToStop(grpDesc, req.destroy());

            assert exchangeActions.checkStopRequestConsistency(grpDesc.groupId());
        }

        return true;
    }

    /**
     * @param persistedCfgs {@code True} if process start of persisted caches during cluster activation.
     * @param res Accumulator for cache change process results.
     * @param req Dynamic cache change request.
     */
    private void processTemplateAddRequest(
        boolean persistedCfgs,
        CacheChangeProcessResult res,
        DynamicCacheChangeRequest req
    ) {
        CacheConfiguration ccfg = req.startCacheConfiguration();

        assert ccfg != null : req;

        DynamicCacheDescriptor desc = registeredTemplates.get(req.cacheName());

        if (desc == null) {
            DynamicCacheDescriptor templateDesc = new DynamicCacheDescriptor(ctx,
                ccfg,
                req.cacheType(),
                null,
                true,
                req.initiatingNodeId(),
                false,
                false,
                req.deploymentId(),
                req.schema(),
                req.cacheConfigurationEnrichment()
            );

            DynamicCacheDescriptor old = registeredTemplates().put(ccfg.getName(), templateDesc);

            assert old == null;

            res.addedDescs.add(templateDesc);
        }

        if (!persistedCfgs)
            ctx.cache().completeTemplateAddFuture(ccfg.getName(), req.deploymentId());
    }

    /**
     * @param topVer Topology version.
     * @param persistedCfgs {@code True} if process start of persisted caches during cluster activation.
     * @param req Cache change request.
     * @param cacheName Cache name.
     * @param desc Dynamic cache descriptor.
     * @return True if it is needed to start client cache.
     */
    private boolean processStartAlreadyStartedCacheRequest(
        AffinityTopologyVersion topVer,
        boolean persistedCfgs,
        DynamicCacheChangeRequest req,
        String cacheName,
        DynamicCacheDescriptor desc
    ) {
        assert !persistedCfgs;
        assert req.initiatingNodeId() != null : req;

        if (req.failIfExists()) {
            ctx.cache().completeCacheStartFuture(req, false,
                new CacheExistsException("Failed to start cache " +
                    "(a cache with the same name is already started): " + cacheName));
        }
        else {
            // Cache already exists, it is possible client cache is needed.
            ClusterNode node = ctx.discovery().node(req.initiatingNodeId());

            boolean clientReq = node != null &&
                !ctx.discovery().cacheAffinityNode(node, cacheName);

            if (clientReq) {
                ctx.discovery().addClientNode(cacheName,
                    req.initiatingNodeId(),
                    req.nearCacheConfiguration() != null);

                if (node.id().equals(req.initiatingNodeId())) {
                    desc.clientCacheStartVersion(topVer);

                    ctx.discovery().clientCacheStartEvent(req.requestId(), F.asMap(cacheName, req), null);

                    return true;
                }
            }
        }

        return false;
    }

    /**
     * @param exchangeActions Exchange actions to update.
     * @param topVer Topology version.
     * @param persistedCfgs {@code True} if process start of persisted caches during cluster activation.
     * @param res Accumulator for cache change process results.
     * @param req Cache change request.
     * @param cacheName Cache name.
     * @return True if there was no errors.
     */
    private boolean processStartNewCacheRequest(
        ExchangeActions exchangeActions,
        AffinityTopologyVersion topVer,
        boolean persistedCfgs,
        CacheChangeProcessResult res,
        DynamicCacheChangeRequest req,
        String cacheName
    ) {
        assert exchangeActions != null;

        CacheConfiguration<?, ?> ccfg = req.startCacheConfiguration();

        IgniteCheckedException err = null;

        if (ctx.cache().context().readOnlyMode()) {
            err = new IgniteClusterReadOnlyException(
                String.format(CLUSTER_READ_ONLY_MODE_ERROR_MSG_FORMAT, "start cache", ccfg.getGroupName(), cacheName)
            );
        }

        if (err == null)
            err = QueryUtils.checkQueryEntityConflicts(req.startCacheConfiguration(), registeredCaches.values());

        if (err == null) {
            String conflictErr = checkCacheConflict(req.startCacheConfiguration(), false);

            if (conflictErr != null) {
                U.warn(log, "Ignore cache start request. " + conflictErr);

                err = new IgniteCheckedException("Failed to start cache. " + conflictErr);
            }
        }

        if (err == null) {
            GridEncryptionManager encMgr = ctx.encryption();

            if (ccfg.isEncryptionEnabled()) {
                if (encMgr.isMasterKeyChangeInProgress())
                    err = new IgniteCheckedException("Cache start failed. Master key change is in progress.");
                else if (encMgr.masterKeyDigest() != null &&
                    !Arrays.equals(encMgr.masterKeyDigest(), req.masterKeyDigest())) {
                    err = new IgniteCheckedException("Cache start failed. The request was initiated before " +
                        "the master key change and can't be processed.");
                }

                if (err != null)
                    U.warn(log, "Ignore cache start request during the master key change process.", err);
            }
        }

        if (err == null && req.restartId() == null) {
            IgniteSnapshotManager snapshotMgr = ctx.cache().context().snapshotMgr();

            if (snapshotMgr.isRestoring(ccfg)) {
                err = new IgniteCheckedException("Cache start failed. A cache or group with the same name is " +
                    "currently being restored from a snapshot [cache=" + cacheName +
                    (ccfg.getGroupName() == null ? "" : ", group=" + ccfg.getGroupName()) + ']');
            }
        }

        if (containsInvalidFileNameChars(ccfg)) {
            err = new IgniteCheckedException("Cache start failed. Cache or group name contains the characters " +
                "that are not allowed in file names [cache=" + cacheName +
                (ccfg.getGroupName() == null ? "" : ", group=" + ccfg.getGroupName()) + ']');
        }

        if (err != null) {
            if (persistedCfgs)
                res.errs.add(err);
            else
                ctx.cache().completeCacheStartFuture(req, false, err);

            return false;
        }

        assert req.cacheType() != null : req;
        assert F.eq(ccfg.getName(), cacheName) : req;

        int cacheId = CU.cacheId(cacheName);

        CacheGroupDescriptor grpDesc = registerCacheGroup(exchangeActions,
            topVer,
            ccfg,
            cacheId,
            req.initiatingNodeId(),
            req.deploymentId(),
            req.encryptionKey(),
            req.encryptionKeyId(),
            req.cacheConfigurationEnrichment()
        );

        DynamicCacheDescriptor startDesc = new DynamicCacheDescriptor(ctx,
            ccfg,
            req.cacheType(),
            grpDesc,
            false,
            req.initiatingNodeId(),
            false,
            req.sql(),
            req.deploymentId(),
            req.schema(),
            req.cacheConfigurationEnrichment()
        );

        DynamicCacheDescriptor old = registeredCaches.put(ccfg.getName(), startDesc);

        registeredCachesById.put(startDesc.cacheId(), startDesc);

        restartingCaches.remove(ccfg.getName());

        assert old == null;

        ctx.discovery().setCacheFilter(
            startDesc.cacheId(),
            grpDesc.groupId(),
            ccfg.getName(),
            ccfg.getNearConfiguration() != null);

        if (!persistedCfgs) {
            ctx.discovery().addClientNode(cacheName,
                req.initiatingNodeId(),
                req.nearCacheConfiguration() != null);
        }

        res.addedDescs.add(startDesc);

        exchangeActions.addCacheToStart(req, startDesc);

        return true;
    }

    /** @return {@code True} if cache directory contains the characters that are not allowed in file names. */
    private boolean containsInvalidFileNameChars(CacheConfiguration<?, ?> ccfg) {
        if (!CU.isPersistentCache(ccfg, ctx.config().getDataStorageConfiguration()))
            return false;

        String expDir = cacheDirName(ccfg);

        try {
            return !expDir.equals(Paths.get(expDir).toFile().getName());
        }
        catch (InvalidPathException ignored) {
            return true;
        }
    }

    /**
     * Validate correcteness of new cache start request.
     *
     * @param err Current error.
     * @param persistedCfgs {@code True} if process start of persisted caches during cluster activation.
     * @param res Accumulator for cache change process results.
     * @param req Cache change request.
     *
     * @return {@code True} if there is no errors due initialization.
     */
    private boolean validateStartNewCache(
        @Nullable IgniteCheckedException err,
        boolean persistedCfgs,
        CacheChangeProcessResult res,
        DynamicCacheChangeRequest req
    ) {
        if (err != null) {
            if (persistedCfgs)
                res.errs.add(err);
            else
                ctx.cache().completeCacheStartFuture(req, false, err);

            return false;
        }

        return true;
    }

    /**
     * @param dataBag Discovery data bag.
     */
    void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        dataBag.addJoiningNodeData(CACHE_PROC.ordinal(), joinDiscoveryData());
    }

    /**
     * @return {@code True} if there are currently restarting caches.
     */
    boolean hasRestartingCaches() {
        return !F.isEmpty(restartingCaches);
    }

    /**
     * @return Collection of currently restarting caches.
     */
    Collection<String> restartingCaches() {
        return restartingCaches.keySet();
    }

    /**
     * @return Discovery date sent on local node join.
     */
    private Serializable joinDiscoveryData() {
        if (cachesOnDisconnect != null) {
            Map<Integer, CacheClientReconnectDiscoveryData.CacheGroupInfo> cacheGrpsInfo = new HashMap<>();
            Map<String, CacheClientReconnectDiscoveryData.CacheInfo> cachesInfo = new HashMap<>();

            Map<Integer, CacheGroupDescriptor> grps = cachesOnDisconnect.cacheGrps;
            Map<String, DynamicCacheDescriptor> caches = cachesOnDisconnect.caches;

            for (CacheGroupContext grp : ctx.cache().cacheGroups()) {
                CacheGroupDescriptor desc = grps.get(grp.groupId());

                assert desc != null : grp.cacheOrGroupName();

                cacheGrpsInfo.put(grp.groupId(), new CacheClientReconnectDiscoveryData.CacheGroupInfo(desc.config(),
                    desc.deploymentId(),
                    0));
            }

            for (IgniteInternalCache cache : ctx.cache().caches()) {
                DynamicCacheDescriptor desc = caches.get(cache.name());

                assert desc != null : cache.name();

                cachesInfo.put(cache.name(), new CacheClientReconnectDiscoveryData.CacheInfo(desc.cacheConfiguration(),
                    desc.cacheType(),
                    desc.deploymentId(),
                    cache.context().isNear(),
                    0));
            }

            return new CacheClientReconnectDiscoveryData(cacheGrpsInfo, cachesInfo);
        }
        else {
            assert joinDiscoData != null;

            return joinDiscoData;
        }
    }

    /**
     * Called from exchange worker.
     *
     * @return Caches to be started when this node starts.
     */
    @Nullable public LocalJoinCachesContext localJoinCachesContext() {
        LocalJoinCachesContext result = locJoinCachesCtx;

        locJoinCachesCtx = null;

        return result;
    }

    /**
     * @param joinedNodeId Joined node ID.
     * @return {@code True} if there are new caches received from joined node.
     */
    boolean hasCachesReceivedFromJoin(UUID joinedNodeId) {
        for (DynamicCacheDescriptor desc : registeredCaches.values()) {
            if (desc.staticallyConfigured()) {
                assert desc.receivedFrom() != null : desc;

                if (joinedNodeId.equals(desc.receivedFrom()))
                    return true;
            }
        }

        return false;
    }

    /**
     * @param joinedNodeId Joined node ID.
     * @return New caches received from joined node.
     */
    List<DynamicCacheDescriptor> cachesReceivedFromJoin(UUID joinedNodeId) {
        assert joinedNodeId != null;

        List<DynamicCacheDescriptor> started = null;

        for (DynamicCacheDescriptor desc : orderedCaches(CacheComparators.DIRECT)) {
            if (desc.staticallyConfigured()) {
                assert desc.receivedFrom() != null : desc;

                if (joinedNodeId.equals(desc.receivedFrom())) {
                    if (started == null)
                        started = new ArrayList<>();

                    started.add(desc);
                }
            }
        }

        return started != null ? started : Collections.<DynamicCacheDescriptor>emptyList();
    }

    /**
     * Discovery event callback, executed from discovery thread.
     *
     * @param type Event type.
     * @param node Event node.
     * @param topVer Topology version.
     */
    public void onDiscoveryEvent(int type, ClusterNode node, AffinityTopologyVersion topVer) {
        if (type == EVT_NODE_JOINED) {
            for (CacheGroupDescriptor desc : registeredCacheGrps.values()) {
                if (node.id().equals(desc.receivedFrom()))
                    desc.receivedFromStartVersion(topVer);
            }

            for (DynamicCacheDescriptor desc : registeredCaches.values()) {
                if (node.id().equals(desc.receivedFrom()))
                    desc.receivedFromStartVersion(topVer);
            }

            for (DynamicCacheDescriptor desc : registeredTemplates.values()) {
                if (node.id().equals(desc.receivedFrom()))
                    desc.receivedFromStartVersion(topVer);
            }

            if (node.id().equals(ctx.discovery().localNode().id())) {
                if (gridData == null) { // First node starts.
                    assert joinDiscoData != null;

                    initStartCachesForLocalJoin(true, false);
                }
            }
        }
    }

    /**
     * @param dataBag Discovery data bag.
     * @param splitter Cache configuration splitter.
     */
    public void collectGridNodeData(DiscoveryDataBag dataBag, CacheConfigurationSplitter splitter) {
        if (!dataBag.commonDataCollectedFor(CACHE_PROC.ordinal()))
            dataBag.addGridCommonData(CACHE_PROC.ordinal(), collectCommonDiscoveryData(splitter));
    }

    /**
     * @return Information about started caches.
     * @param cfgSplitter Cache configuration splitter.
     */
    private CacheNodeCommonDiscoveryData collectCommonDiscoveryData(CacheConfigurationSplitter cfgSplitter) {
        Map<Integer, CacheGroupData> cacheGrps = new HashMap<>();

        for (CacheGroupDescriptor grpDesc : registeredCacheGrps.values()) {
            T2<CacheConfiguration, CacheConfigurationEnrichment> splitCfg = cfgSplitter.split(grpDesc);

            CacheGroupData grpData = new CacheGroupData(splitCfg.get1(),
                grpDesc.groupName(),
                grpDesc.groupId(),
                grpDesc.receivedFrom(),
                grpDesc.startTopologyVersion(),
                grpDesc.deploymentId(),
                grpDesc.caches(),
                0,
                grpDesc.persistenceEnabled(),
                grpDesc.walEnabled(),
                grpDesc.walChangeRequests(),
                splitCfg.get2() != null ? grpDesc.cacheConfigurationEnrichment() : null
            );

            cacheGrps.put(grpDesc.groupId(), grpData);
        }

        Map<String, CacheData> caches = new HashMap<>();

        for (DynamicCacheDescriptor desc : registeredCaches.values()) {
            T2<CacheConfiguration, CacheConfigurationEnrichment> splitCfg = cfgSplitter.split(desc);

            CacheData cacheData = new CacheData(splitCfg.get1(),
                desc.cacheId(),
                desc.groupId(),
                desc.cacheType(),
                desc.deploymentId(),
                desc.schema(),
                desc.receivedFrom(),
                desc.staticallyConfigured(),
                desc.sql(),
                false,
                0,
                splitCfg.get2() != null ? desc.cacheConfigurationEnrichment() : null
            );

            caches.put(desc.cacheName(), cacheData);
        }

        Map<String, CacheData> templates = new HashMap<>();

        for (DynamicCacheDescriptor desc : registeredTemplates.values()) {
            T2<CacheConfiguration, CacheConfigurationEnrichment> splitCfg = cfgSplitter.split(desc);

            CacheData cacheData = new CacheData(
                splitCfg.get1(),
                0,
                0,
                desc.cacheType(),
                desc.deploymentId(),
                desc.schema(),
                desc.receivedFrom(),
                desc.staticallyConfigured(),
                false,
                true,
                0,
                splitCfg.get2() != null ? desc.cacheConfigurationEnrichment() : null
            );

            templates.put(desc.cacheName(), cacheData);
        }

        Collection<String> restarting = new HashSet<>(restartingCaches.keySet());

        return new CacheNodeCommonDiscoveryData(caches,
            templates,
            cacheGrps,
            ctx.discovery().clientNodesMap(),
            restarting);
    }

    /**
     * @param data Discovery data.
     */
    public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        if (data.commonData() == null)
            return;

        assert joinDiscoData != null || disconnectedState();
        assert data.commonData() instanceof CacheNodeCommonDiscoveryData : data;

        CacheNodeCommonDiscoveryData cachesData = (CacheNodeCommonDiscoveryData)data.commonData();

        // CacheGroup configurations that were created from local node configuration.
        Map<Integer, CacheGroupDescriptor> locCacheGrps = new HashMap<>(registeredCacheGroups());

        //Replace locally registered data with actual data received from cluster.
        cleanCachesAndGroups();

        registerReceivedCacheGroups(cachesData, locCacheGrps);

        registerReceivedCacheTemplates(cachesData);

        registerReceivedCaches(cachesData);

        addReceivedClientNodesToDiscovery(cachesData);

        String conflictErr = validateRegisteredCaches();

        gridData = new GridData(joinDiscoData, cachesData, conflictErr);

        if (cachesOnDisconnect == null || cachesOnDisconnect.clusterActive())
            initStartCachesForLocalJoin(false, disconnectedState());
    }

    /**
     * Validation {@link #registeredCaches} on conflicts.
     *
     * @return Error message if conflicts was found.
     */
    @Nullable private String validateRegisteredCaches() {
        String conflictErr = null;

        if (joinDiscoData != null) {
            for (Map.Entry<String, CacheJoinNodeDiscoveryData.CacheInfo> e : joinDiscoData.caches().entrySet()) {
                if (!registeredCaches.containsKey(e.getKey())) {
                    conflictErr = checkCacheConflict(e.getValue().cacheData().config(), true);

                    if (conflictErr != null) {
                        conflictErr = "Failed to start configured cache due to conflict with started caches. " +
                            conflictErr;

                        break;
                    }
                }
            }
        }

        return conflictErr;
    }

    /**
     * Adding received client nodes to discovery if needed.
     *
     * @param cachesData Data received from cluster.
     */
    private void addReceivedClientNodesToDiscovery(CacheNodeCommonDiscoveryData cachesData) {
        if (!F.isEmpty(cachesData.clientNodesMap())) {
            for (Map.Entry<String, Map<UUID, Boolean>> entry : cachesData.clientNodesMap().entrySet()) {
                String cacheName = entry.getKey();

                for (Map.Entry<UUID, Boolean> tup : entry.getValue().entrySet())
                    ctx.discovery().addClientNode(cacheName, tup.getKey(), tup.getValue());
            }
        }
    }

    /**
     * Register caches received from cluster.
     *
     * @param cachesData Data received from cluster.
     */
    private void registerReceivedCaches(CacheNodeCommonDiscoveryData cachesData) {
        Map<DynamicCacheDescriptor, QuerySchemaPatch> patchesToApply = new HashMap<>();
        Collection<DynamicCacheDescriptor> cachesToSave = new HashSet<>();

        boolean hasSchemaPatchConflict = false;

        for (CacheData cacheData : cachesData.caches().values()) {
            CacheGroupDescriptor grpDesc = registeredCacheGrps.get(cacheData.groupId());

            assert grpDesc != null : cacheData.cacheConfiguration().getName();

            CacheConfiguration<?, ?> cfg = cacheData.cacheConfiguration();

            DynamicCacheDescriptor desc = new DynamicCacheDescriptor(
                ctx,
                cacheData.cacheConfiguration(),
                cacheData.cacheType(),
                grpDesc,
                false,
                cacheData.receivedFrom(),
                cacheData.staticallyConfigured(),
                cacheData.sql(),
                cacheData.deploymentId(),
                new QuerySchema(cacheData.schema().entities()),
                cacheData.cacheConfigurationEnrichment()
            );

            Collection<QueryEntity> locQryEntities = getLocalQueryEntities(cfg.getName());

            QuerySchemaPatch schemaPatch = desc.makeSchemaPatch(locQryEntities);

            if (schemaPatch.hasConflicts()) {
                hasSchemaPatchConflict = true;

                log.warning("Skipping apply patch because conflicts : " + schemaPatch.getConflictsMessage());
            }
            else if (!schemaPatch.isEmpty())
                patchesToApply.put(desc, schemaPatch);
            else if (!GridFunc.eqNotOrdered(desc.schema().entities(), locQryEntities))
                cachesToSave.add(desc); //received config is different of local config - need to resave

            desc.receivedOnDiscovery(true);

            registeredCaches.put(cacheData.cacheConfiguration().getName(), desc);
            registeredCachesById.put(desc.cacheId(), desc);

            ctx.discovery().setCacheFilter(
                desc.cacheId(),
                grpDesc.groupId(),
                cfg.getName(),
                cfg.getNearConfiguration() != null);
        }

        if (!hasSchemaPatchConflict)
            updateRegisteredCaches(patchesToApply, cachesToSave);
    }

    /**
     * Merging config, resaving it if it needed.
     *
     * @param patchesToApply Patches which need to apply.
     * @param cachesToSave Caches which need to resave.
     */
    private void updateRegisteredCaches(
        Map<DynamicCacheDescriptor, QuerySchemaPatch> patchesToApply,
        Collection<DynamicCacheDescriptor> cachesToSave
    ) {
        // Store config only if cluster is nactive.
        boolean isClusterActive = ctx.state().clusterState().active();

        //Merge of config for cluster only for inactive grid.
        if (!patchesToApply.isEmpty()) {
            for (Map.Entry<DynamicCacheDescriptor, QuerySchemaPatch> entry : patchesToApply.entrySet()) {
                if (entry.getKey().applySchemaPatch(entry.getValue()) && !isClusterActive)
                    saveCacheConfiguration(entry.getKey());
            }
        }

        if (isClusterActive) {
            for (DynamicCacheDescriptor descriptor : cachesToSave)
                saveCacheConfiguration(descriptor);
        }
    }

    /**
     * Register cache templates received from cluster.
     *
     * @param cachesData Data received from cluster.
     */
    private void registerReceivedCacheTemplates(CacheNodeCommonDiscoveryData cachesData) {
        for (CacheData cacheData : cachesData.templates().values()) {
            DynamicCacheDescriptor desc = new DynamicCacheDescriptor(
                ctx,
                cacheData.cacheConfiguration(),
                cacheData.cacheType(),
                null,
                true,
                cacheData.receivedFrom(),
                cacheData.staticallyConfigured(),
                false,
                cacheData.deploymentId(),
                cacheData.schema(),
                cacheData.cacheConfigurationEnrichment()
            );

            registeredTemplates.put(cacheData.cacheConfiguration().getName(), desc);
        }
    }

    /**
     * Register cache groups received from cluster.
     *
     * @param cachesData Data received from cluster.
     * @param locCacheGrps Current local cache groups.
     */
    private void registerReceivedCacheGroups(CacheNodeCommonDiscoveryData cachesData,
        Map<Integer, CacheGroupDescriptor> locCacheGrps) {
        for (CacheGroupData grpData : cachesData.cacheGroups().values()) {
            CacheGroupDescriptor grpDesc = new CacheGroupDescriptor(
                grpData.config(),
                grpData.groupName(),
                grpData.groupId(),
                grpData.receivedFrom(),
                grpData.startTopologyVersion(),
                grpData.deploymentId(),
                grpData.caches(),
                grpData.persistenceEnabled(),
                grpData.walEnabled(),
                grpData.walChangeRequests(),
                grpData.cacheConfigurationEnrichment()
            );

            if (locCacheGrps.containsKey(grpDesc.groupId())) {
                CacheGroupDescriptor locGrpCfg = locCacheGrps.get(grpDesc.groupId());

                grpDesc.mergeWith(locGrpCfg);
            }

            CacheGroupDescriptor old = registeredCacheGrps.put(grpDesc.groupId(), grpDesc);

            assert old == null : old;

            ctx.discovery().addCacheGroup(grpDesc,
                grpData.config().getNodeFilter(),
                grpData.config().getCacheMode());
        }
    }

    /**
     * Clean local registered caches and groups
     */
    private void cleanCachesAndGroups() {
        registeredCaches.clear();
        registeredCachesById.clear();
        registeredCacheGrps.clear();
        ctx.discovery().cleanCachesAndGroups();
    }

    /**
     * Cleanups cache descriptors that belong to the {@code topVer} and earlier.
     *
     * @param topVer Topology version.
     */
    public void cleanupRemovedCaches(AffinityTopologyVersion topVer) {
        markedForDeletionCaches.headMap(topVer, true).clear();
    }

    /**
     * Cleanups cache group descriptors that belong to the {@code topVer} and earlier.
     *
     * @param topVer Topology version.
     */
    public void cleanupRemovedCacheGroups(AffinityTopologyVersion topVer) {
        markedForDeletionCacheGrps.headMap(topVer, true).clear();
    }

    /**
     * @param grpId Group id.
     */
    public @Nullable CacheGroupDescriptor markedForDeletionCacheGroupDesc(int grpId) {
        // Find the "earliest" available descriptor.
        for (Map<Integer, CacheGroupDescriptor> descriptors : markedForDeletionCacheGrps.values()) {
            CacheGroupDescriptor desc = descriptors.get(grpId);

            if (desc != null)
                return desc;
        }

        return null;
    }

    /**
     * @param cacheName Cache name.
     */
    public @Nullable DynamicCacheDescriptor markedForDeletionCache(String cacheName) {
        // Find the "earliest" available descriptor.
        for (Map<String, DynamicCacheDescriptor> descriptors : markedForDeletionCaches.values()) {
            DynamicCacheDescriptor desc = descriptors.get(cacheName);

            if (desc != null)
                return desc;
        }

        return null;
    }

    /**
     * Save dynamic cache descriptor on disk.
     *
     * @param desc Cache to save.
     */
    private void saveCacheConfiguration(DynamicCacheDescriptor desc) {
        try {
            ctx.cache().saveCacheConfiguration(desc);
        }
        catch (IgniteCheckedException e) {
            log.error("Error while saving cache configuration to disk, cfg = " + desc.cacheConfiguration(), e);
        }
    }

    /**
     * Get started node query entities by cacheName.
     *
     * @param cacheName Cache for which query entities will be returned.
     * @return Local query entities.
     */
    private Collection<QueryEntity> getLocalQueryEntities(String cacheName) {
        if (joinDiscoData == null)
            return Collections.emptyList();

        CacheJoinNodeDiscoveryData.CacheInfo cacheInfo = joinDiscoData.caches().get(cacheName);

        if (cacheInfo == null)
            return Collections.emptyList();

        return cacheInfo.cacheData().queryEntities();
    }

    /**
     * Initialize collection with caches to be start:
     * {@code locJoinStartCaches} or {@code locCfgsForActivation} if cluster is inactive.
     *
     * @param firstNode {@code True} if first node in cluster starts.
     */
    private void initStartCachesForLocalJoin(boolean firstNode, boolean reconnect) {
        if (ctx.state().clusterState().transition()) {
            joinOnTransition = true;

            return;
        }

        if (joinDiscoData != null) {
            List<T2<DynamicCacheDescriptor, NearCacheConfiguration>> locJoinStartCaches = new ArrayList<>();
            List<DynamicCacheDescriptor> locJoinInitCaches = new ArrayList<>();
            locCfgsForActivation = new HashMap<>();

            boolean active = ctx.state().clusterState().active();

            for (DynamicCacheDescriptor desc : orderedCaches(CacheComparators.DIRECT)) {
                if (firstNode && !joinDiscoData.caches().containsKey(desc.cacheName()))
                    continue;

                CacheConfiguration<?, ?> cfg = desc.cacheConfiguration();

                if (reconnect && surviveReconnect(cfg.getName()) && cachesOnDisconnect.state.active() && active)
                    continue;

                CacheJoinNodeDiscoveryData.CacheInfo locCfg = joinDiscoData.caches().get(cfg.getName());

                NearCacheConfiguration nearCfg = null;

                if (locCfg != null) {
                    nearCfg = locCfg.cacheData().config().getNearConfiguration();

                    DynamicCacheDescriptor desc0 = new DynamicCacheDescriptor(ctx,
                        mergeConfigs(locCfg.cacheData().config(), cfg),
                        desc.cacheType(),
                        desc.groupDescriptor(),
                        desc.template(),
                        desc.receivedFrom(),
                        desc.staticallyConfigured(),
                        desc.sql(),
                        desc.deploymentId(),
                        desc.schema().copy(),
                        locCfg.cacheData().cacheConfigurationEnrichment()
                    );

                    desc0.startTopologyVersion(desc.startTopologyVersion());
                    desc0.receivedFromStartVersion(desc.receivedFromStartVersion());
                    desc0.clientCacheStartVersion(desc.clientCacheStartVersion());

                    desc0.cacheConfiguration().setStatisticsEnabled(cfg.isStatisticsEnabled());

                    desc = desc0;
                }

                if (locCfg != null ||
                    joinDiscoData.startCaches() ||
                    CU.affinityNode(ctx.discovery().localNode(), desc.groupDescriptor().config().getNodeFilter())) {
                    if (active)
                        locJoinStartCaches.add(new T2<>(desc, nearCfg));
                    else
                        locCfgsForActivation.put(desc.cacheName(), new T2<>(desc.cacheConfiguration(), nearCfg));
                }
                else
                    locJoinInitCaches.add(desc);
            }

            locJoinCachesCtx = new LocalJoinCachesContext(
                locJoinStartCaches,
                locJoinInitCaches,
                new HashMap<>(registeredCacheGrps),
                new HashMap<>(registeredCaches));
        }
    }

    /**
     * Merges local cache configuration with the received. Local cache can contain cache listeners while a remote one
     * can bring new schema for a non-persistent node.
     *
     * @param loc Local cache configuration.
     * @param received Cache configuration received from the cluster.
     * @see #registerReceivedCaches
     * @see #updateRegisteredCaches
     * @see CacheConfiguration#writeReplace()
     */
    private CacheConfiguration<?, ?> mergeConfigs(CacheConfiguration<?, ?> loc, CacheConfiguration<?, ?> received) {
        for (CacheEntryListenerConfiguration lsnrCfg : loc.getCacheEntryListenerConfigurations())
            received.addCacheEntryListenerConfiguration(lsnrCfg);

        return received;
    }

    /**
     * @param msg Message.
     */
    public void onStateChangeFinish(ChangeGlobalStateFinishMessage msg) {
        if (joinOnTransition) {
            initStartCachesForLocalJoin(false, false);

            joinOnTransition = false;
        }
    }

    /**
     * @param msg Message.
     * @param topVer Current topology version.
     * @param curState Current cluster state.
     * @return Exchange action.
     * @throws IgniteCheckedException If configuration validation failed.
     */
    public ExchangeActions onStateChangeRequest(ChangeGlobalStateMessage msg, AffinityTopologyVersion topVer,
        DiscoveryDataClusterState curState)
        throws IgniteCheckedException {
        ExchangeActions exchangeActions = new ExchangeActions();

        if (msg.activate() == curState.active())
            return exchangeActions;

        if (msg.activate()) {
            for (DynamicCacheDescriptor desc : orderedCaches(CacheComparators.DIRECT)) {
                desc.startTopologyVersion(topVer);

                DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(msg.requestId(),
                    desc.cacheName(),
                    msg.initiatorNodeId());

                req.startCacheConfiguration(desc.cacheConfiguration());
                req.cacheType(desc.cacheType());

                T2<CacheConfiguration, NearCacheConfiguration> locCfg = locCfgsForActivation.get(desc.cacheName());

                if (locCfg != null) {
                    if (locCfg.get1() != null)
                        req.startCacheConfiguration(locCfg.get1());

                    req.nearCacheConfiguration(locCfg.get2());

                    req.locallyConfigured(true);
                }

                exchangeActions.addCacheToStart(req, desc);
            }

            for (CacheGroupDescriptor grpDesc : registeredCacheGroups().values())
                exchangeActions.addCacheGroupToStart(grpDesc);

            List<StoredCacheData> storedCfgs = msg.storedCacheConfigurations();

            if (storedCfgs != null) {
                List<DynamicCacheChangeRequest> reqs = new ArrayList<>();

                IgniteUuid deploymentId = msg.id();

                for (StoredCacheData storedCfg : storedCfgs) {
                    CacheConfiguration ccfg = storedCfg.config();

                    if (!registeredCaches.containsKey(ccfg.getName())) {
                        DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(msg.requestId(),
                            ccfg.getName(),
                            msg.initiatorNodeId());

                        req.deploymentId(deploymentId);
                        req.startCacheConfiguration(ccfg);
                        req.cacheType(ctx.cache().cacheType(ccfg.getName()));
                        req.schema(new QuerySchema(storedCfg.queryEntities()));
                        req.sql(storedCfg.sql());

                        reqs.add(req);
                    }
                }

                CacheChangeProcessResult res = processCacheChangeRequests(exchangeActions, reqs, topVer, true);

                if (!res.errs.isEmpty()) {
                    IgniteCheckedException err = new IgniteCheckedException("Failed to activate cluster.");

                    for (IgniteCheckedException err0 : res.errs)
                        err.addSuppressed(err0);

                    throw err;
                }
            }
        }
        else {
            locCfgsForActivation = new HashMap<>();

            for (DynamicCacheDescriptor desc : orderedCaches(CacheComparators.REVERSE)) {
                DynamicCacheChangeRequest req = DynamicCacheChangeRequest.stopRequest(ctx,
                    desc.cacheName(),
                    desc.sql(),
                    false);

                exchangeActions.addCacheToStop(req, desc);

                if (ctx.discovery().cacheClientNode(ctx.discovery().localNode(), desc.cacheName()))
                    locCfgsForActivation.put(desc.cacheName(), new T2<>((CacheConfiguration)null, (NearCacheConfiguration)null));
            }

            for (CacheGroupDescriptor grpDesc : registeredCacheGroups().values())
                exchangeActions.addCacheGroupToStop(grpDesc, false);
        }

        return exchangeActions;
    }

    /**
     * @param data Joining node data.
     */
    public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        if (data.hasJoiningNodeData()) {
            Serializable joiningNodeData = data.joiningNodeData();

            if (joiningNodeData instanceof CacheClientReconnectDiscoveryData) {
                if (disconnectedState()) {
                    if (clientReconnectReqs == null)
                        clientReconnectReqs = new LinkedHashMap<>();

                    clientReconnectReqs.put(data.joiningNodeId(), (CacheClientReconnectDiscoveryData)joiningNodeData);
                }
                else
                    processClientReconnectData((CacheClientReconnectDiscoveryData)joiningNodeData, data.joiningNodeId());
            }
            else if (joiningNodeData instanceof CacheJoinNodeDiscoveryData)
                processJoiningNode((CacheJoinNodeDiscoveryData)joiningNodeData, data.joiningNodeId(), false);
        }
    }

    /**
     * @param data Joining node data.
     * @param joiningNodeClient Joining node is client flag.
     * @return Message with error or null if everything was OK.
     */
    public String validateJoiningNodeData(DiscoveryDataBag.JoiningNodeDiscoveryData data, boolean joiningNodeClient) {
        if (data.hasJoiningNodeData()) {
            Serializable joiningNodeData = data.joiningNodeData();

            if (joiningNodeData instanceof CacheJoinNodeDiscoveryData) {
                CacheJoinNodeDiscoveryData joinData = (CacheJoinNodeDiscoveryData)joiningNodeData;

                Set<String> problemCaches = null;
                Set<String> encClientCaches = null;

                for (CacheJoinNodeDiscoveryData.CacheInfo cacheInfo : joinData.caches().values()) {
                    CacheConfiguration<?, ?> cfg = cacheInfo.cacheData().config();

                    if (!registeredCaches.containsKey(cfg.getName())) {
                        String conflictErr = checkCacheConflict(cfg, true);

                        if (conflictErr != null) {
                            U.warn(log, "Ignore cache received from joining node. " + conflictErr);

                            continue;
                        }

                        long flags = cacheInfo.getFlags();

                        if (flags == 1L) {
                            if (problemCaches == null)
                                problemCaches = new HashSet<>();

                            problemCaches.add(cfg.getName());
                        }
                        else if (joiningNodeClient && cfg.isEncryptionEnabled()) {
                            if (encClientCaches == null)
                                encClientCaches = new HashSet<>();

                            encClientCaches.add(cfg.getName());
                        }
                    }
                }

                if (!F.isEmpty(problemCaches))
                    return problemCaches.stream().collect(Collectors.joining(", ",
                        "Joining node has caches with data which are not presented on cluster, " +
                            "it could mean that they were already destroyed, to add the node to cluster - " +
                            "remove directories with the caches[", "]"));

                if (!F.isEmpty(encClientCaches)) {
                    return encClientCaches.stream().collect(Collectors.joining(", ",
                        "Joining node has encrypted caches which are not presented on the cluster, " +
                            "encrypted caches configured on client node cannot be started when such node joins " +
                            "the cluster, these caches can be started manually (dynamically) after node joined" +
                            "[caches=", "]"));
                }
            }
        }

        return null;
    }

    /**
     * @param clientData Discovery data.
     * @param clientNodeId Client node ID.
     */
    private void processClientReconnectData(CacheClientReconnectDiscoveryData clientData, UUID clientNodeId) {
        DiscoveryDataClusterState state = ctx.state().clusterState();

        if (state.state().active() && !state.transition()) {
            for (CacheClientReconnectDiscoveryData.CacheInfo cacheInfo : clientData.clientCaches().values()) {
                String cacheName = cacheInfo.config().getName();

                if (surviveReconnect(cacheName))
                    ctx.discovery().addClientNode(cacheName, clientNodeId, false);
                else {
                    DynamicCacheDescriptor desc = registeredCaches.get(cacheName);

                    if (desc != null && desc.deploymentId().equals(cacheInfo.deploymentId()))
                        ctx.discovery().addClientNode(cacheName, clientNodeId, cacheInfo.nearCache());
                }
            }
        }
    }

    /**
     * Checks cache configuration on conflict with already registered caches and cache groups.
     *
     * @param cfg Cache configuration.
     * @param checkIndexes If {@code true} check indexes conflicts.
     * @return {@code null} if validation passed, error message in other case.
     */
    private String checkCacheConflict(CacheConfiguration<?, ?> cfg, boolean checkIndexes) {
        if (checkIndexes) {
            Collection<CacheConfiguration<?, ?>> processedCfgs = F.viewReadOnly(registeredCaches.values(),
                (C1<DynamicCacheDescriptor, CacheConfiguration<?, ?>>)DynamicCacheDescriptor::cacheConfiguration);

            String err = validateIncomingConfiguration(processedCfgs, cfg);

            if (err != null)
                return err;
        }

        int cacheId = CU.cacheId(cfg.getName());

        if (cacheGroupByName(cfg.getName()) != null)
            return "Cache name conflict with existing cache group (change cache name) [cacheName=" + cfg.getName() + ']';

        if (cfg.getGroupName() != null) {
            DynamicCacheDescriptor desc = registeredCaches.get(cfg.getGroupName());

            if (desc != null)
                return "Cache group name conflict with existing cache (change group name) [cacheName=" + cfg.getName() +
                    ", conflictingCacheName=" + desc.cacheName() + ']';
        }

        for (DynamicCacheDescriptor desc : registeredCaches.values()) {
            if (desc.cacheId() == cacheId)
                return "Cache ID conflict (change cache name) [cacheName=" + cfg.getName() +
                    ", conflictingCacheName=" + desc.cacheName() + ']';
        }

        int grpId = CU.cacheGroupId(cfg);

        if (cfg.getGroupName() != null) {
            if (cacheGroupByName(cfg.getGroupName()) == null) {
                CacheGroupDescriptor desc = registeredCacheGrps.get(grpId);

                if (desc != null)
                    return "Cache group ID conflict (change cache group name) [cacheName=" + cfg.getName() +
                        ", groupName=" + cfg.getGroupName() +
                        (desc.sharedGroup() ? ", conflictingGroupName=" : ", conflictingCacheName=") + desc.cacheOrGroupName() + ']';
            }
        }
        else {
            CacheGroupDescriptor desc = registeredCacheGrps.get(grpId);

            if (desc != null)
                return "Cache group ID conflict (change cache name) [cacheName=" + cfg.getName() +
                    (desc.sharedGroup() ? ", conflictingGroupName=" : ", conflictingCacheName=") + desc.cacheOrGroupName() + ']';
        }

        return null;
    }

    /**
     * @param joinData Joined node discovery data.
     * @param nodeId Joined node ID.
     * @param locJoin {@code True} if called on local node join.
     * @return Configuration conflict error.
     */
    private String processJoiningNode(CacheJoinNodeDiscoveryData joinData, UUID nodeId, boolean locJoin) {
        registerNewCacheTemplates(joinData, nodeId);

        Map<DynamicCacheDescriptor, QuerySchemaPatch> patchesToApply = new HashMap<>();

        boolean hasSchemaPatchConflict = false;
        boolean active = ctx.state().clusterState().active();

        for (CacheJoinNodeDiscoveryData.CacheInfo cacheInfo : joinData.caches().values()) {
            CacheConfiguration<?, ?> cfg = cacheInfo.cacheData().config();

            if (!registeredCaches.containsKey(cfg.getName())) {
                String conflictErr = checkCacheConflict(cfg, true);

                if (conflictErr != null) {
                    if (locJoin)
                        return conflictErr;

                    U.warn(log, "Ignore cache received from joining node. " + conflictErr);

                    continue;
                }

                registerNewCache(joinData, nodeId, cacheInfo);
            }
            else if (!active) {
                DynamicCacheDescriptor desc = registeredCaches.get(cfg.getName());

                QuerySchemaPatch schemaPatch = desc.makeSchemaPatch(cacheInfo.cacheData());

                if (schemaPatch.hasConflicts()) {
                    hasSchemaPatchConflict = true;

                    log.error("Error during making schema patch : " + schemaPatch.getConflictsMessage());
                }
                else if (!schemaPatch.isEmpty() && !hasSchemaPatchConflict)
                    patchesToApply.put(desc, schemaPatch);
            }

            ctx.discovery().addClientNode(cfg.getName(), nodeId, cfg.getNearConfiguration() != null);
        }

        //If conflict was detected we don't merge config and we leave existed config.
        if (!hasSchemaPatchConflict && !patchesToApply.isEmpty())
            for (Map.Entry<DynamicCacheDescriptor, QuerySchemaPatch> entry : patchesToApply.entrySet()) {
                if (entry.getKey().applySchemaPatch(entry.getValue()))
                    saveCacheConfiguration(entry.getKey());
            }

        if (joinData.startCaches()) {
            for (DynamicCacheDescriptor desc : registeredCaches.values()) {
                ctx.discovery().addClientNode(desc.cacheName(),
                    nodeId,
                    desc.cacheConfiguration().getNearConfiguration() != null);
            }
        }

        return null;
    }

    /**
     * Register new cache received from joining node.
     *
     * @param joinData Data from joining node.
     * @param nodeId Joining node id.
     * @param cacheInfo Cache info of new node.
     */
    private void registerNewCache(
        CacheJoinNodeDiscoveryData joinData,
        UUID nodeId,
        CacheJoinNodeDiscoveryData.CacheInfo cacheInfo
    ) {
        CacheConfiguration<?, ?> cfg = cacheInfo.cacheData().config();

        int cacheId = CU.cacheId(cfg.getName());

        CacheGroupDescriptor grpDesc = registerCacheGroup(
            null,
            null,
            cfg,
            cacheId,
            nodeId,
            joinData.cacheDeploymentId(),
            null,
            null,
            cacheInfo.cacheData().cacheConfigurationEnrichment()
        );

        ctx.discovery().setCacheFilter(
            cacheId,
            grpDesc.groupId(),
            cfg.getName(),
            cfg.getNearConfiguration() != null
        );

        DynamicCacheDescriptor desc = new DynamicCacheDescriptor(ctx,
            cfg,
            cacheInfo.cacheType(),
            grpDesc,
            false,
            nodeId,
            cacheInfo.isStaticallyConfigured(),
            cacheInfo.sql(),
            joinData.cacheDeploymentId(),
            new QuerySchema(cacheInfo.cacheData().queryEntities()),
            cacheInfo.cacheData().cacheConfigurationEnrichment()
        );

        DynamicCacheDescriptor old = registeredCaches.put(cfg.getName(), desc);
        registeredCachesById.put(desc.cacheId(), desc);

        if (cacheInfo.cacheData().groupKeyEncrypted() != null) {
            int grpId = CU.cacheGroupId(cacheInfo.cacheData().config());

            assert cacheInfo.cacheData().config().isEncryptionEnabled();

            GroupKeyEncrypted restoredKey = cacheInfo.cacheData().groupKeyEncrypted();
            GroupKey activeKey = ctx.encryption().getActiveKey(grpId);

            if (activeKey == null)
                ctx.encryption().setInitialGroupKey(grpId, restoredKey.key(), restoredKey.id());
            else {
                assert activeKey.equals(new GroupKey(restoredKey.id(),
                    ctx.config().getEncryptionSpi().decryptKey(restoredKey.key())));
            }
        }

        assert old == null : old;
    }

    /**
     * Register new cache templates received from joining node.
     *
     * @param joinData Data from joining node.
     * @param nodeId Joining node id.
     */
    private void registerNewCacheTemplates(CacheJoinNodeDiscoveryData joinData, UUID nodeId) {
        for (CacheJoinNodeDiscoveryData.CacheInfo cacheInfo : joinData.templates().values()) {
            CacheConfiguration<?, ?> cfg = cacheInfo.cacheData().config();

            if (!registeredTemplates.containsKey(cfg.getName())) {
                DynamicCacheDescriptor desc = new DynamicCacheDescriptor(ctx,
                    cfg,
                    cacheInfo.cacheType(),
                    null,
                    true,
                    nodeId,
                    true,
                    false,
                    joinData.cacheDeploymentId(),
                    new QuerySchema(cacheInfo.cacheData().queryEntities()),
                    cacheInfo.cacheData().cacheConfigurationEnrichment()
                );

                DynamicCacheDescriptor old = registeredTemplates.put(cfg.getName(), desc);

                assert old == null : old;
            }
        }
    }

    /**
     * @param grpName Group name.
     * @return Group descriptor if group found.
     */
    @Nullable private CacheGroupDescriptor cacheGroupByName(String grpName) {
        assert grpName != null;

        for (CacheGroupDescriptor grpDesc : registeredCacheGrps.values()) {
            if (grpName.equals(grpDesc.groupName()))
                return grpDesc;
        }

        return null;
    }

    /**
     * @param cacheName Cache name.
     * @return Group descriptor.
     */
    @Nullable private CacheGroupDescriptor nonSharedCacheGroupByCacheName(String cacheName) {
        assert cacheName != null;

        for (CacheGroupDescriptor grpDesc : registeredCacheGrps.values()) {
            if (!grpDesc.sharedGroup() && grpDesc.caches().containsKey(cacheName))
                return grpDesc;
        }

        return null;
    }

    /**
     * @param exchActions Optional exchange actions to update if new group was added.
     * @param curTopVer Current topology version if dynamic cache started.
     * @param startedCacheCfg Cache configuration.
     * @param cacheId Cache ID.
     * @param rcvdFrom Node ID cache was recived from.
     * @param deploymentId Deployment ID.
     * @param encKey Encryption key.
     * @param encKeyId Id of encryption key.
     * @param cacheCfgEnrichment Cache configuration enrichment.
     * @return Group descriptor.
     */
    private CacheGroupDescriptor registerCacheGroup(
        @Nullable ExchangeActions exchActions,
        @Nullable AffinityTopologyVersion curTopVer,
        CacheConfiguration<?, ?> startedCacheCfg,
        Integer cacheId,
        UUID rcvdFrom,
        IgniteUuid deploymentId,
        @Nullable byte[] encKey,
        @Nullable Integer encKeyId,
        CacheConfigurationEnrichment cacheCfgEnrichment
    ) {
        if (startedCacheCfg.getGroupName() != null) {
            CacheGroupDescriptor desc = cacheGroupByName(startedCacheCfg.getGroupName());

            if (desc != null) {
                desc.onCacheAdded(startedCacheCfg.getName(), cacheId);

                return desc;
            }
        }

        int grpId = CU.cacheGroupId(startedCacheCfg);

        Map<String, Integer> caches = Collections.singletonMap(startedCacheCfg.getName(), cacheId);

        boolean persistent = resolvePersistentFlag(exchActions, startedCacheCfg);
        boolean walGloballyEnabled;

        // client nodes cannot read wal enabled/disabled status so they should use default one
        if (ctx.clientNode())
            walGloballyEnabled = persistent;
        else if (persistent)
            walGloballyEnabled = ctx.cache().context().database().walEnabled(grpId, false);
        else {
            DataRegionConfiguration drCfg =
                CU.findDataRegion(ctx.config().getDataStorageConfiguration(), startedCacheCfg.getDataRegionName());

            walGloballyEnabled = drCfg != null && drCfg.isCdcEnabled();
        }

        CacheGroupDescriptor grpDesc = new CacheGroupDescriptor(
            startedCacheCfg,
            startedCacheCfg.getGroupName(),
            grpId,
            rcvdFrom,
            curTopVer != null ? curTopVer.nextMinorVersion() : null,
            deploymentId,
            caches,
            persistent,
            walGloballyEnabled,
            null,
            cacheCfgEnrichment
        );

        if (startedCacheCfg.isEncryptionEnabled())
            ctx.encryption().setInitialGroupKey(grpId, encKey, encKeyId);

        CacheGroupDescriptor old = registeredCacheGrps.put(grpId, grpDesc);

        assert old == null : old;

        ctx.discovery().addCacheGroup(grpDesc, grpDesc.config().getNodeFilter(), startedCacheCfg.getCacheMode());

        if (exchActions != null)
            exchActions.addCacheGroupToStart(grpDesc);

        return grpDesc;
    }

    /**
     * Resolves persistent flag for new cache group descriptor.
     *
     * @param exchActions Optional exchange actions to update if new group was added.
     * @param startedCacheCfg Started cache configuration.
     */
    private boolean resolvePersistentFlag(@Nullable ExchangeActions exchActions,
        CacheConfiguration<?, ?> startedCacheCfg) {
        if (!ctx.clientNode()) {
            // On server, we always can determine whether cache is persistent by local storage configuration.
            return CU.isPersistentCache(startedCacheCfg, ctx.config().getDataStorageConfiguration());
        }
        else if (exchActions == null) {
            // It's either client local join event or cache is statically configured on another node.
            // No need to resolve on client - we'll anyway receive group descriptor from server with correct flag.
            return false;
        }
        else {
            // Dynamic cache start. Initiator of the start may not have known whether cache should be persistent.
            // On client, we should peek attributes of any affinity server node to get data storage configuration.
            Collection<ClusterNode> aliveSrvNodes = ctx.discovery().aliveServerNodes();

            assert !aliveSrvNodes.isEmpty() : "No alive server nodes";

            for (ClusterNode srvNode : aliveSrvNodes) {
                if (CU.affinityNode(srvNode, startedCacheCfg.getNodeFilter())) {
                    Object dsCfgBytes = srvNode.attribute(IgniteNodeAttributes.ATTR_DATA_STORAGE_CONFIG);

                    if (dsCfgBytes instanceof byte[]) {
                        try {
                            DataStorageConfiguration crdDsCfg = ctx.marshallerContext().jdkMarshaller().unmarshal(
                                (byte[])dsCfgBytes, U.resolveClassLoader(ctx.config()));

                            return CU.isPersistentCache(startedCacheCfg, crdDsCfg);
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to unmarshal remote data storage configuration [remoteNode=" +
                                srvNode + ", cacheName=" + startedCacheCfg.getName() + "]", e);
                        }
                    }
                    else {
                        U.error(log, "Remote marshalled data storage configuration is absent [remoteNode=" + srvNode +
                            ", cacheName=" + startedCacheCfg.getName() + ", dsCfg=" + dsCfgBytes + "]");
                    }
                }
            }

            U.error(log, "Failed to find affinity server node with data storage configuration for starting cache " +
                "[cacheName=" + startedCacheCfg.getName() + ", aliveSrvNodes=" + aliveSrvNodes + "]");

            return false;
        }
    }

    /**
     * @param ccfg Cache configuration to start.
     * @throws IgniteCheckedException If failed.
     */
    void validateStartCacheConfiguration(CacheConfiguration ccfg) throws IgniteCheckedException {
        if (ccfg.getGroupName() != null) {
            CacheGroupDescriptor grpDesc = cacheGroupByName(ccfg.getGroupName());

            if (grpDesc != null) {
                assert ccfg.getGroupName().equals(grpDesc.groupName());

                validateCacheGroupConfiguration(grpDesc.config(), ccfg);
            }
        }
    }

    /**
     * @param cfg Existing configuration.
     * @param startCfg Cache configuration to start.
     * @throws IgniteCheckedException If validation failed.
     */
    private void validateCacheGroupConfiguration(CacheConfiguration cfg, CacheConfiguration startCfg)
        throws IgniteCheckedException {
        GridCacheAttributes attr1 = new GridCacheAttributes(cfg);
        GridCacheAttributes attr2 = new GridCacheAttributes(startCfg);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "cacheMode", "Cache mode",
            cfg.getCacheMode(), startCfg.getCacheMode(), true);

        if (!IgniteSystemProperties.getBoolean(IGNITE_ALLOW_MIXED_CACHE_GROUPS) // TODO https://issues.apache.org/jira/browse/IGNITE-12622
            && !DataStructuresProcessor.isDataStructureCache(cfg.getName())) // TODO https://issues.apache.org/jira/browse/IGNITE-20623
            CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "atomicityMode", "Atomicity mode",
                attr1.atomicityMode(), attr2.atomicityMode(), true);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "affinity", "Affinity function",
            attr1.cacheAffinityClassName(), attr2.cacheAffinityClassName(), true);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "affinityPartitionsCount",
            "Affinity partitions count", attr1.affinityPartitionsCount(), attr2.affinityPartitionsCount(), true);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "nodeFilter", "Node filter",
            attr1.nodeFilterClassName(), attr2.nodeFilterClassName(), true);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "dataRegionName", "Data region",
            cfg.getDataRegionName(), startCfg.getDataRegionName(), true);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "topologyValidator", "Topology validator",
            attr1.topologyValidatorClassName(), attr2.topologyValidatorClassName(), true);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "partitionLossPolicy", "Partition Loss Policy",
            cfg.getPartitionLossPolicy(), startCfg.getPartitionLossPolicy(), true);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "rebalanceMode", "Rebalance mode",
            cfg.getRebalanceMode(), startCfg.getRebalanceMode(), true);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "rebalanceDelay", "Rebalance delay",
            cfg.getRebalanceDelay(), startCfg.getRebalanceDelay(), false);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "rebalanceOrder", "Rebalance order",
            cfg.getRebalanceOrder(), startCfg.getRebalanceOrder(), false);

        if (cfg.getCacheMode() == PARTITIONED) {
            CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "backups", "Backups",
                cfg.getBackups(), startCfg.getBackups(), true);
        }

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "encryptionEnabled", "Encrypted",
            cfg.isEncryptionEnabled(), startCfg.isEncryptionEnabled(), true);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg,
            "diskPageCompression", "Disk page compression",
            cfg.getDiskPageCompression(), startCfg.getDiskPageCompression(), true);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg,
            "diskPageCompressionLevel", "Disk page compression level",
            cfg.getDiskPageCompressionLevel(), startCfg.getDiskPageCompressionLevel(), true);
    }

    /**
     * @return Registered caches.
     */
    ConcurrentMap<String, DynamicCacheDescriptor> registeredCaches() {
        return registeredCaches;
    }

    /**
     * @return Registered caches by Id
     */
    ConcurrentMap<Integer, DynamicCacheDescriptor> registeredCachesById() {
        return registeredCachesById;
    }

    /**
     * @return Registered cache templates.
     */
    ConcurrentMap<String, DynamicCacheDescriptor> registeredTemplates() {
        return registeredTemplates;
    }

    /**
     * @return Registered cache groups.
     */
    ConcurrentMap<Integer, CacheGroupDescriptor> registeredCacheGroups() {
        return registeredCacheGrps;
    }

    /**
     * Returns registered cache descriptors ordered by {@code comparator}
     *
     * @param comparator Comparator (DIRECT, REVERSE or custom) to order cache descriptors.
     * @return Ordered by comparator cache descriptors.
     */
    private Collection<DynamicCacheDescriptor> orderedCaches(Comparator<DynamicCacheDescriptor> comparator) {
        List<DynamicCacheDescriptor> ordered = new ArrayList<>();
        ordered.addAll(registeredCaches.values());

        Collections.sort(ordered, comparator);
        return ordered;
    }

    /**
     *
     */
    public void onDisconnected() {
        cachesOnDisconnect = new CachesOnDisconnect(
            ctx.state().clusterState(),
            new HashMap<>(registeredCacheGrps),
            new HashMap<>(registeredCaches));

        registeredCacheGrps.clear();
        registeredCaches.clear();
        registeredCachesById.clear();
        registeredTemplates.clear();

        clientReconnectReqs = null;
    }

    /**
     * @param active {@code True} if reconnected to active cluster.
     * @param transition {@code True} if reconnected while state transition in progress.
     * @return Information about stopped caches and cache groups.
     */
    public ClusterCachesReconnectResult onReconnected(boolean active, boolean transition) {
        assert disconnectedState();

        Set<String> stoppedCaches = new HashSet<>();
        Set<Integer> stoppedCacheGrps = new HashSet<>();

        Set<String> survivedCaches = new HashSet<>();

        if (!active) {
            joinOnTransition = transition;

            if (F.isEmpty(locCfgsForActivation)) {
                locCfgsForActivation = new HashMap<>();

                for (IgniteInternalCache cache : ctx.cache().caches()) {
                    locCfgsForActivation.put(cache.name(),
                        new T2<>((CacheConfiguration)null, cache.configuration().getNearConfiguration()));
                }
            }

            for (Map.Entry<Integer, CacheGroupDescriptor> e : cachesOnDisconnect.cacheGrps.entrySet())
                stoppedCacheGrps.add(e.getValue().groupId());

            for (Map.Entry<String, DynamicCacheDescriptor> e : cachesOnDisconnect.caches.entrySet())
                stoppedCaches.add(e.getKey());
        }
        else {
            for (Map.Entry<Integer, CacheGroupDescriptor> e : cachesOnDisconnect.cacheGrps.entrySet()) {
                CacheGroupDescriptor locDesc = e.getValue();

                CacheGroupDescriptor desc;
                boolean stopped = true;

                if (locDesc.sharedGroup()) {
                    desc = cacheGroupByName(locDesc.groupName());

                    if (desc != null && desc.deploymentId().equals(locDesc.deploymentId()))
                        stopped = false;
                }
                else {
                    desc = nonSharedCacheGroupByCacheName(locDesc.config().getName());

                    if (desc != null &&
                        (surviveReconnect(locDesc.config().getName()) || desc.deploymentId().equals(locDesc.deploymentId())))
                        stopped = false;
                }

                if (stopped)
                    stoppedCacheGrps.add(locDesc.groupId());
                else
                    assert locDesc.groupId() == desc.groupId();
            }

            for (Map.Entry<String, DynamicCacheDescriptor> e : cachesOnDisconnect.caches.entrySet()) {
                DynamicCacheDescriptor desc = e.getValue();

                String cacheName = e.getKey();

                boolean stopped;

                if (!surviveReconnect(cacheName)) {
                    DynamicCacheDescriptor newDesc = registeredCaches.get(cacheName);

                    stopped = newDesc == null || !desc.deploymentId().equals(newDesc.deploymentId());
                }
                else
                    stopped = false;

                if (stopped)
                    stoppedCaches.add(cacheName);
                else
                    survivedCaches.add(cacheName);
            }

            if (locJoinCachesCtx != null) {
                locJoinCachesCtx.removeSurvivedCaches(survivedCaches);

                if (locJoinCachesCtx.isEmpty())
                    locJoinCachesCtx = null;
            }

            if (!cachesOnDisconnect.clusterActive())
                initStartCachesForLocalJoin(false, true);
        }

        if (clientReconnectReqs != null) {
            for (Map.Entry<UUID, CacheClientReconnectDiscoveryData> e : clientReconnectReqs.entrySet())
                processClientReconnectData(e.getValue(), e.getKey());

            clientReconnectReqs = null;
        }

        cachesOnDisconnect = null;

        return new ClusterCachesReconnectResult(stoppedCacheGrps, stoppedCaches);
    }

    /**
     * @return {@code True} if client node is currently in disconnected state.
     */
    private boolean disconnectedState() {
        return cachesOnDisconnect != null;
    }

    /**
     * @param cacheName Cache name.
     * @return {@code True} if cache with given name if system cache which should always survive client node disconnect.
     */
    private boolean surviveReconnect(String cacheName) {
        return CU.isUtilityCache(cacheName);
    }

    /**
     * @param cacheName Cache name.
     * @return {@code True} if cache is restarting.
     */
    public boolean isRestarting(String cacheName) {
        return restartingCaches.containsKey(cacheName);
    }

    /**
     * @param cacheName Cache name which restart were cancelled.
     */
    public void removeRestartingCache(String cacheName) {
        restartingCaches.remove(cacheName);
    }

    /**
     * Clear up information about restarting caches.
     */
    public void removeRestartingCaches() {
        restartingCaches.clear();
    }

    /**
     * Holds direct comparator (first system caches) and reverse comparator (first user caches).
     * Use DIRECT comparator for ordering cache start operations.
     * Use REVERSE comparator for ordering cache stop operations.
     */
    static class CacheComparators {
        /**
         * DIRECT comparator for cache descriptors (first system caches).
         */
        static Comparator<DynamicCacheDescriptor> DIRECT = new Comparator<DynamicCacheDescriptor>() {
            @Override public int compare(DynamicCacheDescriptor o1, DynamicCacheDescriptor o2) {
                if (o1.cacheType().userCache() ^ o2.cacheType().userCache())
                    return o2.cacheType().userCache() ? -1 : 1;

                return Integer.compare(o1.cacheId(), o2.cacheId());
            }
        };

        /**
         * REVERSE comparator for cache descriptors (first user caches).
         */
        static Comparator<DynamicCacheDescriptor> REVERSE = new Comparator<DynamicCacheDescriptor>() {
            @Override public int compare(DynamicCacheDescriptor o1, DynamicCacheDescriptor o2) {
                return -DIRECT.compare(o1, o2);
            }
        };
    }

    /**
     *
     */
    private static class GridData {
        /** */
        private final CacheJoinNodeDiscoveryData joinDiscoData;

        /** */
        private final CacheNodeCommonDiscoveryData gridData;

        /** */
        private final String conflictErr;

        /**
         * @param joinDiscoData Discovery data collected for local node join.
         * @param gridData Grid data.
         * @param conflictErr Cache configuration conflict error.
         */
        GridData(CacheJoinNodeDiscoveryData joinDiscoData, CacheNodeCommonDiscoveryData gridData, String conflictErr) {
            this.joinDiscoData = joinDiscoData;
            this.gridData = gridData;
            this.conflictErr = conflictErr;
        }
    }

    /**
     *
     */
    private static class CachesOnDisconnect {
        /** */
        final DiscoveryDataClusterState state;

        /** */
        final Map<Integer, CacheGroupDescriptor> cacheGrps;

        /** */
        final Map<String, DynamicCacheDescriptor> caches;

        /**
         * @param state Cluster state.
         * @param cacheGrps Cache groups.
         * @param caches Caches.
         */
        CachesOnDisconnect(DiscoveryDataClusterState state,
            Map<Integer, CacheGroupDescriptor> cacheGrps,
            Map<String, DynamicCacheDescriptor> caches) {
            this.state = state;
            this.cacheGrps = cacheGrps;
            this.caches = caches;
        }

        /**
         * @return {@code True} if cluster was in active state.
         */
        boolean clusterActive() {
            return state.active() && !state.transition();
        }
    }

    /**
     *
     */
    private static class CacheChangeProcessResult {
        /** */
        private boolean needExchange;

        /** */
        private final List<DynamicCacheDescriptor> addedDescs = new ArrayList<>();

        /** */
        private final List<IgniteCheckedException> errs = new ArrayList<>();
    }
}
