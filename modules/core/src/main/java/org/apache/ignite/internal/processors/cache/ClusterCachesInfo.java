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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.GridCachePluginContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.query.QuerySchema;
import org.apache.ignite.internal.processors.query.QuerySchemaPatch;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CACHE_PROC;

/**
 * Logic related to cache discovery data processing.
 */
class ClusterCachesInfo {
    /** Version since which merge of config is supports. */
    private static final IgniteProductVersion V_MERGE_CONFIG_SINCE = IgniteProductVersion.fromString("2.5.0");

    /** */
    private final GridKernalContext ctx;

    /** Dynamic caches. */
    private final ConcurrentMap<String, DynamicCacheDescriptor> registeredCaches = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<Integer, CacheGroupDescriptor> registeredCacheGrps = new ConcurrentHashMap<>();

    /** Cache templates. */
    private final ConcurrentMap<String, DynamicCacheDescriptor> registeredTemplates = new ConcurrentHashMap<>();

    /** Caches currently being restarted. */
    private final Collection<String> restartingCaches = new GridConcurrentHashSet<>();

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

    /**
     * @param ctx Context.
     */
    public ClusterCachesInfo(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());
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
     * @param cacheName Cache name.
     * @param grpName Group name.
     * @return Group ID.
     */
    private int cacheGroupId(String cacheName, @Nullable String grpName) {
        assert cacheName != null;

        return grpName != null ? CU.cacheId(grpName) : CU.cacheId(cacheName);
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
        GridCacheAttributes rmtAttr = new GridCacheAttributes(rmtData.cacheConfiguration());
        GridCacheAttributes locAttr = new GridCacheAttributes(locInfo.cacheData().config());

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cacheMode", "Cache mode",
            locAttr.cacheMode(), rmtAttr.cacheMode(), true);

        CU.checkAttributeMismatch(log, rmtAttr.groupName(), rmt, "groupName", "Cache group name",
            locAttr.groupName(), rmtAttr.groupName(), true);

        if (rmtAttr.cacheMode() != LOCAL) {
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
            }
        }
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
     * @param topVer Topology version.
     */
    public void onCacheChangeRequested(DynamicCacheChangeFailureMessage failMsg, AffinityTopologyVersion topVer) {
        ExchangeActions exchangeActions = new ExchangeActions();

        List<DynamicCacheChangeRequest> requests = new ArrayList<>(failMsg.cacheNames().size());

        for (String cacheName : failMsg.cacheNames()) {
            DynamicCacheDescriptor cacheDescr = registeredCaches.get(cacheName);

            assert cacheDescr != null : "Dynamic cache descriptor is missing [cacheName=" + cacheName + "]";

            requests.add(DynamicCacheChangeRequest.stopRequest(ctx, cacheName, cacheDescr.sql(), true));
        }

        processCacheChangeRequests(exchangeActions, requests, topVer,false);

        failMsg.exchangeActions(exchangeActions);
    }

    /**
     * @param batch Cache change request.
     * @param topVer Topology version.
     * @return {@code True} if minor topology version should be increased.
     */
    public boolean onCacheChangeRequested(DynamicCacheChangeBatch batch, AffinityTopologyVersion topVer) {
        DiscoveryDataClusterState state = ctx.state().clusterState();

        if (state.active() && !state.transition()) {
            ExchangeActions exchangeActions = new ExchangeActions();

            CacheChangeProcessResult res = processCacheChangeRequests(exchangeActions,
                batch.requests(),
                topVer,
                false);

            if (res.needExchange) {
                assert !exchangeActions.empty() : exchangeActions;

                batch.exchangeActions(exchangeActions);
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
     * @param topVer Topology version.
     * @param persistedCfgs {@code True} if process start of persisted caches during cluster activation.
     * @return Process result.
     */
    private CacheChangeProcessResult processCacheChangeRequests(
        ExchangeActions exchangeActions,
        Collection<DynamicCacheChangeRequest> reqs,
        AffinityTopologyVersion topVer,
        boolean persistedCfgs) {
        CacheChangeProcessResult res = new CacheChangeProcessResult();

        final List<T2<DynamicCacheChangeRequest, AffinityTopologyVersion>> reqsToComplete = new ArrayList<>();

        for (DynamicCacheChangeRequest req : reqs) {
            if (req.template()) {
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
                        req.schema());

                    DynamicCacheDescriptor old = registeredTemplates().put(ccfg.getName(), templateDesc);

                    assert old == null;

                    res.addedDescs.add(templateDesc);
                }

                if (!persistedCfgs)
                    ctx.cache().completeTemplateAddFuture(ccfg.getName(), req.deploymentId());

                continue;
            }

            assert !req.clientStartOnly() : req;

            DynamicCacheDescriptor desc = registeredCaches.get(req.cacheName());

            boolean needExchange = false;

            boolean clientCacheStart = false;

            AffinityTopologyVersion waitTopVer = null;

            if (req.start()) {
                // Starting a new cache.
                if (desc == null) {
                    String conflictErr = checkCacheConflict(req.startCacheConfiguration());

                    if (conflictErr != null) {
                        U.warn(log, "Ignore cache start request. " + conflictErr);

                        IgniteCheckedException err = new IgniteCheckedException("Failed to start " +
                            "cache. " + conflictErr);

                        if (persistedCfgs)
                            res.errs.add(err);
                        else
                            ctx.cache().completeCacheStartFuture(req, false, err);

                        continue;
                    }

                    if (req.clientStartOnly()) {
                        assert !persistedCfgs;

                        ctx.cache().completeCacheStartFuture(req, false, new IgniteCheckedException("Failed to start " +
                            "client cache (a cache with the given name is not started): " + req.cacheName()));
                    }
                    else {
                        SchemaOperationException err = QueryUtils.checkQueryEntityConflicts(
                            req.startCacheConfiguration(), registeredCaches.values());

                        if (err != null) {
                            if (persistedCfgs)
                                res.errs.add(err);
                            else
                                ctx.cache().completeCacheStartFuture(req, false, err);

                            continue;
                        }

                        CacheConfiguration<?, ?> ccfg = req.startCacheConfiguration();

                        assert req.cacheType() != null : req;
                        assert F.eq(ccfg.getName(), req.cacheName()) : req;

                        int cacheId = CU.cacheId(req.cacheName());

                        CacheGroupDescriptor grpDesc = registerCacheGroup(exchangeActions,
                            topVer,
                            ccfg,
                            cacheId,
                            req.initiatingNodeId(),
                            req.deploymentId());

                        DynamicCacheDescriptor startDesc = new DynamicCacheDescriptor(ctx,
                            ccfg,
                            req.cacheType(),
                            grpDesc,
                            false,
                            req.initiatingNodeId(),
                            false,
                            req.sql(),
                            req.deploymentId(),
                            req.schema());

                        DynamicCacheDescriptor old = registeredCaches.put(ccfg.getName(), startDesc);

                        restartingCaches.remove(ccfg.getName());

                        assert old == null;

                        ctx.discovery().setCacheFilter(
                            startDesc.cacheId(),
                            grpDesc.groupId(),
                            ccfg.getName(),
                            ccfg.getNearConfiguration() != null);

                        if (!persistedCfgs) {
                            ctx.discovery().addClientNode(req.cacheName(),
                                req.initiatingNodeId(),
                                req.nearCacheConfiguration() != null);
                        }

                        res.addedDescs.add(startDesc);

                        exchangeActions.addCacheToStart(req, startDesc);

                        needExchange = true;
                    }
                }
                else {
                    assert !persistedCfgs;
                    assert req.initiatingNodeId() != null : req;

                    if (req.failIfExists()) {
                        ctx.cache().completeCacheStartFuture(req, false,
                            new CacheExistsException("Failed to start cache " +
                                "(a cache with the same name is already started): " + req.cacheName()));
                    }
                    else {
                        // Cache already exists, it is possible client cache is needed.
                        ClusterNode node = ctx.discovery().node(req.initiatingNodeId());

                        boolean clientReq = node != null &&
                            !ctx.discovery().cacheAffinityNode(node, req.cacheName());

                        if (clientReq) {
                            ctx.discovery().addClientNode(req.cacheName(),
                                req.initiatingNodeId(),
                                req.nearCacheConfiguration() != null);

                            if (node.id().equals(req.initiatingNodeId())) {
                                desc.clientCacheStartVersion(topVer);

                                clientCacheStart = true;

                                ctx.discovery().clientCacheStartEvent(req.requestId(), F.asMap(req.cacheName(), req), null);
                            }
                        }
                    }
                }

                if (!needExchange && !clientCacheStart && desc != null) {
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
            else if (req.resetLostPartitions()) {
                if (desc != null) {
                    needExchange = true;

                    exchangeActions.addCacheToResetLostPartitions(req, desc);
                }
            }
            else if (req.stop()) {
                if (desc != null) {
                    if (req.sql() && !desc.sql()) {
                        ctx.cache().completeCacheStartFuture(req, false,
                            new IgniteCheckedException("Only cache created with CREATE TABLE may be removed with " +
                                "DROP TABLE [cacheName=" + req.cacheName() + ']'));

                        continue;
                    }

                    if (!req.sql() && desc.sql()) {
                        ctx.cache().completeCacheStartFuture(req, false,
                            new IgniteCheckedException("Only cache created with cache API may be removed with " +
                                "direct call to destroyCache [cacheName=" + req.cacheName() + ']'));

                        continue;
                    }

                    DynamicCacheDescriptor old = registeredCaches.remove(req.cacheName());

                    if (req.restart())
                        restartingCaches.add(req.cacheName());

                    assert old != null && old == desc : "Dynamic cache map was concurrently modified [req=" + req + ']';

                    ctx.discovery().removeCacheFilter(req.cacheName());

                    needExchange = true;

                    exchangeActions.addCacheToStop(req, desc);

                    CacheGroupDescriptor grpDesc = registeredCacheGrps.get(desc.groupId());

                    assert grpDesc != null && grpDesc.groupId() == desc.groupId() : desc;

                    grpDesc.onCacheStopped(desc.cacheName(), desc.cacheId());

                    if (!grpDesc.hasCaches()) {
                        registeredCacheGrps.remove(grpDesc.groupId());

                        ctx.discovery().removeCacheGroup(grpDesc);

                        exchangeActions.addCacheGroupToStop(grpDesc, req.destroy());

                        assert exchangeActions.checkStopRequestConsistency(grpDesc.groupId());

                        // If all caches in group will be destroyed it is not necessary to destroy single cache
                        // because group will be stopped anyway.
                        if (req.destroy()) {
                            for (ExchangeActions.CacheActionData action : exchangeActions.cacheStopRequests()) {
                                if (action.descriptor().groupId() == grpDesc.groupId())
                                    action.request().destroy(false);
                            }
                        }
                    }
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

        if (!F.isEmpty(res.addedDescs)) {
            AffinityTopologyVersion startTopVer = res.needExchange ? topVer.nextMinorVersion() : topVer;

            for (DynamicCacheDescriptor desc : res.addedDescs) {
                assert desc.template() || res.needExchange;

                desc.startTopologyVersion(startTopVer);
            }
        }

        if (!F.isEmpty(reqsToComplete)) {
            ctx.closure().callLocalSafe(new Callable<Void>() {
                @Override public Void call() throws Exception {
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
     * @param dataBag Discovery data bag.
     */
    void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        if (!ctx.isDaemon())
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
        return restartingCaches;
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
            assert ctx.config().isDaemon() || joinDiscoData != null;

            return joinDiscoData;
        }
    }

    /**
     * Called from exchange worker.
     *
     * @return Caches to be started when this node starts.
     */
    @Nullable public LocalJoinCachesContext localJoinCachesContext() {
        if (ctx.isDaemon())
            return null;

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

        if (!ctx.isDaemon()) {
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
        if (type == EVT_NODE_JOINED && !ctx.isDaemon()) {
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
     */
    public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (ctx.isDaemon())
            return;

        if (!dataBag.commonDataCollectedFor(CACHE_PROC.ordinal()))
            dataBag.addGridCommonData(CACHE_PROC.ordinal(), collectCommonDiscoveryData());
    }

    /**
     * @return Information about started caches.
     */
    private CacheNodeCommonDiscoveryData collectCommonDiscoveryData() {
        Map<Integer, CacheGroupData> cacheGrps = new HashMap<>();

        for (CacheGroupDescriptor grpDesc : registeredCacheGrps.values()) {
            CacheGroupData grpData = new CacheGroupData(grpDesc.config(),
                grpDesc.groupName(),
                grpDesc.groupId(),
                grpDesc.receivedFrom(),
                grpDesc.startTopologyVersion(),
                grpDesc.deploymentId(),
                grpDesc.caches(),
                0,
                grpDesc.persistenceEnabled(),
                grpDesc.walEnabled(),
                grpDesc.walChangeRequests());

            cacheGrps.put(grpDesc.groupId(), grpData);
        }

        Map<String, CacheData> caches = new HashMap<>();

        for (DynamicCacheDescriptor desc : registeredCaches.values()) {
            CacheData cacheData = new CacheData(desc.cacheConfiguration(),
                desc.cacheId(),
                desc.groupId(),
                desc.cacheType(),
                desc.deploymentId(),
                desc.schema(),
                desc.receivedFrom(),
                desc.staticallyConfigured(),
                desc.sql(),
                false,
                0);

            caches.put(desc.cacheName(), cacheData);
        }

        Map<String, CacheData> templates = new HashMap<>();

        for (DynamicCacheDescriptor desc : registeredTemplates.values()) {
            CacheData cacheData = new CacheData(desc.cacheConfiguration(),
                0,
                0,
                desc.cacheType(),
                desc.deploymentId(),
                desc.schema(),
                desc.receivedFrom(),
                desc.staticallyConfigured(),
                false,
                true,
                0);

            templates.put(desc.cacheName(), cacheData);
        }

        Collection<String> restarting = new HashSet<>(restartingCaches);

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
        if (ctx.isDaemon() || data.commonData() == null)
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
                    conflictErr = checkCacheConflict(e.getValue().cacheData().config());

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
                new QuerySchema(cacheData.schema().entities())
            );

            Collection<QueryEntity> localQueryEntities = getLocalQueryEntities(cfg.getName());

            QuerySchemaPatch schemaPatch = desc.makeSchemaPatch(localQueryEntities);

            if (schemaPatch.hasConflicts()) {
                hasSchemaPatchConflict = true;

                log.warning("Skipping apply patch because conflicts : " + schemaPatch.getConflictsMessage());
            }
            else if (!schemaPatch.isEmpty())
                patchesToApply.put(desc, schemaPatch);
            else if (!GridFunc.eqNotOrdered(desc.schema().entities(), localQueryEntities))
                cachesToSave.add(desc); //received config is different of local config - need to resave

            desc.receivedOnDiscovery(true);

            registeredCaches.put(cacheData.cacheConfiguration().getName(), desc);

            ctx.discovery().setCacheFilter(
                desc.cacheId(),
                grpDesc.groupId(),
                cfg.getName(),
                cfg.getNearConfiguration() != null);
        }

        updateRegisteredCachesIfNeeded(patchesToApply, cachesToSave, hasSchemaPatchConflict);
    }

    /**
     * Merging config or resaving it if it needed.
     *
     * @param patchesToApply Patches which need to apply.
     * @param cachesToSave Caches which need to resave.
     * @param hasSchemaPatchConflict {@code true} if we have conflict during making patch.
     */
    private void updateRegisteredCachesIfNeeded(Map<DynamicCacheDescriptor, QuerySchemaPatch> patchesToApply,
        Collection<DynamicCacheDescriptor> cachesToSave, boolean hasSchemaPatchConflict) {
        //Skip merge of config if least one conflict was found.
        if (!hasSchemaPatchConflict && isMergeConfigSupports(ctx.discovery().localNode())) {
            boolean isClusterActive = ctx.state().clusterState().active();

            //Merge of config for cluster only for inactive grid.
            if (!isClusterActive && !patchesToApply.isEmpty()) {
                for (Map.Entry<DynamicCacheDescriptor, QuerySchemaPatch> entry : patchesToApply.entrySet()) {
                    if (entry.getKey().applySchemaPatch(entry.getValue()))
                        saveCacheConfiguration(entry.getKey());
                }

                for (DynamicCacheDescriptor descriptor : cachesToSave) {
                    saveCacheConfiguration(descriptor);
                }
            }
            else if (patchesToApply.isEmpty()) {
                for (DynamicCacheDescriptor descriptor : cachesToSave) {
                    saveCacheConfiguration(descriptor);
                }
            }
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
                cacheData.schema());

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
                grpData.walChangeRequests());

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
        registeredCacheGrps.clear();
        ctx.discovery().cleanCachesAndGroups();
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
        assert locJoinCachesCtx == null : locJoinCachesCtx;

        if (ctx.state().clusterState().transition()) {
            joinOnTransition = true;

            return;
        }

        if (joinDiscoData != null) {
            List<T2<DynamicCacheDescriptor, NearCacheConfiguration>> locJoinStartCaches = new ArrayList<>();
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
                        locCfg.cacheData().config(),
                        desc.cacheType(),
                        desc.groupDescriptor(),
                        desc.template(),
                        desc.receivedFrom(),
                        desc.staticallyConfigured(),
                        desc.sql(),
                        desc.deploymentId(),
                        desc.schema().copy());

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
            }

            locJoinCachesCtx = new LocalJoinCachesContext(
                locJoinStartCaches,
                new HashMap<>(registeredCacheGrps),
                new HashMap<>(registeredCaches));
        }
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
    public ExchangeActions onStateChangeRequest(ChangeGlobalStateMessage msg, AffinityTopologyVersion topVer, DiscoveryDataClusterState curState)
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
     * @param clientData Discovery data.
     * @param clientNodeId Client node ID.
     */
    private void processClientReconnectData(CacheClientReconnectDiscoveryData clientData, UUID clientNodeId) {
        DiscoveryDataClusterState state = ctx.state().clusterState();

        if (state.active() && !state.transition()) {
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
     * @return {@code null} if validation passed, error message in other case.
     */
    private String checkCacheConflict(CacheConfiguration<?, ?> cfg) {
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

        int grpId = cacheGroupId(cfg.getName(), cfg.getGroupName());

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

        boolean isMergeConfigSupport = isMergeConfigSupports(null);

        for (CacheJoinNodeDiscoveryData.CacheInfo cacheInfo : joinData.caches().values()) {
            CacheConfiguration<?, ?> cfg = cacheInfo.cacheData().config();

            if (!registeredCaches.containsKey(cfg.getName())) {
                String conflictErr = checkCacheConflict(cfg);

                if (conflictErr != null) {
                    if (locJoin)
                        return conflictErr;

                    U.warn(log, "Ignore cache received from joining node. " + conflictErr);

                    continue;
                }

                registerNewCache(joinData, nodeId, cacheInfo);
            }
            else if (!active && isMergeConfigSupport) {
                DynamicCacheDescriptor desc = registeredCaches.get(cfg.getName());

                QuerySchemaPatch schemaPatch = desc.makeSchemaPatch(cacheInfo.cacheData().queryEntities());

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
            for(Map.Entry<DynamicCacheDescriptor, QuerySchemaPatch> entry: patchesToApply.entrySet()){
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
        CacheJoinNodeDiscoveryData.CacheInfo cacheInfo) {
        CacheConfiguration<?, ?> cfg = cacheInfo.cacheData().config();

        int cacheId = CU.cacheId(cfg.getName());

        CacheGroupDescriptor grpDesc = registerCacheGroup(null,
            null,
            cfg,
            cacheId,
            nodeId,
            joinData.cacheDeploymentId());

        ctx.discovery().setCacheFilter(
            cacheId,
            grpDesc.groupId(),
            cfg.getName(),
            cfg.getNearConfiguration() != null);

        DynamicCacheDescriptor desc = new DynamicCacheDescriptor(ctx,
            cfg,
            cacheInfo.cacheType(),
            grpDesc,
            false,
            nodeId,
            cacheInfo.isStaticallyConfigured(),
            cacheInfo.sql(),
            joinData.cacheDeploymentId(),
            new QuerySchema(cacheInfo.cacheData().queryEntities()));

        DynamicCacheDescriptor old = registeredCaches.put(cfg.getName(), desc);

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
                    new QuerySchema(cacheInfo.cacheData().queryEntities()));

                DynamicCacheDescriptor old = registeredTemplates.put(cfg.getName(), desc);

                assert old == null : old;
            }
        }
    }

    /**
     * @return {@code true} if grid supports merge of config and {@code False} otherwise.
     */
    public boolean isMergeConfigSupports(ClusterNode joiningNode) {
        DiscoCache discoCache = ctx.discovery().discoCache();

        if (discoCache == null)
            return true;

        if (joiningNode != null && joiningNode.version().compareToIgnoreTimestamp(V_MERGE_CONFIG_SINCE) < 0)
            return false;

        Collection<ClusterNode> nodes = discoCache.allNodes();

        for (ClusterNode node : nodes) {
            IgniteProductVersion version = node.version();

            if (version.compareToIgnoreTimestamp(V_MERGE_CONFIG_SINCE) < 0)
                return false;
        }

        return true;
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
     * @return Group descriptor.
     */
    private CacheGroupDescriptor registerCacheGroup(
        @Nullable ExchangeActions exchActions,
        @Nullable AffinityTopologyVersion curTopVer,
        CacheConfiguration<?, ?> startedCacheCfg,
        Integer cacheId,
        UUID rcvdFrom,
        IgniteUuid deploymentId) {
        if (startedCacheCfg.getGroupName() != null) {
            CacheGroupDescriptor desc = cacheGroupByName(startedCacheCfg.getGroupName());

            if (desc != null) {
                desc.onCacheAdded(startedCacheCfg.getName(), cacheId);

                return desc;
            }
        }

        int grpId = cacheGroupId(startedCacheCfg.getName(), startedCacheCfg.getGroupName());

        Map<String, Integer> caches = Collections.singletonMap(startedCacheCfg.getName(), cacheId);

        boolean persistent = resolvePersistentFlag(exchActions, startedCacheCfg);

        CacheGroupDescriptor grpDesc = new CacheGroupDescriptor(
            startedCacheCfg,
            startedCacheCfg.getGroupName(),
            grpId,
            rcvdFrom,
            curTopVer != null ? curTopVer.nextMinorVersion() : null,
            deploymentId,
            caches,
            persistent,
            persistent,
            null);

        if (ctx.cache().context().pageStore() != null)
            ctx.cache().context().pageStore().beforeCacheGroupStart(grpDesc);

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
    private boolean resolvePersistentFlag(@Nullable ExchangeActions exchActions, CacheConfiguration<?, ?> startedCacheCfg) {
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
                            DataStorageConfiguration crdDsCfg = new JdkMarshaller().unmarshal(
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
    public void validateStartCacheConfiguration(CacheConfiguration ccfg) throws IgniteCheckedException {
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
    }

    /**
     * @return Registered caches.
     */
    ConcurrentMap<String, DynamicCacheDescriptor> registeredCaches() {
        return registeredCaches;
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

                return o1.cacheId().compareTo(o2.cacheId());
            }
        };

        /**
         * REVERSE comparator for cache descriptors (first user caches).
         */
        static Comparator<DynamicCacheDescriptor> REVERSE = new Comparator<DynamicCacheDescriptor>() {
            @Override
            public int compare(DynamicCacheDescriptor o1, DynamicCacheDescriptor o2) {
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
