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
import java.util.Collections;
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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.QuerySchema;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CACHE_PROC;

/**
 * Logic related to cache discovery date processing.
 */
class ClusterCachesInfo {
    /** */
    private final GridKernalContext ctx;

    /** Dynamic caches. */
    private final ConcurrentMap<String, DynamicCacheDescriptor> registeredCaches = new ConcurrentHashMap<>();

    /** Cache templates. */
    private final ConcurrentMap<String, DynamicCacheDescriptor> registeredTemplates = new ConcurrentHashMap<>();

    /** */
    private final IgniteLogger log;

    /** */
    private Map<String, DynamicCacheDescriptor> cachesOnDisconnect;

    /** */
    private CacheJoinNodeDiscoveryData joinDiscoData;

    /** */
    private CacheNodeCommonDiscoveryData gridData;

    /** */
    private List<T2<DynamicCacheDescriptor, NearCacheConfiguration>> locJoinStartCaches;

    /** */
    private Map<UUID, CacheClientReconnectDiscoveryData> clientReconnectReqs;

    /**
     * @param ctx Context.
     */
    ClusterCachesInfo(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());
    }

    /**
     * @param joinDiscoData Information about configured caches and templates.
     */
    void onStart(CacheJoinNodeDiscoveryData joinDiscoData) {
        this.joinDiscoData = joinDiscoData;

        processJoiningNode(joinDiscoData, ctx.localNodeId());
    }

    /**
     * @param checkConsistency {@code True} if need check cache configurations consistency.
     * @throws IgniteCheckedException If failed.
     */
    void onKernalStart(boolean checkConsistency) throws IgniteCheckedException {
        if (checkConsistency && joinDiscoData != null && gridData != null) {
            for (CacheJoinNodeDiscoveryData.CacheInfo locCacheInfo : joinDiscoData.caches().values()) {
                CacheConfiguration locCfg = locCacheInfo.config();

                CacheData cacheData = gridData.caches().get(locCfg.getName());

                if (cacheData != null)
                    checkCache(locCfg, cacheData.cacheConfiguration(), cacheData.receivedFrom());
            }
        }

        joinDiscoData = null;
        gridData = null;
    }
    /**
     * Checks that remote caches has configuration compatible with the local.
     *
     * @param locCfg Local configuration.
     * @param rmtCfg Remote configuration.
     * @param rmt Remote node.
     * @throws IgniteCheckedException If check failed.
     */
    private void checkCache(CacheConfiguration<?, ?> locCfg, CacheConfiguration<?, ?> rmtCfg, UUID rmt)
        throws IgniteCheckedException {
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

            ClusterNode rmtNode = ctx.discovery().node(rmt);

            if (CU.affinityNode(ctx.discovery().localNode(), locCfg.getNodeFilter())
                && rmtNode != null && CU.affinityNode(rmtNode, rmtCfg.getNodeFilter())) {
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

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "transactionManagerLookup",
                "Transaction manager lookup", locAttr.transactionManagerLookupClassName(),
                rmtAttr.transactionManagerLookupClassName(), false);

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "defaultLockTimeout",
                "Default lock timeout", locAttr.defaultLockTimeout(), rmtAttr.defaultLockTimeout(), false);

            CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "preloadBatchSize",
                "Preload batch size", locAttr.rebalanceBatchSize(), rmtAttr.rebalanceBatchSize(), false);

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

            if (locAttr.cacheMode() == PARTITIONED) {
                CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "nearEvictionPolicy",
                    "Near eviction policy", locAttr.nearEvictionPolicyClassName(),
                    rmtAttr.nearEvictionPolicyClassName(), false);

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
     * @param batch Cache change request.
     * @param topVer Topology version.
     * @return {@code True} if minor topology version should be increased.
     */
    boolean onCacheChangeRequested(DynamicCacheChangeBatch batch, AffinityTopologyVersion topVer) {
        ExchangeActions exchangeActions = new ExchangeActions();

        boolean incMinorTopVer = false;

        List<DynamicCacheDescriptor> addedDescs = new ArrayList<>();

        final List<T2<DynamicCacheChangeRequest, AffinityTopologyVersion>> reqsToComplete = new ArrayList<>();

        for (DynamicCacheChangeRequest req : batch.requests()) {
            if (req.template()) {
                CacheConfiguration ccfg = req.startCacheConfiguration();

                assert ccfg != null : req;

                DynamicCacheDescriptor desc = registeredTemplates.get(req.cacheName());

                if (desc == null) {
                    DynamicCacheDescriptor templateDesc = new DynamicCacheDescriptor(ctx,
                        ccfg,
                        req.cacheType(),
                        true,
                        req.initiatingNodeId(),
                        false,
                        req.deploymentId(),
                        req.schema());

                    DynamicCacheDescriptor old = registeredTemplates().put(ccfg.getName(), templateDesc);

                    assert old == null;

                    addedDescs.add(templateDesc);
                }

                ctx.cache().completeTemplateAddFuture(ccfg.getName(), req.deploymentId());

                continue;
            }

            DynamicCacheDescriptor desc = req.globalStateChange() ? null : registeredCaches.get(req.cacheName());

            boolean needExchange = false;

            AffinityTopologyVersion waitTopVer = null;

            if (req.start()) {
                if (desc == null) {
                    if (req.clientStartOnly()) {
                        ctx.cache().completeCacheStartFuture(req, new IgniteCheckedException("Failed to start " +
                            "client cache (a cache with the given name is not started): " + req.cacheName()));
                    }
                    else {
                        CacheConfiguration<?, ?> ccfg = req.startCacheConfiguration();

                        assert req.cacheType() != null : req;
                        assert F.eq(ccfg.getName(), req.cacheName()) : req;

                        DynamicCacheDescriptor startDesc = new DynamicCacheDescriptor(ctx,
                            ccfg,
                            req.cacheType(),
                            false,
                            req.initiatingNodeId(),
                            false,
                            req.deploymentId(),
                            req.schema());

                        DynamicCacheDescriptor old = registeredCaches.put(ccfg.getName(), startDesc);

                        assert old == null;

                        ctx.discovery().setCacheFilter(
                            ccfg.getName(),
                            ccfg.getNodeFilter(),
                            ccfg.getNearConfiguration() != null,
                            ccfg.getCacheMode());

                        ctx.discovery().addClientNode(req.cacheName(),
                            req.initiatingNodeId(),
                            req.nearCacheConfiguration() != null);

                        addedDescs.add(startDesc);

                        exchangeActions.addCacheToStart(req, startDesc);

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
                            ctx.cache().completeCacheStartFuture(req,
                                new CacheExistsException("Failed to start cache " +
                                    "(a cache with the same name is already started): " + req.cacheName()));
                        }
                        else {
                            needExchange = clientReq && ctx.discovery().addClientNode(req.cacheName(),
                                req.initiatingNodeId(),
                                req.nearCacheConfiguration() != null);
                        }
                    }

                    if (needExchange) {
                        req.clientStartOnly(true);

                        desc.clientCacheStartVersion(topVer.nextMinorVersion());

                        exchangeActions.addClientCacheToStart(req, desc);
                    }
                }

                if (!needExchange && desc != null) {
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
            else if (req.globalStateChange())
                exchangeActions.newClusterState(req.state());
            else if (req.resetLostPartitions()) {
                if (desc != null) {
                    needExchange = true;

                    exchangeActions.addCacheToResetLostPartitions(req, desc);
                }
            }
            else if (req.stop()) {
                assert req.stop() ^ req.close() : req;

                if (desc != null) {
                    DynamicCacheDescriptor old = registeredCaches.remove(req.cacheName());

                    assert old != null : "Dynamic cache map was concurrently modified [req=" + req + ']';

                    ctx.discovery().removeCacheFilter(req.cacheName());

                    needExchange = true;

                    exchangeActions.addCacheToStop(req, desc);
                }
            }
            else if (req.close()) {
                if (desc != null) {
                    needExchange = ctx.discovery().onClientCacheClose(req.cacheName(), req.initiatingNodeId());

                    if (needExchange)
                        exchangeActions.addCacheToClose(req, desc);
                }
            }
            else
                assert false : req;

            if (!needExchange) {
                if (req.initiatingNodeId().equals(ctx.localNodeId()))
                    reqsToComplete.add(new T2<>(req, waitTopVer));
            }
            else
                incMinorTopVer = true;
        }

        if (!F.isEmpty(addedDescs)) {
            AffinityTopologyVersion startTopVer = incMinorTopVer ? topVer.nextMinorVersion() : topVer;

            for (DynamicCacheDescriptor desc : addedDescs) {
                assert desc.template() || incMinorTopVer;

                desc.startTopologyVersion(startTopVer);
            }
        }

        if (!F.isEmpty(reqsToComplete)) {
            ctx.closure().callLocalSafe(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    for (T2<DynamicCacheChangeRequest, AffinityTopologyVersion> t :reqsToComplete) {
                        final DynamicCacheChangeRequest req = t.get1();
                        AffinityTopologyVersion waitTopVer = t.get2();

                        IgniteInternalFuture<?> fut = waitTopVer != null ?
                            ctx.cache().context().exchange().affinityReadyFuture(waitTopVer) : null;

                        if (fut == null || fut.isDone())
                            ctx.cache().completeCacheStartFuture(req, null);
                        else {
                            fut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
                                @Override public void apply(IgniteInternalFuture<?> fut) {
                                    ctx.cache().completeCacheStartFuture(req, null);
                                }
                            });
                        }
                    }

                    return null;
                }
            });
        }

        if (incMinorTopVer) {
            assert !exchangeActions.empty() : exchangeActions;

            batch.exchangeActions(exchangeActions);
        }

        return incMinorTopVer;
    }

    /**
     * @param dataBag Discovery data bag.
     */
    void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        if (!ctx.isDaemon())
            dataBag.addJoiningNodeData(CACHE_PROC.ordinal(), joinDiscoveryData());
    }

    /**
     * @return Discovery date sent on local node join.
     */
    private Serializable joinDiscoveryData() {
        if (cachesOnDisconnect != null) {
            Map<String, CacheClientReconnectDiscoveryData.CacheInfo> cachesInfo = new HashMap<>();

            for (IgniteInternalCache cache : ctx.cache().caches()) {
                DynamicCacheDescriptor desc = cachesOnDisconnect.get(cache.name());

                assert desc != null : cache.name();

                cachesInfo.put(cache.name(), new CacheClientReconnectDiscoveryData.CacheInfo(desc.cacheConfiguration(),
                    desc.cacheType(),
                    desc.deploymentId(),
                    cache.context().isNear(),
                    (byte)0));
            }

            return new CacheClientReconnectDiscoveryData(cachesInfo);
        }
        else {
            assert ctx.config().isDaemon() || joinDiscoData != null || !ctx.state().active();

            return joinDiscoData;
        }
    }

    /**
     * Called from exchange worker.
     *
     * @return Caches to be started when this node starts.
     */
    List<T2<DynamicCacheDescriptor, NearCacheConfiguration>> cachesToStartOnLocalJoin() {
        if (ctx.isDaemon())
            return Collections.emptyList();

        assert locJoinStartCaches != null;

        List<T2<DynamicCacheDescriptor, NearCacheConfiguration>> locJoinStartCaches = this.locJoinStartCaches;

        this.locJoinStartCaches = null;

        return locJoinStartCaches;
    }

    /**
     * @param joinedNodeId Joined node ID.
     * @return New caches received from joined node.
     */
    List<DynamicCacheDescriptor> cachesReceivedFromJoin(UUID joinedNodeId) {
        assert joinedNodeId != null;

        List<DynamicCacheDescriptor> started = null;

        if (!ctx.isDaemon()) {
            for (DynamicCacheDescriptor desc : registeredCaches.values()) {
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
    void onDiscoveryEvent(int type, ClusterNode node, AffinityTopologyVersion topVer) {
        if (type == EVT_NODE_JOINED && !ctx.isDaemon()) {
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
                    assert joinDiscoData != null || !ctx.state().active();

                    initStartCachesForLocalJoin(true);
                }
            }
        }
    }

    /**
     * @param dataBag Discovery data bag.
     */
    void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (ctx.isDaemon())
            return;

        if (!dataBag.commonDataCollectedFor(CACHE_PROC.ordinal()))
            dataBag.addGridCommonData(CACHE_PROC.ordinal(), collectCommonDiscoveryData());
    }

    /**
     * @return Information about started caches.
     */
    private CacheNodeCommonDiscoveryData collectCommonDiscoveryData() {
        Map<String, CacheData> caches = new HashMap<>();

        for (DynamicCacheDescriptor desc : registeredCaches.values()) {
            CacheData cacheData = new CacheData(desc.cacheConfiguration(),
                desc.cacheId(),
                desc.cacheType(),
                desc.deploymentId(),
                desc.schema(),
                desc.receivedFrom(),
                desc.staticallyConfigured(),
                false,
                (byte)0);

            caches.put(desc.cacheName(), cacheData);
        }

        Map<String, CacheData> templates = new HashMap<>();

        for (DynamicCacheDescriptor desc : registeredTemplates.values()) {
            CacheData cacheData = new CacheData(desc.cacheConfiguration(),
                0,
                desc.cacheType(),
                desc.deploymentId(),
                desc.schema(),
                desc.receivedFrom(),
                desc.staticallyConfigured(),
                true,
                (byte)0);

            templates.put(desc.cacheName(), cacheData);
        }

        return new CacheNodeCommonDiscoveryData(caches, templates, ctx.discovery().clientNodesMap());
    }

    /**
     * @param data Discovery data.
     */
    void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        if (ctx.isDaemon() || data.commonData() == null)
            return;

        assert joinDiscoData != null || disconnectedState() || !ctx.state().active();
        assert data.commonData() instanceof CacheNodeCommonDiscoveryData : data;

        CacheNodeCommonDiscoveryData cachesData = (CacheNodeCommonDiscoveryData)data.commonData();

        for (CacheData cacheData : cachesData.templates().values()) {
            DynamicCacheDescriptor desc = new DynamicCacheDescriptor(
                ctx,
                cacheData.cacheConfiguration(),
                cacheData.cacheType(),
                true,
                cacheData.receivedFrom(),
                cacheData.staticallyConfigured(),
                cacheData.deploymentId(),
                cacheData.schema());

            registeredTemplates.put(cacheData.cacheConfiguration().getName(), desc);
        }

        for (CacheData cacheData : cachesData.caches().values()) {
            CacheConfiguration<?, ?> cfg = cacheData.cacheConfiguration();

            DynamicCacheDescriptor desc = new DynamicCacheDescriptor(
                ctx,
                cacheData.cacheConfiguration(),
                cacheData.cacheType(),
                false,
                cacheData.receivedFrom(),
                cacheData.staticallyConfigured(),
                cacheData.deploymentId(),
                cacheData.schema());

            desc.receivedOnDiscovery(true);

            registeredCaches.put(cacheData.cacheConfiguration().getName(), desc);

            ctx.discovery().setCacheFilter(
                cfg.getName(),
                cfg.getNodeFilter(),
                cfg.getNearConfiguration() != null,
                cfg.getCacheMode());
        }

        if (!F.isEmpty(cachesData.clientNodesMap())) {
            for (Map.Entry<String, Map<UUID, Boolean>> entry : cachesData.clientNodesMap().entrySet()) {
                String cacheName = entry.getKey();

                for (Map.Entry<UUID, Boolean> tup : entry.getValue().entrySet())
                    ctx.discovery().addClientNode(cacheName, tup.getKey(), tup.getValue());
            }
        }

        gridData = cachesData;

        if (!disconnectedState())
            initStartCachesForLocalJoin(false);
        else
            locJoinStartCaches = Collections.emptyList();
    }

    /**
     * @param firstNode {@code True} if first node in cluster starts.
     */
    private void initStartCachesForLocalJoin(boolean firstNode) {
        assert locJoinStartCaches == null;

        locJoinStartCaches = new ArrayList<>();

        if (joinDiscoData != null) {
            for (DynamicCacheDescriptor desc : registeredCaches.values()) {
                if (firstNode && !joinDiscoData.caches().containsKey(desc.cacheName()))
                    continue;

                CacheConfiguration<?, ?> cfg = desc.cacheConfiguration();

                CacheJoinNodeDiscoveryData.CacheInfo locCfg = joinDiscoData.caches().get(cfg.getName());

                NearCacheConfiguration nearCfg = null;

                if (locCfg != null) {
                    nearCfg = locCfg.config().getNearConfiguration();

                    DynamicCacheDescriptor desc0 = new DynamicCacheDescriptor(ctx,
                            locCfg.config(),
                            desc.cacheType(),
                            desc.template(),
                            desc.receivedFrom(),
                            desc.staticallyConfigured(),
                            desc.deploymentId(),
                            desc.schema());

                    desc0.startTopologyVersion(desc.startTopologyVersion());
                    desc0.receivedFromStartVersion(desc.receivedFromStartVersion());
                    desc0.clientCacheStartVersion(desc.clientCacheStartVersion());

                    desc = desc0;
                }

                if (locCfg != null || joinDiscoData.startCaches() || CU.affinityNode(ctx.discovery().localNode(), cfg.getNodeFilter()))
                    locJoinStartCaches.add(new T2<>(desc, nearCfg));
            }
        }
    }

    /**
     * @param data Joining node data.
     */
    void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        if (data.hasJoiningNodeData()) {
            Serializable joiningNodeData = data.joiningNodeData();

            if (joiningNodeData instanceof CacheClientReconnectDiscoveryData) {
                if (disconnectedState()) {
                    if (clientReconnectReqs == null)
                        clientReconnectReqs = new LinkedHashMap<>();

                    clientReconnectReqs.put(data.joiningNodeId(), (CacheClientReconnectDiscoveryData)joiningNodeData);
                }
                else
                    processClientReconnectData((CacheClientReconnectDiscoveryData) joiningNodeData, data.joiningNodeId());
            }
            else if (joiningNodeData instanceof CacheJoinNodeDiscoveryData)
                processJoiningNode((CacheJoinNodeDiscoveryData)joiningNodeData, data.joiningNodeId());
        }
    }

    /**
     * @param clientData Discovery data.
     * @param clientNodeId Client node ID.
     */
    private void processClientReconnectData(CacheClientReconnectDiscoveryData clientData, UUID clientNodeId) {
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

    /**
     * @param joinData Joined node discovery data.
     * @param nodeId Joined node ID.
     */
    private void processJoiningNode(CacheJoinNodeDiscoveryData joinData, UUID nodeId) {
        for (CacheJoinNodeDiscoveryData.CacheInfo cacheInfo : joinData.templates().values()) {
            CacheConfiguration<?, ?> cfg = cacheInfo.config();

            if (!registeredTemplates.containsKey(cfg.getName())) {
                DynamicCacheDescriptor desc = new DynamicCacheDescriptor(ctx,
                    cfg,
                    cacheInfo.cacheType(),
                    true,
                    nodeId,
                    true,
                    joinData.cacheDeploymentId(),
                    new QuerySchema(cfg.getQueryEntities()));

                DynamicCacheDescriptor old = registeredTemplates.put(cfg.getName(), desc);

                assert old == null : old;
            }
        }

        for (CacheJoinNodeDiscoveryData.CacheInfo cacheInfo : joinData.caches().values()) {
            CacheConfiguration<?, ?> cfg = cacheInfo.config();

            if (!registeredCaches.containsKey(cfg.getName())) {
                DynamicCacheDescriptor desc = new DynamicCacheDescriptor(ctx,
                    cfg,
                    cacheInfo.cacheType(),
                    false,
                    nodeId,
                    true,
                    joinData.cacheDeploymentId(),
                    new QuerySchema(cfg.getQueryEntities()));

                DynamicCacheDescriptor old = registeredCaches.put(cfg.getName(), desc);

                assert old == null : old;

                ctx.discovery().setCacheFilter(
                    cfg.getName(),
                    cfg.getNodeFilter(),
                    cfg.getNearConfiguration() != null,
                    cfg.getCacheMode());
            }

            ctx.discovery().addClientNode(cfg.getName(), nodeId, cfg.getNearConfiguration() != null);
        }

        if (joinData.startCaches()) {
            for (DynamicCacheDescriptor desc : registeredCaches.values()) {
                ctx.discovery().addClientNode(desc.cacheName(),
                    nodeId,
                    desc.cacheConfiguration().getNearConfiguration() != null);
            }
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
     *
     */
    void onDisconnect() {
        cachesOnDisconnect = new HashMap<>(registeredCaches);

        registeredCaches.clear();
        registeredTemplates.clear();

        clientReconnectReqs = null;
    }

    /**
     * @return Stopped caches names.
     */
    Set<String> onReconnected() {
        assert disconnectedState();

        Set<String> stoppedCaches = new HashSet<>();

        for(Map.Entry<String, DynamicCacheDescriptor> e : cachesOnDisconnect.entrySet()) {
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
        }

        if (clientReconnectReqs != null) {
            for (Map.Entry<UUID, CacheClientReconnectDiscoveryData> e : clientReconnectReqs.entrySet())
                processClientReconnectData(e.getValue(), e.getKey());

            clientReconnectReqs = null;
        }

        cachesOnDisconnect = null;

        return stoppedCaches;
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
        return CU.isUtilityCache(cacheName) || CU.isAtomicsCache(cacheName);
    }

    /**
     *
     */
    void clearCaches() {
        registeredCaches.clear();
    }
}
