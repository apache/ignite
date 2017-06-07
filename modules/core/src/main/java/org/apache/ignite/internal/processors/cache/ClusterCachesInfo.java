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
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
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
    /** */
    private final GridKernalContext ctx;

    /** Dynamic caches. */
    private final ConcurrentMap<String, DynamicCacheDescriptor> registeredCaches = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentMap<Integer, CacheGroupDescriptor> registeredCacheGrps = new ConcurrentHashMap<>();

    /** Cache templates. */
    private final ConcurrentMap<String, DynamicCacheDescriptor> registeredTemplates = new ConcurrentHashMap<>();

    /** */
    private final IgniteLogger log;

    /** */
    private CachesOnDisconnect cachesOnDisconnect;

    /** */
    private CacheJoinNodeDiscoveryData joinDiscoData;

    /** */
    private GridData gridData;

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
     * @throws IgniteCheckedException If configuration validation failed.
     */
    void onStart(CacheJoinNodeDiscoveryData joinDiscoData) throws IgniteCheckedException {
        this.joinDiscoData = joinDiscoData;

        Map<String, CacheConfiguration> grpCfgs = new HashMap<>();

        for (CacheJoinNodeDiscoveryData.CacheInfo info : joinDiscoData.caches().values()) {
            if (info.config().getGroupName() == null)
                continue;

            CacheConfiguration ccfg = grpCfgs.get(info.config().getGroupName());

            if (ccfg == null)
                grpCfgs.put(info.config().getGroupName(), info.config());
            else
                validateCacheGroupConfiguration(ccfg, info.config());
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
    void onKernalStart(boolean checkConsistency) throws IgniteCheckedException {
        if (gridData != null && gridData.conflictErr != null)
            throw new IgniteCheckedException(gridData.conflictErr);

        if (checkConsistency && joinDiscoData != null && gridData != null) {
            for (CacheJoinNodeDiscoveryData.CacheInfo locCacheInfo : joinDiscoData.caches().values()) {
                CacheConfiguration locCfg = locCacheInfo.config();

                CacheData cacheData = gridData.gridData.caches().get(locCfg.getName());

                if (cacheData != null)
                    checkCache(locCacheInfo, cacheData, cacheData.receivedFrom());

                validateStartCacheConfiguration(locCfg);
            }
        }

        joinDiscoData = null;
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
        GridCacheAttributes rmtAttr = new GridCacheAttributes(rmtData.cacheConfiguration(), rmtData.sql());
        GridCacheAttributes locAttr = new GridCacheAttributes(locInfo.config(), locInfo.sql());

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "cacheMode", "Cache mode",
            locAttr.cacheMode(), rmtAttr.cacheMode(), true);

        CU.checkAttributeMismatch(log, rmtAttr.groupName(), rmt, "groupName", "Cache group name",
            locAttr.groupName(), rmtAttr.groupName(), true);

        CU.checkAttributeMismatch(log, rmtAttr.cacheName(), rmt, "sql", "SQL flag",
            locAttr.sql(), rmtAttr.sql(), true);

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

            if (CU.affinityNode(ctx.discovery().localNode(), locInfo.config().getNodeFilter())
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
                        null,
                        true,
                        req.initiatingNodeId(),
                        false,
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
                    String conflictErr = checkCacheConflict(req.startCacheConfiguration());

                    if (conflictErr != null) {
                        U.warn(log, "Ignore cache start request. " + conflictErr);

                        ctx.cache().completeCacheStartFuture(req, false, new IgniteCheckedException("Failed to start " +
                            "cache. " + conflictErr));

                        continue;
                    }

                    if (req.clientStartOnly()) {
                        ctx.cache().completeCacheStartFuture(req, false, new IgniteCheckedException("Failed to start " +
                            "client cache (a cache with the given name is not started): " + req.cacheName()));
                    }
                    else {
                        SchemaOperationException err = QueryUtils.checkQueryEntityConflicts(
                            req.startCacheConfiguration(), ctx.cache().cacheDescriptors().values());

                        if (err != null) {
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

                        assert old == null;

                        ctx.discovery().setCacheFilter(
                            grpDesc.groupId(),
                            ccfg.getName(),
                            ccfg.getNearConfiguration() != null);

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
                            ctx.cache().completeCacheStartFuture(req, false,
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

                        exchangeActions.addCacheGroupToStop(grpDesc);
                    }
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
        Map<Integer, CacheGroupData> cacheGrps = new HashMap<>();

        for (CacheGroupDescriptor grpDesc : registeredCacheGrps.values()) {
            CacheGroupData grpData = new CacheGroupData(grpDesc.config(),
                grpDesc.groupName(),
                grpDesc.groupId(),
                grpDesc.receivedFrom(),
                grpDesc.startTopologyVersion(),
                grpDesc.deploymentId(),
                grpDesc.caches(),
                0);

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

        return new CacheNodeCommonDiscoveryData(caches,
            templates,
            cacheGrps,
            ctx.discovery().clientNodesMap());
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

        // Replace locally registered data with actual data received from cluster.
        registeredCaches.clear();
        registeredCacheGrps.clear();
        ctx.discovery().onLocalNodeJoin();

        for (CacheGroupData grpData : cachesData.cacheGroups().values()) {
            CacheGroupDescriptor grpDesc = new CacheGroupDescriptor(
                grpData.config(),
                grpData.groupName(),
                grpData.groupId(),
                grpData.receivedFrom(),
                grpData.startTopologyVersion(),
                grpData.deploymentId(),
                grpData.caches());

            CacheGroupDescriptor old = registeredCacheGrps.put(grpDesc.groupId(), grpDesc);

            assert old == null : old;

            ctx.discovery().addCacheGroup(grpDesc,
                grpData.config().getNodeFilter(),
                grpData.config().getCacheMode());
        }

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
                cacheData.schema());

            desc.receivedOnDiscovery(true);

            registeredCaches.put(cacheData.cacheConfiguration().getName(), desc);

            ctx.discovery().setCacheFilter(
                grpDesc.groupId(),
                cfg.getName(),
                cfg.getNearConfiguration() != null);
        }

        if (!F.isEmpty(cachesData.clientNodesMap())) {
            for (Map.Entry<String, Map<UUID, Boolean>> entry : cachesData.clientNodesMap().entrySet()) {
                String cacheName = entry.getKey();

                for (Map.Entry<UUID, Boolean> tup : entry.getValue().entrySet())
                    ctx.discovery().addClientNode(cacheName, tup.getKey(), tup.getValue());
            }
        }

        String conflictErr = null;

        if (joinDiscoData != null) {
            for (Map.Entry<String, CacheJoinNodeDiscoveryData.CacheInfo> e : joinDiscoData.caches().entrySet()) {
                if (!registeredCaches.containsKey(e.getKey())) {
                    conflictErr = checkCacheConflict(e.getValue().config());

                    if (conflictErr != null) {
                        conflictErr = "Failed to start configured cache due to conflict with started caches. " +
                            conflictErr;

                        break;
                    }
                }
            }
        }

        gridData = new GridData(cachesData, conflictErr);

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
                            desc.groupDescriptor(),
                            desc.template(),
                            desc.receivedFrom(),
                            desc.staticallyConfigured(),
                            desc.sql(),
                            desc.deploymentId(),
                            desc.schema());

                    desc0.startTopologyVersion(desc.startTopologyVersion());
                    desc0.receivedFromStartVersion(desc.receivedFromStartVersion());
                    desc0.clientCacheStartVersion(desc.clientCacheStartVersion());

                    desc = desc0;
                }

                if (locCfg != null ||
                    joinDiscoData.startCaches() ||
                    CU.affinityNode(ctx.discovery().localNode(), desc.groupDescriptor().config().getNodeFilter())) {
                    // Move system and internal caches first.
                    if (desc.cacheType().userCache())
                        locJoinStartCaches.add(new T2<>(desc, nearCfg));
                    else
                        locJoinStartCaches.add(0, new T2<>(desc, nearCfg));
                }
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
                processJoiningNode((CacheJoinNodeDiscoveryData)joiningNodeData, data.joiningNodeId(), false);
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
     * @param cfg Cache configuration.
     * @return {@code True} if validation passed.
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
        for (CacheJoinNodeDiscoveryData.CacheInfo cacheInfo : joinData.templates().values()) {
            CacheConfiguration<?, ?> cfg = cacheInfo.config();

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
                    new QuerySchema(cfg.getQueryEntities()));

                DynamicCacheDescriptor old = registeredTemplates.put(cfg.getName(), desc);

                assert old == null : old;
            }
        }

        for (CacheJoinNodeDiscoveryData.CacheInfo cacheInfo : joinData.caches().values()) {
            CacheConfiguration<?, ?> cfg = cacheInfo.config();

            if (!registeredCaches.containsKey(cfg.getName())) {
                String conflictErr = checkCacheConflict(cfg);

                if (conflictErr != null) {
                    if (locJoin)
                        return conflictErr;

                    U.warn(log, "Ignore cache received from joining node. " + conflictErr);

                    continue;
                }

                int cacheId = CU.cacheId(cfg.getName());

                CacheGroupDescriptor grpDesc = registerCacheGroup(null,
                    null,
                    cfg,
                    cacheId,
                    nodeId,
                    joinData.cacheDeploymentId());

                ctx.discovery().setCacheFilter(
                    grpDesc.groupId(),
                    cfg.getName(),
                    cfg.getNearConfiguration() != null);

                DynamicCacheDescriptor desc = new DynamicCacheDescriptor(ctx,
                    cfg,
                    cacheInfo.cacheType(),
                    grpDesc,
                    false,
                    nodeId,
                    true,
                    cacheInfo.sql(),
                    joinData.cacheDeploymentId(),
                    new QuerySchema(cfg.getQueryEntities()));

                DynamicCacheDescriptor old = registeredCaches.put(cfg.getName(), desc);

                assert old == null : old;
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

        return null;
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

        CacheGroupDescriptor grpDesc = new CacheGroupDescriptor(
            startedCacheCfg,
            startedCacheCfg.getGroupName(),
            grpId,
            rcvdFrom,
            curTopVer != null ? curTopVer.nextMinorVersion() : null,
            deploymentId,
            caches);

        CacheGroupDescriptor old = registeredCacheGrps.put(grpId, grpDesc);

        assert old == null : old;

        ctx.discovery().addCacheGroup(grpDesc, grpDesc.config().getNodeFilter(), startedCacheCfg.getCacheMode());

        if (exchActions != null)
            exchActions.addCacheGroupToStart(grpDesc);

        return grpDesc;
    }

    /**
     * @return Registered cache groups.
     */
    ConcurrentMap<Integer, CacheGroupDescriptor> registeredCacheGroups() {
        return registeredCacheGrps;
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
        GridCacheAttributes attr1 = new GridCacheAttributes(cfg, false);
        GridCacheAttributes attr2 = new GridCacheAttributes(startCfg, false);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "cacheMode", "Cache mode",
            cfg.getCacheMode(), startCfg.getCacheMode(), true);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "affinity", "Affinity function",
            attr1.cacheAffinityClassName(), attr2.cacheAffinityClassName(), true);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "affinityPartitionsCount",
            "Affinity partitions count", attr1.affinityPartitionsCount(), attr2.affinityPartitionsCount(), true);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "nodeFilter", "Node filter",
            attr1.nodeFilterClassName(), attr2.nodeFilterClassName(), true);

        CU.validateCacheGroupsAttributesMismatch(log, cfg, startCfg, "memoryPolicyName", "Memory policy",
            cfg.getMemoryPolicyName(), startCfg.getMemoryPolicyName(), true);

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
     *
     */
    void onDisconnect() {
        cachesOnDisconnect = new CachesOnDisconnect(
            new HashMap<>(registeredCacheGrps),
            new HashMap<>(registeredCaches));

        registeredCacheGrps.clear();
        registeredCaches.clear();
        registeredTemplates.clear();

        clientReconnectReqs = null;
    }

    /**
     * @return Information about stopped caches and cache groups.
     */
    ClusterCachesReconnectResult onReconnected() {
        assert disconnectedState();

        Set<String> stoppedCaches = new HashSet<>();
        Set<Integer> stoppedCacheGrps = new HashSet<>();

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
        return CU.isUtilityCache(cacheName) || CU.isAtomicsCache(cacheName);
    }

    /**
     *
     */
    void clearCaches() {
        registeredCacheGrps.clear();

        registeredCaches.clear();
    }

    /**
     *
     */
    static class GridData {
        /** */
        private final CacheNodeCommonDiscoveryData gridData;

        /** */
        private final String conflictErr;

        /**
         * @param gridData Grid data.
         * @param conflictErr Cache configuration conflict error.
         */
        GridData(CacheNodeCommonDiscoveryData gridData, String conflictErr) {
            this.gridData = gridData;
            this.conflictErr = conflictErr;
        }
    }

    /**
     *
     */
    private static class CachesOnDisconnect {
        /** */
        final Map<Integer, CacheGroupDescriptor> cacheGrps;

        /** */
        final Map<String, DynamicCacheDescriptor> caches;

        /**
         * @param cacheGrps Cache groups.
         * @param caches Caches.
         */
        CachesOnDisconnect(Map<Integer, CacheGroupDescriptor> cacheGrps, Map<String, DynamicCacheDescriptor> caches) {
            this.cacheGrps = cacheGrps;
            this.caches = caches;
        }
    }
}
