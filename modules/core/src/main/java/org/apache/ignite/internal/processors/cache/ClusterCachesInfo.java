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

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.QuerySchema;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 *
 */
class ClusterCachesInfo {
    /** */
    private final GridKernalContext ctx;

    /** Dynamic caches. */
    private final ConcurrentMap<String, DynamicCacheDescriptor> registeredCaches = new ConcurrentHashMap<>();

    /** Cache templates. */
    private final ConcurrentMap<String, DynamicCacheDescriptor> registeredTemplates = new ConcurrentHashMap<>();

    /** */
    private CacheJoinNodeDiscoveryData joinDiscoData;

    /** */
    private CacheNodeCommonDiscoveryData gridData;

    /** */
    private List<DynamicCacheDescriptor> locJoinStartCaches;

    /**
     * @param ctx Context.
     */
    ClusterCachesInfo(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    void onStart(CacheJoinNodeDiscoveryData joinDiscoData) {
        this.joinDiscoData = joinDiscoData;
    }

    void onKernalStart() throws IgniteCheckedException {

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

                DynamicCacheDescriptor desc = registeredTemplates().get(req.cacheName());

                if (desc == null) {
                    DynamicCacheDescriptor templateDesc = new DynamicCacheDescriptor(ctx,
                        ccfg,
                        req.cacheType(),
                        true,
                        req.deploymentId(),
                        req.schema());

                    templateDesc.receivedFrom(req.initiatingNodeId());

                    DynamicCacheDescriptor old = registeredTemplates().put(ccfg.getName(), templateDesc);

                    assert old == null;

                    addedDescs.add(templateDesc);
                }

                ctx.cache().completeTemplateAddFuture(ccfg.getName(), req.deploymentId());

                continue;
            }

            DynamicCacheDescriptor desc = registeredCaches.get(req.cacheName());

            boolean needExchange = false;

            AffinityTopologyVersion waitTopVer = null;

            if (req.start()) {
                if (desc == null) {
                    if (req.clientStartOnly()) {
                        ctx.cache().completeCacheStartFuture(req, new IgniteCheckedException("Failed to start " +
                            "client cache (a cache with the given name is not started): " + req.cacheName()));
                    }
                    else {
                        CacheConfiguration ccfg = req.startCacheConfiguration();

                        assert req.cacheType() != null : req;
                        assert F.eq(ccfg.getName(), req.cacheName()) : req;

                        DynamicCacheDescriptor startDesc = new DynamicCacheDescriptor(ctx,
                            ccfg,
                            req.cacheType(),
                            false,
                            req.deploymentId(),
                            req.schema());

                        startDesc.receivedFrom(req.initiatingNodeId());

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
                        desc.clientCacheStartVersion(topVer.nextMinorVersion());

                        exchangeActions.addClientCacheToStart(req, desc);
                    }
                }

                if (!needExchange) {
                    if (desc != null) {
                        if (desc.clientCacheStartVersion() != null)
                            waitTopVer = desc.clientCacheStartVersion();
                        else
                            waitTopVer = desc.startTopologyVersion();
                    }
                }
            }
            else if (req.globalStateChange())
                needExchange = true;
            else if (req.resetLostPartitions()) {
                needExchange = desc != null;

                if (needExchange)
                    exchangeActions.addCacheToResetLostPartitions(req, desc);
            }
            else {
                assert req.stop() ^ req.close() : req;

                if (desc != null) {
                    if (req.stop()) {
                        DynamicCacheDescriptor old = registeredCaches.remove(req.cacheName());

                        assert old != null : "Dynamic cache map was concurrently modified [req=" + req + ']';

                        ctx.discovery().removeCacheFilter(req.cacheName());

                        needExchange = true;

                        exchangeActions.addCacheToStop(req, desc);
                    }
                    else {
                        assert req.close() : req;

                        needExchange = ctx.discovery().onClientCacheClose(req.cacheName(), req.initiatingNodeId());

                        if (needExchange) {
                            exchangeActions.addCacheToStop(req, desc);

                            exchangeActions.addCacheToClose(req, desc);
                        }
                    }
                }
            }

            if (!needExchange) {
                if (req.initiatingNodeId().equals(ctx.localNodeId()))
                    reqsToComplete.add(new T2<>(req, waitTopVer));
            }
            else
                incMinorTopVer = true;
        }

        if (!F.isEmpty(addedDescs)) {
            AffinityTopologyVersion startTopVer = incMinorTopVer ? topVer.nextMinorVersion() : topVer;

            for (DynamicCacheDescriptor desc : addedDescs)
                desc.startTopologyVersion(startTopVer);
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

    Serializable joinDiscoveryData() {
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
            assert ctx.config().isDaemon() || joinDiscoData != null;

            return joinDiscoData;
        }
    }

    /**
     * Called from exchange worker.
     *
     * @return Caches to be started when this node starts.
     */
    List<DynamicCacheDescriptor> cachesToStartOnLocalJoin() {
        assert locJoinStartCaches != null;

        List<DynamicCacheDescriptor> locJoinStartCaches = this.locJoinStartCaches;

        this.locJoinStartCaches = null;

        return locJoinStartCaches;
    }

    List<DynamicCacheDescriptor> cachesReceivedFromJoin(UUID joinedNodeId) {
        assert joinedNodeId != null;

        List<DynamicCacheDescriptor> started = null;

        if (!ctx.clientNode() && !ctx.isDaemon()) {
            for (DynamicCacheDescriptor desc : registeredCaches.values()) {
                if (desc.staticallyConfigured()) {
                    assert desc.receivedFrom() != null : desc;

                    IgnitePredicate<ClusterNode> filter = desc.cacheConfiguration().getNodeFilter();

                    if (joinedNodeId.equals(desc.receivedFrom()) &&
                        CU.affinityNode(ctx.discovery().localNode(), filter)) {
                        if (started == null)
                            started = new ArrayList<>();

                        started.add(desc);
                    }
                }
            }
        }

        return started;
    }

    /**
     * Discovery event callback, executed from discovery thread.
     *
     * @param type Event type.
     * @param node Event node.
     * @param topVer Topology version.
     */
    void onDiscoveryEvent(int type, ClusterNode node, AffinityTopologyVersion topVer) {
        if (type == EVT_NODE_JOINED) {
            if (node.id().equals(ctx.discovery().localNode().id())) {
                if (gridData == null) { // First node starts.
                    assert registeredCaches.isEmpty();
                    assert registeredTemplates.isEmpty();
                    assert joinDiscoData != null;

                    processJoiningNode(joinDiscoData, node.id());
                }

                assert locJoinStartCaches == null;

                locJoinStartCaches = new ArrayList<>();

                for (DynamicCacheDescriptor desc : registeredCaches.values()) {
                    CacheConfiguration cfg = desc.cacheConfiguration();

                    boolean locCfg = joinDiscoData.caches().containsKey(cfg.getName());

                    if (locCfg || CU.affinityNode(ctx.discovery().localNode(), cfg.getNodeFilter()))
                        locJoinStartCaches.add(desc);
                }

                joinDiscoData = null;
            }

            initStartVersionOnJoin(registeredCaches.values(), node, topVer);

            initStartVersionOnJoin(registeredTemplates.values(), node, topVer);
        }
    }

    private void initStartVersionOnJoin(Collection<DynamicCacheDescriptor> descs,
        ClusterNode joinedNode,
        AffinityTopologyVersion topVer) {
        for (DynamicCacheDescriptor cacheDesc : descs) {
            if (cacheDesc.staticallyConfigured() && joinedNode.id().equals(cacheDesc.receivedFrom()))
                cacheDesc.startTopologyVersion(topVer);
        }
    }

    CacheNodeCommonDiscoveryData collectCommonDiscoveryData() {
        Map<String, CacheData> caches = new HashMap<>();

        for (DynamicCacheDescriptor desc : registeredCaches.values()) {
            CacheData cacheData = new CacheData(desc.cacheConfiguration(),
                desc.cacheId(),
                desc.cacheType(),
                desc.startTopologyVersion(),
                desc.deploymentId(),
                desc.schema(),
                desc.receivedFrom(),
                desc.staticallyConfigured(),
                false);

            caches.put(desc.cacheConfiguration().getName(), cacheData);
        }

        Map<String, CacheData> templates = new HashMap<>();

        for (DynamicCacheDescriptor desc : registeredTemplates.values()) {
            CacheData cacheData = new CacheData(desc.cacheConfiguration(),
                0,
                desc.cacheType(),
                desc.startTopologyVersion(),
                null,
                desc.schema(),
                desc.receivedFrom(),
                desc.staticallyConfigured(),
                true);

            templates.put(desc.cacheConfiguration().getName(), cacheData);
        }

        return new CacheNodeCommonDiscoveryData(caches, templates, ctx.discovery().clientNodesMap());
    }

    void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        assert joinDiscoData != null;
        assert data.commonData() instanceof CacheNodeCommonDiscoveryData : data;

        CacheNodeCommonDiscoveryData cachesData = (CacheNodeCommonDiscoveryData)data.commonData();

        for (CacheData cacheData : cachesData.templates().values()) {
            DynamicCacheDescriptor desc = new DynamicCacheDescriptor(
                ctx,
                cacheData.cacheConfiguration(),
                cacheData.cacheType(),
                true,
                cacheData.deploymentId(),
                cacheData.schema());

            desc.startTopologyVersion(cacheData.startTopologyVersion());
            desc.receivedFrom(cacheData.receivedFrom());
            desc.staticallyConfigured(cacheData.staticallyConfigured());

            DynamicCacheDescriptor old = registeredTemplates.put(cacheData.cacheConfiguration().getName(), desc);

            assert old == null;
        }

        for (CacheData cacheData : cachesData.caches().values()) {
            CacheConfiguration cfg = cacheData.cacheConfiguration();

            DynamicCacheDescriptor desc = new DynamicCacheDescriptor(
                ctx,
                cacheData.cacheConfiguration(),
                cacheData.cacheType(),
                false,
                cacheData.deploymentId(),
                cacheData.schema());

            desc.startTopologyVersion(cacheData.startTopologyVersion());
            desc.receivedFrom(cacheData.receivedFrom());
            desc.staticallyConfigured(cacheData.staticallyConfigured());

            DynamicCacheDescriptor old = registeredCaches.put(cacheData.cacheConfiguration().getName(), desc);

            assert old == null;

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
    }

    void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        if (data.hasJoiningNodeData()) {
            Serializable joiningNodeData = data.joiningNodeData();

            if (joiningNodeData instanceof CacheClientReconnectDiscoveryData)
                processClientReconnectData((CacheClientReconnectDiscoveryData)joiningNodeData, data.joiningNodeId());
            else if (joiningNodeData instanceof CacheJoinNodeDiscoveryData)
                processJoiningNode((CacheJoinNodeDiscoveryData)joiningNodeData, data.joiningNodeId());
        }
    }

    /**
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

    private void processJoiningNode(CacheJoinNodeDiscoveryData joinData, UUID nodeId) {
        for (CacheJoinNodeDiscoveryData.CacheInfo cacheInfo : joinData.templates().values()) {
            CacheConfiguration cfg = cacheInfo.config();

            if (!registeredTemplates.containsKey(cfg.getName())) {
                DynamicCacheDescriptor desc = new DynamicCacheDescriptor(ctx,
                    cfg,
                    cacheInfo.cacheType(),
                    true,
                    joinData.cacheDeploymentId(),
                    new QuerySchema(cfg.getQueryEntities()));

                desc.staticallyConfigured(true);
                desc.receivedFrom(nodeId);

                DynamicCacheDescriptor old = registeredTemplates.put(cfg.getName(), desc);

                assert old == null : old;
            }
        }

        for (CacheJoinNodeDiscoveryData.CacheInfo cacheInfo : joinData.caches().values()) {
            CacheConfiguration cfg = cacheInfo.config();

            if (!registeredCaches.containsKey(cfg.getName())) {
                DynamicCacheDescriptor desc = new DynamicCacheDescriptor(ctx,
                    cfg,
                    cacheInfo.cacheType(),
                    false,
                    joinData.cacheDeploymentId(),
                    new QuerySchema(cfg.getQueryEntities()));

                desc.staticallyConfigured(true);
                desc.receivedFrom(nodeId);

                DynamicCacheDescriptor old = registeredCaches.put(cfg.getName(), desc);

                assert old == null : old;

                ctx.discovery().setCacheFilter(
                    cfg.getName(),
                    cfg.getNodeFilter(),
                    cfg.getNearConfiguration() != null,
                    cfg.getCacheMode());
            }

            ctx.discovery().addClientNode(cfg.getName(),
                nodeId,
                cfg.getNearConfiguration() != null);
        }
    }

    ConcurrentMap<String, DynamicCacheDescriptor> registeredCaches() {
        return registeredCaches;
    }

    ConcurrentMap<String, DynamicCacheDescriptor> registeredTemplates() {
        return registeredTemplates;
    }

    /** */
    private Map<String, DynamicCacheDescriptor> cachesOnDisconnect;

    void onDisconnect() {
        cachesOnDisconnect = new HashMap<>(registeredCaches);

        registeredCaches.clear();
        registeredTemplates.clear();
    }

    Set<String> onReconnected() {
        assert cachesOnDisconnect != null;

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

        cachesOnDisconnect = null;

        return stoppedCaches;
    }

    private boolean surviveReconnect(String cacheName) {
        return CU.isUtilityCache(cacheName) || CU.isAtomicsCache(cacheName);
    }

    void clearCaches() {
        registeredCaches.clear();
    }
}
