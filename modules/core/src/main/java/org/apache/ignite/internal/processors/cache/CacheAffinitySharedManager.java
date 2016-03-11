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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridClientPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAssignmentFetchFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 *
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class CacheAffinitySharedManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /** */
    public static final IgniteProductVersion DELAY_AFF_ASSIGN_SINCE = IgniteProductVersion.fromString("1.6.0");

    /** */
    public static final boolean LOG_AFF_CHANGE = true;

    /** */
    public static final String LOG_AFF_CACHE = "aff_log_cache";

    /** */
    private RebalancingInfo rebalancingInfo;

    /** */
    private ConcurrentMap<Integer, CacheHolder> caches = new ConcurrentHashMap<>();

    /** */
    private AffinityTopologyVersion affCalcVer;

    /** */
    private AffinityTopologyVersion lastAffVer;

    /** */
    private final Object mux = new Object();

    /** Pending affinity assignment futures. */
    private final ConcurrentMap<T2<Integer, AffinityTopologyVersion>, GridDhtAssignmentFetchFuture>
        pendingAssignmentFetchFuts = new ConcurrentHashMap8<>();

    /** Discovery listener. */
    private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            DiscoveryEvent e = (DiscoveryEvent)evt;

            assert e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED;

            ClusterNode n = e.eventNode();

            for (GridDhtAssignmentFetchFuture fut : pendingAssignmentFetchFuts.values())
                fut.onNodeLeft(n.id());
        }
    };

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        cctx.kernalContext().event().addLocalEventListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /**
     * @param type Event type.
     * @param node Event node.
     * @param topVer Topology version.
     */
    public void onDiscoveryEvent(int type, ClusterNode node, AffinityTopologyVersion topVer) {
        if (!CU.clientNode(node) && (type == EVT_NODE_FAILED || type == EVT_NODE_JOINED || type == EVT_NODE_LEFT)) {
            assert lastAffVer == null || topVer.compareTo(lastAffVer) > 0;

            lastAffVer = topVer;
        }
    }

    /**
     * Callback invoked from discovery thread when discovery custom message is received.
     *
     * @param msg Customer message.
     * @return {@code True} if minor topology version should be increased.
     */
    public boolean onCustomEvent(CacheAffinityChangeMessage msg) {
        boolean exchangeNeeded =
            msg.exchangeId() == null && (lastAffVer == null || lastAffVer.equals(msg.topologyVersion()));

        msg.exchangeNeeded(exchangeNeeded);

        if (exchangeNeeded) {
            log.info("Need process affinity change message [lastAffVer=" + lastAffVer +
                ", msgExchId=" + msg.exchangeId() +
                ", msgVer=" + msg.topologyVersion() +']');
        }
        else {
            log.info("Ignore affinity change message [lastAffVer=" + lastAffVer +
                ", msgExchId=" + msg.exchangeId() +
                ", msgVer=" + msg.topologyVersion() +']');
        }

        return exchangeNeeded;
    }

    /**
     * @param top Topology.
     * @param checkCacheId Cache ID.
     */
    public void checkRebalanceState(GridDhtPartitionTopology top, Integer checkCacheId) {
        CacheAffinityChangeMessage msg = null;

        synchronized (mux) {
            if (rebalancingInfo == null)
                return;

            assert affCalcVer != null;

            assert affCalcVer.equals(rebalancingInfo.topVer);

            Map<Integer, UUID> partWait = rebalancingInfo.waitCaches.get(checkCacheId);

            boolean rebalanced = true;

            if (partWait != null) {
                CacheHolder cache = caches.get(checkCacheId);

                if (cache != null) {
                    for (Iterator<Map.Entry<Integer, UUID>> it =  partWait.entrySet().iterator(); it.hasNext();) {
                        Map.Entry<Integer, UUID> e = it.next();

                        Integer part = e.getKey();
                        UUID waitNode = e.getValue();

                        GridDhtPartitionState state = top.partitionState(waitNode, part);

                        if (state != GridDhtPartitionState.OWNING) {
                            rebalanced = false;

                            break;
                        }
                        else
                            it.remove();
                    }

                    if (LOG_AFF_CHANGE) {
                        logAffinityChange(log, cache.name(), "Cache rebalance state [cache=" + cache.name() +
                            ", rebalanced=" + rebalanced + ']');
                    }
                }

                if (rebalanced) {
                    rebalancingInfo.waitCaches.remove(checkCacheId);

                    if (rebalancingInfo.waitCaches.isEmpty()) {
                        assert !F.isEmpty(rebalancingInfo.assignments);

                        Map<Integer, Map<Integer, List<UUID>>> assignmentsChange =
                            U.newHashMap(rebalancingInfo.assignments.size());

                        for (Map.Entry<Integer, Map<Integer, List<ClusterNode>>> e : rebalancingInfo.assignments.entrySet()) {
                            Integer cacheId = e.getKey();

                            Map<Integer, List<ClusterNode>> assignment = e.getValue();

                            Map<Integer, List<UUID>> assignment0 = U.newHashMap(assignment.size());

                            for (Map.Entry<Integer, List<ClusterNode>> e0 : assignment.entrySet())
                                assignment0.put(e0.getKey(), toIds0(e0.getValue()));

                            assignmentsChange.put(cacheId, assignment0);
                        }

                        msg = new CacheAffinityChangeMessage(rebalancingInfo.topVer, assignmentsChange);
                    }
                }
            }
        }

        try {
            if (msg != null) {
                if (LOG_AFF_CHANGE)
                    log.info("Rebalance finished, send affinity change message: " + msg);

                cctx.discovery().sendCustomEvent(msg);
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send affinity change message.", e);
        }
    }

    /**
     * @param log Logger.
     * @param cacheName Cache name.
     * @param msg Message.
     */
    public static void logAffinityChange(IgniteLogger log, String cacheName, String msg) {
        if (F.eq(cacheName, LOG_AFF_CACHE))
            log.info(msg);
    }

    /**
     * @param cctx Cache context.
     */
    public void onCacheCreated(GridCacheContext cctx) {
        final Integer cacheId = cctx.cacheId();

        if (!caches.containsKey(cctx.cacheId())) {
            cctx.io().addHandler(cacheId, GridDhtAffinityAssignmentResponse.class,
                new IgniteBiInClosure<UUID, GridDhtAffinityAssignmentResponse>() {
                    @Override public void apply(UUID nodeId, GridDhtAffinityAssignmentResponse res) {
                        processAffinityAssignmentResponse(cacheId, nodeId, res);
                    }
                });
        }
    }

    /**
     * @param topVer Topology version.
     * @return {@code True} if can use delayed affinity assignment.
     */
    public boolean delayedAffinityAssignment(AffinityTopologyVersion topVer) {
        Collection<ClusterNode> nodes = cctx.discovery().nodes(topVer);

        for (ClusterNode node : nodes) {
            if (node.version().compareTo(DELAY_AFF_ASSIGN_SINCE) < 0)
                return false;
        }

        return true;
    }

    /**
     * @param nodes Nodes.
     * @return IDs.
     */
    private List<UUID> toIds0(List<ClusterNode> nodes) {
        List<UUID> partIds = new ArrayList<>(nodes.size());

        for (int i = 0; i < nodes.size(); i++)
            partIds.add(nodes.get(i).id());

        return partIds;
    }

    /**
     * @param topVer Topology version.
     * @param ids IDs.
     * @return Nodes.
     */
    private List<ClusterNode> toNodes(AffinityTopologyVersion topVer, List<UUID> ids) {
        List<ClusterNode> nodes = new ArrayList<>(ids.size());

        for (int i = 0; i < ids.size(); i++) {
            UUID id = ids.get(i);

            ClusterNode node = cctx.discovery().node(topVer, id);

            assert node != null : id;

            nodes.add(node);
        }

        return nodes;
    }

    /**
     * @param fut Exchange future.
     * @param reqs Cache change requests.
     * @throws IgniteCheckedException If failed.
     */
    public boolean onCacheChangeRequest(final GridDhtPartitionsExchangeFuture fut, boolean crd, Collection<DynamicCacheChangeRequest> reqs)
        throws IgniteCheckedException {
        assert !F.isEmpty(reqs) : fut;

        boolean clientOnly = true;

        // Affinity did not change for existing caches.
        forAllCaches(crd, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                if (fut.stopping(aff.cacheId()))
                    return;

                aff.clientEventTopologyChange(fut.discoveryEvent(), fut.topologyVersion());
            }
        });

        for (DynamicCacheChangeRequest req : reqs) {
            if (!(req.clientStartOnly() || req.close()))
                clientOnly = false;

            Integer cacheId = CU.cacheId(req.cacheName());

            if (req.start()) {
                cctx.cache().prepareCacheStart(req, fut.topologyVersion());

                if (fut.isCacheAdded(cacheId, fut.topologyVersion())) {
                    if (cctx.discovery().cacheAffinityNodes(req.cacheName(), fut.topologyVersion()).isEmpty())
                        U.quietAndWarn(log, "No server nodes found for cache client: " + req.cacheName());
                }

                if (!crd) {
                    GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                    if (cacheCtx != null) {
                        boolean clientCacheStarted =
                            req.clientStartOnly() && req.initiatingNodeId().equals(cctx.localNodeId());

                        if (clientCacheStarted)
                            initAffinity(cacheCtx.affinity().affinityCache(), fut);
                        else {
                            GridAffinityAssignmentCache aff = cacheCtx.affinity().affinityCache();

                            List<List<ClusterNode>> assignment =
                                aff.calculate(fut.topologyVersion(), fut.discoveryEvent());

                            aff.initialize(fut.topologyVersion(), assignment);
                        }
                    }
                }
                else
                    initStartedCache(fut, cacheId);
            }
            else if (req.stop() || req.close()) {
                cctx.cache().blockGateway(req);

                if (crd) {
                    boolean rmvCache = req.stop() || (req.close() &&
                        req.initiatingNodeId().equals(cctx.localNodeId()) &&
                        !cctx.discovery().cacheAffinityNode(cctx.localNode(), req.cacheName()));

                    if (rmvCache) {
                        CacheHolder cache = caches.remove(cacheId);

                        assert cache != null;

                        if (!req.stop()) {
                            cache = new CacheHolder2(cctx.cache().cacheDescriptor(cacheId), fut, cache.affinity());

                            caches.put(cacheId, cache);
                        }
                        else
                            cctx.io().removeHandler(cacheId, GridDhtAffinityAssignmentResponse.class);
                    }
                }
            }
        }

        return clientOnly;
    }

    /**
     * @param exchFut Exchange future.
     * @param msg Affinity change message.
     */
    public void onExchangeChangeAffinityMessage(GridDhtPartitionsExchangeFuture exchFut, boolean crd, CacheAffinityChangeMessage msg) {
        log.info("Process exchange affinity change message [exchVer=" + exchFut.topologyVersion() + ']');

        assert exchFut.exchangeId().equals(msg.exchangeId()) : msg;

        final AffinityTopologyVersion topVer = exchFut.topologyVersion();

        final Map<Integer, Map<Integer, List<UUID>>> assignment = msg.assignmentChange();

        assert !F.isEmpty(assignment) : msg;

        forAllCaches(crd, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                int parts = aff.partitions();

                List<List<ClusterNode>> newAssignment = new ArrayList<>(parts);

                // TODO: send only ideal affinity delta.
                Map<Integer, List<UUID>> cacheAssignment = assignment.get(aff.cacheId());

                assert cacheAssignment != null;
                assert cacheAssignment.size() == parts;

                for (int p = 0; p < parts; p++) {
                    List<UUID> ids = cacheAssignment.get(p);

                    newAssignment.add(toNodes(topVer, ids));
                }

                aff.initialize(topVer, newAssignment);
            }
        });
    }



    /**
     * @param exchFut Exchange future.
     * @param msg Message.
     * @throws IgniteCheckedException If failed.
     */
    public void onChangeAffinityMessage(final GridDhtPartitionsExchangeFuture exchFut, boolean crd, final CacheAffinityChangeMessage msg)
        throws IgniteCheckedException {
        assert affCalcVer != null || cctx.kernalContext().clientNode();
        assert msg.topologyVersion() != null && msg.exchangeId() == null: msg;
        assert affCalcVer == null || affCalcVer.equals(msg.topologyVersion());

        final AffinityTopologyVersion topVer = exchFut.topologyVersion();

        log.info("Process affinity change message [exchVer=" + exchFut.topologyVersion() +
                ", affCalcVer=" + affCalcVer +
                ", msgVer=" + msg.topologyVersion() +']');

        final Map<Integer, Map<Integer, List<UUID>>> affChange = msg.assignmentChange();

        assert !F.isEmpty(affChange) : msg;

        forAllCaches(crd, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                AffinityTopologyVersion affTopVer = aff.lastVersion();

                assert affTopVer.topologyVersion() > 0 : affTopVer;

                Map<Integer, List<UUID>> change = affChange.get(aff.cacheId());

                if (change != null) {
                    assert !change.isEmpty() : msg;

                    List<List<ClusterNode>> curAff = aff.assignments(affTopVer);

                    List<List<ClusterNode>> assignment = new ArrayList<>(curAff);

                    for (Map.Entry<Integer, List<UUID>> e : change.entrySet()) {
                        Integer part = e.getKey();

                        List<ClusterNode> nodes = toNodes(topVer, e.getValue());

                        if (LOG_AFF_CHANGE) {
                            logAffinityChange(log, aff.cacheName(), "New assignment [cache=" + aff.cacheName() +
                                    ", part=" + part +
                                    ", cur=" + F.nodeIds(assignment.get(part)) +
                                    ", new=" + F.nodeIds(nodes) + ']');
                        }

                        assert !nodes.equals(assignment.get(part)) : "Assignment did not change " +
                                "[cache=" + aff.cacheName() +
                                ", part=" + part +
                                ", cur=" + F.nodeIds(assignment.get(part)) +
                                ", cur=" + F.nodeIds(nodes) +
                                ", exchVer=" + exchFut.topologyVersion() +
                                ", msgVer=" + msg.topologyVersion() +
                                ']';

                        assignment.set(part, nodes);
                    }

                    aff.initialize(topVer, assignment);
                }
                else
                    aff.clientEventTopologyChange(exchFut.discoveryEvent(), topVer);
            }
        });

        synchronized (mux) {
            if (affCalcVer == null)
                affCalcVer = msg.topologyVersion();
        }
    }

    /**
     *
     */
    public void dumpDebugInfo() {
        if (!pendingAssignmentFetchFuts.isEmpty()) {
            U.warn(log, "Pending assignment fetch futures:");

            for (GridDhtAssignmentFetchFuture fut : pendingAssignmentFetchFuts.values())
                U.warn(log, ">>> " + fut);
        }
    }

    /**
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     */
    public void onClientEvent(final GridDhtPartitionsExchangeFuture fut, boolean crd) throws IgniteCheckedException {
        boolean locJoin = fut.discoveryEvent().eventNode().isLocal();

        if (!locJoin) {
            forAllCaches(crd, new IgniteInClosureX<GridAffinityAssignmentCache>() {
                @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                    AffinityTopologyVersion topVer = fut.topologyVersion();

                    aff.clientEventTopologyChange(fut.discoveryEvent(), topVer);
                }
            });
        }
        else
            fetchAffinity(fut);
    }

    /**
     * @param fut Future to add.
     */
    public void addDhtAssignmentFetchFuture(GridDhtAssignmentFetchFuture fut) {
        GridDhtAssignmentFetchFuture old = pendingAssignmentFetchFuts.putIfAbsent(fut.key(), fut);

        assert old == null : "More than one thread is trying to fetch partition assignments: " + fut.key();
    }

    /**
     * @param fut Future to remove.
     */
    public void removeDhtAssignmentFetchFuture(GridDhtAssignmentFetchFuture fut) {
        boolean rmv = pendingAssignmentFetchFuts.remove(fut.key(), fut);

        assert rmv : "Failed to remove assignment fetch future: " + fut.key();
    }

    /**
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processAffinityAssignmentResponse(Integer cacheId, UUID nodeId, GridDhtAffinityAssignmentResponse res) {
        if (log.isDebugEnabled())
            log.debug("Processing affinity assignment response [node=" + nodeId + ", res=" + res + ']');

        for (GridDhtAssignmentFetchFuture fut : pendingAssignmentFetchFuts.values()) {
            if (fut.key().get1().equals(cacheId))
                fut.onResponse(nodeId, res);
        }
    }

    private void forAllRegisteredCaches(AffinityTopologyVersion topVer, IgniteInClosureX<DynamicCacheDescriptor> c)
        throws IgniteCheckedException {
        for (DynamicCacheDescriptor cacheDesc : cctx.cache().cacheDescriptors()) {
            AffinityTopologyVersion startVer = cacheDesc.startTopologyVersion();

            if (startVer == null || startVer.compareTo(topVer) <= 0)
                c.applyx(cacheDesc);
        }
    }

    private void forAllCaches(boolean crd, IgniteInClosureX<GridAffinityAssignmentCache> c) {
        if (crd) {
            for (CacheHolder cache : caches.values())
                c.apply(cache.affinity());
        }
        else {
            for (GridCacheContext cacheCtx : cctx.cacheContexts())
                c.apply(cacheCtx.affinity().affinityCache());
        }
    }

    /**
     * @param fut Exchange future.
     * @param cacheId Cache ID.
     * @throws IgniteCheckedException If failed.
     */
    private void initStartedCache(GridDhtPartitionsExchangeFuture fut, final Integer cacheId)
        throws IgniteCheckedException {
        CacheHolder cache = caches.get(cacheId);

        GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

        if (cache == null) {
            DynamicCacheDescriptor desc = cctx.cache().cacheDescriptor(cacheId);

            cache = cacheCtx != null ? new CacheHolder1(cacheCtx) : new CacheHolder2(desc, fut, null);

            CacheHolder old = caches.put(cacheId, cache);

            assert old == null : old;

            List<List<ClusterNode>> newAff = cache.affinity().calculate(fut.topologyVersion(), fut.discoveryEvent());

            cache.affinity().initialize(fut.topologyVersion(), newAff);
        }
        else if (cache.client() && cacheCtx != null) {
            assert cache.affinity().idealAssignment() != null;

            cache = new CacheHolder1(cacheCtx, (CacheHolder2)cache);

            caches.put(cacheId, cache);
        }
    }

    /**
     * @param crd Coordinator flag.
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     */
    public void initStartedCaches(boolean crd, final GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        if (crd) {
            forAllRegisteredCaches(fut.topologyVersion(), new IgniteInClosureX<DynamicCacheDescriptor>() {
                @Override public void applyx(DynamicCacheDescriptor desc) throws IgniteCheckedException {
                    CacheHolder cache = cache(fut, desc);

                    if (cache.affinity().lastVersion().equals(AffinityTopologyVersion.NONE)) {
                        List<List<ClusterNode>> assignment =
                            cache.affinity().calculate(fut.topologyVersion(), fut.discoveryEvent());

                        cache.affinity().initialize(fut.topologyVersion(), assignment);
                    }
                }
            });
        }
        else {
            forAllCaches(false, new IgniteInClosureX<GridAffinityAssignmentCache>() {
                @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                    if (aff.lastVersion().equals(AffinityTopologyVersion.NONE))
                        initAffinity(aff, fut);
                }
            });
        }
    }

    private void initAffinity(GridAffinityAssignmentCache aff, GridDhtPartitionsExchangeFuture fut)
        throws IgniteCheckedException {
        if (canCalculateAffinity(aff, fut)) {
            List<List<ClusterNode>> assignment = aff.calculate(fut.topologyVersion(), fut.discoveryEvent());

            aff.initialize(fut.topologyVersion(), assignment);
        }
        else {
            GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cctx,
                aff.cacheName(),
                fut.topologyVersion());

            fetchFut.init();

            fetchAffinity(fut, aff, fetchFut);
        }
    }

    /**
     * @param aff Affinity.
     * @return {@code True} if local node can calculate affinity on it's own for this partition map exchange.
     */
    private boolean canCalculateAffinity(GridAffinityAssignmentCache aff, GridDhtPartitionsExchangeFuture fut) {
        // Do not request affinity from remote nodes if affinity function is not centralized.
        if (!aff.centralizedAffinityFunction())
            return true;

        // If local node did not initiate exchange or local node is the only cache node in grid.
        Collection<ClusterNode> affNodes = cctx.discovery().cacheAffinityNodes(aff.cacheName(), fut.topologyVersion());

        return fut.cacheStarted(aff.cacheId()) ||
                !fut.exchangeId().nodeId().equals(cctx.localNodeId()) ||
                (affNodes.size() == 1 && affNodes.contains(cctx.localNode()));
    }

    /**
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     */
    public void onServerJoin(final GridDhtPartitionsExchangeFuture fut, boolean crd) throws IgniteCheckedException {
        assert !fut.discoveryEvent().eventNode().isClient();

        boolean locJoin = fut.discoveryEvent().eventNode().isLocal();

        RebalancingInfo rebalancingInfo = null;

        if (locJoin) {
            if (crd) {
                forAllRegisteredCaches(fut.topologyVersion(), new IgniteInClosureX<DynamicCacheDescriptor>() {
                    @Override public void applyx(DynamicCacheDescriptor cacheDesc) throws IgniteCheckedException {
                        AffinityTopologyVersion topVer = fut.topologyVersion();

                        CacheHolder cache = cache(fut, cacheDesc);

                        List<List<ClusterNode>> newAff = cache.affinity().calculate(topVer, fut.discoveryEvent());

                        cache.affinity().initialize(topVer, newAff);
                    }
                });
            }
            else
                fetchAffinity(fut);
        }
        else
            rebalancingInfo = initAffinityDelayNewPrimary(fut, crd);

        synchronized (mux) {
            affCalcVer = fut.topologyVersion();

            this.rebalancingInfo = rebalancingInfo != null && !rebalancingInfo.empty() ? rebalancingInfo : null;
        }
    }

    /**
     * @param topVer Actual topology version.
     */
    public void cleanUpCache(AffinityTopologyVersion topVer) {
        for (CacheHolder cache : caches.values()) {
            if (cache.client())
                cache.affinity().cleanUpCache(topVer);
        }
    }

    /**
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     */
    private void fetchAffinity(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        AffinityTopologyVersion topVer = fut.topologyVersion();

        List<GridDhtAssignmentFetchFuture> fetchFuts = new ArrayList<>();

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cctx,
                cacheCtx.name(),
                topVer);

            fetchFut.init();

            fetchFuts.add(fetchFut);
        }

        for (int i = 0; i < fetchFuts.size(); i++) {
            GridDhtAssignmentFetchFuture fetchFut = fetchFuts.get(i);

            Integer cacheId = fetchFut.key().get1();

            fetchAffinity(fut, cctx.cacheContext(cacheId).affinity().affinityCache(), fetchFut);
        }
    }

    /**
     * @param fut Exchange future.
     * @param fetchFut Affinity fetch future.
     * @throws IgniteCheckedException If failed.
     */
    private void fetchAffinity(GridDhtPartitionsExchangeFuture fut,
        GridAffinityAssignmentCache affCache,
        GridDhtAssignmentFetchFuture fetchFut)
        throws IgniteCheckedException {
        assert affCache != null;

        AffinityTopologyVersion topVer = fut.topologyVersion();

        GridDhtAffinityAssignmentResponse res = fetchFut.get();

        if (res == null) {
            List<List<ClusterNode>> aff = affCache.calculate(topVer, fut.discoveryEvent());

            affCache.initialize(topVer, aff);
        }
        else {
            List<List<ClusterNode>> idealAff = res.idealAffinityAssignment();

            if (idealAff != null)
                affCache.idealAssignment(idealAff);
            else {
                assert !affCache.centralizedAffinityFunction();

                affCache.calculate(topVer, fut.discoveryEvent());
            }

            List<List<ClusterNode>> aff = res.affinityAssignment();

            assert aff != null;

            affCache.initialize(topVer, aff);
        }
    }

    /**
     * @param fut Exchange future.
     * @param oldest Oldest node flag.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if affinity should be assigned by coordinator.
     */
    public boolean onServerLeft(final GridDhtPartitionsExchangeFuture fut, boolean oldest) throws IgniteCheckedException {
        ClusterNode leftNode = fut.discoveryEvent().eventNode();

        assert !leftNode.isClient() : leftNode;

//        RebalancingInfo rebalancingInfo = null;
//
// TODO: move to initAffinityConsiderState?
//        boolean centralizedAff = false;
//
//        for (CacheHolder cache : caches.values()) {
//            AffinityTopologyVersion affTopVer = cache.affinity().lastVersion();
//
//            assert affTopVer.topologyVersion() > 0 : affTopVer;
//
//            GridAffinityAssignment assignment = cache.affinity().cachedAffinity(affTopVer);
//
//            if (!assignment.primaryPartitions(leftNode.id()).isEmpty()) {
//                centralizedAff = true;
//
//                break;
//            }
//        }
//
//        if (centralizedAff) {
//            for (CacheHolder cache : caches.values()) {
//                assert cache.affinity().idealAssignment() != null : "Previous assignment is not available.";
//
//                cache.affinity().calculate(fut.topologyVersion(), fut.discoveryEvent());
//            }
//        }
//        else
//            rebalancingInfo = initAffinityDelayNewPrimary(fut, oldest);

        forAllCaches(false, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                aff.calculate(fut.topologyVersion(), fut.discoveryEvent());
            }
        });

        synchronized (mux) {
            affCalcVer = fut.topologyVersion();

            this.rebalancingInfo = null;
        }

        return true;
    }

    private void initCoordinatorCaches(final GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        forAllRegisteredCaches(fut.topologyVersion(), new IgniteInClosureX<DynamicCacheDescriptor>() {
            @Override public void applyx(DynamicCacheDescriptor desc) throws IgniteCheckedException {
                if (caches.get(desc.cacheId()) != null)
                    return;

                final Integer cacheId = desc.cacheId();

                GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                CacheHolder cache;

                if (cacheCtx == null) {
                    cctx.io().addHandler(desc.cacheId(), GridDhtAffinityAssignmentResponse.class,
                        new IgniteBiInClosure<UUID, GridDhtAffinityAssignmentResponse>() {
                            @Override public void apply(UUID nodeId, GridDhtAffinityAssignmentResponse res) {
                                processAffinityAssignmentResponse(cacheId, nodeId, res);
                            }
                        }
                    );

                    cache = new CacheHolder2(desc, fut, null);

                    GridAffinityAssignmentCache aff = cache.affinity();

                    // TODO: fetch current
                    GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cctx,
                        aff.cacheName(),
                        fut.topologyVersion());

                    fetchFut.init();

                    fetchAffinity(fut, aff, fetchFut);

                    aff.calculate(fut.topologyVersion(), fut.discoveryEvent());
                }
                else
                    cache = new CacheHolder1(cacheCtx);

                CacheHolder old = caches.put(cache.cacheId(), cache);

                assert old == null : old;
            }
        });
    }

    private CacheHolder cache(GridDhtPartitionsExchangeFuture fut, DynamicCacheDescriptor desc)
        throws IgniteCheckedException {
        final Integer cacheId = desc.cacheId();

        CacheHolder cache = caches.get(cacheId);

        if (cache != null)
            return cache;

        GridCacheContext cacheCtx = cctx.cacheContext(desc.cacheId());

        if (cacheCtx == null) {
            cctx.io().addHandler(cacheId, GridDhtAffinityAssignmentResponse.class,
                new IgniteBiInClosure<UUID, GridDhtAffinityAssignmentResponse>() {
                    @Override public void apply(UUID nodeId, GridDhtAffinityAssignmentResponse res) {
                        processAffinityAssignmentResponse(cacheId, nodeId, res);
                    }
                }
            );

            cache = new CacheHolder2(desc, fut, null);
        }
        else
            cache = new CacheHolder1(cacheCtx);

        CacheHolder old = caches.put(cache.cacheId(), cache);

        assert old == null : old;

        return cache;
    }

    /**
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     * @return
     */
    @Nullable private RebalancingInfo initAffinityDelayNewPrimary(final GridDhtPartitionsExchangeFuture fut, boolean crd)
        throws IgniteCheckedException {
        AffinityTopologyVersion topVer = fut.topologyVersion();

        if (!crd) {
            for (GridCacheContext cacheCtx : cctx.cacheContexts())
                initAffinityDelayNewPrimary(fut, cacheCtx.affinity().affinityCache(), null);

            return null;
        }
        else {
            final RebalancingInfo rebalancingInfo = new RebalancingInfo(topVer);

            forAllRegisteredCaches(topVer, new IgniteInClosureX<DynamicCacheDescriptor>() {
                @Override public void applyx(DynamicCacheDescriptor cacheDesc) throws IgniteCheckedException {
                    CacheHolder cache = cache(fut, cacheDesc);

                    initAffinityDelayNewPrimary(fut, cache.affinity(), rebalancingInfo);
                }
            });

            return rebalancingInfo;
        }
    }

    void initAffinityDelayNewPrimary(GridDhtPartitionsExchangeFuture fut,
        GridAffinityAssignmentCache aff,
        RebalancingInfo rebalanceInfo)
        throws IgniteCheckedException
    {
        AffinityTopologyVersion topVer = fut.topologyVersion();

        AffinityTopologyVersion affTopVer = aff.lastVersion();

        assert affTopVer.topologyVersion() > 0 : affTopVer;

        List<List<ClusterNode>> curAff = aff.assignments(affTopVer);

        assert aff.idealAssignment() != null : "Previous assignment is not available.";

        List<List<ClusterNode>> idealAssignment = aff.calculate(topVer, fut.discoveryEvent());
        List<List<ClusterNode>> newAssignment = null;

        for (int p = 0; p < idealAssignment.size(); p++) {
            List<ClusterNode> newNodes = idealAssignment.get(p);
            List<ClusterNode> curNodes = curAff.get(p);

            ClusterNode curPrimary = curNodes.size() > 0 ? curNodes.get(0) : null;
            ClusterNode newPrimary = newNodes.size() > 0 ? newNodes.get(0) : null;

            if (curPrimary != null && newPrimary != null && !curPrimary.equals(newPrimary)) {
                assert cctx.discovery().node(topVer, curPrimary.id()) != null : curPrimary;

                List<ClusterNode> nodes0 = delayedPrimaryAssignment(aff,
                    p,
                    curPrimary,
                    newNodes,
                    rebalanceInfo);

                if (newAssignment == null)
                    newAssignment = new ArrayList<>(idealAssignment);

                newAssignment.set(p, nodes0);
            }
        }

        if (newAssignment == null)
            newAssignment = idealAssignment;

        aff.initialize(fut.topologyVersion(), newAssignment);
    }

    /**
     * @param aff Cache.
     * @param part Partition.
     * @param curPrimary Current primary.
     * @param newNodes New ideal assignment.
     * @param rebalance Rabalance information holder.
     * @return Assignment.
     */
    private List<ClusterNode> delayedPrimaryAssignment(
        GridAffinityAssignmentCache aff,
        int part,
        ClusterNode curPrimary,
        List<ClusterNode> newNodes,
        RebalancingInfo rebalance) {
        assert curPrimary != null;
        assert !F.isEmpty(newNodes);
        assert !curPrimary.equals(newNodes.get(0));

        List<ClusterNode> nodes0 = new ArrayList<>(newNodes.size() + 1);

        nodes0.add(curPrimary);

        for (int i = 0; i < newNodes.size(); i++) {
            ClusterNode node = newNodes.get(i);

            if (!node.equals(curPrimary))
                nodes0.add(node);
        }

        if (rebalance != null) {
            if (LOG_AFF_CHANGE) {
                String cacheName = aff.cacheName();

                logAffinityChange(log, cacheName, "Delayed primary assignment [cache=" + cacheName +
                    ", part=" + part +
                    ", curPrimary=" + curPrimary.id() +
                    ", newNodes=" + F.nodeIds(newNodes) + ']');
            }

            rebalance.add(aff.cacheId(), part, newNodes.get(0).id(), newNodes);
        }

        return nodes0;
    }

    /**
     * @param fut Exchange future.
     * @return Affinity assignment.
     * @throws IgniteCheckedException If failed.
     */
    public Map<Integer, Map<Integer, List<UUID>>> initAffinityConsiderState(final GridDhtPartitionsExchangeFuture fut)
        throws IgniteCheckedException {
        initCoordinatorCaches(fut);

        AffinityTopologyVersion topVer = fut.topologyVersion();

        final RebalancingInfo rebalancingInfo = new RebalancingInfo(topVer);

        final Collection<ClusterNode> aliveNodes = cctx.discovery().nodes(topVer);

        final Map<Integer, Map<Integer, List<UUID>>> assignment = new HashMap<>();

        forAllRegisteredCaches(topVer, new IgniteInClosureX<DynamicCacheDescriptor>() {
            @Override public void applyx(DynamicCacheDescriptor cacheDesc) throws IgniteCheckedException {
                CacheHolder cache = cache(fut, cacheDesc);

                AffinityTopologyVersion affTopVer = cache.affinity().lastVersion();

                assert affTopVer.topologyVersion() > 0 : affTopVer;

                List<List<ClusterNode>> curAssignment = cache.affinity().assignments(affTopVer);
                List<List<ClusterNode>> newAssignment = cache.affinity().idealAssignment();

                assert newAssignment != null;

                GridDhtPartitionTopology top = cache.topology(fut);

                Map<Integer, List<UUID>> cacheAssignment = new HashMap<>();

                // TODO 10885, add 'boolean top.owner(UUID id)' method.
                for (int p = 0; p < newAssignment.size(); p++) {
                    List<ClusterNode> newNodes = newAssignment.get(p);
                    List<ClusterNode> curNodes = curAssignment.get(p);

                    ClusterNode curPrimary = curNodes.size() > 0 ? curNodes.get(0) : null;
                    ClusterNode newPrimary = newNodes.size() > 0 ? newNodes.get(0) : null;

                    if (curPrimary != null && newPrimary != null) {
                        if (!curPrimary.equals(newPrimary)) {
                            if (aliveNodes.contains(curPrimary)) {
                                if (!top.owners(p).isEmpty()) {
                                    GridDhtPartitionState state = top.partitionState(newPrimary.id(), p);

                                    if (state != GridDhtPartitionState.OWNING) {
                                        newNodes = delayedPrimaryAssignment(cache.affinity(),
                                            p,
                                            curPrimary,
                                            newNodes,
                                            rebalancingInfo);
                                    }
                                }
                            }
                            else {
                                GridDhtPartitionState state = top.partitionState(newPrimary.id(), p);

                                if (state != GridDhtPartitionState.OWNING) {
                                    List<ClusterNode> owners = top.owners(p);

                                    if (!owners.isEmpty()) {
                                        ClusterNode primary = owners.get(0);

                                        newNodes = delayedPrimaryAssignment(cache.affinity(),
                                            p,
                                            primary,
                                            newNodes,
                                            rebalancingInfo);
                                    }
                                }
                            }
                        }
                    }

                    cacheAssignment.put(p, toIds0(newNodes));
                }

                assignment.put(cache.cacheId(), cacheAssignment);
            }
        });

        synchronized (mux) {
            assert affCalcVer.equals(topVer);

            this.rebalancingInfo = !rebalancingInfo.empty() ? rebalancingInfo : null;
        }

        return assignment;
    }

    /**
     *
     */
    abstract class CacheHolder {
        /** */
        protected GridAffinityAssignmentCache aff;

        abstract boolean client();

        abstract int cacheId();

        abstract int partitions();

        abstract String name();

        abstract GridDhtPartitionTopology topology(GridDhtPartitionsExchangeFuture fut);

        GridAffinityAssignmentCache affinity() {
            return aff;
        }
    }

    /**
     *
     */
    class CacheHolder2 extends CacheHolder {
        /** */
        private final int cacheId;

        /** */
        private final String cacheName;

        /**
         * @param cacheDesc Cache descriptor.
         * @throws IgniteCheckedException If failed.
         */
        CacheHolder2(DynamicCacheDescriptor cacheDesc,
            GridDhtPartitionsExchangeFuture fut,
            @Nullable GridAffinityAssignmentCache aff) throws IgniteCheckedException {
            assert cacheDesc != null;
            assert !cctx.kernalContext().clientNode();

            CacheConfiguration ccfg = cacheDesc.cacheConfiguration();

            assert ccfg != null : cacheDesc;

            cacheName = ccfg.getName();
            cacheId = CU.cacheId(ccfg.getName());

            assert !cctx.discovery().cacheAffinityNodes(cacheName, fut.topologyVersion()).contains(cctx.localNode());

            AffinityFunction affFunc = cctx.cache().clone(ccfg.getAffinity());

            cctx.kernalContext().resource().injectGeneric(affFunc);
            cctx.kernalContext().resource().injectCacheName(affFunc, cacheName);
            U.startLifecycleAware(F.asList(affFunc));

            this.aff = new GridAffinityAssignmentCache(cctx.kernalContext(),
                cacheName,
                affFunc,
                ccfg.getBackups());

            if (aff != null)
                this.aff.init(aff);
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return aff.partitions();
        }

        /** {@inheritDoc} */
        @Override public boolean client() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public int cacheId() {
            return cacheId;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return cacheName;
        }

        /** {@inheritDoc} */
        @Override public GridDhtPartitionTopology topology(GridDhtPartitionsExchangeFuture fut) {
            return cctx.exchange().clientTopology(cacheId, fut);
        }
    }

    /**
     *
     */
    class CacheHolder1 extends CacheHolder {
        /** */
        private final GridCacheContext cctx;

        /**
         * @param cctx Cache context.
         */
        public CacheHolder1(GridCacheContext cctx) {
            this.cctx = cctx;

            aff = cctx.affinity().affinityCache();
        }

        /**
         * @param cctx Cache context.
         */
        public CacheHolder1(GridCacheContext cctx, CacheHolder2 cacheHolder)
            throws IgniteInterruptedCheckedException {
            this(cctx);

            aff.init(cacheHolder.affinity());
        }

        /** {@inheritDoc} */
        @Override public boolean client() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return cctx.affinity().partitions();
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return cctx.name();
        }

        /** {@inheritDoc} */
        @Override public int cacheId() {
            return cctx.cacheId();
        }

        /** {@inheritDoc} */
        @Override public GridDhtPartitionTopology topology(GridDhtPartitionsExchangeFuture fut) {
            return cctx.topology();
        }
    }

    /**
     *
     */
    static class RebalancingInfo {
        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, Map<Integer, UUID>> waitCaches;

        /** */
        private Map<Integer, Map<Integer, List<ClusterNode>>> assignments;

        /**
         * @param topVer Topology version.
         */
        RebalancingInfo(AffinityTopologyVersion topVer) {
            this.topVer = topVer;
        }

        /**
         * @return {@code True} if there are partitions waiting for rebalancing.
         */
        boolean empty() {
            if (waitCaches != null) {
                assert !waitCaches.isEmpty();
                assert waitCaches.size() == assignments.size();

                return false;
            }

            return true;
        }

        /**
         * @param cacheId Cache ID.
         * @param part Partition.
         * @param waitNode Node rebalancing data.
         * @param assignment New assignment.
         */
        void add(Integer cacheId, Integer part, UUID waitNode, List<ClusterNode> assignment) {
            assert !F.isEmpty(assignment) : assignment;

            if (waitCaches == null) {
                waitCaches = new HashMap<>();
                assignments = new HashMap<>();
            }

            Map<Integer, UUID> cacheWaitParts = waitCaches.get(cacheId);

            if (cacheWaitParts == null)
                waitCaches.put(cacheId, cacheWaitParts = new HashMap<>());

            cacheWaitParts.put(part, waitNode);

            Map<Integer, List<ClusterNode>> cacheAssignment = assignments.get(cacheId);

            if (cacheAssignment == null)
                assignments.put(cacheId, cacheAssignment = new HashMap<>());

            cacheAssignment.put(part, assignment);
        }
    }
}
