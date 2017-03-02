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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAssignmentFetchFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 *
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class CacheAffinitySharedManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /** */
    public static final IgniteProductVersion LATE_AFF_ASSIGN_SINCE = IgniteProductVersion.fromString("1.6.0");

    /** Late affinity assignment flag. */
    private boolean lateAffAssign;

    /** Affinity information for all started caches (initialized on coordinator). */
    private ConcurrentMap<Integer, CacheHolder> caches = new ConcurrentHashMap<>();

    /** Last topology version when affinity was calculated (updated from exchange thread). */
    private AffinityTopologyVersion affCalcVer;

    /** Topology version which requires affinity re-calculation (set from discovery thread). */
    private AffinityTopologyVersion lastAffVer;

    /** Registered caches (updated from exchange thread). */
    private final Map<Integer, DynamicCacheDescriptor> registeredCaches = new HashMap<>();

    /** */
    private WaitRebalanceInfo waitInfo;

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

        lateAffAssign = cctx.kernalContext().config().isLateAffinityAssignment();

        cctx.kernalContext().event().addLocalEventListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /**
     * Callback invoked from discovery thread when discovery message is received.
     *
     * @param type Event type.
     * @param node Event node.
     * @param topVer Topology version.
     */
    void onDiscoveryEvent(int type, ClusterNode node, AffinityTopologyVersion topVer) {
        if (type == EVT_NODE_JOINED && node.isLocal()) {
            // Clean-up in case of client reconnect.
            registeredCaches.clear();

            affCalcVer = null;

            lastAffVer = null;

            for (DynamicCacheDescriptor desc : cctx.cache().cacheDescriptors())
                registeredCaches.put(desc.cacheId(), desc);
        }

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
    boolean onCustomEvent(CacheAffinityChangeMessage msg) {
        assert lateAffAssign : msg;

        if (msg.exchangeId() != null) {
            if (log.isDebugEnabled()) {
                log.debug("Need process affinity change message [lastAffVer=" + lastAffVer +
                    ", msgExchId=" + msg.exchangeId() +
                    ", msgVer=" + msg.topologyVersion() + ']');
            }

            return false;
        }

        // Skip message if affinity was already recalculated.
        boolean exchangeNeeded = lastAffVer == null || lastAffVer.equals(msg.topologyVersion());

        msg.exchangeNeeded(exchangeNeeded);

        if (exchangeNeeded) {
            if (log.isDebugEnabled()) {
                log.debug("Need process affinity change message [lastAffVer=" + lastAffVer +
                    ", msgExchId=" + msg.exchangeId() +
                    ", msgVer=" + msg.topologyVersion() +']');
            }
        }
        else {
            if (log.isDebugEnabled()) {
                log.debug("Ignore affinity change message [lastAffVer=" + lastAffVer +
                    ", msgExchId=" + msg.exchangeId() +
                    ", msgVer=" + msg.topologyVersion() +']');
            }
        }

        return exchangeNeeded;
    }

    /**
     * @param topVer Expected topology version.
     */
    private void onCacheStopped(AffinityTopologyVersion topVer) {
        CacheAffinityChangeMessage msg = null;

        synchronized (mux) {
            if (waitInfo == null || !waitInfo.topVer.equals(topVer))
                return;

            if (waitInfo.waitCaches.isEmpty()) {
                msg = affinityChangeMessage(waitInfo);

                waitInfo = null;
            }
        }

        try {
            if (msg != null)
                cctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send affinity change message.", e);
        }
    }

    /**
     * @param top Topology.
     * @param checkCacheId Cache ID.
     */
    void checkRebalanceState(GridDhtPartitionTopology top, Integer checkCacheId) {
        if (!lateAffAssign)
            return;

        CacheAffinityChangeMessage msg = null;

        synchronized (mux) {
            if (waitInfo == null)
                return;

            assert affCalcVer != null;
            assert affCalcVer.equals(waitInfo.topVer) : "Invalid affinity version [calcVer=" + affCalcVer +
                ", waitVer=" + waitInfo.topVer + ']';

            Map<Integer, UUID> partWait = waitInfo.waitCaches.get(checkCacheId);

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
                }

                if (rebalanced) {
                    waitInfo.waitCaches.remove(checkCacheId);

                    if (waitInfo.waitCaches.isEmpty()) {
                        msg = affinityChangeMessage(waitInfo);

                        waitInfo = null;
                    }
                }
            }
        }

        try {
            if (msg != null)
                cctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send affinity change message.", e);
        }
    }

    /**
     * @param waitInfo Cache rebalance information.
     * @return Message.
     */
    @Nullable private CacheAffinityChangeMessage affinityChangeMessage(WaitRebalanceInfo waitInfo) {
        if (waitInfo.assignments.isEmpty()) // Possible if all awaited caches were destroyed.
            return null;

        Map<Integer, Map<Integer, List<UUID>>> assignmentsChange = U.newHashMap(waitInfo.assignments.size());

        for (Map.Entry<Integer, Map<Integer, List<ClusterNode>>> e : waitInfo.assignments.entrySet()) {
            Integer cacheId = e.getKey();

            Map<Integer, List<ClusterNode>> assignment = e.getValue();

            Map<Integer, List<UUID>> assignment0 = U.newHashMap(assignment.size());

            for (Map.Entry<Integer, List<ClusterNode>> e0 : assignment.entrySet())
                assignment0.put(e0.getKey(), toIds0(e0.getValue()));

            assignmentsChange.put(cacheId, assignment0);
        }

        return new CacheAffinityChangeMessage(waitInfo.topVer, assignmentsChange, waitInfo.deploymentIds);
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
     * Called on exchange initiated for cache start/stop request.
     *
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @param reqs Cache change requests.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if client-only exchange is needed.
     */
    public boolean onCacheChangeRequest(final GridDhtPartitionsExchangeFuture fut,
        boolean crd,
        Collection<DynamicCacheChangeRequest> reqs)
        throws IgniteCheckedException {
        assert !F.isEmpty(reqs) : fut;

        for (DynamicCacheChangeRequest req : reqs) {
            Integer cacheId = CU.cacheId(req.cacheName());

            if (req.stop()) {
                DynamicCacheDescriptor desc = registeredCaches.remove(cacheId);

                assert desc != null : cacheId;
            }
            else if (req.start() && !req.clientStartOnly()) {
                DynamicCacheDescriptor desc = new DynamicCacheDescriptor(cctx.kernalContext(),
                    req.startCacheConfiguration(),
                    req.cacheType(),
                    false,
                    req.deploymentId());

                DynamicCacheDescriptor old = registeredCaches.put(cacheId, desc);

                assert old == null : old;
            }
        }

        boolean clientOnly = true;

        // Affinity did not change for existing caches.
        forAllCaches(crd && lateAffAssign, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                if (fut.stopping(aff.cacheId()))
                    return;

                aff.clientEventTopologyChange(fut.discoveryEvent(), fut.topologyVersion());
            }
        });

        Set<Integer> stoppedCaches = null;

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

                if (!crd || !lateAffAssign) {
                    GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                    if (cacheCtx != null && !cacheCtx.isLocal()) {
                        boolean clientCacheStarted =
                            req.clientStartOnly() && req.initiatingNodeId().equals(cctx.localNodeId());

                        if (clientCacheStarted)
                            initAffinity(cacheCtx.affinity().affinityCache(), fut, lateAffAssign);
                        else if (!req.clientStartOnly()) {
                            assert fut.topologyVersion().equals(cacheCtx.startTopologyVersion());

                            GridAffinityAssignmentCache aff = cacheCtx.affinity().affinityCache();

                            assert aff.lastVersion().equals(AffinityTopologyVersion.NONE) : aff.lastVersion();

                            List<List<ClusterNode>> assignment = aff.calculate(fut.topologyVersion(),
                                fut.discoveryEvent());

                            aff.initialize(fut.topologyVersion(), assignment);
                        }
                    }
                }
                else
                    initStartedCacheOnCoordinator(fut, cacheId);
            }
            else if (req.stop() || req.close()) {
                cctx.cache().blockGateway(req);

                if (crd) {
                    boolean rmvCache = false;

                    if (req.close() && req.initiatingNodeId().equals(cctx.localNodeId())) {
                        GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                        rmvCache = cacheCtx != null && !cacheCtx.affinityNode();
                    }
                    else if (req.stop())
                        rmvCache = true;

                    if (rmvCache) {
                        CacheHolder cache = caches.remove(cacheId);

                        if (cache != null) {
                            if (!req.stop()) {
                                assert !cache.client();

                                cache = CacheHolder2.create(cctx,
                                    cctx.cache().cacheDescriptor(cacheId),
                                    fut,
                                    cache.affinity());

                                caches.put(cacheId, cache);
                            }
                            else {
                                if (stoppedCaches == null)
                                    stoppedCaches = new HashSet<>();

                                stoppedCaches.add(cache.cacheId());

                                cctx.io().removeHandler(cacheId, GridDhtAffinityAssignmentResponse.class);
                            }
                        }
                    }
                }
            }
        }

        if (stoppedCaches != null) {
            boolean notify = false;

            synchronized (mux) {
                if (waitInfo != null) {
                    for (Integer cacheId : stoppedCaches) {
                        boolean rmv =  waitInfo.waitCaches.remove(cacheId) != null;

                        if (rmv) {
                            notify = true;

                            waitInfo.assignments.remove(cacheId);
                        }
                    }
                }
            }

            if (notify) {
                final AffinityTopologyVersion topVer = affCalcVer;

                cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                    @Override public void run() {
                        onCacheStopped(topVer);
                    }
                });
            }
        }

        return clientOnly;
    }

    /**
     * Called when received {@link CacheAffinityChangeMessage} which should complete exchange.
     *
     * @param exchFut Exchange future.
     * @param crd Coordinator flag.
     * @param msg Affinity change message.
     */
    public void onExchangeChangeAffinityMessage(GridDhtPartitionsExchangeFuture exchFut,
        boolean crd,
        CacheAffinityChangeMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("Process exchange affinity change message [exchVer=" + exchFut.topologyVersion() +
                ", msg=" + msg + ']');
        }

        assert exchFut.exchangeId().equals(msg.exchangeId()) : msg;

        final AffinityTopologyVersion topVer = exchFut.topologyVersion();

        final Map<Integer, Map<Integer, List<UUID>>> assignment = msg.assignmentChange();

        assert assignment != null;

        final Map<Object, List<List<ClusterNode>>> affCache = new HashMap<>();

        forAllCaches(crd, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                List<List<ClusterNode>> idealAssignment = aff.idealAssignment();

                assert idealAssignment != null;

                Map<Integer, List<UUID>> cacheAssignment = assignment.get(aff.cacheId());

                List<List<ClusterNode>> newAssignment;

                if (cacheAssignment != null) {
                    newAssignment = new ArrayList<>(idealAssignment);

                    for (Map.Entry<Integer, List<UUID>> e : cacheAssignment.entrySet())
                        newAssignment.set(e.getKey(), toNodes(topVer, e.getValue()));
                }
                else
                    newAssignment = idealAssignment;

                aff.initialize(topVer, cachedAssignment(aff, newAssignment, affCache));
            }
        });
    }

    /**
     * Called on exchange initiated by {@link CacheAffinityChangeMessage} which sent after rebalance finished.
     *
     * @param exchFut Exchange future.
     * @param crd Coordinator flag.
     * @param msg Message.
     * @throws IgniteCheckedException If failed.
     */
    public void onChangeAffinityMessage(final GridDhtPartitionsExchangeFuture exchFut,
        boolean crd,
        final CacheAffinityChangeMessage msg)
        throws IgniteCheckedException {
        assert affCalcVer != null || cctx.kernalContext().clientNode();
        assert msg.topologyVersion() != null && msg.exchangeId() == null: msg;
        assert affCalcVer == null || affCalcVer.equals(msg.topologyVersion());

        final AffinityTopologyVersion topVer = exchFut.topologyVersion();

        if (log.isDebugEnabled()) {
            log.debug("Process affinity change message [exchVer=" + exchFut.topologyVersion() +
                ", affCalcVer=" + affCalcVer +
                ", msgVer=" + msg.topologyVersion() +']');
        }

        final Map<Integer, Map<Integer, List<UUID>>> affChange = msg.assignmentChange();

        assert !F.isEmpty(affChange) : msg;

        final Map<Integer, IgniteUuid> deploymentIds = msg.cacheDeploymentIds();

        final Map<Object, List<List<ClusterNode>>> affCache = new HashMap<>();

        forAllCaches(crd, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                AffinityTopologyVersion affTopVer = aff.lastVersion();

                assert affTopVer.topologyVersion() > 0 : affTopVer;

                IgniteUuid deploymentId = registeredCaches.get(aff.cacheId()).deploymentId();

                if (!deploymentId.equals(deploymentIds.get(aff.cacheId()))) {
                    aff.clientEventTopologyChange(exchFut.discoveryEvent(), topVer);

                    return;
                }

                Map<Integer, List<UUID>> change = affChange.get(aff.cacheId());

                if (change != null) {
                    assert !change.isEmpty() : msg;

                    List<List<ClusterNode>> curAff = aff.assignments(affTopVer);

                    List<List<ClusterNode>> assignment = new ArrayList<>(curAff);

                    for (Map.Entry<Integer, List<UUID>> e : change.entrySet()) {
                        Integer part = e.getKey();

                        List<ClusterNode> nodes = toNodes(topVer, e.getValue());

                        assert !nodes.equals(assignment.get(part)) : "Assignment did not change " +
                            "[cache=" + aff.cacheName() +
                            ", part=" + part +
                            ", cur=" + F.nodeIds(assignment.get(part)) +
                            ", new=" + F.nodeIds(nodes) +
                            ", exchVer=" + exchFut.topologyVersion() +
                            ", msgVer=" + msg.topologyVersion() +
                            ']';

                        assignment.set(part, nodes);
                    }

                    aff.initialize(topVer, cachedAssignment(aff, assignment, affCache));
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
     * Called on exchange initiated by client node join/fail.
     *
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     */
    public void onClientEvent(final GridDhtPartitionsExchangeFuture fut, boolean crd) throws IgniteCheckedException {
        boolean locJoin = fut.discoveryEvent().eventNode().isLocal();

        if (lateAffAssign) {
            if (!locJoin) {
                forAllCaches(crd, new IgniteInClosureX<GridAffinityAssignmentCache>() {
                    @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                        AffinityTopologyVersion topVer = fut.topologyVersion();

                        aff.clientEventTopologyChange(fut.discoveryEvent(), topVer);
                    }
                });
            }
            else
                fetchAffinityOnJoin(fut);
        }
        else {
            if (!locJoin) {
                forAllCaches(false, new IgniteInClosureX<GridAffinityAssignmentCache>() {
                    @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                        AffinityTopologyVersion topVer = fut.topologyVersion();

                        aff.clientEventTopologyChange(fut.discoveryEvent(), topVer);
                    }
                });
            }
            else
                initCachesAffinity(fut);
        }
    }

    /**
     * @param fut Future to add.
     */
    public void addDhtAssignmentFetchFuture(GridDhtAssignmentFetchFuture fut) {
        GridDhtAssignmentFetchFuture old = pendingAssignmentFetchFuts.putIfAbsent(fut.key(), fut);

        assert old == null : "More than one thread is trying to fetch partition assignments [fut=" + fut +
            ", allFuts=" + pendingAssignmentFetchFuts + ']';
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
            if (fut.key().get1().equals(cacheId)) {
                fut.onResponse(nodeId, res);

                break;
            }
        }
    }

    /**
     * @param c Cache closure.
     * @throws IgniteCheckedException If failed
     */
    private void forAllRegisteredCaches(IgniteInClosureX<DynamicCacheDescriptor> c) throws IgniteCheckedException {
        assert lateAffAssign;

        for (DynamicCacheDescriptor cacheDesc : registeredCaches.values()) {
            if (cacheDesc.cacheConfiguration().getCacheMode() == LOCAL)
                continue;

            c.applyx(cacheDesc);
        }
    }

    /**
     * @param crd Coordinator flag.
     * @param c Closure.
     */
    private void forAllCaches(boolean crd, IgniteInClosureX<GridAffinityAssignmentCache> c) {
        if (crd) {
            for (CacheHolder cache : caches.values())
                c.apply(cache.affinity());
        }
        else {
            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                if (cacheCtx.isLocal())
                    continue;

                c.apply(cacheCtx.affinity().affinityCache());
            }
        }
    }

    /**
     * @param fut Exchange future.
     * @param cacheId Cache ID.
     * @throws IgniteCheckedException If failed.
     */
    private void initStartedCacheOnCoordinator(GridDhtPartitionsExchangeFuture fut, final Integer cacheId)
        throws IgniteCheckedException {
        CacheHolder cache = caches.get(cacheId);

        GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

        if (cache == null) {
            DynamicCacheDescriptor desc = cctx.cache().cacheDescriptor(cacheId);

            assert desc != null : cacheId;

            if (desc.cacheConfiguration().getCacheMode() == LOCAL)
                return;

            cache = cacheCtx != null ? new CacheHolder1(cacheCtx, null) : CacheHolder2.create(cctx, desc, fut, null);

            CacheHolder old = caches.put(cacheId, cache);

            assert old == null : old;

            List<List<ClusterNode>> newAff = cache.affinity().calculate(fut.topologyVersion(), fut.discoveryEvent());

            cache.affinity().initialize(fut.topologyVersion(), newAff);
        }
        else if (cache.client() && cacheCtx != null) {
            assert cache.affinity().idealAssignment() != null;

            cache = new CacheHolder1(cacheCtx, cache.affinity());

            caches.put(cacheId, cache);
        }
    }

    /**
     * Initialized affinity started on this exchange.
     *
     * @param crd Coordinator flag.
     * @param fut Exchange future.
     * @param descs Cache descriptors.
     * @throws IgniteCheckedException If failed.
     */
    public void initStartedCaches(boolean crd,
        final GridDhtPartitionsExchangeFuture fut,
        @Nullable Collection<DynamicCacheDescriptor> descs) throws IgniteCheckedException {
        if (descs != null) {
            for (DynamicCacheDescriptor desc : descs) {
                if (!registeredCaches.containsKey(desc.cacheId()))
                    registeredCaches.put(desc.cacheId(), desc);
            }
        }

        if (crd && lateAffAssign) {
            forAllRegisteredCaches(new IgniteInClosureX<DynamicCacheDescriptor>() {
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
                        initAffinity(aff, fut, false);
                }
            });
        }
    }

    /**
     * @param aff Affinity.
     * @param fut Exchange future.
     * @param fetch Force fetch flag.
     * @throws IgniteCheckedException If failed.
     */
    private void initAffinity(GridAffinityAssignmentCache aff, GridDhtPartitionsExchangeFuture fut, boolean fetch)
        throws IgniteCheckedException {
        if (!fetch && canCalculateAffinity(aff, fut)) {
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
     * @param fut Exchange future.
     * @return {@code True} if local node can calculate affinity on it's own for this partition map exchange.
     */
    private boolean canCalculateAffinity(GridAffinityAssignmentCache aff, GridDhtPartitionsExchangeFuture fut) {
        // Do not request affinity from remote nodes if affinity function is not centralized.
        if (!aff.centralizedAffinityFunction())
            return true;

        // If local node did not initiate exchange or local node is the only cache node in grid.
        Collection<ClusterNode> affNodes = cctx.discovery().cacheAffinityNodes(aff.cacheId(), fut.topologyVersion());

        DynamicCacheDescriptor cacheDesc = registeredCaches.get(aff.cacheId());

        assert cacheDesc != null : aff.cacheName();

        return fut.cacheStarted(aff.cacheId()) ||
            !fut.exchangeId().nodeId().equals(cctx.localNodeId()) ||
            cctx.localNodeId().equals(cacheDesc.receivedFrom()) ||
            (affNodes.size() == 1 && affNodes.contains(cctx.localNode()));
    }

    /**
     * Called on exchange initiated by server node join.
     *
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     */
    public void onServerJoin(final GridDhtPartitionsExchangeFuture fut, boolean crd) throws IgniteCheckedException {
        assert !fut.discoveryEvent().eventNode().isClient();

        boolean locJoin = fut.discoveryEvent().eventNode().isLocal();

        WaitRebalanceInfo waitRebalanceInfo = null;

        if (lateAffAssign) {
            if (locJoin) {
                if (crd) {
                    forAllRegisteredCaches(new IgniteInClosureX<DynamicCacheDescriptor>() {
                        @Override public void applyx(DynamicCacheDescriptor cacheDesc) throws IgniteCheckedException {
                            AffinityTopologyVersion topVer = fut.topologyVersion();

                            CacheHolder cache = cache(fut, cacheDesc);

                            List<List<ClusterNode>> newAff = cache.affinity().calculate(topVer, fut.discoveryEvent());

                            cache.affinity().initialize(topVer, newAff);
                        }
                    });
                }
                else
                    fetchAffinityOnJoin(fut);
            }
            else
                waitRebalanceInfo = initAffinityOnNodeJoin(fut, crd);
        }
        else
            initCachesAffinity(fut);

        synchronized (mux) {
            affCalcVer = fut.topologyVersion();

            this.waitInfo = waitRebalanceInfo != null && !waitRebalanceInfo.empty() ? waitRebalanceInfo : null;

            WaitRebalanceInfo info = this.waitInfo;

            if (crd && lateAffAssign) {
                if (log.isDebugEnabled()) {
                    log.debug("Computed new affinity after node join [topVer=" + fut.topologyVersion() +
                        ", waitCaches=" + (info != null ? cacheNames(info.waitCaches.keySet()) : null) + ']');
                }
            }
        }
    }

    /**
     * @param cacheIds Cache IDs.
     * @return Cache names.
     */
    private String cacheNames(Collection<Integer> cacheIds) {
        StringBuilder names = new StringBuilder();

        for (Integer cacheId : cacheIds) {
            String name = registeredCaches.get(cacheId).cacheConfiguration().getName();

            if (names.length() != 0)
                names.append(", ");

            names.append(name);
        }

        return names.toString();
    }

    /**
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     */
    private void fetchAffinityOnJoin(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        AffinityTopologyVersion topVer = fut.topologyVersion();

        List<GridDhtAssignmentFetchFuture> fetchFuts = new ArrayList<>();

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal())
                continue;

            DynamicCacheDescriptor cacheDesc = registeredCaches.get(cacheCtx.cacheId());

            if (cctx.localNodeId().equals(cacheDesc.receivedFrom())) {
                List<List<ClusterNode>> assignment =
                    cacheCtx.affinity().affinityCache().calculate(fut.topologyVersion(), fut.discoveryEvent());

                cacheCtx.affinity().affinityCache().initialize(fut.topologyVersion(), assignment);
            }
            else {
                GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cctx,
                    cacheCtx.name(),
                    topVer);

                fetchFut.init();

                fetchFuts.add(fetchFut);
            }
        }

        for (int i = 0; i < fetchFuts.size(); i++) {
            GridDhtAssignmentFetchFuture fetchFut = fetchFuts.get(i);

            Integer cacheId = fetchFut.key().get1();

            fetchAffinity(fut, cctx.cacheContext(cacheId).affinity().affinityCache(), fetchFut);
        }
    }

    /**
     * @param fut Exchange future.
     * @param affCache Affinity.
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
            List<List<ClusterNode>> idealAff = res.idealAffinityAssignment(cctx.discovery());

            if (idealAff != null)
                affCache.idealAssignment(idealAff);
            else {
                assert !affCache.centralizedAffinityFunction() || !lateAffAssign;

                affCache.calculate(topVer, fut.discoveryEvent());
            }

            List<List<ClusterNode>> aff = res.affinityAssignment(cctx.discovery());

            assert aff != null : res;

            affCache.initialize(topVer, aff);
        }
    }

    /**
     * Called on exchange initiated by server node leave.
     *
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if affinity should be assigned by coordinator.
     */
    public boolean onServerLeft(final GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        ClusterNode leftNode = fut.discoveryEvent().eventNode();

        assert !leftNode.isClient() : leftNode;

        boolean centralizedAff;

        if (lateAffAssign) {
            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                if (cacheCtx.isLocal())
                    continue;

                cacheCtx.affinity().affinityCache().calculate(fut.topologyVersion(), fut.discoveryEvent());
            }

            centralizedAff = true;
        }
        else {
            initCachesAffinity(fut);

            centralizedAff = false;
        }

        synchronized (mux) {
            affCalcVer = fut.topologyVersion();

            this.waitInfo = null;
        }

        return centralizedAff;
    }

    /**
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     */
    private void initCachesAffinity(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        assert !lateAffAssign;

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal())
                continue;

            initAffinity(cacheCtx.affinity().affinityCache(), fut, false);
        }
    }

    /**
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     * @return Future completed when caches initialization is done.
     */
    private IgniteInternalFuture<?> initCoordinatorCaches(final GridDhtPartitionsExchangeFuture fut)
        throws IgniteCheckedException {
        final List<IgniteInternalFuture<AffinityTopologyVersion>> futs = new ArrayList<>();

        forAllRegisteredCaches(new IgniteInClosureX<DynamicCacheDescriptor>() {
            @Override public void applyx(DynamicCacheDescriptor desc) throws IgniteCheckedException {
                CacheHolder cache = caches.get(desc.cacheId());

                if (cache != null) {
                    if (cache.client())
                        cache.affinity().calculate(fut.topologyVersion(), fut.discoveryEvent());

                    return;
                }

                final Integer cacheId = desc.cacheId();

                GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                if (cacheCtx == null) {
                    cctx.io().addHandler(desc.cacheId(), GridDhtAffinityAssignmentResponse.class,
                        new IgniteBiInClosure<UUID, GridDhtAffinityAssignmentResponse>() {
                            @Override public void apply(UUID nodeId, GridDhtAffinityAssignmentResponse res) {
                                processAffinityAssignmentResponse(cacheId, nodeId, res);
                            }
                        }
                    );

                    cache = CacheHolder2.create(cctx, desc, fut, null);

                    final GridAffinityAssignmentCache aff = cache.affinity();

                    List<GridDhtPartitionsExchangeFuture> exchFuts = cctx.exchange().exchangeFutures();

                    int idx = exchFuts.indexOf(fut);

                    assert idx >= 0 && idx < exchFuts.size() - 1 : "Invalid exchange futures state [cur=" + idx +
                        ", total=" + exchFuts.size() + ']';

                    final GridDhtPartitionsExchangeFuture prev = exchFuts.get(idx + 1);
                    if (log.isDebugEnabled()) {
                        log.debug("Need initialize affinity on coordinator [" +
                            "cache=" + desc.cacheConfiguration().getName() +
                            "prevAff=" + prev.topologyVersion() + ']');
                    }

                    assert prev.topologyVersion().compareTo(fut.topologyVersion()) < 0 : prev;

                    GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cctx,
                        aff.cacheName(),
                        prev.topologyVersion());

                    fetchFut.init();

                    final GridFutureAdapter<AffinityTopologyVersion> affFut = new GridFutureAdapter<>();

                    fetchFut.listen(new IgniteInClosureX<IgniteInternalFuture<GridDhtAffinityAssignmentResponse>>() {
                        @Override public void applyx(IgniteInternalFuture<GridDhtAffinityAssignmentResponse> fetchFut)
                            throws IgniteCheckedException {
                            fetchAffinity(prev, aff, (GridDhtAssignmentFetchFuture)fetchFut);

                            aff.calculate(fut.topologyVersion(), fut.discoveryEvent());

                            affFut.onDone(fut.topologyVersion());
                        }
                    });

                    futs.add(affFut);
                }
                else
                    cache = new CacheHolder1(cacheCtx, null);

                CacheHolder old = caches.put(cache.cacheId(), cache);

                assert old == null : old;
            }
        });

        if (!futs.isEmpty()) {
            GridCompoundFuture<AffinityTopologyVersion, ?> affFut = new GridCompoundFuture<>();

            for (IgniteInternalFuture<AffinityTopologyVersion> f : futs)
                affFut.add(f);

            affFut.markInitialized();

            return affFut;
        }

        return null;
    }

    /**
     * @param fut Exchange future.
     * @param desc Cache descriptor.
     * @return Cache holder.
     * @throws IgniteCheckedException If failed.
     */
    private CacheHolder cache(GridDhtPartitionsExchangeFuture fut, DynamicCacheDescriptor desc)
        throws IgniteCheckedException {
        assert lateAffAssign;

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

            cache = CacheHolder2.create(cctx, desc, fut, null);
        }
        else
            cache = new CacheHolder1(cacheCtx, null);

        CacheHolder old = caches.put(cache.cacheId(), cache);

        assert old == null : old;

        return cache;
    }

    /**
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     * @return Rabalance info.
     */
    @Nullable private WaitRebalanceInfo initAffinityOnNodeJoin(final GridDhtPartitionsExchangeFuture fut, boolean crd)
        throws IgniteCheckedException {
        AffinityTopologyVersion topVer = fut.topologyVersion();

        final Map<Object, List<List<ClusterNode>>> affCache = new HashMap<>();

        if (!crd) {
            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                if (cacheCtx.isLocal())
                    continue;

                boolean latePrimary = cacheCtx.rebalanceEnabled();

                initAffinityOnNodeJoin(fut, cacheCtx.affinity().affinityCache(), null, latePrimary, affCache);
            }

            return null;
        }
        else {
            final WaitRebalanceInfo waitRebalanceInfo = new WaitRebalanceInfo(topVer);

            forAllRegisteredCaches(new IgniteInClosureX<DynamicCacheDescriptor>() {
                @Override public void applyx(DynamicCacheDescriptor cacheDesc) throws IgniteCheckedException {
                    CacheHolder cache = cache(fut, cacheDesc);

                    boolean latePrimary = cache.rebalanceEnabled;

                    initAffinityOnNodeJoin(fut, cache.affinity(), waitRebalanceInfo, latePrimary, affCache);
                }
            });

            return waitRebalanceInfo;
        }
    }

    /**
     * @param fut Exchange future.
     * @param aff Affinity.
     * @param rebalanceInfo Rebalance information.
     * @param latePrimary If {@code true} delays primary assignment if it is not owner.
     * @param affCache Already calculated assignments (to reduce data stored in history).
     * @throws IgniteCheckedException If failed.
     */
    private void initAffinityOnNodeJoin(GridDhtPartitionsExchangeFuture fut,
        GridAffinityAssignmentCache aff,
        WaitRebalanceInfo rebalanceInfo,
        boolean latePrimary,
        Map<Object, List<List<ClusterNode>>> affCache)
        throws IgniteCheckedException
    {
        assert lateAffAssign;

        AffinityTopologyVersion topVer = fut.topologyVersion();

        AffinityTopologyVersion affTopVer = aff.lastVersion();

        assert affTopVer.topologyVersion() > 0 : "Affinity is not initialized [cache=" + aff.cacheName() +
            ", topVer=" + affTopVer + ", node=" + cctx.localNodeId() + ']';

        List<List<ClusterNode>> curAff = aff.assignments(affTopVer);

        assert aff.idealAssignment() != null : "Previous assignment is not available.";

        List<List<ClusterNode>> idealAssignment = aff.calculate(topVer, fut.discoveryEvent());
        List<List<ClusterNode>> newAssignment = null;

        if (latePrimary) {
            for (int p = 0; p < idealAssignment.size(); p++) {
                List<ClusterNode> newNodes = idealAssignment.get(p);
                List<ClusterNode> curNodes = curAff.get(p);

                ClusterNode curPrimary = curNodes.size() > 0 ? curNodes.get(0) : null;
                ClusterNode newPrimary = newNodes.size() > 0 ? newNodes.get(0) : null;

                if (curPrimary != null && newPrimary != null && !curPrimary.equals(newPrimary)) {
                    assert cctx.discovery().node(topVer, curPrimary.id()) != null : curPrimary;

                    List<ClusterNode> nodes0 = latePrimaryAssignment(aff,
                        p,
                        curPrimary,
                        newNodes,
                        rebalanceInfo);

                    if (newAssignment == null)
                        newAssignment = new ArrayList<>(idealAssignment);

                    newAssignment.set(p, nodes0);
                }
            }
        }

        if (newAssignment == null)
            newAssignment = idealAssignment;

        aff.initialize(fut.topologyVersion(), cachedAssignment(aff, newAssignment, affCache));
    }

    /**
     * @param aff Assignment cache.
     * @param assign Assignment.
     * @param affCache Assignments already calculated for other caches.
     * @return Assignment.
     */
    private List<List<ClusterNode>> cachedAssignment(GridAffinityAssignmentCache aff,
        List<List<ClusterNode>> assign,
        Map<Object, List<List<ClusterNode>>> affCache) {
        List<List<ClusterNode>> assign0 = affCache.get(aff.similarAffinityKey());

        if (assign0 != null && assign0.equals(assign))
            assign = assign0;
        else
            affCache.put(aff.similarAffinityKey(), assign);

        return assign;
    }

    /**
     * @param aff Cache.
     * @param part Partition.
     * @param curPrimary Current primary.
     * @param newNodes New ideal assignment.
     * @param rebalance Rabalance information holder.
     * @return Assignment.
     */
    private List<ClusterNode> latePrimaryAssignment(
        GridAffinityAssignmentCache aff,
        int part,
        ClusterNode curPrimary,
        List<ClusterNode> newNodes,
        WaitRebalanceInfo rebalance) {
        assert lateAffAssign;
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

        if (rebalance != null)
            rebalance.add(aff.cacheId(), part, newNodes.get(0).id(), newNodes);

        return nodes0;
    }

    /**
     * @param fut Exchange future.
     * @return Affinity assignment.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteInternalFuture<Map<Integer, Map<Integer, List<UUID>>>> initAffinityOnNodeLeft(
        final GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        assert lateAffAssign;

        IgniteInternalFuture<?> initFut = initCoordinatorCaches(fut);

        if (initFut != null && !initFut.isDone()) {
            final GridFutureAdapter<Map<Integer, Map<Integer, List<UUID>>>> resFut = new GridFutureAdapter<>();

            initFut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> initFut) {
                    try {
                        resFut.onDone(initAffinityOnNodeLeft0(fut));
                    }
                    catch (IgniteCheckedException e) {
                        resFut.onDone(e);
                    }
                }
            });

            return resFut;
        }
        else
            return new GridFinishedFuture<>(initAffinityOnNodeLeft0(fut));
    }

    /**
     * @param fut Exchange future.
     * @return Affinity assignment.
     * @throws IgniteCheckedException If failed.
     */
    private Map<Integer, Map<Integer, List<UUID>>> initAffinityOnNodeLeft0(final GridDhtPartitionsExchangeFuture fut)
        throws IgniteCheckedException {
        final AffinityTopologyVersion topVer = fut.topologyVersion();

        final WaitRebalanceInfo waitRebalanceInfo = new WaitRebalanceInfo(topVer);

        final Collection<ClusterNode> aliveNodes = cctx.discovery().nodes(topVer);

        final Map<Integer, Map<Integer, List<UUID>>> assignment = new HashMap<>();

        forAllRegisteredCaches(new IgniteInClosureX<DynamicCacheDescriptor>() {
            @Override public void applyx(DynamicCacheDescriptor cacheDesc) throws IgniteCheckedException {
                CacheHolder cache = cache(fut, cacheDesc);

                if (!cache.rebalanceEnabled)
                    return;

                AffinityTopologyVersion affTopVer = cache.affinity().lastVersion();

                assert affTopVer.topologyVersion() > 0 && !affTopVer.equals(topVer): "Invalid affinity version " +
                    "[last=" + affTopVer + ", futVer=" + topVer + ", cache=" + cache.name() + ']';

                List<List<ClusterNode>> curAssignment = cache.affinity().assignments(affTopVer);
                List<List<ClusterNode>> newAssignment = cache.affinity().idealAssignment();

                assert newAssignment != null;

                GridDhtPartitionTopology top = cache.topology(fut);

                Map<Integer, List<UUID>> cacheAssignment = null;

                for (int p = 0; p < newAssignment.size(); p++) {
                    List<ClusterNode> newNodes = newAssignment.get(p);
                    List<ClusterNode> curNodes = curAssignment.get(p);

                    ClusterNode curPrimary = curNodes.size() > 0 ? curNodes.get(0) : null;
                    ClusterNode newPrimary = newNodes.size() > 0 ? newNodes.get(0) : null;

                    List<ClusterNode> newNodes0 = null;

                    assert newPrimary == null || aliveNodes.contains(newPrimary) :  "Invalid new primary [" +
                        "cache=" + cache.name() +
                        ", node=" + newPrimary +
                        ", topVer=" + topVer + ']';

                    if (curPrimary != null && newPrimary != null && !curPrimary.equals(newPrimary)) {
                        if (aliveNodes.contains(curPrimary)) {
                            GridDhtPartitionState state = top.partitionState(newPrimary.id(), p);

                            if (state != GridDhtPartitionState.OWNING) {
                                newNodes0 = latePrimaryAssignment(cache.affinity(),
                                    p,
                                    curPrimary,
                                    newNodes,
                                    waitRebalanceInfo);
                            }
                        }
                        else {
                            GridDhtPartitionState state = top.partitionState(newPrimary.id(), p);

                            if (state != GridDhtPartitionState.OWNING) {
                                for (int i = 1; i < curNodes.size(); i++) {
                                    ClusterNode curNode = curNodes.get(i);

                                    if (top.partitionState(curNode.id(), p) == GridDhtPartitionState.OWNING) {
                                        newNodes0 = latePrimaryAssignment(cache.affinity(),
                                            p,
                                            curNode,
                                            newNodes,
                                            waitRebalanceInfo);

                                        break;
                                    }
                                }

                                if (newNodes0 == null) {
                                    List<ClusterNode> owners = top.owners(p);

                                    for (ClusterNode owner : owners) {
                                        if (aliveNodes.contains(owner)) {
                                            newNodes0 = latePrimaryAssignment(cache.affinity(),
                                                p,
                                                owner,
                                                newNodes,
                                                waitRebalanceInfo);

                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (newNodes0 != null) {
                        if (cacheAssignment == null)
                            cacheAssignment = new HashMap<>();

                        cacheAssignment.put(p, toIds0(newNodes0));
                    }
                }

                if (cacheAssignment != null)
                    assignment.put(cache.cacheId(), cacheAssignment);
            }
        });

        synchronized (mux) {
            assert affCalcVer.equals(topVer);

            this.waitInfo = !waitRebalanceInfo.empty() ? waitRebalanceInfo : null;

            WaitRebalanceInfo info = this.waitInfo;

            if (log.isDebugEnabled()) {
                log.debug("Computed new affinity after node left [topVer=" + topVer +
                    ", waitCaches=" + (info != null ? cacheNames(info.waitCaches.keySet()) : null)+ ']');
            }
        }

        return assignment;
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
     * @param nodes Nodes.
     * @return IDs.
     */
    private static List<UUID> toIds0(List<ClusterNode> nodes) {
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

            assert node != null : "Failed to get node [id=" + id +
                ", topVer=" + topVer +
                ", locNode=" + cctx.localNode() +
                ", allNodes=" + cctx.discovery().nodes(topVer) + ']';

            nodes.add(node);
        }

        return nodes;
    }

    /**
     *
     */
    abstract static class CacheHolder {
        /** */
        private final GridAffinityAssignmentCache aff;

        /** */
        private final boolean rebalanceEnabled;

        /**
         * @param rebalanceEnabled Cache rebalance flag.
         * @param aff Affinity cache.
         * @param initAff Existing affinity cache.
         */
        CacheHolder(boolean rebalanceEnabled,
            GridAffinityAssignmentCache aff,
            @Nullable GridAffinityAssignmentCache initAff) {
            this.aff = aff;

            if (initAff != null)
                aff.init(initAff);

            this.rebalanceEnabled = rebalanceEnabled;
        }

        /**
         * @return Client holder flag.
         */
        abstract boolean client();

        /**
         * @return Cache ID.
         */
        int cacheId() {
            return aff.cacheId();
        }

        /**
         * @return Partitions number.
         */
        int partitions() {
            return aff.partitions();
        }

        /**
         * @return Cache name.
         */
        String name() {
            return aff.cacheName();
        }

        /**
         * @param fut Exchange future.
         * @return Cache topology.
         */
        abstract GridDhtPartitionTopology topology(GridDhtPartitionsExchangeFuture fut);

        /**
         * @return Affinity.
         */
        GridAffinityAssignmentCache affinity() {
            return aff;
        }
    }

    /**
     * Created cache is started on coordinator.
     */
    private class CacheHolder1 extends CacheHolder {
        /** */
        private final GridCacheContext cctx;

        /**
         * @param cctx Cache context.
         * @param initAff Current affinity.
         */
        CacheHolder1(GridCacheContext cctx, @Nullable GridAffinityAssignmentCache initAff) {
            super(cctx.rebalanceEnabled(), cctx.affinity().affinityCache(), initAff);

            assert !cctx.isLocal() : cctx.name();

            this.cctx = cctx;
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
     * Created if cache is not started on coordinator.
     */
    private static class CacheHolder2 extends CacheHolder {
        /** */
        private final GridCacheSharedContext cctx;

        /**
         * @param cctx Context.
         * @param cacheDesc Cache descriptor.
         * @param fut Exchange future.
         * @param initAff Current affinity.
         * @return Cache holder.
         * @throws IgniteCheckedException If failed.
         */
        static CacheHolder2 create(
            GridCacheSharedContext cctx,
            DynamicCacheDescriptor cacheDesc,
            GridDhtPartitionsExchangeFuture fut,
            @Nullable GridAffinityAssignmentCache initAff) throws IgniteCheckedException {
            assert cacheDesc != null;
            assert !cctx.kernalContext().clientNode();

            CacheConfiguration<?, ?> ccfg = cacheDesc.cacheConfiguration();

            assert ccfg != null : cacheDesc;
            assert ccfg.getCacheMode() != LOCAL : ccfg.getName();

            assert !cctx.discovery().cacheAffinityNodes(ccfg.getName(), fut.topologyVersion()).contains(cctx.localNode());

            AffinityFunction affFunc = cctx.cache().clone(ccfg.getAffinity());

            cctx.kernalContext().resource().injectGeneric(affFunc);
            cctx.kernalContext().resource().injectCacheName(affFunc, ccfg.getName());

            U.startLifecycleAware(F.asList(affFunc));

            GridAffinityAssignmentCache aff = new GridAffinityAssignmentCache(cctx.kernalContext(),
                ccfg.getName(),
                affFunc,
                ccfg.getNodeFilter(),
                ccfg.getBackups(),
                ccfg.getCacheMode() == LOCAL);

            return new CacheHolder2(ccfg.getRebalanceMode() != NONE, cctx, aff, initAff);
        }

        /**
         * @param rebalanceEnabled Rebalance flag.
         * @param cctx Context.
         * @param aff Affinity.
         * @param initAff Current affinity.
         */
        public CacheHolder2(
            boolean rebalanceEnabled,
            GridCacheSharedContext cctx,
            GridAffinityAssignmentCache aff,
            @Nullable GridAffinityAssignmentCache initAff) {
            super(rebalanceEnabled, aff, initAff);

            this.cctx = cctx;
        }

        /** {@inheritDoc} */
        @Override public boolean client() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public GridDhtPartitionTopology topology(GridDhtPartitionsExchangeFuture fut) {
            return cctx.exchange().clientTopology(cacheId(), fut);
        }
    }

    /**
     *
     */
    class WaitRebalanceInfo {
        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, Map<Integer, UUID>> waitCaches;

        /** */
        private Map<Integer, Map<Integer, List<ClusterNode>>> assignments;

        /** */
        private Map<Integer, IgniteUuid> deploymentIds;

        /**
         * @param topVer Topology version.
         */
        WaitRebalanceInfo(AffinityTopologyVersion topVer) {
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
                deploymentIds = new HashMap<>();
            }

            Map<Integer, UUID> cacheWaitParts = waitCaches.get(cacheId);

            if (cacheWaitParts == null) {
                waitCaches.put(cacheId, cacheWaitParts = new HashMap<>());

                deploymentIds.put(cacheId, registeredCaches.get(cacheId).deploymentId());
            }

            cacheWaitParts.put(part, waitNode);

            Map<Integer, List<ClusterNode>> cacheAssignment = assignments.get(cacheId);

            if (cacheAssignment == null)
                assignments.put(cacheId, cacheAssignment = new HashMap<>());

            cacheAssignment.put(part, assignment);
        }
    }
}
