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
import org.apache.ignite.configuration.NearCacheConfiguration;
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
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
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
    /** Late affinity assignment flag. */
    private boolean lateAffAssign;

    /** Affinity information for all started caches (initialized on coordinator). */
    private ConcurrentMap<Integer, CacheGroupHolder> grpHolders = new ConcurrentHashMap<>();

    /** Last topology version when affinity was calculated (updated from exchange thread). */
    private AffinityTopologyVersion affCalcVer;

    /** Topology version which requires affinity re-calculation (set from discovery thread). */
    private AffinityTopologyVersion lastAffVer;

    /** Registered caches (updated from exchange thread). */
    private final Map<Integer, CacheGroupDescriptor> registeredGrps = new HashMap<>();

    /** */
    private WaitRebalanceInfo waitInfo;

    /** */
    private final Object mux = new Object();

    /** Pending affinity assignment futures. */
    private final ConcurrentMap<Long, GridDhtAssignmentFetchFuture> pendingAssignmentFetchFuts =
        new ConcurrentHashMap8<>();

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
            registeredGrps.clear();

            affCalcVer = null;

            lastAffVer = null;

            for (CacheGroupDescriptor desc : cctx.cache().cacheGroupDescriptors().values())
                registeredGrps.put(desc.groupId(), desc);
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
                log.debug("Ignore affinity change message [lastAffVer=" + lastAffVer +
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
                    ", msgVer=" + msg.topologyVersion() + ']');
            }
        }
        else {
            if (log.isDebugEnabled()) {
                log.debug("Ignore affinity change message [lastAffVer=" + lastAffVer +
                    ", msgExchId=" + msg.exchangeId() +
                    ", msgVer=" + msg.topologyVersion() + ']');
            }
        }

        return exchangeNeeded;
    }

    /**
     * @param topVer Expected topology version.
     */
    private void onCacheGroupStopped(AffinityTopologyVersion topVer) {
        CacheAffinityChangeMessage msg = null;

        synchronized (mux) {
            if (waitInfo == null || !waitInfo.topVer.equals(topVer))
                return;

            if (waitInfo.waitGrps.isEmpty()) {
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
     * @param checkGrpId Group ID.
     */
    void checkRebalanceState(GridDhtPartitionTopology top, Integer checkGrpId) {
        if (!lateAffAssign)
            return;

        CacheAffinityChangeMessage msg = null;

        synchronized (mux) {
            if (waitInfo == null)
                return;

            assert affCalcVer != null;
            assert affCalcVer.equals(waitInfo.topVer) : "Invalid affinity version [calcVer=" + affCalcVer +
                ", waitVer=" + waitInfo.topVer + ']';

            Map<Integer, UUID> partWait = waitInfo.waitGrps.get(checkGrpId);

            boolean rebalanced = true;

            if (partWait != null) {
                CacheGroupHolder grpHolder = grpHolders.get(checkGrpId);

                if (grpHolder != null) {
                    for (Iterator<Map.Entry<Integer, UUID>> it = partWait.entrySet().iterator(); it.hasNext(); ) {
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
                    waitInfo.waitGrps.remove(checkGrpId);

                    if (waitInfo.waitGrps.isEmpty()) {
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
            Integer grpId = e.getKey();

            Map<Integer, List<ClusterNode>> assignment = e.getValue();

            Map<Integer, List<UUID>> assignment0 = U.newHashMap(assignment.size());

            for (Map.Entry<Integer, List<ClusterNode>> e0 : assignment.entrySet())
                assignment0.put(e0.getKey(), toIds0(e0.getValue()));

            assignmentsChange.put(grpId, assignment0);
        }

        return new CacheAffinityChangeMessage(waitInfo.topVer, assignmentsChange, waitInfo.deploymentIds);
    }

    /**
     * @param grp Cache group.
     */
    void onCacheGroupCreated(CacheGroupContext grp) {
        final Integer grpId = grp.groupId();

        if (!grpHolders.containsKey(grp.groupId())) {
            cctx.io().addCacheGroupHandler(grpId, GridDhtAffinityAssignmentResponse.class,
                new IgniteBiInClosure<UUID, GridDhtAffinityAssignmentResponse>() {
                    @Override public void apply(UUID nodeId, GridDhtAffinityAssignmentResponse res) {
                        processAffinityAssignmentResponse(grpId, nodeId, res);
                    }
                });
        }
    }

    /**
     * @param exchActions Cache change requests to execute on exchange.
     */
    private void updateCachesInfo(ExchangeActions exchActions) {
        for (CacheGroupDescriptor stopDesc : exchActions.cacheGroupsToStop()) {
            CacheGroupDescriptor rmvd = registeredGrps.remove(stopDesc.groupId());

            assert rmvd != null : stopDesc.cacheOrGroupName();
        }

        for (CacheGroupDescriptor startDesc : exchActions.cacheGroupsToStart()) {
            CacheGroupDescriptor old = registeredGrps.put(startDesc.groupId(), startDesc);

            assert old == null : old;
        }
    }

    /**
     * Called on exchange initiated for cache start/stop request.
     *
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @param exchActions Cache change requests.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if client-only exchange is needed.
     */
    public boolean onCacheChangeRequest(final GridDhtPartitionsExchangeFuture fut,
        boolean crd,
        final ExchangeActions exchActions)
        throws IgniteCheckedException
    {
        assert exchActions != null && !exchActions.empty() : exchActions;

        updateCachesInfo(exchActions);

        // Affinity did not change for existing caches.
        forAllCacheGroups(crd && lateAffAssign, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                if (exchActions.cacheGroupStopping(aff.groupId()))
                    return;

                aff.clientEventTopologyChange(fut.discoveryEvent(), fut.topologyVersion());
            }
        });

        for (ExchangeActions.ActionData action : exchActions.newAndClientCachesStartRequests()) {
            DynamicCacheDescriptor cacheDesc = action.descriptor();

            DynamicCacheChangeRequest req = action.request();

            boolean startCache;

            NearCacheConfiguration nearCfg = null;

            if (cctx.localNodeId().equals(req.initiatingNodeId())) {
                startCache = true;

                nearCfg = req.nearCacheConfiguration();
            }
            else {
                startCache = cctx.cacheContext(cacheDesc.cacheId()) == null &&
                    CU.affinityNode(cctx.localNode(), cacheDesc.groupDescriptor().config().getNodeFilter());
            }

            try {
                if (startCache) {
                    cctx.cache().prepareCacheStart(cacheDesc, nearCfg, fut.topologyVersion());

                    if (fut.cacheAddedOnExchange(cacheDesc.cacheId(), cacheDesc.receivedFrom())) {
                        if (fut.discoCache().cacheGroupAffinityNodes(cacheDesc.groupId()).isEmpty())
                            U.quietAndWarn(log, "No server nodes found for cache client: " + req.cacheName());
                    }
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to initialize cache. Will try to rollback cache start routine. " +
                    "[cacheName=" + req.cacheName() + ']', e);

                cctx.cache().forceCloseCache(fut.topologyVersion(), action, e);
            }
        }

            Set<Integer> gprs = new HashSet<>();

                for (ExchangeActions.ActionData action : exchActions.newAndClientCachesStartRequests()) {
                    Integer grpId = action.descriptor().groupId();

                    if (gprs.add(grpId)) {
                        if (crd && lateAffAssign)
                    initStartedGroupOnCoordinator(fut, action.descriptor().groupDescriptor());else  {
                        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                        if (grp != null && !grp.isLocal() && grp.localStartVersion().equals(fut.topologyVersion())) {
                        assert grp.affinity().lastVersion().equals(AffinityTopologyVersion.NONE) : grp.affinity().lastVersion();

                        initAffinity(registeredGrps.get(grp.groupId()), grp.affinity(), fut);
                    }
                }
            }
        }

        List<ExchangeActions.ActionData> closeReqs = exchActions.closeRequests(cctx.localNodeId());

        for (ExchangeActions.ActionData req : closeReqs) {
            cctx.cache().blockGateway(req.request());

            if (crd) {
                CacheGroupContext grp = cctx.cache().cacheGroup(req.descriptor().groupId());

                assert grp != null;

                if (grp.affinityNode())
                    continue;

                boolean grpClosed = false;

                if (grp.sharedGroup()) {
                    boolean cacheRemaining = false;

                    for (GridCacheContext ctx : cctx.cacheContexts()) {
                        if (ctx.group() == grp && !cacheClosed(ctx.cacheId(), closeReqs)) {
                            cacheRemaining = true;

                            break;
                        }
                    }

                    if (!cacheRemaining)
                        grpClosed = true;
                }
                else
                    grpClosed = true;

                // All client cache groups were stopped, need create 'client' CacheGroupHolder.
                if (grpClosed) {
                    CacheGroupHolder grpHolder = grpHolders.remove(grp.groupId());

                    if (grpHolder != null) {
                        assert !grpHolder.client() : grpHolder;

                        grpHolder = CacheGroupHolder2.create(cctx,
                            registeredGrps.get(grp.groupId()),
                            fut,
                            grp.affinity());

                        grpHolders.put(grp.groupId(), grpHolder);
                    }
                }
            }
        }

        for (ExchangeActions.ActionData action : exchActions.cacheStopRequests())
            cctx.cache().blockGateway(action.request());

        Set<Integer> stoppedGrps = null;

        if (crd && lateAffAssign) {
            for (CacheGroupDescriptor grpDesc : exchActions.cacheGroupsToStop()) {
                if (grpDesc.config().getCacheMode() != LOCAL) {
                    CacheGroupHolder cacheGrp = grpHolders.remove(grpDesc.groupId());

                    assert cacheGrp != null : grpDesc;

                    if (stoppedGrps == null)
                        stoppedGrps = new HashSet<>();

                    stoppedGrps.add(cacheGrp.groupId());

                    cctx.io().removeHandler(true, cacheGrp.groupId(), GridDhtAffinityAssignmentResponse.class);
                }
            }
        }

        if (stoppedGrps != null) {
            boolean notify = false;

            synchronized (mux) {
                if (waitInfo != null) {
                    for (Integer grpId : stoppedGrps) {
                        boolean rmv = waitInfo.waitGrps.remove(grpId) != null;

                        if (rmv) {
                            notify = true;

                            waitInfo.assignments.remove(grpId);
                        }
                    }
                }
            }

            if (notify) {
                final AffinityTopologyVersion topVer = affCalcVer;

                cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                    @Override public void run() {
                        onCacheGroupStopped(topVer);
                    }
                });
            }
        }

        return exchActions.clientOnlyExchange();
    }

    /**
     * @param cacheId Cache ID.
     * @param closeReqs Close requests.
     * @return {@code True} if requests contain request for given cache ID.
     */
    private boolean cacheClosed(int cacheId, List<ExchangeActions.ActionData> closeReqs) {
        for (ExchangeActions.ActionData req : closeReqs) {
            if (req.descriptor().cacheId() == cacheId)
                return true;
        }

        return false;
    }

    /**
     *
     */
    public void removeAllCacheInfo(){
        grpHolders.clear();

        registeredGrps.clear();
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

        forAllCacheGroups(crd, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                List<List<ClusterNode>> idealAssignment = aff.idealAssignment();

                assert idealAssignment != null;

                Map<Integer, List<UUID>> cacheAssignment = assignment.get(aff.groupId());

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
        assert msg.topologyVersion() != null && msg.exchangeId() == null : msg;
        assert affCalcVer == null || affCalcVer.equals(msg.topologyVersion());

        final AffinityTopologyVersion topVer = exchFut.topologyVersion();

        if (log.isDebugEnabled()) {
            log.debug("Process affinity change message [exchVer=" + exchFut.topologyVersion() +
                ", affCalcVer=" + affCalcVer +
                ", msgVer=" + msg.topologyVersion() + ']');
        }

        final Map<Integer, Map<Integer, List<UUID>>> affChange = msg.assignmentChange();

        assert !F.isEmpty(affChange) : msg;

        final Map<Integer, IgniteUuid> deploymentIds = msg.cacheDeploymentIds();

        final Map<Object, List<List<ClusterNode>>> affCache = new HashMap<>();

        forAllCacheGroups(crd, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                AffinityTopologyVersion affTopVer = aff.lastVersion();

                assert affTopVer.topologyVersion() > 0 : affTopVer;

                CacheGroupDescriptor desc = registeredGrps.get(aff.groupId());

                assert desc != null : aff.cacheOrGroupName();

                IgniteUuid deploymentId = desc.deploymentId();

                if (!deploymentId.equals(deploymentIds.get(aff.groupId()))) {
                    aff.clientEventTopologyChange(exchFut.discoveryEvent(), topVer);

                    return;
                }

                Map<Integer, List<UUID>> change = affChange.get(aff.groupId());

                if (change != null) {
                    assert !change.isEmpty() : msg;

                    List<List<ClusterNode>> curAff = aff.assignments(affTopVer);

                    List<List<ClusterNode>> assignment = new ArrayList<>(curAff);

                    for (Map.Entry<Integer, List<UUID>> e : change.entrySet()) {
                        Integer part = e.getKey();

                        List<ClusterNode> nodes = toNodes(topVer, e.getValue());

                        assert !nodes.equals(assignment.get(part)) : "Assignment did not change " +
                            "[cacheGrp=" + aff.cacheOrGroupName() +
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
                forAllCacheGroups(crd, new IgniteInClosureX<GridAffinityAssignmentCache>() {
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
                forAllCacheGroups(false, new IgniteInClosureX<GridAffinityAssignmentCache>() {
                    @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                        AffinityTopologyVersion topVer = fut.topologyVersion();

                        aff.clientEventTopologyChange(fut.discoveryEvent(), topVer);
                    }
                });
            }
            else
                initAffinityNoLateAssignment(fut);
        }
    }

    /**
     * @param fut Future to add.
     */
    public void addDhtAssignmentFetchFuture(GridDhtAssignmentFetchFuture fut) {
        GridDhtAssignmentFetchFuture old = pendingAssignmentFetchFuts.putIfAbsent(fut.id(), fut);

        assert old == null : "More than one thread is trying to fetch partition assignments [fut=" + fut +
            ", allFuts=" + pendingAssignmentFetchFuts + ']';
    }

    /**
     * @param fut Future to remove.
     */
    public void removeDhtAssignmentFetchFuture(GridDhtAssignmentFetchFuture fut) {
        boolean rmv = pendingAssignmentFetchFuts.remove(fut.id(), fut);

        assert rmv : "Failed to remove assignment fetch future: " + fut.id();
    }

    /**
     * @param grpId Cache group ID.
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processAffinityAssignmentResponse(Integer grpId, UUID nodeId, GridDhtAffinityAssignmentResponse res) {
        if (log.isDebugEnabled())
            log.debug("Processing affinity assignment response [node=" + nodeId + ", res=" + res + ']');

        GridDhtAssignmentFetchFuture fut = pendingAssignmentFetchFuts.get(res.futureId());

        if (fut != null)
            fut.onResponse(nodeId, res);
    }

    /**
     * @param c Cache closure.
     * @throws IgniteCheckedException If failed
     */
    private void forAllRegisteredCacheGroups(IgniteInClosureX<CacheGroupDescriptor> c) throws IgniteCheckedException {
        assert lateAffAssign;

        for (CacheGroupDescriptor cacheDesc : registeredGrps.values()) {
            if (cacheDesc.config().getCacheMode() == LOCAL)
                continue;

            c.applyx(cacheDesc);
        }
    }

    /**
     * @param crd Coordinator flag.
     * @param c Closure.
     */
    private void forAllCacheGroups(boolean crd, IgniteInClosureX<GridAffinityAssignmentCache> c) {
        if (crd) {
            for (CacheGroupHolder grp : grpHolders.values())
                c.apply(grp.affinity());
        }
        else {
            for (CacheGroupContext grp : cctx.kernalContext().cache().cacheGroups()) {
                if (grp.isLocal())
                    continue;

                c.apply(grp.affinity());
            }
        }
    }

    /**
     * @param fut Exchange future.
     * @param grpDesc Cache group descriptor.
     * @throws IgniteCheckedException If failed.
     */
    private void initStartedGroupOnCoordinator(GridDhtPartitionsExchangeFuture fut, final CacheGroupDescriptor grpDesc)
        throws IgniteCheckedException {
        assert grpDesc != null && grpDesc.groupId() != 0 : grpDesc;

        if (grpDesc.config().getCacheMode() == LOCAL)
            return;

        Integer grpId = grpDesc.groupId();

        CacheGroupHolder grpHolder = grpHolders.get(grpId);

        CacheGroupContext grp = cctx.kernalContext().cache().cacheGroup(grpId);

        if (grpHolder == null) {
            grpHolder = grp != null ?
                new CacheGroupHolder1(grp, null) :
                CacheGroupHolder2.create(cctx, grpDesc, fut, null);

            CacheGroupHolder old = grpHolders.put(grpId, grpHolder);

            assert old == null : old;

            List<List<ClusterNode>> newAff = grpHolder.affinity().calculate(fut.topologyVersion(),
                fut.discoveryEvent(),
                fut.discoCache());

            grpHolder.affinity().initialize(fut.topologyVersion(), newAff);
        }
        else if (grpHolder.client() && grp != null) {
            assert grpHolder.affinity().idealAssignment() != null;

            grpHolder = new CacheGroupHolder1(grp, grpHolder.affinity());

            grpHolders.put(grpId, grpHolder);
        }
    }

    /**
     * Initialized affinity for cache received from node joining on this exchange.
     *
     * @param crd Coordinator flag.
     * @param fut Exchange future.
     * @param descs Cache descriptors.
     * @throws IgniteCheckedException If failed.
     */
    public void initStartedCaches(boolean crd,
        final GridDhtPartitionsExchangeFuture fut,
        Collection<DynamicCacheDescriptor> descs) throws IgniteCheckedException {
        for (DynamicCacheDescriptor desc : descs) {
            CacheGroupDescriptor grpDesc = desc.groupDescriptor();

            if (!registeredGrps.containsKey(grpDesc.groupId()))
                registeredGrps.put(grpDesc.groupId(), grpDesc);
        }

        if (crd && lateAffAssign) {
            forAllRegisteredCacheGroups(new IgniteInClosureX<CacheGroupDescriptor>() {
                @Override public void applyx(CacheGroupDescriptor desc) throws IgniteCheckedException {
                    CacheGroupHolder cache = groupHolder(fut, desc);

                    if (cache.affinity().lastVersion().equals(AffinityTopologyVersion.NONE)) {
                        List<List<ClusterNode>> assignment =
                            cache.affinity().calculate(fut.topologyVersion(), fut.discoveryEvent(), fut.discoCache());

                        cache.affinity().initialize(fut.topologyVersion(), assignment);
                    }
                }
            });
        }
        else {
            forAllCacheGroups(false, new IgniteInClosureX<GridAffinityAssignmentCache>() {
                @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                    if (aff.lastVersion().equals(AffinityTopologyVersion.NONE))
                        initAffinity(registeredGrps.get(aff.groupId()), aff, fut);
                }
            });
        }
    }

    /**
     * @param desc Cache group descriptor.
     * @param aff Affinity.
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     */
    private void initAffinity(CacheGroupDescriptor desc,
        GridAffinityAssignmentCache aff,
        GridDhtPartitionsExchangeFuture fut)
        throws IgniteCheckedException {
        assert desc != null : aff.cacheOrGroupName();

        if (canCalculateAffinity(desc, aff, fut)) {
            List<List<ClusterNode>> assignment = aff.calculate(fut.topologyVersion(), fut.discoveryEvent(), fut.discoCache());

            aff.initialize(fut.topologyVersion(), assignment);
        }
        else {
            GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cctx,
                desc,
                fut.topologyVersion(),
                fut.discoCache());

            fetchFut.init();

            fetchAffinity(fut, aff, fetchFut);
        }
    }

    /**
     * @param desc Cache group descriptor.
     * @param aff Affinity.
     * @param fut Exchange future.
     * @return {@code True} if local node can calculate affinity on it's own for this partition map exchange.
     */
    private boolean canCalculateAffinity(CacheGroupDescriptor desc,
        GridAffinityAssignmentCache aff,
        GridDhtPartitionsExchangeFuture fut) {
        assert desc != null : aff.cacheOrGroupName();

        // Do not request affinity from remote nodes if affinity function is not centralized.
        if (!lateAffAssign && !aff.centralizedAffinityFunction())
            return true;

        // If local node did not initiate exchange or local node is the only cache node in grid.
        Collection<ClusterNode> affNodes = fut.discoCache().cacheGroupAffinityNodes(aff.groupId());

        return fut.cacheGroupAddedOnExchange(aff.groupId(), desc.receivedFrom()) ||
            !fut.exchangeId().nodeId().equals(cctx.localNodeId()) ||
            (affNodes.isEmpty() || (affNodes.size() == 1 && affNodes.contains(cctx.localNode())));
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
                    forAllRegisteredCacheGroups(new IgniteInClosureX<CacheGroupDescriptor>() {
                        @Override public void applyx(CacheGroupDescriptor desc) throws IgniteCheckedException {
                            AffinityTopologyVersion topVer = fut.topologyVersion();

                            CacheGroupHolder cache = groupHolder(fut, desc);

                            List<List<ClusterNode>> newAff = cache.affinity().calculate(topVer,
                                fut.discoveryEvent(),
                                fut.discoCache());

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
            initAffinityNoLateAssignment(fut);

        synchronized (mux) {
            affCalcVer = fut.topologyVersion();

            this.waitInfo = waitRebalanceInfo != null && !waitRebalanceInfo.empty() ? waitRebalanceInfo : null;

            WaitRebalanceInfo info = this.waitInfo;

            if (crd && lateAffAssign) {
                if (log.isDebugEnabled()) {
                    log.debug("Computed new affinity after node join [topVer=" + fut.topologyVersion() +
                        ", waitGrps=" + (info != null ? groupNames(info.waitGrps.keySet()) : null) + ']');
                }
            }
        }
    }

    /**
     * @param grpIds Cache group IDs.
     * @return Cache names.
     */
    private String groupNames(Collection<Integer> grpIds) {
        StringBuilder names = new StringBuilder();

        for (Integer grpId : grpIds) {
            String name = registeredGrps.get(grpId).cacheOrGroupName();

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

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal())
                continue;

            if (fut.cacheGroupAddedOnExchange(grp.groupId(), grp.receivedFrom())) {
                List<List<ClusterNode>> assignment = grp.affinity().calculate(fut.topologyVersion(),
                    fut.discoveryEvent(),
                    fut.discoCache());

                grp.affinity().initialize(fut.topologyVersion(), assignment);
            }
            else {
                CacheGroupDescriptor grpDesc = registeredGrps.get(grp.groupId());

                assert grpDesc != null : grp.cacheOrGroupName();

                GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cctx,
                    grpDesc,
                    topVer,
                    fut.discoCache());

                fetchFut.init();

                fetchFuts.add(fetchFut);
            }
        }

        for (int i = 0; i < fetchFuts.size(); i++) {
            GridDhtAssignmentFetchFuture fetchFut = fetchFuts.get(i);

            Integer grpId = fetchFut.groupId();

            fetchAffinity(fut, cctx.cache().cacheGroup(grpId).affinity(), fetchFut);
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
            List<List<ClusterNode>> aff = affCache.calculate(topVer, fut.discoveryEvent(), fut.discoCache());

            affCache.initialize(topVer, aff);
        }
        else {
            List<List<ClusterNode>> idealAff = res.idealAffinityAssignment(cctx.discovery());

            if (idealAff != null)
                affCache.idealAssignment(idealAff);
            else {
                assert !affCache.centralizedAffinityFunction() || !lateAffAssign;

                affCache.calculate(topVer, fut.discoveryEvent(), fut.discoCache());
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
            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (grp.isLocal())
                    continue;

                grp.affinity().calculate(fut.topologyVersion(), fut.discoveryEvent(), fut.discoCache());
            }

            centralizedAff = true;
        }
        else {
            initAffinityNoLateAssignment(fut);

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
    private void initAffinityNoLateAssignment(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        assert !lateAffAssign;

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal())
                continue;

            initAffinity(registeredGrps.get(grp.groupId()), grp.affinity(), fut);
        }
    }

    /**
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     * @return Future completed when caches initialization is done.
     */
    private IgniteInternalFuture<?> initCoordinatorCaches(final GridDhtPartitionsExchangeFuture fut)
        throws IgniteCheckedException {
        assert lateAffAssign;

        final List<IgniteInternalFuture<AffinityTopologyVersion>> futs = new ArrayList<>();

        forAllRegisteredCacheGroups(new IgniteInClosureX<CacheGroupDescriptor>() {
            @Override public void applyx(CacheGroupDescriptor desc) throws IgniteCheckedException {
                CacheGroupHolder grpHolder = grpHolders.get(desc.groupId());

                if (grpHolder != null) {
                    if (grpHolder.client()) // Affinity for non-client holders calculated in {@link #onServerLeft}.
                        grpHolder.affinity().calculate(fut.topologyVersion(), fut.discoveryEvent(), fut.discoCache());

                    return;
                }

                // Need initialize holders and affinity if this node became coordinator during this exchange.
                final Integer grpId = desc.groupId();

                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                if (grp == null) {
                    cctx.io().addCacheGroupHandler(desc.groupId(), GridDhtAffinityAssignmentResponse.class,
                        new IgniteBiInClosure<UUID, GridDhtAffinityAssignmentResponse>() {
                            @Override public void apply(UUID nodeId, GridDhtAffinityAssignmentResponse res) {
                                processAffinityAssignmentResponse(grpId, nodeId, res);
                            }
                        }
                    );

                    grpHolder = CacheGroupHolder2.create(cctx, desc, fut, null);

                    final GridAffinityAssignmentCache aff = grpHolder.affinity();

                    List<GridDhtPartitionsExchangeFuture> exchFuts = cctx.exchange().exchangeFutures();

                    int idx = exchFuts.indexOf(fut);

                    assert idx >= 0 && idx < exchFuts.size() - 1 : "Invalid exchange futures state [cur=" + idx +
                        ", total=" + exchFuts.size() + ']';

                    final GridDhtPartitionsExchangeFuture prev = exchFuts.get(idx + 1);

                    if (log.isDebugEnabled()) {
                        log.debug("Need initialize affinity on coordinator [" +
                            "cacheGrp=" + desc.cacheOrGroupName() +
                            "prevAff=" + prev.topologyVersion() + ']');
                    }

                    assert prev.topologyVersion().compareTo(fut.topologyVersion()) < 0 : prev;

                    GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cctx,
                        desc,
                        prev.topologyVersion(),
                        prev.discoCache());

                    fetchFut.init();

                    final GridFutureAdapter<AffinityTopologyVersion> affFut = new GridFutureAdapter<>();

                    fetchFut.listen(new IgniteInClosureX<IgniteInternalFuture<GridDhtAffinityAssignmentResponse>>() {
                        @Override public void applyx(IgniteInternalFuture<GridDhtAffinityAssignmentResponse> fetchFut)
                            throws IgniteCheckedException {
                            fetchAffinity(prev, aff, (GridDhtAssignmentFetchFuture)fetchFut);

                            aff.calculate(fut.topologyVersion(), fut.discoveryEvent(), fut.discoCache());

                            affFut.onDone(fut.topologyVersion());
                        }
                    });

                    futs.add(affFut);
                }
                else
                    grpHolder = new CacheGroupHolder1(grp, null);

                CacheGroupHolder old = grpHolders.put(grpHolder.groupId(), grpHolder);

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
    private CacheGroupHolder groupHolder(GridDhtPartitionsExchangeFuture fut, final CacheGroupDescriptor desc)
        throws IgniteCheckedException {
        assert lateAffAssign;

        CacheGroupHolder cacheGrp = grpHolders.get(desc.groupId());

        if (cacheGrp != null)
            return cacheGrp;

        final CacheGroupContext grp = cctx.cache().cacheGroup(desc.groupId());

        if (grp == null) {
            cctx.io().addCacheGroupHandler(desc.groupId(), GridDhtAffinityAssignmentResponse.class,
                new IgniteBiInClosure<UUID, GridDhtAffinityAssignmentResponse>() {
                    @Override public void apply(UUID nodeId, GridDhtAffinityAssignmentResponse res) {
                        processAffinityAssignmentResponse(desc.groupId(), nodeId, res);
                    }
                }
            );

            cacheGrp = CacheGroupHolder2.create(cctx, desc, fut, null);
        }
        else
            cacheGrp = new CacheGroupHolder1(grp, null);

        CacheGroupHolder old = grpHolders.put(desc.groupId(), cacheGrp);

        assert old == null : old;

        return cacheGrp;
    }

    /**
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     * @return Rabalance info.
     */
    @Nullable private WaitRebalanceInfo initAffinityOnNodeJoin(final GridDhtPartitionsExchangeFuture fut, boolean crd)
        throws IgniteCheckedException {
        assert lateAffAssign;

        AffinityTopologyVersion topVer = fut.topologyVersion();

        final Map<Object, List<List<ClusterNode>>> affCache = new HashMap<>();

        if (!crd) {
            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (grp.isLocal())
                    continue;

                boolean latePrimary = grp.rebalanceEnabled();

                initAffinityOnNodeJoin(fut, grp.affinity(), null, latePrimary, affCache);
            }

            return null;
        }
        else {
            final WaitRebalanceInfo waitRebalanceInfo = new WaitRebalanceInfo(topVer);

            forAllRegisteredCacheGroups(new IgniteInClosureX<CacheGroupDescriptor>() {
                @Override public void applyx(CacheGroupDescriptor desc) throws IgniteCheckedException {
                    CacheGroupHolder cache = groupHolder(fut, desc);

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

        assert affTopVer.topologyVersion() > 0 : "Affinity is not initialized [grp=" + aff.cacheOrGroupName() +
            ", topVer=" + affTopVer + ", node=" + cctx.localNodeId() + ']';

        List<List<ClusterNode>> curAff = aff.assignments(affTopVer);

        assert aff.idealAssignment() != null : "Previous assignment is not available.";

        List<List<ClusterNode>> idealAssignment = aff.calculate(topVer, fut.discoveryEvent(), fut.discoCache());
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
            rebalance.add(aff.groupId(), part, newNodes.get(0).id(), newNodes);

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
        assert lateAffAssign;

        final AffinityTopologyVersion topVer = fut.topologyVersion();

        final WaitRebalanceInfo waitRebalanceInfo = new WaitRebalanceInfo(topVer);

        final Collection<ClusterNode> aliveNodes = cctx.discovery().nodes(topVer);

        final Map<Integer, Map<Integer, List<UUID>>> assignment = new HashMap<>();

        forAllRegisteredCacheGroups(new IgniteInClosureX<CacheGroupDescriptor>() {
            @Override public void applyx(CacheGroupDescriptor desc) throws IgniteCheckedException {
                CacheGroupHolder grpHolder = groupHolder(fut, desc);

                if (!grpHolder.rebalanceEnabled)
                    return;

                AffinityTopologyVersion affTopVer = grpHolder.affinity().lastVersion();

                assert affTopVer.topologyVersion() > 0 && !affTopVer.equals(topVer) : "Invalid affinity version " +
                    "[last=" + affTopVer + ", futVer=" + topVer + ", grp=" + desc.cacheOrGroupName() + ']';

                List<List<ClusterNode>> curAssignment = grpHolder.affinity().assignments(affTopVer);
                List<List<ClusterNode>> newAssignment = grpHolder.affinity().idealAssignment();

                assert newAssignment != null;

                GridDhtPartitionTopology top = grpHolder.topology(fut);

                Map<Integer, List<UUID>> cacheAssignment = null;

                for (int p = 0; p < newAssignment.size(); p++) {
                    List<ClusterNode> newNodes = newAssignment.get(p);
                    List<ClusterNode> curNodes = curAssignment.get(p);

                    ClusterNode curPrimary = curNodes.size() > 0 ? curNodes.get(0) : null;
                    ClusterNode newPrimary = newNodes.size() > 0 ? newNodes.get(0) : null;

                    List<ClusterNode> newNodes0 = null;

                    assert newPrimary == null || aliveNodes.contains(newPrimary) : "Invalid new primary [" +
                        "grp=" + desc.cacheOrGroupName() +
                        ", node=" + newPrimary +
                        ", topVer=" + topVer + ']';

                    if (curPrimary != null && newPrimary != null && !curPrimary.equals(newPrimary)) {
                        if (aliveNodes.contains(curPrimary)) {
                            GridDhtPartitionState state = top.partitionState(newPrimary.id(), p);

                            if (state != GridDhtPartitionState.OWNING) {
                                newNodes0 = latePrimaryAssignment(grpHolder.affinity(),
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
                                        newNodes0 = latePrimaryAssignment(grpHolder.affinity(),
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
                                            newNodes0 = latePrimaryAssignment(grpHolder.affinity(),
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
                    assignment.put(grpHolder.groupId(), cacheAssignment);
            }
        });

        synchronized (mux) {
            assert affCalcVer.equals(topVer);

            this.waitInfo = !waitRebalanceInfo.empty() ? waitRebalanceInfo : null;

            WaitRebalanceInfo info = this.waitInfo;

            if (log.isDebugEnabled()) {
                log.debug("Computed new affinity after node left [topVer=" + topVer +
                    ", waitGrps=" + (info != null ? groupNames(info.waitGrps.keySet()) : null) + ']');
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
    abstract static class CacheGroupHolder {
        /** */
        private final GridAffinityAssignmentCache aff;

        /** */
        private final boolean rebalanceEnabled;

        /**
         * @param rebalanceEnabled Cache rebalance flag.
         * @param aff Affinity cache.
         * @param initAff Existing affinity cache.
         */
        CacheGroupHolder(boolean rebalanceEnabled,
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
         * @return Group ID.
         */
        int groupId() {
            return aff.groupId();
        }

        /**
         * @return Partitions number.
         */
        int partitions() {
            return aff.partitions();
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
    private class CacheGroupHolder1 extends CacheGroupHolder {
        /** */
        private final CacheGroupContext grp;

        /**
         * @param grp Cache group.
         * @param initAff Current affinity.
         */
        CacheGroupHolder1(CacheGroupContext grp, @Nullable GridAffinityAssignmentCache initAff) {
            super(grp.rebalanceEnabled(), grp.affinity(), initAff);

            assert !grp.isLocal() : grp;

            this.grp = grp;
        }

        /** {@inheritDoc} */
        @Override public boolean client() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public GridDhtPartitionTopology topology(GridDhtPartitionsExchangeFuture fut) {
            return grp.topology();
        }
    }

    /**
     * Created if cache is not started on coordinator.
     */
    private static class CacheGroupHolder2 extends CacheGroupHolder {
        /** */
        private final GridCacheSharedContext cctx;

        /**
         * @param cctx Context.
         * @param grpDesc Cache group descriptor.
         * @param fut Exchange future.
         * @param initAff Current affinity.
         * @return Cache holder.
         * @throws IgniteCheckedException If failed.
         */
        static CacheGroupHolder2 create(
            GridCacheSharedContext cctx,
            CacheGroupDescriptor grpDesc,
            GridDhtPartitionsExchangeFuture fut,
            @Nullable GridAffinityAssignmentCache initAff) throws IgniteCheckedException {
            assert grpDesc != null;
            assert !cctx.kernalContext().clientNode();

            CacheConfiguration<?, ?> ccfg = grpDesc.config();

            assert ccfg != null : grpDesc;
            assert ccfg.getCacheMode() != LOCAL : ccfg.getName();

            assert !cctx.discovery().cacheGroupAffinityNodes(grpDesc.groupId(),
                fut.topologyVersion()).contains(cctx.localNode()) : grpDesc.cacheOrGroupName();

            AffinityFunction affFunc = cctx.cache().clone(ccfg.getAffinity());

            cctx.kernalContext().resource().injectGeneric(affFunc);
            cctx.kernalContext().resource().injectCacheName(affFunc, ccfg.getName());

            U.startLifecycleAware(F.asList(affFunc));

            GridAffinityAssignmentCache aff = new GridAffinityAssignmentCache(cctx.kernalContext(),
                grpDesc.cacheOrGroupName(),
                grpDesc.groupId(),
                affFunc,
                ccfg.getNodeFilter(),
                ccfg.getBackups(),
                ccfg.getCacheMode() == LOCAL);

            return new CacheGroupHolder2(ccfg.getRebalanceMode() != NONE, cctx, aff, initAff);
        }

        /**
         * @param rebalanceEnabled Rebalance flag.
         * @param cctx Context.
         * @param aff Affinity.
         * @param initAff Current affinity.
         */
        CacheGroupHolder2(
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
            return cctx.exchange().clientTopology(groupId(), fut);
        }
    }

    /**
     *
     */
    class WaitRebalanceInfo {
        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, Map<Integer, UUID>> waitGrps;

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
            if (waitGrps != null) {
                assert !waitGrps.isEmpty();
                assert waitGrps.size() == assignments.size();

                return false;
            }

            return true;
        }

        /**
         * @param grpId Group ID.
         * @param part Partition.
         * @param waitNode Node rebalancing data.
         * @param assignment New assignment.
         */
        void add(Integer grpId, Integer part, UUID waitNode, List<ClusterNode> assignment) {
            assert !F.isEmpty(assignment) : assignment;

            if (waitGrps == null) {
                waitGrps = new HashMap<>();
                assignments = new HashMap<>();
                deploymentIds = new HashMap<>();
            }

            Map<Integer, UUID> cacheWaitParts = waitGrps.get(grpId);

            if (cacheWaitParts == null) {
                waitGrps.put(grpId, cacheWaitParts = new HashMap<>());

                deploymentIds.put(grpId, registeredGrps.get(grpId).deploymentId());
            }

            cacheWaitParts.put(part, waitNode);

            Map<Integer, List<ClusterNode>> cacheAssignment = assignments.get(grpId);

            if (cacheAssignment == null)
                assignments.put(grpId, cacheAssignment = new HashMap<>());

            cacheAssignment.put(part, assignment);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "WaitRebalanceInfo [topVer=" + topVer +
                ", grps=" + (waitGrps != null ? waitGrps.keySet() : null) + ']';
        }
    }
}
