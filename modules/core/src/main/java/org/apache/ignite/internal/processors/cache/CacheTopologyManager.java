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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityCentralizedFunction;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignment;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridClientPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAssignmentFetchFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jsr166.ConcurrentHashMap8;

import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.*;

/**
 *
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class CacheTopologyManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /** */
    public static final IgniteProductVersion DELAY_AFF_ASSIGN_SINCE = IgniteProductVersion.fromString("1.6.0");

    /** */
    public static final boolean LOG_AFF_CHANGE = true;

    /** */
    private RebalancingInfo rebalancingInfo;

    /** */
    @GridToStringExclude
    private final ConcurrentMap<Integer, GridClientPartitionTopology> clientTops = new ConcurrentHashMap8<>();

    /** */
    private final GridBoundedConcurrentLinkedHashMap<AffinityTopologyVersion, Map<Integer, List<List<UUID>>>>
        affHist = new GridBoundedConcurrentLinkedHashMap<>(100, 100, 0.75f, 64, PER_SEGMENT_Q);

    /** */
    private AffinityTopologyVersion affCalcVer;

    /** */
    private final Object mux = new Object();

    public void checkRebalanceState(AffinityTopologyVersion topVer, Integer cacheId) {
        CacheAffinityChangeMessage msg = null;

        synchronized (mux) {
            if (rebalancingInfo == null)
                return;

            assert affCalcVer != null;

            if (affCalcVer.compareTo(topVer) > 0)
                return;

            assert affCalcVer.equals(rebalancingInfo.topVer);

            Map<Integer, UUID> partWait = rebalancingInfo.waitCaches.get(cacheId);

            if (partWait != null) {
                GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                GridDhtPartitionTopology top = cctx.cacheContext(cacheId).topology();

                boolean rebalanced = true;

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
                    logAffinityChange(log, cacheCtx.name(), "Cache rebalance state [cache=" + cacheCtx +
                        ", rebalanced=" + rebalanced + ']');
                }

                if (rebalanced) {
                    rebalancingInfo.waitCaches.remove(cacheId);

                    if (rebalancingInfo.waitCaches.isEmpty()) {
                        Map<Integer, Map<Integer, List<UUID>>> assignmentsChange =
                            U.newHashMap(rebalancingInfo.delayParts.size());

                        for (Map.Entry<Integer, Set<Integer>> e : rebalancingInfo.delayParts.entrySet()) {
                            cacheCtx = cctx.cacheContext(e.getKey());

                            List<List<ClusterNode>> idealAssignment = cacheCtx.affinity().idealAssignment();

                            assert idealAssignment != null;

                            Set<Integer> parts = e.getValue();

                            Map<Integer, List<UUID>> partAssignments = U.newHashMap(parts.size());

                            for (Integer part : parts)
                                partAssignments.put(part, toIds0(idealAssignment.get(part)));

                            assignmentsChange.put(e.getKey(), partAssignments);
                        }

                        msg = new CacheAffinityChangeMessage(rebalancingInfo.topVer, assignmentsChange);
                    }
                }
            }
        }

        try {
            if (msg != null) {
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
        if (F.eq(cacheName, "aff_log_cache"))
            log.info(msg);
    }

    /**
     * @param cacheId Cache ID.
     * @return Topology.
     */
    public GridDhtPartitionTopology clientTopology(int cacheId) {
        return clientTops.get(cacheId);
    }

    /**
     * @param cacheId Cache ID.
     * @param exchFut Exchange future.
     * @return Topology.
     */
    public GridDhtPartitionTopology clientTopology(int cacheId, GridDhtPartitionsExchangeFuture exchFut) {
        GridClientPartitionTopology top = clientTops.get(cacheId);

        if (top != null)
            return top;

        GridClientPartitionTopology old = clientTops.putIfAbsent(cacheId,
            top = new GridClientPartitionTopology(cctx, cacheId, exchFut));

        return old != null ? old : top;
    }

    /**
     * @return Collection of client topologies.
     */
    public Collection<GridClientPartitionTopology> clientTopologies() {
        return clientTops.values();
    }

    /**
     * @param cacheId Cache ID.
     * @return Client partition topology.
     */
    public GridClientPartitionTopology clearClientTopology(int cacheId) {
        return clientTops.remove(cacheId);
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

//    public void initAffinity(AffinityTopologyVersion topVer,
//        Map<Integer, List<List<UUID>>> aff,
//        Collection<GridCacheContext> caches) {
//        for (GridCacheContext cctx : caches) {
//            List<List<UUID>> ids = aff.get(cctx.cacheId());
//
//            List<List<ClusterNode>> nodes;
//
//            if (ids != null)
//                nodes = toNodes(topVer, cctx, ids);
//            else
//                nodes = cctx.affinity().pendingAssignment();
//
//            assert nodes != null;
//
//            cctx.affinity().initializeAffinity(topVer, nodes);
//        }
//    }

    private List<List<UUID>> toIds(List<List<ClusterNode>> nodes) {
        List<List<UUID>> ids = new ArrayList<>(nodes.size());

        for (int p = 0; p < nodes.size(); p++) {
            List<ClusterNode> partNodes = nodes.get(p);

            ids.add(toIds0(partNodes));
        }

        return ids;
    }

    private List<UUID> toIds0(List<ClusterNode> nodes) {
        List<UUID> partIds = new ArrayList<>(nodes.size());

        for (int i = 0; i < nodes.size(); i++)
            partIds.add(nodes.get(i).id());

        return partIds;
    }

    private List<List<ClusterNode>> toNodes(AffinityTopologyVersion topVer,
        GridCacheContext cctx,
        List<List<UUID>> ids) {
        assert ids.size() == cctx.affinity().partitions();

        List<List<ClusterNode>> nodes = new ArrayList<>(ids.size());

        for (int p = 0; p < ids.size(); p++) {
            List<UUID> partIds = ids.get(p);

            List<ClusterNode> partNodes = toNodes(topVer, partIds);

            nodes.add(partNodes);
        }

        return nodes;
    }

    private List<ClusterNode> toNodes(AffinityTopologyVersion topVer, List<UUID> ids) {
        List<ClusterNode> nodes = new ArrayList<>(ids.size());

        for (int i = 0; i < ids.size(); i++) {
            ClusterNode node = cctx.discovery().node(topVer, ids.get(i));

            nodes.add(node);
        }

        return nodes;
    }

    public Map<Integer, List<List<UUID>>> affinity(AffinityTopologyVersion topVer) {
        assert affHist.containsKey(topVer) : topVer;

        return affHist.get(topVer);
    }

    /**
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     */
    public void onClientExchange(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        GridDhtPartitionExchangeId exchId = fut.exchangeId();

        AffinityTopologyVersion topVer = fut.topologyVersion();

        //Map<Integer, List<List<UUID>>> affHist = new HashMap<>();

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal() || fut.stopping(cacheCtx.cacheId()))
                continue;

            GridDhtPartitionTopology top = cacheCtx.topology();

            top.updateTopologyVersion(exchId, fut, -1, fut.stopping(cacheCtx.cacheId()));

            if (cacheCtx.affinity().affinityTopologyVersion() == AffinityTopologyVersion.NONE) {
                List<List<ClusterNode>> aff = calculateAffinity(cacheCtx, fut);

                cacheCtx.affinity().initializeAffinity(topVer, aff);

                top.beforeExchange(fut, aff);
            }
            else
                cacheCtx.affinity().clientEventTopologyChange(fut.discoveryEvent(), topVer);

           // affHist.put(cacheCtx.cacheId(), toIds(cacheCtx.affinity().assignments(topVer)));
        }

       // this.affHist.put(topVer, affHist);
    }

    /**
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     */
    public void onExchangeStart(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
//        AffinityTopologyVersion topVer = fut.topologyVersion();
//
//        boolean delayedAffAssign = delayedAffinityAssignment(topVer);
//
//        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
//            if (cacheCtx.isLocal() || fut.stopping(cacheCtx.cacheId()))
//                continue;
//
//            List<List<ClusterNode>> aff = calculateAffinity(cacheCtx, fut);
//
//            if (!delayedAffAssign) {
//                cacheCtx.affinity().pendingAssignment(null);
//
//                cacheCtx.affinity().initializeAffinity(topVer, aff);
//            }
//            else
//                cacheCtx.affinity().pendingAssignment(aff);
//
//            cacheCtx.preloader().onTopologyChanged(fut);
//        }
    }

    public void onCacheCreate(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
    }

    public void onCacheDestroy(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
    }

    /**
     * @param exchFut Exchange future.
     * @param msg Affinity change message.
     */
    public void onExchangeAffinityMessage(GridDhtPartitionsExchangeFuture exchFut, CacheAffinityChangeMessage msg) {
        log.info("Process exchange affinity change message [exchVer=" + exchFut.topologyVersion() + ']');

        assert exchFut.exchangeId().equals(msg.exchangeId()) : msg;

        AffinityTopologyVersion topVer = exchFut.topologyVersion();

        Map<Integer, Map<Integer, List<UUID>>> assignment = msg.assignmentChange();

        assert !F.isEmpty(assignment) : msg;

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal())
                continue;

            int parts = cacheCtx.affinity().partitions();

            List<List<ClusterNode>> newAssignment = new ArrayList<>(parts);

            Map<Integer, List<UUID>> cacheAssignment = assignment.get(cacheCtx.cacheId());

            assert cacheAssignment != null;

            for (int p = 0; p < parts; p++) {
                List<UUID> ids = cacheAssignment.get(p);

                newAssignment.add(toNodes(topVer, ids));
            }

            cacheCtx.affinity().initializeAffinity(topVer, newAssignment);
        }
    }

    /**
     * @param exchFut Exchange future.
     * @param msg Message.
     * @return {@code True} if affinity changed.
     * @throws IgniteCheckedException If failed.
     */
    public boolean onChangeAffinityMessage(GridDhtPartitionsExchangeFuture exchFut, CacheAffinityChangeMessage msg)
        throws IgniteCheckedException {
        assert affCalcVer != null && affCalcVer.topologyVersion() > 0 : affCalcVer;
        assert msg.topologyVersion() != null : msg;
        assert msg.exchangeId() == null : msg;

        AffinityTopologyVersion topVer = exchFut.topologyVersion();

        if (affCalcVer.equals(msg.topologyVersion())) {
            log.info("Process affinity change message [exchVer=" + exchFut.topologyVersion() +
                ", affCalcVer=" + affCalcVer +
                ", msgVer=" + msg.topologyVersion() +']');

            Map<Integer, Map<Integer, List<UUID>>> affChange = msg.assignmentChange();

            assert !F.isEmpty(affChange) : msg;

            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                if (cacheCtx.isLocal())
                    continue;

                AffinityTopologyVersion affTopVer = cacheCtx.affinity().affinityTopologyVersion();

                assert affTopVer.topologyVersion() > 0 : affTopVer;

                List<List<ClusterNode>> curAff = cacheCtx.affinity().assignments(affTopVer);

                Map<Integer, List<UUID>> change = affChange.get(cacheCtx.cacheId());

                if (change != null) {
                    assert !change.isEmpty() : msg;

                    List<List<ClusterNode>> assignment = new ArrayList<>(curAff);

                    for (Map.Entry<Integer, List<UUID>> e : change.entrySet()) {
                        Integer part = e.getKey();

                        List<ClusterNode> nodes = toNodes(topVer, e.getValue());

                        if (LOG_AFF_CHANGE) {
                            logAffinityChange(log, cacheCtx.name(), "New assignment [cache=" + cacheCtx.name() +
                                ", part=" + part +
                                ", cur=" + F.nodeIds(assignment.get(part)) +
                                ", new=" + F.nodeIds(nodes) + ']');
                        }

                        assert !nodes.equals(assignment.get(part));

                        assignment.set(part, nodes);
                    }

                    cacheCtx.affinity().initializeAffinity(topVer, assignment);
                }
                else
                    cacheCtx.affinity().clientEventTopologyChange(exchFut.discoveryEvent(), topVer);
            }

            return true;
        }
        else {
            log.info("Ignore affinity change message [exchVer=" + exchFut.topologyVersion() +
                ", affCalcVer=" + affCalcVer +
                ", msgVer=" + msg.topologyVersion() +']');

            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                if (cacheCtx.isLocal())
                    continue;

                AffinityTopologyVersion affTopVer = cacheCtx.affinity().affinityTopologyVersion();

                assert affTopVer.topologyVersion() > 0 : affTopVer;

                cacheCtx.affinity().clientEventTopologyChange(exchFut.discoveryEvent(), topVer);
            }

            return false;
        }
    }

    /**
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     */
    public void onServerJoin(GridDhtPartitionsExchangeFuture fut, boolean crd) throws IgniteCheckedException {
        AffinityTopologyVersion topVer = fut.topologyVersion();

        ClusterNode joinNode = fut.discoveryEvent().eventNode();

        assert !joinNode.isClient();

        RebalanceWait rebalanceWait = null;

        if (joinNode.isLocal()) {
            if (crd) {
                for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                    if (cacheCtx.isLocal())
                        continue;

                    List<List<ClusterNode>> newAff = calculateAffinity(cacheCtx, fut);

                    cacheCtx.affinity().initializeAffinity(topVer, newAff);
                }
            }
            else {
                List<GridDhtAssignmentFetchFuture> fetchFuts = new ArrayList<>();

                for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                    if (cacheCtx.isLocal())
                        continue;

                    GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cacheCtx,
                        topVer,
                        CU.affinityNodes(cacheCtx, topVer));

                    fetchFut.init();

                    fetchFuts.add(fetchFut);
                }

                for (int i = 0; i < fetchFuts.size(); i++) {
                    GridDhtAssignmentFetchFuture fetchFut = fetchFuts.get(i);

                    List<List<ClusterNode>> aff = fetchFut.get();

                    if (aff == null)
                        aff = fetchFut.context().affinity().calculateAffinity(topVer, fut.discoveryEvent());

                    fetchFut.context().affinity().initializeAffinity(topVer, aff);
                }
            }
        }
        else
            rebalanceWait = initAffinityDelayNewPrimary(fut, crd);

        synchronized (mux) {
            affCalcVer = fut.topologyVersion();

            if (rebalanceWait != null)
                rebalancingInfo = new RebalancingInfo(topVer, rebalanceWait.waitCaches);
            else
                rebalancingInfo = null;
        }
    }

    /**
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     */
    private RebalanceWait initAffinityDelayNewPrimary(GridDhtPartitionsExchangeFuture fut, boolean crd)
        throws IgniteCheckedException {
        RebalanceWait rebalanceWait = crd ? new RebalanceWait() : null;

        AffinityTopologyVersion topVer = fut.topologyVersion();

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal())
                continue;

            AffinityTopologyVersion affTopVer = cacheCtx.affinity().affinityTopologyVersion();

            assert affTopVer.topologyVersion() > 0 : affTopVer;

            List<List<ClusterNode>> curAff = cacheCtx.affinity().assignments(affTopVer);

            assert canCalculateAffinity(cacheCtx, fut);

            List<List<ClusterNode>> idealAssignment = calculateAffinity(cacheCtx, fut);
            List<List<ClusterNode>> newAssignment = null;

            for (int p = 0; p < idealAssignment.size(); p++) {
                List<ClusterNode> newNodes = idealAssignment.get(p);
                List<ClusterNode> curNodes = curAff.get(p);

                ClusterNode curPrimary = curNodes.size() > 0 ? curNodes.get(0) : null;
                ClusterNode newPrimary = newNodes.size() > 0 ? newNodes.get(0) : null;

                if (curPrimary != null && newPrimary != null && !curPrimary.equals(newPrimary)) {
                    assert cctx.discovery().node(topVer, curPrimary.id()) != null : curPrimary;

                    List<ClusterNode> nodes0 = delayedPrimary(cacheCtx.cacheId(),
                        p,
                        curPrimary,
                        newNodes,
                        rebalanceWait);

                    if (newAssignment == null)
                        newAssignment = new ArrayList<>(idealAssignment);

                    newAssignment.set(p, nodes0);
                }
            }

            cacheCtx.affinity().idealAssignment(idealAssignment);
            cacheCtx.affinity().initializeAffinity(fut.topologyVersion(), newAssignment);
        }

        return (rebalanceWait != null && rebalanceWait.waitCaches != null) ? rebalanceWait : null;
    }

    private static class RebalanceWait {
        Map<Integer, Map<Integer, UUID>> waitCaches = null;
    }

    private List<ClusterNode> delayedPrimary(
        Integer cacheId,
        int part,
        ClusterNode curPrimary,
        List<ClusterNode> newNodes,
        RebalanceWait rebalance) {
        List<ClusterNode> nodes0 = new ArrayList<>(newNodes.size() + 1);

        nodes0.add(curPrimary);

        for (int i = 0; i < newNodes.size(); i++) {
            ClusterNode node = newNodes.get(i);

            if (!node.equals(curPrimary))
                nodes0.add(node);
        }

        if (rebalance != null) {
            if (LOG_AFF_CHANGE) {
                String cacheName = cctx.cacheContext(cacheId).name();

                logAffinityChange(log, cacheName, "Delayed primary assignment [cache=" + cacheName +
                    ", part=" + part +
                    ", curPrimary=" + curPrimary.id() +
                    ", newNodes=" + F.nodeIds(newNodes) + ']');
            }

            if (rebalance.waitCaches == null)
                rebalance.waitCaches = new HashMap<>();

            Map<Integer, UUID> cacheRebalanceWait = rebalance.waitCaches.get(cacheId);

            if (cacheRebalanceWait == null)
                rebalance.waitCaches.put(cacheId, cacheRebalanceWait = new HashMap<>());

            cacheRebalanceWait.put(part, newNodes.get(0).id());
        }

        return nodes0;
    }

    /**
     * @param fut Exchange future.
     * @return Affinity assignment.
     * @throws IgniteCheckedException If failed.
     */
    public Map<Integer, Map<Integer, List<UUID>>> initAffinityConsiderState(GridDhtPartitionsExchangeFuture fut)
        throws IgniteCheckedException {
        RebalanceWait rebalanceWait = new RebalanceWait();

        AffinityTopologyVersion topVer = fut.topologyVersion();

        Collection<ClusterNode> aliveNodes = cctx.discovery().nodes(topVer);

        Map<Integer, Map<Integer, List<UUID>>> assignment = new HashMap<>();

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal())
                continue;

            AffinityTopologyVersion affTopVer = cacheCtx.affinity().affinityTopologyVersion();

            assert affTopVer.topologyVersion() > 0 : affTopVer;

            List<List<ClusterNode>> curAssignment = cacheCtx.affinity().assignments(affTopVer);

            List<List<ClusterNode>> newAssignment = calculateAffinity(cacheCtx, fut);

            GridDhtPartitionTopology top = cacheCtx.topology();

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
                                    newNodes = delayedPrimary(cacheCtx.cacheId(),
                                        p,
                                        curPrimary,
                                        newNodes,
                                        rebalanceWait);
                                }
                            }
                        }
                        else {
                            GridDhtPartitionState state = top.partitionState(newPrimary.id(), p);

                            if (state != GridDhtPartitionState.OWNING) {
                                List<ClusterNode> owners = top.owners(p);

                                if (!owners.isEmpty()) {
                                    ClusterNode primary = owners.get(0);

                                    newNodes = delayedPrimary(cacheCtx.cacheId(),
                                        p,
                                        primary,
                                        newNodes,
                                        rebalanceWait);
                                }
                            }
                        }
                    }
                }

                cacheAssignment.put(p, toIds0(newNodes));
            }

            assignment.put(cacheCtx.cacheId(), cacheAssignment);
        }

        synchronized (mux) {
            affCalcVer = topVer;

            if (rebalanceWait.waitCaches != null)
                rebalancingInfo = new RebalancingInfo(topVer, rebalanceWait.waitCaches);
            else
                rebalancingInfo = null;
        }

        return assignment;
    }

    /**
     * @param fut Exchange future.
     * @param oldest Oldest node flag.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if affinity should be assigned by coordinator.
     */
    public boolean onServerLeft(GridDhtPartitionsExchangeFuture fut, boolean oldest) throws IgniteCheckedException {
        ClusterNode leftNode = fut.discoveryEvent().eventNode();

        assert !leftNode.isClient() : leftNode;

        RebalanceWait rebalanceWait = null;

        boolean centralizedAff = false;

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal())
                continue;

            AffinityTopologyVersion affTopVer = cacheCtx.affinity().affinityTopologyVersion();

            assert affTopVer.topologyVersion() > 0 : affTopVer;

            GridAffinityAssignment assignment = cacheCtx.affinity().assignment(affTopVer);

            if (!assignment.primaryPartitions(leftNode.id()).isEmpty()) {
                centralizedAff = true;

                break;
            }
        }

        if (centralizedAff) {
            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                if (cacheCtx.isLocal())
                    continue;

                assert canCalculateAffinity(cacheCtx, fut);

                List<List<ClusterNode>> assignment = calculateAffinity(cacheCtx, fut);

                cacheCtx.affinity().idealAssignment(assignment);
            }
        }
        else
            rebalanceWait = initAffinityDelayNewPrimary(fut, oldest);

        synchronized (mux) {
            affCalcVer = fut.topologyVersion();

            if (rebalanceWait != null)
                rebalancingInfo = new RebalancingInfo(fut.topologyVersion(), rebalanceWait.waitCaches);
            else
                rebalancingInfo = null;
        }

        return centralizedAff;
    }

    /**
     * @param cacheCtx Cache context.
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     * @return Affinity assignments.
     */
    private List<List<ClusterNode>> calculateAffinity(GridCacheContext cacheCtx,
        GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        GridDhtPartitionExchangeId exchId = fut.exchangeId();

        if (canCalculateAffinity(cacheCtx, fut)) {
            if (log.isDebugEnabled())
                log.debug("Will recalculate affinity [locNodeId=" + cctx.localNodeId() + ", exchId=" + exchId + ']');

            List<List<ClusterNode>> assignment =
                    cacheCtx.affinity().calculateAffinity(exchId.topologyVersion(), fut.discoveryEvent());

            cacheCtx.affinity().idealAssignment(assignment);

            return assignment;
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Will request affinity from remote node [locNodeId=" + cctx.localNodeId() + ", exchId=" +
                    exchId + ']');

            // Fetch affinity assignment from remote node.
            GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cacheCtx,
                exchId.topologyVersion(),
                CU.affinityNodes(cacheCtx, exchId.topologyVersion()));

            fetchFut.init();

            List<List<ClusterNode>> affAssignment = fetchFut.get();

            if (log.isDebugEnabled())
                log.debug("Fetched affinity from remote node, initializing affinity assignment [locNodeId=" +
                    cctx.localNodeId() + ", topVer=" + exchId.topologyVersion() + ']');

            if (affAssignment == null) {
                affAssignment = new ArrayList<>(cacheCtx.affinity().partitions());

                List<ClusterNode> empty = Collections.emptyList();

                for (int i = 0; i < cacheCtx.affinity().partitions(); i++)
                    affAssignment.add(empty);
            }

            return affAssignment;
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @param fut Exchange future.
     * @return {@code True} if local node can calculate affinity on it's own for this partition map exchange.
     */
    private boolean canCalculateAffinity(GridCacheContext cacheCtx, GridDhtPartitionsExchangeFuture fut) {
        GridDhtPartitionExchangeId exchId = fut.exchangeId();

        AffinityFunction affFunc = cacheCtx.config().getAffinity();

        // Do not request affinity from remote nodes if affinity function is not centralized.
        if (!U.hasAnnotation(affFunc, AffinityCentralizedFunction.class))
            return true;

        // If local node did not initiate exchange or local node is the only cache node in grid.
        Collection<ClusterNode> affNodes = CU.affinityNodes(cacheCtx, exchId.topologyVersion());

        return fut.cacheStarted(cacheCtx.cacheId()) ||
            !exchId.nodeId().equals(cctx.localNodeId()) ||
            (affNodes.size() == 1 && affNodes.contains(cctx.localNode()));
    }

    /**
     *
     */
    class RebalancingInfo {
        /** */
        private AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, Map<Integer, UUID>> waitCaches;

        /** */
        private Map<Integer, Set<Integer>> delayParts;

        /**
         * @param topVer
         * @param waitCaches
         */
        public RebalancingInfo(AffinityTopologyVersion topVer, Map<Integer, Map<Integer, UUID>> waitCaches) {
            this.topVer = topVer;
            this.waitCaches = waitCaches;

            delayParts = U.newHashMap(waitCaches.size());

            for (Map.Entry<Integer, Map<Integer, UUID>> e : waitCaches.entrySet()) {
                Set<Integer> parts = U.newHashSet(e.getValue().size());

                parts.addAll(e.getValue().keySet());

                delayParts.put(e.getKey(), parts);
            }
        }
    }
}
