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
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
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
    private RebalancingInfo rebalancingInfo;

    /** */
    @GridToStringExclude
    private final ConcurrentMap<Integer, GridClientPartitionTopology> clientTops = new ConcurrentHashMap8<>();

    /** */
    private final GridBoundedConcurrentLinkedHashMap<AffinityTopologyVersion, Map<Integer, List<List<UUID>>>>
        affHist = new GridBoundedConcurrentLinkedHashMap<>(100, 100, 0.75f, 64, PER_SEGMENT_Q);

    public synchronized void checkRebalanceState(Integer cacheId) {
        if (rebalancingInfo != null) {
            Map<Integer, UUID> partWait = rebalancingInfo.rebalanceWait.get(cacheId);

            if (partWait != null) {
                GridCacheContext ctx = cctx.cacheContext(cacheId);

                GridDhtPartitionTopology top = ctx.topology();

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

                log.info("Check rebalance state " + cacheId + " " + rebalanced);

                if (rebalanced) {
                    rebalancingInfo.rebalanceWait.remove(cacheId);

                    if (rebalancingInfo.rebalanceWait.isEmpty()) {
                        log.info("Rebalance finished");

                        CacheAffinityChangeMessage msg = new CacheAffinityChangeMessage(rebalancingInfo.topVer);

                        try {
                            cctx.discovery().sendCustomEvent(msg);
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to send affinity change message.", e);
                        }

                        rebalancingInfo = null;
                    }
                }
            }
        }
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

    public void initAffinity(AffinityTopologyVersion topVer,
        Map<Integer, List<List<UUID>>> aff,
        Collection<GridCacheContext> caches) {
        for (GridCacheContext cctx : caches) {
            List<List<UUID>> ids = aff.get(cctx.cacheId());

            List<List<ClusterNode>> nodes;

            if (ids != null)
                nodes = toNodes(topVer, cctx, ids);
            else
                nodes = cctx.affinity().pendingAssignment();

            assert nodes != null;

            cctx.affinity().initializeAffinity(topVer, nodes);
        }
    }

    private List<List<UUID>> toIds(List<List<ClusterNode>> nodes) {
        List<List<UUID>> ids = new ArrayList<>(nodes.size());

        for (int p = 0; p < nodes.size(); p++) {
            List<ClusterNode> partNodes = nodes.get(p);

            List<UUID> partIds = new ArrayList<>(partNodes.size());

            for (int i = 0; i < partNodes.size(); i++)
                partIds.add(partNodes.get(i).id());

            ids.add(partIds);
        }

        return ids;
    }

    private List<List<ClusterNode>> toNodes(AffinityTopologyVersion topVer,
        GridCacheContext cctx,
        List<List<UUID>> ids) {
        assert ids.size() == cctx.affinity().partitions();

        List<List<ClusterNode>> nodes = new ArrayList<>(ids.size());

        for (int p = 0; p < ids.size(); p++) {
            List<UUID> partIds = ids.get(p);

            List<ClusterNode> partNodes = new ArrayList<>(partIds.size());

            for (int i = 0; i < partIds.size(); i++) {
                ClusterNode node = cctx.discovery().node(topVer, partIds.get(i));

                assert node != null : partIds.get(i);

                partNodes.add(node);
            }

            nodes.add(partNodes);
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

        Map<Integer, List<List<UUID>>> affHist = new HashMap<>();

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

            affHist.put(cacheCtx.cacheId(), toIds(cacheCtx.affinity().assignments(topVer)));
        }

        this.affHist.put(topVer, affHist);
    }

    /**
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     */
    public void onExchangeStart(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        AffinityTopologyVersion topVer = fut.topologyVersion();

        boolean delayedAffAssign = delayedAffinityAssignment(topVer);

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal() || fut.stopping(cacheCtx.cacheId()))
                continue;

            List<List<ClusterNode>> aff = calculateAffinity(cacheCtx, fut);

            if (!delayedAffAssign) {
                cacheCtx.affinity().pendingAssignment(null);

                cacheCtx.affinity().initializeAffinity(topVer, aff);
            }
            else
                cacheCtx.affinity().pendingAssignment(aff);

            cacheCtx.preloader().onTopologyChanged(fut);
        }
    }

    public void onCacheCreate(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
    }

    public void onCacheDestroy(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
    }

    /**
     * @param fut Exchange future.
     * @param oldest Oldest node flag.
     * @throws IgniteCheckedException If failed.
     */
    public void onServerJoin(GridDhtPartitionsExchangeFuture fut, boolean oldest) throws IgniteCheckedException {
        AffinityTopologyVersion topVer = fut.topologyVersion();

        ClusterNode joinNode = fut.discoveryEvent().eventNode();

        assert !joinNode.isClient();

        if (joinNode.isLocal()) {
            if (oldest) {
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
            initAffinityDelayNewPrimary(fut, oldest);
    }

    private void initAffinityDelayNewPrimary(GridDhtPartitionsExchangeFuture fut, boolean oldest)
        throws IgniteCheckedException {
        RebalanceWait rebalanceWait = oldest ? new RebalanceWait() : null;

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal())
                continue;

            AffinityTopologyVersion affTopVer = cacheCtx.affinity().affinityTopologyVersion();

            assert affTopVer.topologyVersion() > 0 : affTopVer;

            List<List<ClusterNode>> curAff = cacheCtx.affinity().assignments(affTopVer);

            List<List<ClusterNode>> newAff = calculateAffinity(cacheCtx, fut);

            for (int p = 0; p < newAff.size(); p++) {
                List<ClusterNode> newNodes = newAff.get(p);
                List<ClusterNode> curNodes = curAff.get(p);

                ClusterNode curPrimary = curNodes.size() > 0 ? curNodes.get(0) : null;
                ClusterNode newPrimary = newNodes.size() > 0 ? newNodes.get(0) : null;

                if (curPrimary != null && newPrimary != null && !curPrimary.equals(newPrimary)) {
                    assert cctx.discovery().node(fut.topologyVersion(), curPrimary.id()) != null : curPrimary;

                    List<ClusterNode> nodes0 = dealayedPrimary(cacheCtx.cacheId(),
                        p,
                        curPrimary,
                        newNodes,
                        rebalanceWait);

                    newAff.set(p, nodes0);
                }
            }

            cacheCtx.affinity().initializeAffinity(fut.topologyVersion(), newAff);
        }
    }

    private static class RebalanceWait {
        Map<Integer, Map<Integer, UUID>> waitCaches = null;
    }

    private List<ClusterNode> dealayedPrimary(Integer cacheId,
        int p,
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
            if (rebalance.waitCaches == null)
                rebalance.waitCaches = new HashMap<>();

            Map<Integer, UUID> cacheRebalanceWait = rebalance.waitCaches.get(cacheId);

            if (cacheRebalanceWait == null)
                rebalance.waitCaches.put(cacheId, cacheRebalanceWait = new HashMap<>());

            cacheRebalanceWait.put(p, newNodes.get(0).id());
        }

        return nodes0;
    }

    public void initAffinityConsiderState(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        RebalanceWait rebalanceWait = new RebalanceWait();

        Collection<ClusterNode> aliveNodes = cctx.discovery().nodes(fut.topologyVersion());

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.isLocal())
                continue;

            AffinityTopologyVersion affTopVer = cacheCtx.affinity().affinityTopologyVersion();

            assert affTopVer.topologyVersion() > 0 : affTopVer;

            List<List<ClusterNode>> curAff = cacheCtx.affinity().assignments(affTopVer);

            List<List<ClusterNode>> newAff = calculateAffinity(cacheCtx, fut);

            GridDhtPartitionTopology top = cacheCtx.topology();

            // TODO 10885, add 'boolean top.owner(UUID id)' method.

            for (int p = 0; p < newAff.size(); p++) {
                List<ClusterNode> newNodes = newAff.get(p);
                List<ClusterNode> curNodes = curAff.get(p);

                ClusterNode curPrimary = curNodes.size() > 0 ? curNodes.get(0) : null;
                ClusterNode newPrimary = newNodes.size() > 0 ? newNodes.get(0) : null;

                if (curPrimary != null && newPrimary != null) {
                    if (aliveNodes.contains(curPrimary)) {
                        if (!newPrimary.equals(curPrimary) && !top.owners(p).isEmpty()) {
                            GridDhtPartitionState state = top.partitionState(newPrimary.id(), p);

                            if (state != GridDhtPartitionState.OWNING) {
                                List<ClusterNode> nodes0 = dealayedPrimary(cacheCtx.cacheId(),
                                    p,
                                    curPrimary,
                                    newNodes,
                                    rebalanceWait);

                                newAff.set(p, nodes0);
                            }
                        }
                    }
                    else {
                        GridDhtPartitionState state = top.partitionState(newPrimary.id(), p);

                        if (state != GridDhtPartitionState.OWNING) {
                            List<ClusterNode> owners = top.owners(p);

                            if (!owners.isEmpty()) {
                                ClusterNode primary = owners.get(0);

                                List<ClusterNode> nodes0 = dealayedPrimary(cacheCtx.cacheId(),
                                    p,
                                    primary,
                                    newNodes,
                                    rebalanceWait);

                                newAff.set(p, nodes0);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * @param fut Exchange future.
     * @param oldest Oldest node flag.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if affinity should be assigned by coordinator.
     */
    public boolean onServerLeft(GridDhtPartitionsExchangeFuture fut, boolean oldest) throws IgniteCheckedException {
        ClusterNode leftNode = fut.discoveryEvent().eventNode();

        assert !leftNode.isClient();

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

        if (!centralizedAff)
            initAffinityDelayNewPrimary(fut, oldest);

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

            return cacheCtx.affinity().calculateAffinity(exchId.topologyVersion(), fut.discoveryEvent());
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

    public void changeAffinity(GridDhtPartitionsExchangeFuture exchFut,
        CacheAffinityChangeMessage msg) {
        AffinityTopologyVersion topVer = exchFut.topologyVersion();

        for (GridCacheContext ctx : cctx.cacheContexts()) {
            if (ctx.isLocal())
                continue;

            List<List<ClusterNode>> aff = ctx.affinity().pendingAssignment();

            if (aff != null)
                ctx.affinity().initializeAffinity(topVer, aff);
        }
    }

    /**
     *
     */
    class RebalancingInfo {
        /** */
        private AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, Map<Integer, UUID>> rebalanceWait;
    }
}
