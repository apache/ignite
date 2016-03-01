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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap2;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.GridBoundedLinkedHashMap;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.*;

/**
 *
 */
public class CacheTopologyManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /** */
    public static final IgniteProductVersion DELAY_AFF_ASSIGN_SINCE = IgniteProductVersion.fromString("1.6.0");

    /** */
    private RebalancingInfo rebalancingInfo;

    /** */
    private AffinityTopologyVersion affCalcVer;

    /** */
    private final GridBoundedConcurrentLinkedHashMap<AffinityTopologyVersion, Map<Integer, List<List<UUID>>>>
        affHist = new GridBoundedConcurrentLinkedHashMap<>(100, 100, 0.75f, 64, PER_SEGMENT_Q);

    public void onDiscoveryEvent(int type, ClusterNode node, AffinityTopologyVersion topVer) {
        if (!CU.clientNode(node) && (type == EVT_NODE_FAILED || type == EVT_NODE_LEFT || type == EVT_NODE_JOINED))
            affCalcVer = topVer;
    }

    public synchronized void checkRebalanceState(Integer cacheId) {
        if (rebalancingInfo != null) {
            List<List<ClusterNode>> aff = rebalancingInfo.caches.get(cacheId);

            if (aff != null) {
                GridCacheContext ctx = cctx.cacheContext(cacheId);

                GridDhtPartitionTopology top = ctx.topology();

                boolean rebalanced = true;

                for (int part = 0; part < aff.size(); part++) {
                    List<ClusterNode> affNodes = aff.get(part);

                    ClusterNode primary = affNodes.get(0);

                    GridDhtPartitionState state = top.partitionState(primary.id(), part);

                    if (state != GridDhtPartitionState.OWNING) {
                        rebalanced = false;

                        break;
                    }
                }

                log.info("Check rebalance state " + cacheId + " " + rebalanced);

                if (rebalanced) {
                    rebalancingInfo.caches.remove(cacheId);

                    if (rebalancingInfo.caches.isEmpty()) {
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

    public void initAffinity(AffinityTopologyVersion topVer,
        Collection<GridCacheContext> caches,
        Map<Integer, GridDhtPartitionFullMap> partMap) {
        boolean oldest = CU.oldestAliveCacheServerNode(cctx, topVer).isLocal();

        Map<Integer, List<List<ClusterNode>>> waitCaches = null;

        Map<Integer, List<List<UUID>>> aff = new HashMap<>();

        for (GridCacheContext cctx : caches) {
            AffinityTopologyVersion curVer = cctx.affinity().affinityTopologyVersion();

            List<List<ClusterNode>> newAff = cctx.affinity().pendingAssignment();

            assert newAff != null;

            if (curVer.equals(AffinityTopologyVersion.NONE)) {
                assert oldest;

                cctx.affinity().initializeAffinity(topVer, newAff);
            }
            else {
                List<List<ClusterNode>> assignments = cctx.affinity().assignments(curVer);

                GridDhtPartitionFullMap parts = partMap.get(cctx.cacheId());

                AffResult res = newAffinity(topVer, assignments, newAff, parts);

                cctx.affinity().initializeAffinity(topVer, res.aff);

                if (oldest) {
                    log.info("Will wait for cache rebalance: " + cctx.cacheId());

                    if (waitCaches == null)
                        waitCaches = new HashMap<>();

                    waitCaches.put(cctx.cacheId(), newAff);

                    aff.put(cctx.cacheId(), toIds(res.aff));
                }
            }
//            if (curVer.equals(AffinityTopologyVersion.NONE)) {
//                cctx.affinity().initializeAffinity(topVer, newAff);
//            }
//            else {
//            }
        }

        affHist.put(topVer, aff);

        if (waitCaches != null) {
            rebalancingInfo = new RebalancingInfo();

            rebalancingInfo.caches = waitCaches;
            rebalancingInfo.topVer = topVer;

            for (Integer cache : waitCaches.keySet())
                checkRebalanceState(cache);
        }
    }

    public boolean processMessage(CacheAffinityChangeMessage msg) {
        return affCalcVer.equals(msg.topologyVersion());
    }

    public void changeAffinity(GridDhtPartitionsExchangeFuture exchFut, CacheAffinityChangeMessage msg) {
        AffinityTopologyVersion topVer = exchFut.topologyVersion();

        for (GridCacheContext ctx : cctx.cacheContexts()) {
            if (ctx.isLocal())
                continue;

            List<List<ClusterNode>> aff = ctx.affinity().pendingAssignment();

            if (aff != null)
                ctx.affinity().initializeAffinity(topVer, aff);
        }
    }

    private AffResult newAffinity(
        AffinityTopologyVersion topVer,
        List<List<ClusterNode>> curAff,
        List<List<ClusterNode>> newAff,
        GridDhtPartitionFullMap partMap) {
        int parts = curAff.size();

        List<List<ClusterNode>> resAff = new ArrayList<>();

        Collection<ClusterNode> nodes = cctx.discovery().nodes(topVer);

        AffResult res = new AffResult();

        for (int part = 0; part < parts; part++) {
            List<ClusterNode> curNodes = curAff.get(part);
            List<ClusterNode> newNodes = newAff.get(part);

            ClusterNode curPrimary = curNodes.get(0);
            ClusterNode newPrimary = newNodes.get(0);

            if (nodes.contains(curPrimary)) {
                if (!curPrimary.equals(newPrimary)) {
                    GridDhtPartitionMap2 map = partMap.get(newPrimary.id());

                    GridDhtPartitionState state = map.get(part);

                    if (state != GridDhtPartitionState.OWNING && hasOwners(part, partMap)) {
                        res.tmpPrimary = true;

                        List<ClusterNode> resNodes = new ArrayList<>();

                        resNodes.add(curPrimary);

                        for (ClusterNode newNode : newNodes) {
                            if (!newNode.equals(curPrimary))
                                resNodes.add(newNode);
                        }

                        newNodes = resNodes;
                    }
                }
            }

            resAff.add(newNodes);
        }

        res.aff = resAff;

        return res;
    }

    /**
     * @param part Partition.
     * @param partMap Partition map.
     * @return {@code True} if there are partition owners for given partition.
     */
    private boolean hasOwners(Integer part, GridDhtPartitionFullMap partMap) {
        for (GridDhtPartitionMap2 map : partMap.values()) {
            GridDhtPartitionState state = map.get(part);

            if (state == GridDhtPartitionState.OWNING)
                return true;
        }

        return false;
    }

    private List<ClusterNode> owners(Integer part, GridDhtPartitionFullMap partMap) {
        return null;
    }

    static class AffResult {
        List<List<ClusterNode>> aff;

        boolean tmpPrimary;
    }

    /**
     *
     */
    class RebalancingInfo {
        /** */
        private AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, List<List<ClusterNode>>> caches;
    }
}
