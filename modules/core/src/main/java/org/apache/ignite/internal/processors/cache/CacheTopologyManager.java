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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap2;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 *
 */
public class CacheTopologyManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /** */
    public static final IgniteProductVersion DELAY_AFF_ASSIGN_SINCE = IgniteProductVersion.fromString("1.6.0");

    /** */
    private final ConcurrentHashMap<Integer, CacheInfo> cachesInfo = new ConcurrentHashMap<>();

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

    public void onExchangeStart() {

    }

    void cacheCreated(Integer cacheId, List<List<ClusterNode>> aff) {
        List<PartitionInfo> parts = new ArrayList<>(aff.size());

        for (int part = 0; part < aff.size(); part++) {
            List<ClusterNode> nodes = aff.get(part);

            parts.add(new PartitionInfo(nodes, nodes.get(0)));
        }

        CacheInfo cacheInfo = new CacheInfo(parts);

        CacheInfo old = cachesInfo.putIfAbsent(cacheId, cacheInfo);

        assert old == null : old;
    }

    void affinityCalculated(Integer cacheId, List<List<ClusterNode>> assignment) {
        CacheInfo cacheInfo = cachesInfo.get(cacheId);

        assert cacheInfo != null : cacheId;

        List<PartitionInfo> parts = cacheInfo.parts;

        for (int part = 0; part < assignment.size(); part++) {
            PartitionInfo info = parts.get(part);

            info.affNodes = assignment.get(part);
        }
    }

    private void changeAffinity(List<PartitionInfo> curAff, List<List<ClusterNode>> newAff, GridDhtPartitionFullMap partMap) {
        int parts = curAff.size();

        List<List<ClusterNode>> resAff = new ArrayList<>(parts);

        for (int part = 0; part < parts; part++) {
            PartitionInfo partInfo = curAff.get(part);

            List<ClusterNode> newNodes = newAff.get(part);

            List<ClusterNode> resNodes = newNodes;

            ClusterNode curPrimary = partInfo.primary;
            ClusterNode newPrimary = newNodes.get(0);

            if (!curPrimary.equals(newPrimary)) {
                GridDhtPartitionMap2 map = partMap.get(newPrimary.id());

                GridDhtPartitionState state = map.get(part);

                if (state != GridDhtPartitionState.OWNING && !ownersEmpty(part, partMap)) {
                    resNodes = new ArrayList<>();

                    resNodes.add(curPrimary);

                    for (ClusterNode newNode : newNodes) {
                        if (!newNode.equals(curPrimary))
                            resNodes.add(newNode);
                    }
                }
            }

            resAff.add(resNodes);
        }
    }

    /**
     * @param part Partition.
     * @param partMap Partition map.
     * @return {@code True} if there are no partition owners for given partition.
     */
    private boolean ownersEmpty(Integer part, GridDhtPartitionFullMap partMap) {
        for (GridDhtPartitionMap2 map : partMap.values()) {
            GridDhtPartitionState state = map.get(part);

            if (state == GridDhtPartitionState.OWNING)
                return false;
        }

        return true;
    }

    /**
     *
     */
    private static class CacheInfo {
        /** */
        final List<PartitionInfo> parts;

        /**
         * @param parts Partitions info.
         */
        public CacheInfo(List<PartitionInfo> parts) {
            this.parts = parts;
        }
    }

    /**
     *
     */
    private static class PartitionInfo {
        /** */
        List<ClusterNode> affNodes;

        /** */
        ClusterNode primary;

        /**
         * @param affNodes Affinity function nodes.
         * @param primary Primary node.
         */
        public PartitionInfo(List<ClusterNode> affNodes, ClusterNode primary) {
            this.affNodes = affNodes;
            this.primary = primary;
        }
    }
}
