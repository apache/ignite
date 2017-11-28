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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.mxbean.CacheGroupMetricsMXBean;

/**
 * Management bean that provides access to {@link CacheGroupContext}.
 */
public class CacheGroupMetricsMXBeanImpl implements CacheGroupMetricsMXBean {
    /** Cache group context. */
    private final CacheGroupContext ctx;

    /** Interface describing a predicate of two integers. */
    private interface IntBiPredicate {
        /**
         * Predicate body.
         */
        boolean apply(int a, int b);
    }

    /**
     * Creates MBean;
     *
     * @param ctx Cache group context.
     */
    public CacheGroupMetricsMXBeanImpl(CacheGroupContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public int getCacheGroupId() {
        return ctx.groupId();
    }

    /** {@inheritDoc} */
    @Override public List<String> getCaches() {
        List<String> caches = new ArrayList<>(ctx.caches().size());

        for (GridCacheContext cache : ctx.caches())
            caches.add(cache.name());

        Collections.sort(caches);

        return caches;
    }

    /** {@inheritDoc} */
    @Override public int getBackups() {
        return ctx.config().getBackups();
    }

    /** {@inheritDoc} */
    @Override public int getPartitions() {
        return ctx.topology().partitions();
    }

    /**
     * Calculates the number of partition copies for all partitions of this cache group and filter values by the
     * predicate.
     *
     * @param pred Predicate.
     */
    private int numberOfPartitionCopies(IntBiPredicate pred) {
        int partitions = ctx.topology().partitions();

        GridDhtPartitionFullMap partFullMap = ctx.topology().partitionMap(true);

        int resNumOfCopies = 0;

        for (int part = 0; part < partitions; part++) {
            int partNumOfCopies = 0;

            for (Map.Entry<UUID, GridDhtPartitionMap> entry : partFullMap.entrySet()) {
                if (entry.getValue().get(part) == GridDhtPartitionState.OWNING)
                    partNumOfCopies++;
            }

            if (part == 0 || pred.apply(resNumOfCopies, partNumOfCopies))
                resNumOfCopies = partNumOfCopies;
        }

        return resNumOfCopies;
    }

    /** {@inheritDoc} */
    @Override public int getMinimumNumberOfPartitionCopies() {
        return numberOfPartitionCopies(new IntBiPredicate() {
            @Override public boolean apply(int a, int b) {
                return b < a;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public int getMaximumNumberOfPartitionCopies() {
        return numberOfPartitionCopies(new IntBiPredicate() {
            @Override public boolean apply(int a, int b) {
                return b > a;
            }
        });
    }

    /**
     * Count of partitions with a given state on the node.
     *
     * @param nodeId Node id.
     * @param state State.
     */
    private int nodePartitionsCountByState(UUID nodeId, GridDhtPartitionState state) {
        int partitions = ctx.topology().partitions();

        GridDhtPartitionMap partMap = ctx.topology().partitionMap(true).get(nodeId);

        int cnt = 0;

        for (int part = 0; part < partitions; part++)
            if (partMap.get(part) == state)
                cnt++;

        return cnt;
    }

    /**
     * Count of partitions with a given state in the entire cluster.
     *
     * @param state State.
     */
    private int clusterPartitionsCountByState(GridDhtPartitionState state) {
        GridDhtPartitionFullMap partFullMap = ctx.topology().partitionMap(true);

        int cnt = 0;

        for (UUID nodeId : partFullMap.keySet())
            cnt += nodePartitionsCountByState(nodeId, state);

        return cnt;
    }

    /** {@inheritDoc} */
    @Override public int getLocalNodeOwningPartitionsCount() {
        return nodePartitionsCountByState(ctx.shared().localNodeId(), GridDhtPartitionState.OWNING);
    }

    /** {@inheritDoc} */
    @Override public int getLocalNodeMovingPartitionsCount() {
        return nodePartitionsCountByState(ctx.shared().localNodeId(), GridDhtPartitionState.MOVING);
    }

    /** {@inheritDoc} */
    @Override public int getClusterOwningPartitionsCount() {
        return clusterPartitionsCountByState(GridDhtPartitionState.OWNING);
    }

    /** {@inheritDoc} */
    @Override public int getClusterMovingPartitionsCount() {
        return clusterPartitionsCountByState(GridDhtPartitionState.MOVING);
    }

    /**
     * Gets partitions allocation map with a given state.
     *
     * @param state State.
     * @return Partitions allocation map.
     */
    private Map<Integer, Set<String>> clusterPartitionsMapByState(GridDhtPartitionState state) {
        int partitions = ctx.topology().partitions();

        GridDhtPartitionFullMap partFullMap = ctx.topology().partitionMap(true);

        Map<Integer, Set<String>> partitionsMap = new LinkedHashMap<>();

        for (int part = 0; part < partitions; part++) {
            Set<String> partNodesSet = new HashSet<>();

            for (Map.Entry<UUID, GridDhtPartitionMap> entry : partFullMap.entrySet()) {
                if (entry.getValue().get(part) == state)
                    partNodesSet.add(entry.getKey().toString());
            }

            partitionsMap.put(part, partNodesSet);
        }

        return partitionsMap;
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Set<String>> getOwningPartitionsAllocationMap() {
        return clusterPartitionsMapByState(GridDhtPartitionState.OWNING);
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Set<String>> getMovingPartitionsAllocationMap() {
        return clusterPartitionsMapByState(GridDhtPartitionState.MOVING);
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, List<String>> getAffinityPartitionsAssignmentMap() {
        AffinityAssignment assignment = ctx.affinity().cachedAffinity(AffinityTopologyVersion.NONE);

        int part = 0;

        Map<Integer, List<String>> assignmentMap = new LinkedHashMap<>();

        for (List<ClusterNode> partAssignment : assignment.assignment()) {
            List<String> partNodeIds = new ArrayList<>(partAssignment.size());

            for (ClusterNode node : partAssignment)
                partNodeIds.add(node.id().toString());

            assignmentMap.put(part, partNodeIds);

            part++;
        }

        return assignmentMap;
    }
}
