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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CommunicationFailureContext;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
class ZkCommunicationFailureContext implements CommunicationFailureContext {
    /** */
    private static final Comparator<ClusterNode> NODE_ORDER_CMP = new Comparator<ClusterNode>() {
        @Override public int compare(ClusterNode node1, ClusterNode node2) {
            return Long.compare(node1.order(), node2.order());
        }
    };

    /** */
    private Set<ClusterNode> killedNodes = new HashSet<>();

    /** */
    private final Map<UUID, BitSet> nodesState;

    /** */
    private final List<ClusterNode> initialNodes;

    /** */
    private final List<ClusterNode> curNodes;

    /** */
    private final GridCacheSharedContext<?, ?> ctx;

    /**
     * @param ctx Context.
     * @param curNodes Current topology snapshot.
     * @param initialNodes Topology snapshot when communication error resolve started.
     * @param nodesState Nodes communication state.
     */
    ZkCommunicationFailureContext(
        GridCacheSharedContext<?, ?> ctx,
        List<ClusterNode> curNodes,
        List<ClusterNode> initialNodes,
        Map<UUID, BitSet> nodesState)
    {
        this.ctx = ctx;
        this.curNodes = Collections.unmodifiableList(curNodes);
        this.initialNodes = initialNodes;
        this.nodesState = nodesState;
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> topologySnapshot() {
        return curNodes;
    }

    /** {@inheritDoc} */
    @Override public boolean connectionAvailable(ClusterNode node1, ClusterNode node2) {
        BitSet nodeState = nodesState.get(node1.id());

        if (nodeState == null)
            throw new IllegalArgumentException("Invalid node: " + node1);

        int nodeIdx = Collections.binarySearch(initialNodes, node2, NODE_ORDER_CMP);

        if (nodeIdx < 0)
            throw new IllegalArgumentException("Invalid node: " + node2);

        assert nodeIdx < nodeState.size() : nodeIdx;

        return nodeState.get(nodeIdx);
    }

    /** {@inheritDoc} */
    @Override public Map<String, CacheConfiguration<?, ?>> startedCaches() {
        Map<Integer, DynamicCacheDescriptor> cachesMap = ctx.affinity().caches();

        Map<String, CacheConfiguration<?, ?>> res = U.newHashMap(cachesMap.size());

        for (DynamicCacheDescriptor desc : cachesMap.values()) {
            if (desc.cacheType().userCache())
                res.put(desc.cacheName(), desc.cacheConfiguration());
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> cacheAffinity(String cacheName) {
        if (cacheName == null)
            throw new NullPointerException("Null cache name.");

        DynamicCacheDescriptor cacheDesc = ctx.affinity().caches().get(CU.cacheId(cacheName));

        if (cacheDesc == null)
            throw new IllegalArgumentException("Invalid cache name: " + cacheName);

        GridAffinityAssignmentCache aff = ctx.affinity().groupAffinity(cacheDesc.groupId());

        assert aff != null : cacheName;

        return aff.readyAssignments(aff.lastVersion());
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> cachePartitionOwners(String cacheName) {
        if (cacheName == null)
            throw new NullPointerException("Null cache name.");

        DynamicCacheDescriptor cacheDesc = ctx.affinity().caches().get(CU.cacheId(cacheName));

        if (cacheDesc == null)
            throw new IllegalArgumentException("Invalid cache name: " + cacheName);

        if (cacheDesc.cacheConfiguration().getCacheMode() == CacheMode.LOCAL)
            return Collections.emptyList();

        CacheGroupContext grp = ctx.cache().cacheGroup(cacheDesc.groupId());

        GridDhtPartitionTopology top;

        if (grp == null) {
            top = ctx.exchange().clientTopologyIfExists(cacheDesc.groupId());

            assert top != null : cacheName;
        }
        else
            top = grp.topology();

        return top.allOwners();
    }

    /** {@inheritDoc} */
    @Override public void killNode(ClusterNode node) {
        if (node == null)
            throw new NullPointerException();

        if (Collections.binarySearch(curNodes, node, NODE_ORDER_CMP) < 0)
            throw new IllegalArgumentException("Invalid node: " + node);

        killedNodes.add(node);
    }

    /**
     * @return Nodes to fail.
     */
    Set<ClusterNode> killedNodes() {
        return killedNodes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ZkCommunicationFailureContext []";
    }
}
