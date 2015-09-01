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

package org.apache.ignite.internal.client;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.client.util.GridClientConsistentHash;
import org.apache.ignite.internal.client.util.GridClientUtils;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Affinity function for partitioned cache. This function supports the following
 * configuration:
 * <ul>
 * <li>
 *      {@code backupFilter} - Optional filter for back up nodes. If provided, then only
 *      nodes that pass this filter will be selected as backup nodes and only nodes that
 *      don't pass this filter will be selected as primary nodes. If not provided, then
 *      primary and backup nodes will be selected out of all nodes available for this cache.
 *      <p>
 *      NOTE: In situations where there are no primary nodes at all, i.e. no nodes for which backup
 *      filter returns {@code false}, first backup node for the key will be considered primary.
 * </li>
 * </ul>
*/
@SuppressWarnings("NullableProblems")
public class GridClientPartitionAffinity implements GridClientDataAffinity, GridClientTopologyListener {
    /**
     * This resolver is used to provide alternate hash ID, other than node ID.
     * <p>
     * Node IDs constantly change when nodes get restarted, which causes them to
     * be placed on different locations in the hash ring, and hence causing
     * repartitioning. Providing an alternate hash ID, which survives node restarts,
     * puts node on the same location on the hash ring, hence minimizing required
     * repartitioning.
     */
    @SuppressWarnings("PublicInnerClass")
    public static interface HashIdResolver {
        /**
         * Gets alternate hash ID, other than node ID.
         *
         * @param node Node.
         * @return Hash ID.
         */
        public Object getHashId(GridClientNode node);
    }

    /** Default number of partitions. */
    public static final int DFLT_PARTITION_CNT = 10000;

    /** Node hash. */
    private final GridClientConsistentHash<NodeInfo> nodeHash;

    /** Hash id resolver. */
    private HashIdResolver hashIdRslvr = new HashIdResolver() {
        @Override public Object getHashId(GridClientNode node) {
            return node.consistentId();
        }
    };

    /** Total number of partitions. */
    private int parts = DFLT_PARTITION_CNT;

    /** Cached added node infos. */
    private final ConcurrentMap<UUID, NodeInfo> addedNodes = new ConcurrentHashMap<>();

    /** Optional backup filter. */
    private GridClientPredicate<UUID> backupFilter;

    /** Optional backup filter. */
    private final GridClientPredicate<NodeInfo> backupIdFilter =new GridClientPredicate<NodeInfo>() {
        @Override public boolean apply(NodeInfo info) {
            return backupFilter == null || backupFilter.apply(info.nodeId());
        }
    };

    /** Optional primary filter. */
    private final GridClientPredicate<NodeInfo> primaryIdFilter = new GridClientPredicate<NodeInfo>() {
        @Override public boolean apply(NodeInfo info) {
            return backupFilter == null || !backupFilter.apply(info.nodeId());
        }
    };

    /**
     * Empty constructor with all defaults.
     */
    public GridClientPartitionAffinity() {
        this(null, null);
    }

    /**
     * Initializes optional counts for replicas and backups.
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code backupFilter} is set.
     *
     * @param parts Total number of partitions. If {@code null} than {@link #DFLT_PARTITION_CNT} will be used.
     * @param backupFilter Optional back up filter for nodes. If provided, then primary nodes
     *      will be selected from all nodes outside of this filter, and backups will be selected
     *      from all nodes inside it.
     */
    public GridClientPartitionAffinity(Integer parts, GridClientPredicate<UUID> backupFilter) {
        this.parts = parts == null ? DFLT_PARTITION_CNT : parts;
        this.backupFilter = backupFilter;

        nodeHash = new GridClientConsistentHash<>();
    }

    /**
     * Gets total number of key partitions. To ensure that all partitions are
     * equally distributed across all nodes, please make sure that this
     * number is significantly larger than a number of nodes. Also, partition
     * size should be relatively small. Try to avoid having partitions with more
     * than quarter million keys.
     * <p>
     * Note that for fully replicated caches this method should always
     * return {@code 1}.
     *
     * @return Total partition count.
     */
    public int getPartitions() {
        return parts;
    }

    /**
     * Sets total number of partitions.
     *
     * @param parts Total number of partitions.
     */
    public void setPartitions(int parts) {
        this.parts = parts;
    }

    /**
     * Gets hash ID resolver for nodes. This resolver is used to provide
     * alternate hash ID, other than node ID.
     * <p>
     * Node IDs constantly change when nodes get restarted, which causes them to
     * be placed on different locations in the hash ring, and hence causing
     * repartitioning. Providing an alternate hash ID, which survives node restarts,
     * puts node on the same location on the hash ring, hence minimizing required
     * repartitioning.
     *
     * @return Hash ID resolver.
     */
    public HashIdResolver getHashIdResolver() {
        return hashIdRslvr;
    }

    /**
     * Sets hash ID resolver for nodes. This resolver is used to provide
     * alternate hash ID, other than node ID.
     * <p>
     * Node IDs constantly change when nodes get restarted, which causes them to
     * be placed on different locations in the hash ring, and hence causing
     * repartitioning. Providing an alternate hash ID, which survives node restarts,
     * puts node on the same location on the hash ring, hence minimizing required
     * repartitioning.
     *
     * @param hashIdRslvr Hash ID resolver.
     */
    public void setHashIdResolver(HashIdResolver hashIdRslvr) {
        this.hashIdRslvr = hashIdRslvr;
    }

    /**
     * Gets optional backup filter. If not {@code null}, then primary nodes will be
     * selected from all nodes outside of this filter, and backups will be selected
     * from all nodes inside it.
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code backupFilter} is set.
     *
     * @return Optional backup filter.
     */
    public GridClientPredicate<UUID> getBackupFilter() {
        return backupFilter;
    }

    /**
     * Sets optional backup filter. If provided, then primary nodes will be selected
     * from all nodes outside of this filter, and backups will be selected from all
     * nodes inside it.
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code backupFilter} is set.
     *
     * @param backupFilter Optional backup filter.
     */
    public void setBackupFilter(GridClientPredicate<UUID> backupFilter) {
        this.backupFilter = backupFilter;
    }

    /** {@inheritDoc} */
    @Override public GridClientNode node(Object key, Collection<? extends GridClientNode> nodes) {
        if (nodes == null || nodes.isEmpty())
            return null;

        if (nodes.size() == 1) // Minor optimization.
            return GridClientUtils.first(nodes);

        final Map<NodeInfo, GridClientNode> lookup = U.newHashMap(nodes.size());

        // Store nodes in map for fast lookup.
        for (GridClientNode node : nodes)
            // Get node info and update consistent hash, if required.
            lookup.put(resolveNodeInfo(node), node);

        final Collection<NodeInfo> nodeInfos = lookup.keySet();

        NodeInfo nodeInfo;
        int part = partition(key);

        if (backupFilter == null)
            nodeInfo = nodeHash.node(part, nodeInfos);
        else {
            nodeInfo = nodeHash.node(part, primaryIdFilter, GridClientUtils.contains(nodeInfos));

            if (nodeInfo == null)
                // Select from backup nodes.
                nodeInfo = nodeHash.node(part, backupIdFilter, GridClientUtils.contains(nodeInfos));
        }

        return lookup.get(nodeInfo);
    }

    /** {@inheritDoc} */
    private int partition(Object key) {
        return Math.abs(key.hashCode() % getPartitions());
    }

    /** {@inheritDoc} */
    @Override public void onNodeAdded(GridClientNode node) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onNodeRemoved(GridClientNode node) {
        UUID nodeId = node.nodeId();
        NodeInfo nodeInfo = addedNodes.remove(nodeId);

        if (nodeInfo == null)
            return;

        nodeHash.removeNode(nodeInfo);
    }

    /**
     * Resolve node info for specified node.
     * Add node to hash circle if this is the first node invocation.
     *
     * @param n Node to get info for.
     * @return Node info.
     */
    private NodeInfo resolveNodeInfo(GridClientNode n) {
        UUID nodeId = n.nodeId();
        NodeInfo nodeInfo = addedNodes.get(nodeId);

        if (nodeInfo != null)
            return nodeInfo;

        nodeInfo = new NodeInfo(nodeId, hashIdRslvr == null ? nodeId : hashIdRslvr.getHashId(n));

        addedNodes.put(nodeId, nodeInfo);
        nodeHash.addNode(nodeInfo, 1);

        return nodeInfo;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());

        sb.append(" [nodeHash=").append(nodeHash).
            append(", hashIdRslvr=").append(hashIdRslvr).
            append(", parts=").append(parts).
            append(", addedNodes=").append(addedNodes).
            append(", backupFilter=").append(backupFilter).append("]");

        return sb.toString();
    }

    /**
     * Node hash ID.
     */
    private static final class NodeInfo implements Comparable<NodeInfo> {
        /** Node ID. */
        private final UUID nodeId;

        /** Hash ID. */
        private final Object hashId;

        /**
         * @param nodeId Node ID.
         * @param hashId Hash ID.
         */
        private NodeInfo(UUID nodeId, Object hashId) {
            assert nodeId != null;
            assert hashId != null;

            this.hashId = hashId;
            this.nodeId = nodeId;
        }

        /**
         * @return Node ID.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Hash ID.
         */
        public Object hashId() {
            return hashId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hashId.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (!(obj instanceof NodeInfo))
                return false;

            NodeInfo that = (NodeInfo)obj;

            // If objects are equal, hash codes should be the same.
            // Cannot use that.hashId.equals(hashId) due to Comparable<N> interface restrictions.
            return that.nodeId.equals(nodeId) && that.hashCode() == hashCode();
        }

        /** {@inheritDoc} */
        @Override public int compareTo(NodeInfo o) {
            int diff = nodeId.compareTo(o.nodeId);

            if (diff == 0)
                diff = Integer.compare(hashCode(), o.hashCode());

            return diff;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return NodeInfo.class.getSimpleName() +
                " [nodeId=" + nodeId +
                ", hashId=" + hashId + ']';
        }
    }
}