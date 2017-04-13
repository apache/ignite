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

package org.apache.ignite.spi.discovery.tcp.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.PN;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Convenient way to represent topology for {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi}
 */
public class TcpDiscoveryNodesRing {
    /** Visible nodes filter. */
    public static final IgnitePredicate<TcpDiscoveryNode> VISIBLE_NODES = new P1<TcpDiscoveryNode>() {
        @Override public boolean apply(TcpDiscoveryNode node) {
            if (node.visible()) {
                assert node.order() > 0 : "Invalid node order: " + node;

                return true;
            }

            return false;
        }
    };

    /** Client nodes filter. */
    private static final PN CLIENT_NODES = new PN() {
        @Override public boolean apply(ClusterNode node) {
            return node.isClient();
        }
    };

    /** Local node. */
    private TcpDiscoveryNode locNode;

    /** All nodes in topology. */
    @GridToStringInclude
    private NavigableSet<TcpDiscoveryNode> nodes;

    /** All started nodes. */
    @GridToStringExclude
    private Map<UUID, TcpDiscoveryNode> nodesMap = new HashMap<>();

    /** Number of old nodes in topology. */
    @GridToStringExclude
    private long oldNodesCount = 0L;

    /** Current topology version */
    private long topVer;

    /** */
    private long nodeOrder;

    /** */
    private long maxInternalOrder;

    /** Lock. */
    @GridToStringExclude
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** */
    private IgniteProductVersion minNodeVer;

    /** Comparator is used for choice next node in ring. */
    private final Comparator<TcpDiscoveryNode> nodeComparator = new RegionNodeComparator();

    public TcpDiscoveryNodesRing() {
        nodes = new TreeSet<>(nodeComparator);
    }

    /**
     * @return Minimum node version.
     */
    public IgniteProductVersion minimumNodeVersion() {
        rwLock.readLock().lock();

        try {
            return minNodeVer;
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Sets local node.
     *
     * @param locNode Local node.
     */
    public void localNode(TcpDiscoveryNode locNode) {
        assert locNode != null;

        rwLock.writeLock().lock();

        try {
            this.locNode = locNode;

            notSyncClear();
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Gets all nodes in the topology.
     *
     * @return Collection of all nodes.
     */
    public Collection<TcpDiscoveryNode> allNodes() {
        return nodes();
    }

    /**
     * Gets visible nodes in the topology.
     *
     * @return Collection of visible nodes.
     */
    public Collection<TcpDiscoveryNode> visibleNodes() {
        return nodes(VISIBLE_NODES);
    }

    /**
     * Gets remote nodes.
     *
     * @return Collection of remote nodes in grid.
     */
    public Collection<TcpDiscoveryNode> remoteNodes() {
        return nodes(F.remoteNodes(locNode.id()));
    }

    /**
     * Gets visible remote nodes in the topology.
     *
     * @return Collection of visible remote nodes.
     */
    public Collection<TcpDiscoveryNode> visibleRemoteNodes() {
        return nodes(VISIBLE_NODES, F.remoteNodes(locNode.id()));
    }

    /**
     * @return Client nodes.
     */
    public Collection<TcpDiscoveryNode> clientNodes() {
        return nodes(CLIENT_NODES);
    }

    /**
     * Checks whether the topology has remote nodes in.
     *
     * @return {@code true} if the topology has remote nodes in.
     */
    public boolean hasRemoteNodes() {
        rwLock.readLock().lock();

        try {
            return nodes.size() > 1;
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Checks whether the topology has remote server nodes in.
     *
     * @return {@code true} if the topology has remote server nodes in.
     */
    public boolean hasRemoteServerNodes() {
        rwLock.readLock().lock();

        try {
            if (nodes.size() < 2)
                return false;

            for (TcpDiscoveryNode node : nodes)
                if (!node.isClient() && !node.id().equals(locNode.id()))
                    return true;

            return false;
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Adds node to topology, also initializes node last update time with current
     * system time.
     *
     * @param node Node to add.
     * @return {@code true} if such node was added and did not present previously in the topology.
     */
    public boolean add(TcpDiscoveryNode node) {
        assert node != null;
        assert node.internalOrder() > 0;

        rwLock.writeLock().lock();

        try {
            if (nodesMap.containsKey(node.id()))
                return false;

            long maxInternalOrder0 = notSyncMaxInternalOrder();

            assert node.internalOrder() > maxInternalOrder0 : "Adding node to the middle of the ring " +
                "[ring=" + this + ", node=" + node + ']';

            nodesMap.put(node.id(), node);

            if (!node.version().greaterThanEqual(2, 0, 0)) {
                oldNodesCount++;
                TreeSet<TcpDiscoveryNode> nodesTmp = new TreeSet<>();
                nodesTmp.addAll(nodes);
                nodes = nodesTmp;
            }
            else
                nodes = new TreeSet<>(nodes);

            node.lastUpdateTime(U.currentTimeMillis());
            nodes.add(node);

            nodeOrder = node.internalOrder();

            if (maxInternalOrder < node.internalOrder())
                maxInternalOrder = node.internalOrder();

            initializeMinimumVersion();
        }
        finally {
            rwLock.writeLock().unlock();
        }

        return true;
    }

    /**
     * Must called on read lock.
     *
     * @return Max internal order.
     */
    private long notSyncMaxInternalOrder() {
        if (maxInternalOrder == 0)
            return -1;
        return maxInternalOrder;
    }

    /**
     * @return Max internal order.
     */
    public long maxInternalOrder() {
        rwLock.readLock().lock();

        try {
            return notSyncMaxInternalOrder();
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Restores topology from parameters values.
     * <p>
     * This method is called when new node receives topology from coordinator.
     * In this case all nodes received are remote for local.
     * <p>
     * Also initializes nodes last update time with current system time.
     *
     * @param nodes List of remote nodes.
     * @param topVer Topology version.
     */
    public void restoreTopology(Iterable<TcpDiscoveryNode> nodes, long topVer) {
        assert !F.isEmpty(nodes);
        assert topVer > 0;

        rwLock.writeLock().lock();

        try {
            locNode.internalOrder(topVer);

            notSyncClear();

            for (TcpDiscoveryNode node : nodes) {
                if (!node.version().greaterThanEqual(2, 0, 0))
                    oldNodesCount++;
            }

            if (oldNodesCount > 0) {
                TreeSet<TcpDiscoveryNode> nodesTmp = new TreeSet<>();
                nodesTmp.addAll(this.nodes);
                this.nodes = nodesTmp;
            }
            else
                this.nodes = new TreeSet<>(this.nodes);

            for (TcpDiscoveryNode node : nodes) {
                if (nodesMap.containsKey(node.id()))
                    continue;

                nodesMap.put(node.id(), node);
                node.lastUpdateTime(U.currentTimeMillis());
                this.nodes.add(node);

                if (maxInternalOrder < node.internalOrder())
                    maxInternalOrder = node.internalOrder();
            }

            nodeOrder = topVer;

            initializeMinimumVersion();
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Finds node by ID.
     *
     * @param nodeId Node id to find.
     * @return Node with ID provided or {@code null} if not found.
     */
    @Nullable public TcpDiscoveryNode node(UUID nodeId) {
        assert nodeId != null;

        rwLock.readLock().lock();

        try {
            return nodesMap.get(nodeId);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Removes node from the topology.
     *
     * @param nodeId ID of the node to remove.
     * @return {@code true} if node was removed.
     */
    @Nullable public TcpDiscoveryNode removeNode(UUID nodeId) {
        assert nodeId != null;
        assert !locNode.id().equals(nodeId);

        rwLock.writeLock().lock();

        try {
            TcpDiscoveryNode rmv = nodesMap.remove(nodeId);

            if (rmv != null) {
                if (!rmv.version().greaterThanEqual(2, 0, 0) && --oldNodesCount <= 0) {
                    TreeSet<TcpDiscoveryNode> nodesTmp = new TreeSet<>(nodeComparator);
                    nodesTmp.addAll(nodes);
                    nodes = nodesTmp;
                }
                else
                    nodes = new TreeSet<>(nodes);

                nodes.remove(rmv);
                if (maxInternalOrder == rmv.internalOrder()) {
                    long newMaxInternalOrder = 0;
                    for (TcpDiscoveryNode node : nodes) {
                        if (newMaxInternalOrder < node.internalOrder())
                            newMaxInternalOrder = node.internalOrder();
                    }
                    maxInternalOrder = newMaxInternalOrder;
                }
            }

            initializeMinimumVersion();

            return rmv;
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Reset local node's order.
     *
     */
    public void resetLocalNodeOrder() {
        rwLock.writeLock().lock();
        try {
            locNode.order(1);
            locNode.internalOrder(1);
            locNode.visible(true);

            notSyncClear();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Removes all remote nodes, leaves only local node.
     * <p>
     * Must be called on write lock
     */
    private void notSyncClear() {
        nodes = new TreeSet<>(nodeComparator);
        oldNodesCount = 0L;

        if (locNode != null) {
            nodes.add(locNode);
            maxInternalOrder = locNode.internalOrder();
        } else
            maxInternalOrder = 0;

        nodesMap = new HashMap<>();

        if (locNode != null)
            nodesMap.put(locNode.id(), locNode);

        nodeOrder = 0;
        topVer = 0;
        minNodeVer = locNode.version();
    }

    /**
     * Removes all remote nodes, leaves only local node.
     * <p>
     * This should be called when SPI should be disconnected from topology and
     * reconnected back after.
     */
    public void clear() {
        rwLock.writeLock().lock();

        try {
            notSyncClear();
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Finds coordinator in the topology.
     *
     * @return Coordinator node that gives versions to topology (node with the smallest order).
     */
    @Nullable public TcpDiscoveryNode coordinator() {
        rwLock.readLock().lock();

        try {
            if (F.isEmpty(nodes))
                return null;

            return notSyncCoordinator(null);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Finds coordinator in the topology filtering excluded nodes from the search.
     * <p>
     * Must called on read lock.
     * <p>
     * This may be used when handling current coordinator leave or failure.
     *
     * @param excluded Nodes to exclude from the search (optional).
     * @return Coordinator node among remaining nodes or {@code null} if all nodes are excluded.
     */
    @Nullable private TcpDiscoveryNode notSyncCoordinator(@Nullable Collection<TcpDiscoveryNode> excluded) {
        Collection<TcpDiscoveryNode> filtered = serverNodes(excluded);

        if (F.isEmpty(filtered))
            return null;

        return Collections.min(filtered);
    }

    /**
     * Finds coordinator in the topology filtering excluded nodes from the search.
     * <p>
     * This may be used when handling current coordinator leave or failure.
     *
     * @param excluded Nodes to exclude from the search (optional).
     * @return Coordinator node among remaining nodes or {@code null} if all nodes are excluded.
     */
    @Nullable public TcpDiscoveryNode coordinator(@Nullable Collection<TcpDiscoveryNode> excluded) {
        rwLock.readLock().lock();

        try {
            return notSyncCoordinator(excluded);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Finds next node in the topology.
     *
     * @return Next node.
     */
    @Nullable public TcpDiscoveryNode nextNode() {
        rwLock.readLock().lock();

        try {
            if (nodes.size() < 2)
                return null;

            return notSyncNextNode(null);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Finds next node in the topology filtering excluded nodes from search.
     * <p>
     * Must called on read lock.
     * <p>
     * This may be used when detecting and handling nodes failure.
     *
     * @param excluded Nodes to exclude from the search (optional). If provided, cannot contain local node.
     * @return Next node or {@code null} if all nodes were filtered out or topology contains less than two nodes.
     */
    @Nullable private TcpDiscoveryNode notSyncNextNode(@Nullable Collection<TcpDiscoveryNode> excluded) {
        assert locNode.internalOrder() > 0 : locNode;
        assert excluded == null || excluded.isEmpty() || !excluded.contains(locNode) : excluded;

        Iterator<TcpDiscoveryNode> filtered = serverNodes(excluded, locNode).iterator();

        if (filtered.hasNext())
            return filtered.next();
        else {
            filtered = serverNodes(excluded).iterator();

            if (filtered.hasNext()) {
                TcpDiscoveryNode firstNode = filtered.next();
                //When locNode is first and last.
                if (firstNode.equals(locNode))
                    return null;
                else
                    return firstNode;
            }
            else
                return null;
        }
    }

    /**
     * Finds next node in the topology filtering excluded nodes from search.
     * <p>
     * This may be used when detecting and handling nodes failure.
     *
     * @param excluded Nodes to exclude from the search (optional). If provided, cannot contain local node.
     * @return Next node or {@code null} if all nodes were filtered out or topology contains less than two nodes.
     */
    @Nullable public TcpDiscoveryNode nextNode(@Nullable Collection<TcpDiscoveryNode> excluded) {
        assert locNode.internalOrder() > 0 : locNode;
        assert excluded == null || excluded.isEmpty() || !excluded.contains(locNode) : excluded;

        rwLock.readLock().lock();

        try {
            return notSyncNextNode(excluded);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Gets current topology version.
     *
     * @return Current topology version.
     */
    public long topologyVersion() {
        rwLock.readLock().lock();

        try {
            return topVer;
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Sets new topology version.
     *
     * @param topVer New topology version (should be greater than current, otherwise no-op).
     * @return {@code True} if topology has been changed.
     */
    public boolean topologyVersion(long topVer) {
        rwLock.writeLock().lock();

        try {
            if (this.topVer < topVer) {
                this.topVer = topVer;

                return true;
            }

            return false;
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Increments topology version and gets new value.
     *
     * @return Topology version (incremented).
     */
    public long incrementTopologyVersion() {
        rwLock.writeLock().lock();

        try {
            return ++topVer;
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Increments topology version and gets new value.
     *
     * @return Topology version (incremented).
     */
    public long nextNodeOrder() {
        rwLock.writeLock().lock();

        try {
            if (nodeOrder == 0)
                nodeOrder = notSyncMaxInternalOrder();

            long temp = ++nodeOrder;
            return temp;
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * @param p Filters.
     * @return Unmodifiable collection of nodes.
     */
    private Collection<TcpDiscoveryNode> nodes(IgnitePredicate<? super TcpDiscoveryNode>... p) {
        rwLock.readLock().lock();

        try {
            List<TcpDiscoveryNode> list = U.arrayList(nodes, p);

            return Collections.unmodifiableCollection(list);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Gets server nodes from topology.
     *
     * @param excluded Nodes to exclude from the search (optional).
     * @return Collection of server nodes.
     */
    private Collection<TcpDiscoveryNode> serverNodes(@Nullable final Collection<TcpDiscoveryNode> excluded) {
        final boolean excludedEmpty = F.isEmpty(excluded);

        return F.view(nodes, new P1<TcpDiscoveryNode>() {
            @Override public boolean apply(TcpDiscoveryNode node) {
                return !node.isClient() && (excludedEmpty || !excluded.contains(node));
            }
        });
    }

    /**
     * Gets server nodes from topology in part of ring is started from specific node.
     *
     * @param excluded Nodes to exclude from the search (optional).
     * @param from Start position in ring (exclude)
     * @return Collection of server nodes.
     */
    private Collection<TcpDiscoveryNode> serverNodes(@Nullable final Collection<TcpDiscoveryNode> excluded,
        TcpDiscoveryNode from) {
        final boolean excludedEmpty = F.isEmpty(excluded);
        NavigableSet<TcpDiscoveryNode> nodesSet = nodes.tailSet(from, false);
        if (nodesSet.isEmpty()) return Collections.EMPTY_SET;
        return F.view(nodesSet, new P1<TcpDiscoveryNode>() {
            @Override
            public boolean apply(TcpDiscoveryNode node) {
                return !node.isClient() && (excludedEmpty || !excluded.contains(node));
            }
        });
    }

    /** */
    private void initializeMinimumVersion() {
        minNodeVer = null;

        for (TcpDiscoveryNode node : nodes) {
            if (minNodeVer == null || node.version().compareTo(minNodeVer) < 0)
                minNodeVer = node.version();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        rwLock.readLock().lock();

        try {
            return S.toString(TcpDiscoveryNodesRing.class, this);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }
}
