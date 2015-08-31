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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.PN;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

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
    private NavigableSet<TcpDiscoveryNode> nodes = new TreeSet<>();

    /** All started nodes. */
    @GridToStringExclude
    private Map<UUID, TcpDiscoveryNode> nodesMap = new HashMap<>();

    /** Current topology version */
    private long topVer;

    /** */
    private long nodeOrder;

    /** Lock. */
    @GridToStringExclude
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

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

            clear();
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

            assert node.internalOrder() > maxInternalOrder() : "Adding node to the middle of the ring " +
                "[ring=" + this + ", node=" + node + ']';

            nodesMap.put(node.id(), node);

            nodes = new TreeSet<>(nodes);

            node.lastUpdateTime(U.currentTimeMillis());

            nodes.add(node);

            nodeOrder = node.internalOrder();
        }
        finally {
            rwLock.writeLock().unlock();
        }

        return true;
    }

    /**
     * @return Max internal order.
     */
    public long maxInternalOrder() {
        rwLock.readLock().lock();

        try {
            TcpDiscoveryNode last = nodes.last();

            return last != null ? last.internalOrder() : -1;
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

            clear();

            boolean firstAdd = true;

            for (TcpDiscoveryNode node : nodes) {
                if (nodesMap.containsKey(node.id()))
                    continue;

                nodesMap.put(node.id(), node);

                if (firstAdd) {
                    this.nodes = new TreeSet<>(this.nodes);

                    firstAdd = false;
                }

                node.lastUpdateTime(U.currentTimeMillis());

                this.nodes.add(node);
            }

            nodeOrder = topVer;
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
                nodes = new TreeSet<>(nodes);

                nodes.remove(rmv);
            }

            return rmv;
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Removes nodes from the topology.
     *
     * @param nodeIds IDs of the nodes to remove.
     * @return Collection of removed nodes.
     */
    public Collection<TcpDiscoveryNode> removeNodes(Collection<UUID> nodeIds) {
        assert !F.isEmpty(nodeIds);

        rwLock.writeLock().lock();

        try {
            boolean firstRmv = true;

            Collection<TcpDiscoveryNode> res = null;

            for (UUID id : nodeIds) {
                TcpDiscoveryNode rmv = nodesMap.remove(id);

                if (rmv != null) {
                    if (firstRmv) {
                        nodes = new TreeSet<>(nodes);

                        res = new ArrayList<>(nodeIds.size());

                        firstRmv = false;
                    }

                    nodes.remove(rmv);

                    res.add(rmv);
                }
            }

            return res == null ? Collections.<TcpDiscoveryNode>emptyList() : res;
        }
        finally {
            rwLock.writeLock().unlock();
        }
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
            nodes = new TreeSet<>();

            if (locNode != null)
                nodes.add(locNode);

            nodesMap = new HashMap<>();

            if (locNode != null)
                nodesMap.put(locNode.id(), locNode);

            nodeOrder = 0;

            topVer = 0;
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

            return coordinator(null);
        }
        finally {
            rwLock.readLock().unlock();
        }
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
            Collection<TcpDiscoveryNode> filtered = serverNodes(excluded);

            if (F.isEmpty(filtered))
                return null;

            return Collections.min(filtered);
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

            return nextNode(null);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Finds next node in the topology filtering excluded nodes from search.
     * <p>
     * This may be used when detecting and handling nodes failure.
     *
     * @param excluded Nodes to exclude from the search (optional). If provided,
     * cannot contain local node.
     * @return Next node or {@code null} if all nodes were filtered out or
     * topology contains less than two nodes.
     */
    @Nullable public TcpDiscoveryNode nextNode(@Nullable Collection<TcpDiscoveryNode> excluded) {
        assert excluded == null || excluded.isEmpty() || !excluded.contains(locNode);

        rwLock.readLock().lock();

        try {
            Collection<TcpDiscoveryNode> filtered = serverNodes(excluded);

            if (filtered.size() < 2)
                return null;

            Iterator<TcpDiscoveryNode> iter = filtered.iterator();

            while (iter.hasNext()) {
                TcpDiscoveryNode node = iter.next();

                if (locNode.equals(node))
                    break;
            }

            return iter.hasNext() ? iter.next() : F.first(filtered);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Finds previous node in the topology.
     *
     * @return Previous node.
     */
    @Nullable public TcpDiscoveryNode previousNode() {
        rwLock.readLock().lock();

        try {
            if (nodes.size() < 2)
                return null;

            return previousNode(null);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Finds previous node in the topology filtering excluded nodes from search.
     *
     * @param excluded Nodes to exclude from the search (optional). If provided,
     * cannot contain local node.
     * @return Previous node or {@code null} if all nodes were filtered out or
     * topology contains less than two nodes.
     */
    @Nullable public TcpDiscoveryNode previousNode(@Nullable Collection<TcpDiscoveryNode> excluded) {
        assert excluded == null || excluded.isEmpty() || !excluded.contains(locNode);

        rwLock.readLock().lock();

        try {
            Collection<TcpDiscoveryNode> filtered = serverNodes(excluded);

            if (filtered.size() < 2)
                return null;

            Iterator<TcpDiscoveryNode> iter = filtered.iterator();

            while (iter.hasNext()) {
                TcpDiscoveryNode node = iter.next();

                if (locNode.equals(node))
                    break;
            }

            return iter.hasNext() ? iter.next() : F.first(filtered);
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
            if (nodeOrder == 0) {
                TcpDiscoveryNode last = nodes.last();

                assert last != null;

                nodeOrder = last.internalOrder();
            }

            return ++nodeOrder;
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