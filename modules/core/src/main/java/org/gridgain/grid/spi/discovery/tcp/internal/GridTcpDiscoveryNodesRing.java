/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.internal;

import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.locks.*;

/**
 * Convenient way to represent topology for {@link GridTcpDiscoverySpi}
 */
public class GridTcpDiscoveryNodesRing {
    /** Local node. */
    private GridTcpDiscoveryNode locNode;

    /** All nodes in topology. */
    @GridToStringInclude
    private NavigableSet<GridTcpDiscoveryNode> nodes = new TreeSet<>();

    /** All started nodes. */
    @GridToStringExclude
    private Map<UUID, GridTcpDiscoveryNode> nodesMap = new HashMap<>();

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
    public void localNode(GridTcpDiscoveryNode locNode) {
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
    public Collection<GridTcpDiscoveryNode> allNodes() {
        rwLock.readLock().lock();

        try {
            return Collections.unmodifiableCollection(new ArrayList<>(nodes));
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Gets remote nodes.
     *
     * @return Collection of remote nodes in grid.
     */
    public Collection<GridTcpDiscoveryNode> remoteNodes() {
        rwLock.readLock().lock();

        try {
            return Collections.unmodifiableCollection(new ArrayList<>(F.view(nodes,
                F.<GridTcpDiscoveryNode>remoteNodes(locNode.id()))));
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * @return Client nodes.
     */
    public Collection<GridTcpDiscoveryNode> clientNodes() {
        rwLock.readLock().lock();

        try {
            return new ArrayList<>(F.view(nodes, new P1<GridTcpDiscoveryNode>() {
                @Override public boolean apply(GridTcpDiscoveryNode node) {
                    return node.isClient();
                }
            }));
        }
        finally {
            rwLock.readLock().unlock();
        }
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
     * Adds node to topology, also initializes node last update time with current
     * system time.
     *
     * @param node Node to add.
     * @return {@code true} if such node was added and did not present previously in the topology.
     */
    public boolean add(GridTcpDiscoveryNode node) {
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
            GridTcpDiscoveryNode last = nodes.last();

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
    public void restoreTopology(Iterable<GridTcpDiscoveryNode> nodes, long topVer) {
        assert !F.isEmpty(nodes);
        assert topVer > 0;

        rwLock.writeLock().lock();

        try {
            locNode.internalOrder(topVer);

            clear();

            boolean firstAdd = true;

            for (GridTcpDiscoveryNode node : nodes) {
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
    @Nullable public GridTcpDiscoveryNode node(UUID nodeId) {
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
    @Nullable public GridTcpDiscoveryNode removeNode(UUID nodeId) {
        assert nodeId != null;
        assert !locNode.id().equals(nodeId);

        rwLock.writeLock().lock();

        try {
            GridTcpDiscoveryNode rmv = nodesMap.remove(nodeId);

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
    public Collection<GridTcpDiscoveryNode> removeNodes(Collection<UUID> nodeIds) {
        assert !F.isEmpty(nodeIds);

        rwLock.writeLock().lock();

        try {
            boolean firstRmv = true;

            Collection<GridTcpDiscoveryNode> res = null;

            for (UUID id : nodeIds) {
                GridTcpDiscoveryNode rmv = nodesMap.remove(id);

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

            return res == null ? Collections.<GridTcpDiscoveryNode>emptyList() : res;
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
    @Nullable public GridTcpDiscoveryNode coordinator() {
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
    @Nullable public GridTcpDiscoveryNode coordinator(@Nullable Collection<GridTcpDiscoveryNode> excluded) {
        rwLock.readLock().lock();

        try {
            Collection<GridTcpDiscoveryNode> filtered = serverNodes(excluded);

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
    @Nullable public GridTcpDiscoveryNode nextNode() {
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
    @Nullable public GridTcpDiscoveryNode nextNode(@Nullable Collection<GridTcpDiscoveryNode> excluded) {
        assert excluded == null || excluded.isEmpty() || !excluded.contains(locNode);

        rwLock.readLock().lock();

        try {
            Collection<GridTcpDiscoveryNode> filtered = serverNodes(excluded);

            if (filtered.size() < 2)
                return null;

            Iterator<GridTcpDiscoveryNode> iter = filtered.iterator();

            while (iter.hasNext()) {
                GridTcpDiscoveryNode node = iter.next();

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
    @Nullable public GridTcpDiscoveryNode previousNode() {
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
    @Nullable public GridTcpDiscoveryNode previousNode(@Nullable Collection<GridTcpDiscoveryNode> excluded) {
        assert excluded == null || excluded.isEmpty() || !excluded.contains(locNode);

        rwLock.readLock().lock();

        try {
            Collection<GridTcpDiscoveryNode> filtered = serverNodes(excluded);

            if (filtered.size() < 2)
                return null;

            Iterator<GridTcpDiscoveryNode> iter = filtered.iterator();

            while (iter.hasNext()) {
                GridTcpDiscoveryNode node = iter.next();

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
                GridTcpDiscoveryNode last = nodes.last();

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
     * Gets server nodes from topology.
     *
     * @param excluded Nodes to exclude from the search (optional).
     * @return Collection of server nodes.
     */
    private Collection<GridTcpDiscoveryNode> serverNodes(@Nullable final Collection<GridTcpDiscoveryNode> excluded) {
        final boolean excludedEmpty = F.isEmpty(excluded);

        return F.view(nodes, new P1<GridTcpDiscoveryNode>() {
            @Override public boolean apply(GridTcpDiscoveryNode node) {
                return !node.isClient() && (excludedEmpty || !excluded.contains(node));
            }
        });
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        rwLock.readLock().lock();

        try {
            return S.toString(GridTcpDiscoveryNodesRing.class, this);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }
}
