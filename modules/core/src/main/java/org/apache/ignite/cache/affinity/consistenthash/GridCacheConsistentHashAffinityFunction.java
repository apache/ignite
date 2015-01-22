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

package org.apache.ignite.cache.affinity.consistenthash;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Affinity function for partitioned cache. This function supports the following
 * configuration:
 * <ul>
 * <li>
 *      {@code backups} - Use this flag to control how many back up nodes will be
 *      assigned to every key. The default value is {@code 0}.
 * </li>
 * <li>
 *      {@code replicas} - Generally the more replicas a node gets, the more key assignments
 *      it will receive. You can configure different number of replicas for a node by
 *      setting user attribute with name {@link #getReplicaCountAttributeName()} to some
 *      number. Default value is {@code 512} defined by {@link #DFLT_REPLICA_COUNT} constant.
 * </li>
 * <li>
 *      {@code backupFilter} - Optional filter for back up nodes. If provided, then only
 *      nodes that pass this filter will be selected as backup nodes. If not provided, then
 *      primary and backup nodes will be selected out of all nodes available for this cache.
 * </li>
 * </ul>
 * <p>
 * Cache affinity can be configured for individual caches via {@link CacheConfiguration#getAffinity()} method.
 */
public class GridCacheConsistentHashAffinityFunction implements GridCacheAffinityFunction {
    /** */
    private static final long serialVersionUID = 0L;

    /** Flag to enable/disable consistency check (for internal use only). */
    private static final boolean AFFINITY_CONSISTENCY_CHECK = Boolean.getBoolean("GRIDGAIN_AFFINITY_CONSISTENCY_CHECK");

    /** Default number of partitions. */
    public static final int DFLT_PARTITION_COUNT = 10000;

    /** Default replica count for partitioned caches. */
    public static final int DFLT_REPLICA_COUNT = 128;

    /**
     * Name of node attribute to specify number of replicas for a node.
     * Default value is {@code gg:affinity:node:replicas}.
     */
    public static final String DFLT_REPLICA_COUNT_ATTR_NAME = "gg:affinity:node:replicas";

    /** Node hash. */
    private transient GridConsistentHash<NodeInfo> nodeHash;

    /** Total number of partitions. */
    private int parts = DFLT_PARTITION_COUNT;

    /** */
    private int replicas = DFLT_REPLICA_COUNT;

    /** */
    private String attrName = DFLT_REPLICA_COUNT_ATTR_NAME;

    /** */
    private boolean exclNeighbors;

    /**
     * Optional backup filter. First node passed to this filter is primary node,
     * and second node is a node being tested.
     */
    private IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter;

    /** */
    private GridCacheAffinityNodeHashResolver hashIdRslvr = new GridCacheAffinityNodeAddressHashResolver();

    /** Injected grid. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Injected cache name. */
    @IgniteCacheNameResource
    private String cacheName;

    /** Injected logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** Initialization flag. */
    @SuppressWarnings("TransientFieldNotInitialized")
    private transient AtomicBoolean init = new AtomicBoolean();

    /** Latch for initializing. */
    @SuppressWarnings({"TransientFieldNotInitialized"})
    private transient CountDownLatch initLatch = new CountDownLatch(1);

    /** Nodes IDs. */
    @GridToStringInclude
    @SuppressWarnings({"TransientFieldNotInitialized"})
    private transient ConcurrentMap<UUID, NodeInfo> addedNodes = new ConcurrentHashMap<>();

    /** Optional backup filter. */
    @GridToStringExclude
    private final IgniteBiPredicate<NodeInfo, NodeInfo> backupIdFilter = new IgniteBiPredicate<NodeInfo, NodeInfo>() {
        @Override public boolean apply(NodeInfo primaryNodeInfo, NodeInfo nodeInfo) {
            return backupFilter == null || backupFilter.apply(primaryNodeInfo.node(), nodeInfo.node());
        }
    };

    /** Map of neighbors. */
    @SuppressWarnings("TransientFieldNotInitialized")
    private transient ConcurrentMap<UUID, Collection<UUID>> neighbors =
        new ConcurrentHashMap8<>();

    /**
     * Empty constructor with all defaults.
     */
    public GridCacheConsistentHashAffinityFunction() {
        // No-op.
    }

    /**
     * Initializes affinity with flag to exclude same-host-neighbors from being backups of each other
     * and specified number of backups.
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code #getBackupFilter()} is set.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups
     *      of each other.
     */
    public GridCacheConsistentHashAffinityFunction(boolean exclNeighbors) {
        this.exclNeighbors = exclNeighbors;
    }

    /**
     * Initializes affinity with flag to exclude same-host-neighbors from being backups of each other,
     * and specified number of backups and partitions.
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code #getBackupFilter()} is set.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups
     *      of each other.
     * @param parts Total number of partitions.
     */
    public GridCacheConsistentHashAffinityFunction(boolean exclNeighbors, int parts) {
        A.ensure(parts != 0, "parts != 0");

        this.exclNeighbors = exclNeighbors;
        this.parts = parts;
    }

    /**
     * Initializes optional counts for replicas and backups.
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code backupFilter} is set.
     *
     * @param parts Total number of partitions.
     * @param backupFilter Optional back up filter for nodes. If provided, backups will be selected
     *      from all nodes that pass this filter. First argument for this filter is primary node, and second
     *      argument is node being tested.
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code backupFilter} is set.
     */
    public GridCacheConsistentHashAffinityFunction(int parts,
        @Nullable IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
        A.ensure(parts != 0, "parts != 0");

        this.parts = parts;
        this.backupFilter = backupFilter;
    }

    /**
     * Gets default count of virtual replicas in consistent hash ring.
     * <p>
     * To determine node replicas, node attribute with {@link #getReplicaCountAttributeName()}
     * name will be checked first. If it is absent, then this value will be used.
     *
     * @return Count of virtual replicas in consistent hash ring.
     */
    public int getDefaultReplicas() {
        return replicas;
    }

    /**
     * Sets default count of virtual replicas in consistent hash ring.
     * <p>
     * To determine node replicas, node attribute with {@link #getReplicaCountAttributeName} name
     * will be checked first. If it is absent, then this value will be used.
     *
     * @param replicas Count of virtual replicas in consistent hash ring.s
     */
    public void setDefaultReplicas(int replicas) {
        this.replicas = replicas;
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
    public GridCacheAffinityNodeHashResolver getHashIdResolver() {
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
    public void setHashIdResolver(GridCacheAffinityNodeHashResolver hashIdRslvr) {
        this.hashIdRslvr = hashIdRslvr;
    }

    /**
     * Gets optional backup filter. If not {@code null}, backups will be selected
     * from all nodes that pass this filter. First node passed to this filter is primary node,
     * and second node is a node being tested.
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code backupFilter} is set.
     *
     * @return Optional backup filter.
     */
    @Nullable public IgniteBiPredicate<ClusterNode, ClusterNode> getBackupFilter() {
        return backupFilter;
    }

    /**
     * Sets optional backup filter. If provided, then backups will be selected from all
     * nodes that pass this filter. First node being passed to this filter is primary node,
     * and second node is a node being tested.
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code backupFilter} is set.
     *
     * @param backupFilter Optional backup filter.
     */
    public void setBackupFilter(@Nullable IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
        this.backupFilter = backupFilter;
    }

    /**
     * Gets optional attribute name for replica count. If not provided, the
     * default is {@link #DFLT_REPLICA_COUNT_ATTR_NAME}.
     *
     * @return User attribute name for replica count for a node.
     */
    public String getReplicaCountAttributeName() {
        return attrName;
    }

    /**
     * Sets optional attribute name for replica count. If not provided, the
     * default is {@link #DFLT_REPLICA_COUNT_ATTR_NAME}.
     *
     * @param attrName User attribute name for replica count for a node.
     */
    public void setReplicaCountAttributeName(String attrName) {
        this.attrName = attrName;
    }

    /**
     * Checks flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code #getBackupFilter()} is set.
     *
     * @return {@code True} if nodes residing on the same host may not act as backups of each other.
     */
    public boolean isExcludeNeighbors() {
        return exclNeighbors;
    }

    /**
     * Sets flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
     * <p>
     * Note that {@code excludeNeighbors} parameter is ignored if {@code #getBackupFilter()} is set.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups of each other.
     */
    public void setExcludeNeighbors(boolean exclNeighbors) {
        this.exclNeighbors = exclNeighbors;
    }

    /**
     * Gets neighbors for a node.
     *
     * @param node Node.
     * @return Neighbors.
     */
    private Collection<UUID> neighbors(final ClusterNode node) {
        Collection<UUID> ns = neighbors.get(node.id());

        if (ns == null) {
            Collection<ClusterNode> nodes = ignite.cluster().forHost(node).nodes();

            ns = F.addIfAbsent(neighbors, node.id(), new ArrayList<>(F.nodeIds(nodes)));
        }

        return ns;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public List<List<ClusterNode>> assignPartitions(GridCacheAffinityFunctionContext ctx) {
        List<List<ClusterNode>> res = new ArrayList<>(parts);

        Collection<ClusterNode> topSnapshot = ctx.currentTopologySnapshot();

        for (int part = 0; part < parts; part++) {
            res.add(F.isEmpty(topSnapshot) ?
                Collections.<ClusterNode>emptyList() :
                // Wrap affinity nodes with unmodifiable list since unmodifiable generic collection
                // doesn't provide equals and hashCode implementations.
                U.sealList(nodes(part, topSnapshot, ctx.backups())));
        }

        return res;
    }

    /**
     * Assigns nodes to one partition.
     *
     * @param part Partition to assign nodes for.
     * @param nodes Cache topology nodes.
     * @return Assigned nodes, first node is primary, others are backups.
     */
    public Collection<ClusterNode> nodes(int part, Collection<ClusterNode> nodes, int backups) {
        if (nodes == null)
            return Collections.emptyList();

        int nodesSize = nodes.size();

        if (nodesSize == 0)
            return Collections.emptyList();

        if (nodesSize == 1) // Minor optimization.
            return nodes;

        initialize();

        final Map<NodeInfo, ClusterNode> lookup = new GridLeanMap<>(nodesSize);

        // Store nodes in map for fast lookup.
        for (ClusterNode n : nodes)
            // Add nodes into hash circle, if absent.
            lookup.put(resolveNodeInfo(n), n);

        Collection<NodeInfo> selected;

        if (backupFilter != null) {
            final IgnitePredicate<NodeInfo> p = new P1<NodeInfo>() {
                @Override public boolean apply(NodeInfo id) {
                    return lookup.containsKey(id);
                }
            };

            final NodeInfo primaryId = nodeHash.node(part, p);

            IgnitePredicate<NodeInfo> backupPrimaryIdFilter = new IgnitePredicate<NodeInfo>() {
                @Override public boolean apply(NodeInfo node) {
                    return backupIdFilter.apply(primaryId, node);
                }
            };

            Collection<NodeInfo> backupIds = nodeHash.nodes(part, backups, p, backupPrimaryIdFilter);

            if (F.isEmpty(backupIds) && primaryId != null) {
                ClusterNode n = lookup.get(primaryId);

                assert n != null;

                return Collections.singletonList(n);
            }

            selected = primaryId != null ? F.concat(false, primaryId, backupIds) : backupIds;
        }
        else {
            if (!exclNeighbors) {
                selected = nodeHash.nodes(part, backups == Integer.MAX_VALUE ? backups : backups + 1, new P1<NodeInfo>() {
                    @Override public boolean apply(NodeInfo id) {
                        return lookup.containsKey(id);
                    }
                });

                if (selected.size() == 1) {
                    NodeInfo id = F.first(selected);

                    assert id != null : "Node ID cannot be null in affinity node ID collection: " + selected;

                    ClusterNode n = lookup.get(id);

                    assert n != null;

                    return Collections.singletonList(n);
                }
            }
            else {
                int primaryAndBackups = backups + 1;

                selected = new ArrayList<>(primaryAndBackups);

                final Collection<NodeInfo> selected0 = selected;

                List<NodeInfo> ids = nodeHash.nodes(part, primaryAndBackups, new P1<NodeInfo>() {
                    @Override public boolean apply(NodeInfo id) {
                        ClusterNode n = lookup.get(id);

                        if (n == null)
                            return false;

                        Collection<UUID> neighbors = neighbors(n);

                        for (NodeInfo id0 : selected0) {
                            ClusterNode n0 = lookup.get(id0);

                            if (n0 == null)
                                return false;

                            Collection<UUID> neighbors0 = neighbors(n0);

                            if (F.containsAny(neighbors0, neighbors))
                                return false;
                        }

                        selected0.add(id);

                        return true;
                    }
                });

                if (AFFINITY_CONSISTENCY_CHECK)
                    assert F.eqOrdered(ids, selected);
            }
        }

        Collection<ClusterNode> ret = new ArrayList<>(selected.size());

        for (NodeInfo id : selected) {
            ClusterNode n = lookup.get(id);

            assert n != null;

            ret.add(n);
        }

        return ret;
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key) {
        initialize();

        return U.safeAbs(key.hashCode() % parts);
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        initialize();

        return parts;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        addedNodes = new ConcurrentHashMap<>();
        neighbors = new ConcurrentHashMap8<>();

        initLatch = new CountDownLatch(1);

        init = new AtomicBoolean();
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        NodeInfo info = addedNodes.remove(nodeId);

        if (info == null)
            return;

        nodeHash.removeNode(info);

        neighbors.clear();
    }

    /**
     * Resolve node info for specified node.
     * Add node to hash circle if this is the first node invocation.
     *
     * @param n Node to get info for.
     * @return Node info.
     */
    private NodeInfo resolveNodeInfo(ClusterNode n) {
        UUID nodeId = n.id();
        NodeInfo nodeInfo = addedNodes.get(nodeId);

        if (nodeInfo != null)
            return nodeInfo;

        assert hashIdRslvr != null;

        nodeInfo = new NodeInfo(nodeId, hashIdRslvr.resolve(n), n);

        neighbors.clear();

        nodeHash.addNode(nodeInfo, replicas(n));

        addedNodes.put(nodeId, nodeInfo);

        return nodeInfo;
    }

    /** {@inheritDoc} */
    private void initialize() {
        if (!init.get() && init.compareAndSet(false, true)) {
            if (log.isInfoEnabled())
                log.info("Consistent hash configuration [cacheName=" + cacheName + ", partitions=" + parts +
                    ", excludeNeighbors=" + exclNeighbors + ", replicas=" + replicas +
                    ", backupFilter=" + backupFilter + ", hashIdRslvr=" + hashIdRslvr + ']');

            nodeHash = new GridConsistentHash<>();

            initLatch.countDown();
        }
        else {
            if (initLatch.getCount() > 0) {
                try {
                    U.await(initLatch);
                }
                catch (GridInterruptedException ignored) {
                    // Recover interrupted state flag.
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * @param n Node.
     * @return Replicas.
     */
    private int replicas(ClusterNode n) {
        Integer nodeReplicas = n.attribute(attrName);

        if (nodeReplicas == null)
            nodeReplicas = replicas;

        return nodeReplicas;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheConsistentHashAffinityFunction.class, this);
    }

    /**
     * Node hash ID.
     */
    private static final class NodeInfo implements Comparable<NodeInfo> {
        /** Node ID. */
        private UUID nodeId;

        /** Hash ID. */
        private Object hashId;

        /** Grid node. */
        private ClusterNode node;

        /**
         * @param nodeId Node ID.
         * @param hashId Hash ID.
         * @param node Rich node.
         */
        private NodeInfo(UUID nodeId, Object hashId, ClusterNode node) {
            assert nodeId != null;
            assert hashId != null;

            this.hashId = hashId;
            this.nodeId = nodeId;
            this.node = node;
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

        /**
         * @return Node.
         */
        public ClusterNode node() {
            return node;
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

            if (diff == 0) {
                int h1 = hashCode();
                int h2 = o.hashCode();

                diff = h1 == h2 ? 0 : (h1 < h2 ? -1 : 1);
            }

            return diff;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(NodeInfo.class, this);
        }
    }
}
