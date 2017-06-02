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

package org.apache.ignite.cache.affinity.rendezvous;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.AffinityNodeHashResolver;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/**
 * Affinity function for partitioned cache based on Highest Random Weight algorithm.
 * This function supports the following configuration:
 * <ul>
 * <li>
 *      {@code partitions} - Number of partitions to spread across nodes.
 * </li>
 * <li>
 *      {@code excludeNeighbors} - If set to {@code true}, will exclude same-host-neighbors
 *      from being backups of each other. This flag can be ignored in cases when topology has no enough nodes
 *      for assign backups.
 *      Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
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
public class RendezvousAffinityFunction implements AffinityFunction, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default number of partitions. */
    public static final int DFLT_PARTITION_COUNT = 1024;

    /** Comparator. */
    private static final Comparator<NodeWithHash> COMPARATOR = new HashComparator();

    /** Number of partitions. */
    private int parts;

    /** Exclude neighbors flag. */
    private boolean exclNeighbors;

    /** Exclude neighbors warning. */
    private transient boolean exclNeighborsWarn;

    /** Optional backup filter. First node is primary, second node is a node being tested. */
    private IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter;

    /** Optional affinity backups filter. The first node is a node being tested,
     *  the second is a list of nodes that are already assigned for a given partition (the first node in the list
     *  is primary). */
    private IgniteBiPredicate<ClusterNode, List<ClusterNode>> affinityBackupFilter;

    /** Hash ID resolver. */
    private AffinityNodeHashResolver hashIdRslvr = null;

    /** Logger instance. */
    @LoggerResource
    private transient IgniteLogger log;

    /**
     * Empty constructor with all defaults.
     */
    public RendezvousAffinityFunction() {
        this(false);
    }

    /**
     * Initializes affinity with flag to exclude same-host-neighbors from being backups of each other
     * and specified number of backups.
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups
     *      of each other.
     */
    public RendezvousAffinityFunction(boolean exclNeighbors) {
        this(exclNeighbors, DFLT_PARTITION_COUNT);
    }

    /**
     * Initializes affinity with flag to exclude same-host-neighbors from being backups of each other,
     * and specified number of backups and partitions.
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups
     *      of each other.
     * @param parts Total number of partitions.
     */
    public RendezvousAffinityFunction(boolean exclNeighbors, int parts) {
        this(exclNeighbors, parts, null);
    }

    /**
     * Initializes optional counts for replicas and backups.
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param parts Total number of partitions.
     * @param backupFilter Optional back up filter for nodes. If provided, backups will be selected
     *      from all nodes that pass this filter. First argument for this filter is primary node, and second
     *      argument is node being tested.
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     */
    public RendezvousAffinityFunction(int parts, @Nullable IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
        this(false, parts, backupFilter);
    }

    /**
     * Private constructor.
     *
     * @param exclNeighbors Exclude neighbors flag.
     * @param parts Partitions count.
     * @param backupFilter Backup filter.
     */
    private RendezvousAffinityFunction(boolean exclNeighbors, int parts,
        IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
        A.ensure(parts > 0, "parts > 0");

        this.exclNeighbors = exclNeighbors;
        this.parts = parts;
        this.backupFilter = backupFilter;
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
        A.ensure(parts <= CacheConfiguration.MAX_PARTITIONS_COUNT,
            "parts <= " + CacheConfiguration.MAX_PARTITIONS_COUNT);

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
    @Deprecated
    public AffinityNodeHashResolver getHashIdResolver() {
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
     *
     * @deprecated Use {@link IgniteConfiguration#setConsistentId(Serializable)} instead.
     */
    @Deprecated
    public void setHashIdResolver(AffinityNodeHashResolver hashIdRslvr) {
        this.hashIdRslvr = hashIdRslvr;
    }

    /**
     * Gets optional backup filter. If not {@code null}, backups will be selected
     * from all nodes that pass this filter. First node passed to this filter is primary node,
     * and second node is a node being tested.
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
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
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param backupFilter Optional backup filter.
     * @deprecated Use {@code affinityBackupFilter} instead.
     */
    @Deprecated
    public void setBackupFilter(@Nullable IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
        this.backupFilter = backupFilter;
    }

    /**
     * Gets optional backup filter. If not {@code null}, backups will be selected
     * from all nodes that pass this filter. First node passed to this filter is a node being tested,
     * and the second parameter is a list of nodes that are already assigned for a given partition (primary node is the first in the list).
     * <p>
     * Note that {@code affinityBackupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @return Optional backup filter.
     */
    @Nullable public IgniteBiPredicate<ClusterNode, List<ClusterNode>> getAffinityBackupFilter() {
        return affinityBackupFilter;
    }

    /**
     * Sets optional backup filter. If provided, then backups will be selected from all
     * nodes that pass this filter. First node being passed to this filter is a node being tested,
     * and the second parameter is a list of nodes that are already assigned for a given partition (primary node is the first in the list).
     * <p>
     * Note that {@code affinityBackupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param affinityBackupFilter Optional backup filter.
     */
    public void setAffinityBackupFilter(@Nullable IgniteBiPredicate<ClusterNode, List<ClusterNode>> affinityBackupFilter) {
        this.affinityBackupFilter = affinityBackupFilter;
    }

    /**
     * Checks flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @return {@code True} if nodes residing on the same host may not act as backups of each other.
     */
    public boolean isExcludeNeighbors() {
        return exclNeighbors;
    }

    /**
     * Sets flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups of each other.
     */
    public void setExcludeNeighbors(boolean exclNeighbors) {
        this.exclNeighbors = exclNeighbors;
    }

    /**
     * Resolves node hash.
     *
     * @param node Cluster node;
     * @return Node hash.
     */
    public Object resolveNodeHash(ClusterNode node) {
        if (hashIdRslvr != null)
            return hashIdRslvr.resolve(node);
        else
            return node.consistentId();
    }

    /**
     * Returns collection of nodes (primary first) for specified partition.
     *
     * @param part Partition.
     * @param nodes Nodes.
     * @param backups Number of backups.
     * @param neighborhoodCache Neighborhood.
     * @return Assignment.
     */
    public List<ClusterNode> assignPartition(int part,
        List<ClusterNode> nodes,
        int backups,
        @Nullable Map<UUID, Collection<ClusterNode>> neighborhoodCache,
        NodeWithHash[] sortContainer
    ) {
        if (nodes.size() <= 1)
            return nodes;

        assert sortContainer.length == nodes.size() : "Invalid sort container [part=" + part +
            "nodes=" + nodes + ", container=" + Arrays.toString(sortContainer) + ']';

        for (int i = 0; i < nodes.size(); i++) {
            ClusterNode node = nodes.get(i);

            Object nodeHash = resolveNodeHash(node);

            sortContainer[i].node = node;
            sortContainer[i].hash = hash(nodeHash.hashCode(), part);
        }

        final int primaryAndBackups = backups == Integer.MAX_VALUE ? nodes.size() : Math.min(backups + 1, nodes.size());

        Iterable<ClusterNode> sortedNodes = new LazyLinearSortedContainer(sortContainer, primaryAndBackups);

        Iterator<ClusterNode> it = sortedNodes.iterator();

        List<ClusterNode> res = new ArrayList<>(primaryAndBackups);

        Collection<ClusterNode> allNeighbors = new HashSet<>();

        ClusterNode primary = it.next();

        res.add(primary);

        if (exclNeighbors)
            allNeighbors.addAll(neighborhoodCache.get(primary.id()));

        // Select backups.
        if (backups > 0) {
            while (it.hasNext() && res.size() < primaryAndBackups) {
                ClusterNode node = it.next();

                if (exclNeighbors) {
                    if (!allNeighbors.contains(node)) {
                        res.add(node);

                        allNeighbors.addAll(neighborhoodCache.get(node.id()));
                    }
                }
                else if ((backupFilter != null && backupFilter.apply(primary, node))
                    || (affinityBackupFilter != null && affinityBackupFilter.apply(node, res))
                    || (affinityBackupFilter == null && backupFilter == null) ) {
                    res.add(node);

                    if (exclNeighbors)
                        allNeighbors.addAll(neighborhoodCache.get(node.id()));
                }
            }
        }

        if (res.size() < primaryAndBackups && nodes.size() >= primaryAndBackups && exclNeighbors) {
            // Need to iterate again in case if there are no nodes which pass exclude neighbors backups criteria.
            it = sortedNodes.iterator();

            it.next();

            while (it.hasNext() && res.size() < primaryAndBackups) {
                ClusterNode node = it.next();

                if (!res.contains(node))
                    res.add(node);
            }

            if (!exclNeighborsWarn) {
                LT.warn(log, "Affinity function excludeNeighbors property is ignored " +
                        "because topology has no enough nodes to assign backups.",
                    "Affinity function excludeNeighbors property is ignored " +
                        "because topology has no enough nodes to assign backups.");

                exclNeighborsWarn = true;
            }
        }

        assert res.size() <= primaryAndBackups;

        return res;
    }

    /**
     * The pack partition number and nodeHash.hashCode to long and mix it by hash function based on the Wang/Jenkins
     * hash.
     *
     * @param key0 Hash key.
     * @param key1 Hash key.
     * @see <a href="https://gist.github.com/badboy/6267743#64-bit-mix-functions">64 bit mix functions</a>
     * @return Long hash key.
     */
    private static long hash(int key0, int key1) {
        long key = (key0 & 0xFFFFFFFFL)
            | ((key1 & 0xFFFFFFFFL) << 32);

        key = (~key) + (key << 21); // key = (key << 21) - key - 1;
        key ^= (key >>> 24);
        key += (key << 3) + (key << 8); // key * 265
        key ^= (key >>> 14);
        key += (key << 2) + (key << 4); // key * 21
        key ^= (key >>> 28);
        key += (key << 31);

        return key;
    }


    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return parts;
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key) {
        if (key == null)
            throw new IllegalArgumentException("Null key is passed for a partition calculation. " +
                "Make sure that an affinity key that is used is initialized properly.");

        return U.safeAbs(key.hashCode() % parts);
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        List<List<ClusterNode>> assignments = new ArrayList<>(parts);

        Map<UUID, Collection<ClusterNode>> neighborhoodCache = exclNeighbors ?
            GridCacheUtils.neighbors(affCtx.currentTopologySnapshot()) : null;

        List<ClusterNode> nodes = affCtx.currentTopologySnapshot();

        NodeWithHash[] container = getOrCreateSortContainer(affCtx);

        for (int i = 0; i < parts; i++) {
            List<ClusterNode> partAssignment = assignPartition(i, nodes, affCtx.backups(), neighborhoodCache, container);

            assignments.add(partAssignment);
        }

        return assignments;
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        // No-op.
    }

    /**
     * @param ctx Affinity function context.
     * @return Array to use for partition assignment.
     */
    private NodeWithHash[] getOrCreateSortContainer(AffinityFunctionContext ctx) {
        NodeWithHash[] prev = ctx.attachment();
        List<ClusterNode> topSnapshot = ctx.currentTopologySnapshot();

        if (prev == null || prev.length != topSnapshot.size()) {
            prev = new NodeWithHash[topSnapshot.size()];

            for (int i = 0; i < prev.length; i++)
                prev[i] = new NodeWithHash();

            ctx.attachment(prev);
        }

        return prev;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(parts);
        out.writeBoolean(exclNeighbors);
        out.writeObject(hashIdRslvr);
        out.writeObject(backupFilter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        parts = in.readInt();
        exclNeighbors = in.readBoolean();
        hashIdRslvr = (AffinityNodeHashResolver)in.readObject();
        backupFilter = (IgniteBiPredicate<ClusterNode, ClusterNode>)in.readObject();
    }

    /**
     *
     */
    private static class HashComparator implements Comparator<NodeWithHash>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public int compare(NodeWithHash o1, NodeWithHash o2) {
            return o1.hash < o2.hash ? -1 : o1.hash > o2.hash ? 1 :
                o1.node.id().compareTo(o2.node.id());
        }
    }

    /**
     * Sorts the initial array with linear sort algorithm array
     */
    private static class LazyLinearSortedContainer implements Iterable<ClusterNode> {
        /** Initial node-hash array. */
        private final NodeWithHash[] arr;

        /** Count of the sorted elements */
        private int sorted;

        /**
         * @param arr Node / partition hash list.
         * @param needFirstSortedCnt Estimate count of elements to return by iterator.
         */
        LazyLinearSortedContainer(NodeWithHash[] arr, int needFirstSortedCnt) {
            this.arr = arr;

            if (needFirstSortedCnt > (int)Math.log(arr.length)) {
                Arrays.sort(arr, COMPARATOR);

                sorted = arr.length;
            }
        }

        /** {@inheritDoc} */
        @Override public Iterator<ClusterNode> iterator() {
            return new SortIterator();
        }

        /**
         *
         */
        private class SortIterator implements Iterator<ClusterNode> {
            /** Index of the first unsorted element. */
            private int cur;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return cur < arr.length;
            }

            /** {@inheritDoc} */
            @Override public ClusterNode next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                if (cur < sorted)
                    return arr[cur++].node;

                NodeWithHash min = arr[cur];

                int minIdx = cur;

                for (int i = cur + 1; i < arr.length; i++) {
                    if (COMPARATOR.compare(arr[i], min) < 0) {
                        minIdx = i;

                        min = arr[i];
                    }
                }

                if (minIdx != cur) {
                    arr[minIdx] = arr[cur];

                    arr[cur] = min;
                }

                sorted = cur++;

                return min.node;
            }

            /** {@inheritDoc} */
            @Override public void remove() {
                throw new UnsupportedOperationException("Remove doesn't supported");
            }
        }
    }

    /**
     * A specific tuple which holds a primitive long to avoid unneccessary boxing.
     */
    public static class NodeWithHash {
        /** */
        private ClusterNode node;

        /** */
        private long hash;
    }
}
