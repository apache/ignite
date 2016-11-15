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

package org.apache.ignite.cache.affinity.fair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.RandomAccess;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityCentralizedFunction;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/**
 * Fair affinity function which tries to ensure that all nodes get equal number of partitions with
 * minimum amount of reassignments between existing nodes.
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
@AffinityCentralizedFunction
public class FairAffinityFunction implements AffinityFunction {
    /** Default partition count. */
    public static final int DFLT_PART_CNT = 256;

    /** */
    private static final long serialVersionUID = 0L;

    /** Ascending comparator. */
    private static final Comparator<PartitionSet> ASC_CMP = new PartitionSetComparator();

    /** Descending comparator. */
    private static final Comparator<PartitionSet> DESC_CMP = Collections.reverseOrder(ASC_CMP);

    /** Number of partitions. */
    private int parts;

    /** Exclude neighbors flag. */
    private boolean exclNeighbors;

    /** Exclude neighbors warning. */
    private transient boolean exclNeighborsWarn;

    /** Logger instance. */
    @LoggerResource
    private transient IgniteLogger log;

    /** Optional backup filter. First node is primary, second node is a node being tested. */
    private IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter;

    /** Optional affinity backups filter. The first node is a node being tested, the second is a list of nodes that are already assigned for a given partition (primary node is the first in the list). */
    private IgniteBiPredicate<ClusterNode, List<ClusterNode>> affinityBackupFilter;

    /**
     * Empty constructor with all defaults.
     */
    public FairAffinityFunction() {
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
    public FairAffinityFunction(boolean exclNeighbors) {
        this(exclNeighbors, DFLT_PART_CNT);
    }

    /**
     * @param parts Number of partitions.
     */
    public FairAffinityFunction(int parts) {
        this(false, parts);
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
    public FairAffinityFunction(boolean exclNeighbors, int parts) {
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
     */
    public FairAffinityFunction(int parts, @Nullable IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
        this(false, parts, backupFilter);
    }

    /**
     * Private constructor.
     *
     * @param exclNeighbors Exclude neighbors flag.
     * @param parts Partitions count.
     * @param backupFilter Backup filter.
     */
    private FairAffinityFunction(boolean exclNeighbors, int parts,
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
        A.ensure(parts <= CacheConfiguration.MAX_PARTITIONS_COUNT, "parts <= " + CacheConfiguration.MAX_PARTITIONS_COUNT);

        this.parts = parts;
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

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext ctx) {
        List<ClusterNode> topSnapshot = ctx.currentTopologySnapshot();

        if (topSnapshot.size() == 1) {
            ClusterNode primary = topSnapshot.get(0);

            return Collections.nCopies(parts, Collections.singletonList(primary));
        }

        Map<UUID, Collection<ClusterNode>> neighborhoodMap = exclNeighbors
            ? GridCacheUtils.neighbors(ctx.currentTopologySnapshot())
            : null;

        List<List<ClusterNode>> assignment = createCopy(ctx, neighborhoodMap);

        int backups = ctx.backups();

        int tiers = backups == Integer.MAX_VALUE ? topSnapshot.size() : Math.min(backups + 1, topSnapshot.size());

        // Per tier pending partitions.
        Map<Integer, Queue<Integer>> pendingParts = new HashMap<>();

        FullAssignmentMap fullMap = new FullAssignmentMap(tiers, assignment, topSnapshot, neighborhoodMap);

        for (int tier = 0; tier < tiers; tier++) {
            // Check if this is a new tier and add pending partitions.
            Queue<Integer> pending = pendingParts.get(tier);

            for (int part = 0; part < parts; part++) {
                if (fullMap.assignments.get(part).size() < tier + 1) {
                    if (pending == null)
                        pendingParts.put(tier, pending = new LinkedList<>());

                    if (!pending.contains(part))
                        pending.add(part);
                }
            }

            // Assign pending partitions, if any.
            assignPending(tier, pendingParts, fullMap, topSnapshot, false);

            // Balance assignments.
            boolean balanced = balance(tier, pendingParts, fullMap, topSnapshot, false);

            if (!balanced && exclNeighbors) {
                assignPending(tier, pendingParts, fullMap, topSnapshot, true);

                balance(tier, pendingParts, fullMap, topSnapshot, true);

                if (!exclNeighborsWarn) {
                    LT.warn(log, "Affinity function excludeNeighbors property is ignored " +
                        "because topology has no enough nodes to assign backups.");

                    exclNeighborsWarn = true;
                }
            }
        }

        return fullMap.assignments;
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

        return U.safeAbs(hash(key.hashCode())) % parts;
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        // No-op.
    }

    /**
     * Assigns pending (unassigned) partitions to nodes.
     *
     * @param tier Tier to assign (0 is primary, 1 - 1st backup,...).
     * @param pendingMap Pending partitions per tier.
     * @param fullMap Full assignment map to modify.
     * @param topSnapshot Topology snapshot.
     * @param allowNeighbors Allow neighbors nodes for partition.
     */
    private void assignPending(int tier,
        Map<Integer, Queue<Integer>> pendingMap,
        FullAssignmentMap fullMap,
        List<ClusterNode> topSnapshot,
        boolean allowNeighbors)
    {
        Queue<Integer> pending = pendingMap.get(tier);

        if (F.isEmpty(pending))
            return;

        int idealPartCnt = parts / topSnapshot.size();

        Map<UUID, PartitionSet> tierMapping = fullMap.tierMapping(tier);

        PrioritizedPartitionMap underloadedNodes = filterNodes(tierMapping, idealPartCnt, false);

        // First iterate over underloaded nodes.
        assignPendingToUnderloaded(tier, pendingMap, fullMap, underloadedNodes, topSnapshot, false, allowNeighbors);

        if (!pending.isEmpty() && !underloadedNodes.isEmpty()) {
            // Same, forcing updates.
            assignPendingToUnderloaded(tier, pendingMap, fullMap, underloadedNodes, topSnapshot, true, allowNeighbors);
        }

        if (!pending.isEmpty())
            assignPendingToNodes(tier, pendingMap, fullMap, topSnapshot, allowNeighbors);

        if (pending.isEmpty())
            pendingMap.remove(tier);
    }

    /**
     * Assigns pending partitions to underloaded nodes.
     *
     * @param tier Tier to assign.
     * @param pendingMap Pending partitions per tier.
     * @param fullMap Full assignment map to modify.
     * @param underloadedNodes Underloaded nodes.
     * @param topSnapshot Topology snapshot.
     * @param force {@code True} if partitions should be moved.
     * @param allowNeighbors Allow neighbors nodes for partition.
     */
    private void assignPendingToUnderloaded(
        int tier,
        Map<Integer, Queue<Integer>> pendingMap,
        FullAssignmentMap fullMap,
        PrioritizedPartitionMap underloadedNodes,
        Collection<ClusterNode> topSnapshot,
        boolean force,
        boolean allowNeighbors) {
        Iterator<Integer> it = pendingMap.get(tier).iterator();

        int ideal = parts / topSnapshot.size();

        while (it.hasNext()) {
            int part = it.next();

            for (PartitionSet set : underloadedNodes.assignments()) {
                ClusterNode node = set.node();

                assert node != null;

                if (fullMap.assign(part, tier, node, pendingMap, force, allowNeighbors)) {
                    // We could add partition to partition map without forcing, remove partition from pending.
                    it.remove();

                    if (set.size() <= ideal)
                        underloadedNodes.remove(set.nodeId());
                    else
                        underloadedNodes.update();

                    break; // for, continue to the next partition.
                }
            }

            if (underloadedNodes.isEmpty())
                return;
        }
    }

    /**
     * Spreads pending partitions equally to all nodes in topology snapshot.
     *
     * @param tier Tier to assign.
     * @param pendingMap Pending partitions per tier.
     * @param fullMap Full assignment map to modify.
     * @param topSnapshot Topology snapshot.
     * @param allowNeighbors Allow neighbors nodes for partition.
     */
    private void assignPendingToNodes(int tier, Map<Integer, Queue<Integer>> pendingMap,
        FullAssignmentMap fullMap, List<ClusterNode> topSnapshot, boolean allowNeighbors) {
        Iterator<Integer> it = pendingMap.get(tier).iterator();

        int idx = 0;

        while (it.hasNext()) {
            int part = it.next();

            int i = idx;

            boolean assigned = false;

            do {
                ClusterNode node = topSnapshot.get(i);

                if (fullMap.assign(part, tier, node, pendingMap, false, allowNeighbors)) {
                    it.remove();

                    assigned = true;
                }

                i = (i + 1) % topSnapshot.size();

                if (assigned)
                    idx = i;
            } while (i != idx);

            if (!assigned) {
                do {
                    ClusterNode node = topSnapshot.get(i);

                    if (fullMap.assign(part, tier, node, pendingMap, true, allowNeighbors)) {
                        it.remove();

                        assigned = true;
                    }

                    i = (i + 1) % topSnapshot.size();

                    if (assigned)
                        idx = i;
                } while (i != idx);
            }

            if (!assigned && (!exclNeighbors || exclNeighbors && allowNeighbors))
                throw new IllegalStateException("Failed to find assignable node for partition.");
        }
    }

    /**
     * Tries to balance assignments between existing nodes in topology.
     *
     * @param tier Tier to assign.
     * @param pendingParts Pending partitions per tier.
     * @param fullMap Full assignment map to modify.
     * @param topSnapshot Topology snapshot.
     * @param allowNeighbors Allow neighbors nodes for partition.
     */
    private boolean balance(int tier, Map<Integer, Queue<Integer>> pendingParts, FullAssignmentMap fullMap,
        Collection<ClusterNode> topSnapshot, boolean allowNeighbors) {
        int idealPartCnt = parts / topSnapshot.size();

        Map<UUID, PartitionSet> mapping = fullMap.tierMapping(tier);

        PrioritizedPartitionMap underloadedNodes = filterNodes(mapping, idealPartCnt, false);
        PrioritizedPartitionMap overloadedNodes = filterNodes(mapping, idealPartCnt, true);

        do {
            boolean retry = false;

            for (PartitionSet overloaded : overloadedNodes.assignments()) {
                for (Integer part : overloaded.partitions()) {
                    boolean assigned = false;

                    for (PartitionSet underloaded : underloadedNodes.assignments()) {
                        if (fullMap.assign(part, tier, underloaded.node(), pendingParts, false, allowNeighbors)) {
                            // Size of partition sets has changed.
                            if (overloaded.size() <= idealPartCnt)
                                overloadedNodes.remove(overloaded.nodeId());
                            else
                                overloadedNodes.update();

                            if (underloaded.size() >= idealPartCnt)
                                underloadedNodes.remove(underloaded.nodeId());
                            else
                                underloadedNodes.update();

                            assigned = true;

                            retry = true;

                            break;
                        }
                    }

                    if (!assigned) {
                        for (PartitionSet underloaded : underloadedNodes.assignments()) {
                            if (fullMap.assign(part, tier, underloaded.node(), pendingParts, true, allowNeighbors)) {
                                // Size of partition sets has changed.
                                if (overloaded.size() <= idealPartCnt)
                                    overloadedNodes.remove(overloaded.nodeId());
                                else
                                    overloadedNodes.update();

                                if (underloaded.size() >= idealPartCnt)
                                    underloadedNodes.remove(underloaded.nodeId());
                                else
                                    underloadedNodes.update();

                                retry = true;

                                break;
                            }
                        }
                    }

                    if (retry)
                        break; // for part.
                }

                if (retry)
                    break; // for overloaded.
            }

            if (!retry)
                break;
        }
        while (true);

        return underloadedNodes.isEmpty();
    }

    /**
     * Constructs underloaded or overloaded partition map.
     *
     * @param mapping Mapping to filter.
     * @param idealPartCnt Ideal number of partitions per node.
     * @param overloaded {@code True} if should create overloaded map, {@code false} for underloaded.
     * @return Prioritized partition map.
     */
    private PrioritizedPartitionMap filterNodes(Map<UUID, PartitionSet> mapping, int idealPartCnt, boolean overloaded) {
        assert mapping != null;

        PrioritizedPartitionMap res = new PrioritizedPartitionMap(overloaded ? DESC_CMP : ASC_CMP);

        for (PartitionSet set : mapping.values()) {
            if ((overloaded && set.size() > idealPartCnt) || (!overloaded && set.size() < idealPartCnt))
               res.add(set);
        }

        return res;
    }

    /**
     * Creates copy of previous partition assignment.
     *
     * @param ctx Affinity function context.
     * @param neighborhoodMap Neighbors nodes grouped by target node.
     * @return Assignment copy and per node partition map.
     */
    private List<List<ClusterNode>> createCopy(AffinityFunctionContext ctx,
        Map<UUID, Collection<ClusterNode>> neighborhoodMap)
    {
        DiscoveryEvent discoEvt = ctx.discoveryEvent();

        UUID leftNodeId = (discoEvt == null || discoEvt.type() == EventType.EVT_NODE_JOINED)
            ? null
            : discoEvt.eventNode().id();

        List<List<ClusterNode>> cp = new ArrayList<>(parts);

        for (int part = 0; part < parts; part++) {
            List<ClusterNode> partNodes = ctx.previousAssignment(part);

            List<ClusterNode> partNodesCp;

            if (partNodes == null)
                partNodesCp = new ArrayList<>();
            else
                partNodesCp = copyAssigments(neighborhoodMap, partNodes, leftNodeId);

            cp.add(partNodesCp);
        }

        return cp;
    }

    /**
     * @param neighborhoodMap Neighbors nodes grouped by target node.
     * @param partNodes Partition nodes.
     * @param leftNodeId Left node id.
     */
    private List<ClusterNode> copyAssigments(Map<UUID, Collection<ClusterNode>> neighborhoodMap,
        List<ClusterNode> partNodes, UUID leftNodeId) {
        final List<ClusterNode> partNodesCp = new ArrayList<>(partNodes.size());

        for (ClusterNode node : partNodes) {
            if (node.id().equals(leftNodeId))
                continue;

            boolean containsNeighbor = false;

            if (neighborhoodMap != null)
                containsNeighbor = F.exist(neighborhoodMap.get(node.id()), new IgnitePredicate<ClusterNode>() {
                    @Override public boolean apply(ClusterNode node) {
                        return partNodesCp.contains(node);
                    }
                });

            if (!containsNeighbor)
                partNodesCp.add(node);
        }

        return partNodesCp;
    }

    /**
     *
     */
    private static class PartitionSetComparator implements Comparator<PartitionSet>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public int compare(PartitionSet o1, PartitionSet o2) {
            return Integer.compare(o1.parts.size(), o2.parts.size());
        }
    }

    /**
     * Prioritized partition map. Ordered structure in which nodes are ordered in ascending or descending order
     * by number of partitions assigned to a node.
     */
    private static class PrioritizedPartitionMap {
        /** Comparator. */
        private Comparator<PartitionSet> cmp;

        /** Assignment map. */
        private Map<UUID, PartitionSet> assignmentMap = new HashMap<>();

        /** Assignment list, ordered according to comparator. */
        private List<PartitionSet> assignmentList = new ArrayList<>();

        /**
         * @param cmp Comparator.
         */
        private PrioritizedPartitionMap(Comparator<PartitionSet> cmp) {
            this.cmp = cmp;
        }

        /**
         * @param set Partition set to add.
         */
        public void add(PartitionSet set) {
            PartitionSet old = assignmentMap.put(set.nodeId(), set);

            if (old == null) {
                assignmentList.add(set);

                update();
            }
        }

        /**
         * Sorts assignment list.
         */
        public void update() {
            Collections.sort(assignmentList, cmp);
        }

        /**
         * @return Sorted assignment list.
         */
        public List<PartitionSet> assignments() {
            return assignmentList;
        }

        /**
         * @param uuid Uuid.
         */
        public void remove(UUID uuid) {
            PartitionSet rmv = assignmentMap.remove(uuid);

            assignmentList.remove(rmv);
        }

        /**
         *
         */
        public boolean isEmpty() {
            return assignmentList.isEmpty();
        }
    }

    /**
     * Full assignment map. Auxiliary data structure which maintains resulting assignment and temporary
     * maps consistent.
     */
    @SuppressWarnings("unchecked")
    private class FullAssignmentMap {
        /** Per-tier assignment maps. */
        private Map<UUID, PartitionSet>[] tierMaps;

        /** Full assignment map. */
        private Map<UUID, PartitionSet> fullMap;

        /** Resulting assignment. */
        private List<List<ClusterNode>> assignments;

        /** Neighborhood map. */
        private final Map<UUID, Collection<ClusterNode>> neighborhoodMap;

        /**
         * @param tiers Number of tiers.
         * @param assignments Assignments to modify.
         * @param topSnapshot Topology snapshot.
         * @param neighborhoodMap Neighbors nodes grouped by target node.
         */
        private FullAssignmentMap(int tiers,
            List<List<ClusterNode>> assignments,
            Collection<ClusterNode> topSnapshot,
            Map<UUID, Collection<ClusterNode>> neighborhoodMap)
        {
            this.assignments = assignments;
            this.neighborhoodMap = neighborhoodMap;
            this.tierMaps = new Map[tiers];

            for (int tier = 0; tier < tiers; tier++)
                tierMaps[tier] = assignments(tier, topSnapshot);

            fullMap = assignments(-1, topSnapshot);
        }

        /**
         * Tries to assign partition to given node on specified tier. If force is false, assignment will succeed
         * only if this partition is not already assigned to a node. If force is true, then assignment will succeed
         * only if partition is not assigned to a tier with number less than passed in. Assigned partition from
         * greater tier will be moved to pending queue.
         *
         * @param part Partition to assign.
         * @param tier Tier number to assign.
         * @param node Node to move partition to.
         * @param pendingParts per tier pending partitions map.
         * @param force Force flag.
         * @param allowNeighbors Allow neighbors nodes for partition.
         * @return {@code True} if assignment succeeded.
         */
        boolean assign(int part,
            int tier,
            ClusterNode node,
            Map<Integer, Queue<Integer>> pendingParts, boolean force,
            boolean allowNeighbors)
        {
            UUID nodeId = node.id();

            if (isAssignable(part, tier, node, allowNeighbors)) {
                tierMaps[tier].get(nodeId).add(part);

                fullMap.get(nodeId).add(part);

                List<ClusterNode> assignment = assignments.get(part);

                if (assignment.size() <= tier)
                    assignment.add(node);
                else {
                    ClusterNode oldNode = assignment.set(tier, node);

                    if (oldNode != null) {
                        UUID oldNodeId = oldNode.id();

                        tierMaps[tier].get(oldNodeId).remove(part);
                        fullMap.get(oldNodeId).remove(part);
                    }
                }

                return true;
            }
            else if (force) {
                assert !tierMaps[tier].get(nodeId).contains(part);

                // Check previous tiers first.
                for (int t = 0; t < tier; t++) {
                    if (tierMaps[t].get(nodeId).contains(part))
                        return false;
                }

                // Partition is on some lower tier, switch it.
                for (int t = tier + 1; t < tierMaps.length; t++) {
                    if (tierMaps[t].get(nodeId).contains(part)) {
                        ClusterNode oldNode = assignments.get(part).get(tier);

                        // Move partition from level t to tier.
                        assignments.get(part).set(tier, node);
                        assignments.get(part).set(t, null);

                        if (oldNode != null) {
                            tierMaps[tier].get(oldNode.id()).remove(part);
                            fullMap.get(oldNode.id()).remove(part);
                        }

                        tierMaps[tier].get(nodeId).add(part);
                        tierMaps[t].get(nodeId).remove(part);

                        Queue<Integer> pending = pendingParts.get(t);

                        if (pending == null)
                            pendingParts.put(t, pending = new LinkedList<>());

                        pending.add(part);

                        return true;
                    }
                }

                return false;
            }

            // !force.
            return false;
        }

        /**
         * Gets tier mapping.
         *
         * @param tier Tier to get mapping.
         * @return Per node map.
         */
        public Map<UUID, PartitionSet> tierMapping(int tier) {
            return tierMaps[tier];
        }

        /**
         * @param part Partition.
         * @param tier Tier.
         * @param node Node.
         * @param allowNeighbors Allow neighbors.
         * @return {@code true} if the partition is assignable to the node.
         */
        private boolean isAssignable(int part, int tier, final ClusterNode node, boolean allowNeighbors) {
            if (containsPartition(part, node))
                return false;

            if (exclNeighbors)
                return allowNeighbors || !neighborsContainPartition(node, part);
            else if (affinityBackupFilter != null) {
                List<ClusterNode> assigment = assignments.get(part);

                assert assigment.size() > 0;

                List<ClusterNode> newAssignment;

                if (tier == 0) {
                    for (int t = 1; t < assigment.size(); t++) {
                        newAssignment = new ArrayList<>(assigment.size() - 1);

                        newAssignment.add(node);

                        if (t != 1)
                            newAssignment.addAll(assigment.subList(1, t));

                        if (t + 1 < assigment.size())
                            newAssignment.addAll(assigment.subList(t + 1, assigment.size()));

                        if (!affinityBackupFilter.apply(assigment.get(t), newAssignment))
                            return false;

                    }

                    return true;
                }
                else if (tier < assigment.size()) {
                    newAssignment = new ArrayList<>(assigment.size() - 1);

                    int i = 0;

                    for (ClusterNode assignmentNode: assigment) {
                        if (i != tier)
                            newAssignment.add(assignmentNode);

                        i++;
                    }
                }
                else
                    newAssignment = assigment;

                return affinityBackupFilter.apply(node, newAssignment);
            }
            else if (backupFilter != null) {
                if (tier == 0) {
                    List<ClusterNode> assigment = assignments.get(part);

                    assert assigment.size() > 0;

                    List<ClusterNode> backups = assigment.subList(1, assigment.size());

                    return !F.exist(backups, new IgnitePredicate<ClusterNode>() {
                        @Override public boolean apply(ClusterNode n) {
                            return !backupFilter.apply(node, n);
                        }
                    });
                }
                else
                    return (backupFilter.apply(assignments.get(part).get(0), node));
            }
            else
                return true;
        }

        /**
         * @param part Partition.
         * @param node Node.
         */
        private boolean containsPartition(int part, ClusterNode node) {
            return fullMap.get(node.id()).contains(part);
        }

        /**
         * @param node Node.
         * @param part Partition.
         */
        private boolean neighborsContainPartition(ClusterNode node, final int part) {
            return F.exist(neighborhoodMap.get(node.id()), new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode n) {
                    return fullMap.get(n.id()).contains(part);
                }
            });
        }

        /**
         * Constructs assignments map for specified tier.
         *
         * @param tier Tier number, -1 for all tiers altogether.
         * @param topSnapshot Topology snapshot.
         * @return Assignment map.
         */
        private Map<UUID, PartitionSet> assignments(int tier, Collection<ClusterNode> topSnapshot) {
            Map<UUID, PartitionSet> tmp = new LinkedHashMap<>();

            for (int part = 0; part < assignments.size(); part++) {
                List<ClusterNode> nodes = assignments.get(part);

                assert nodes instanceof RandomAccess;

                if (nodes.size() <= tier)
                    continue;

                int start = tier < 0 ? 0 : tier;
                int end = tier < 0 ? nodes.size() : tier + 1;

                for (int i = start; i < end; i++) {
                    ClusterNode n = nodes.get(i);

                    PartitionSet set = tmp.get(n.id());

                    if (set == null)
                        tmp.put(n.id(), set = new PartitionSet(n));

                    set.add(part);
                }
            }

            if (tmp.size() < topSnapshot.size()) {
                for (ClusterNode node : topSnapshot) {
                    if (!tmp.containsKey(node.id()))
                        tmp.put(node.id(), new PartitionSet(node));
                }
            }

            return tmp;
        }
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.
     *
     * @param h Hash code.
     * @return Enhanced hash code.
     */
    private static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        return h ^ (h >>> 16);
    }

    /**
     *
     */
    @SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
    private static class PartitionSet {
        /** */
        private ClusterNode node;

        /** Partitions. */
        private Collection<Integer> parts = new LinkedList<>();

        /**
         * @param node Node.
         */
        private PartitionSet(ClusterNode node) {
            this.node = node;
        }

        /**
         * @return Node.
         */
        private ClusterNode node() {
            return node;
        }

        /**
         * @return Node ID.
         */
        private UUID nodeId() {
            return node.id();
        }

        /**
         * @return Partition set size.
         */
        private int size() {
            return parts.size();
        }

        /**
         * Adds partition to partition set.
         *
         * @param part Partition to add.
         * @return {@code True} if partition was added, {@code false} if partition already exists.
         */
        private boolean add(int part) {
            if (!parts.contains(part)) {
                parts.add(part);

                return true;
            }

            return false;
        }

        /**
         * @param part Partition to remove.
         */
        private void remove(Integer part) {
            parts.remove(part); // Remove object, not index.
        }

        /**
         * @return Partitions.
         */
        @SuppressWarnings("TypeMayBeWeakened")
        private Collection<Integer> partitions() {
            return parts;
        }

        /**
         * Checks if partition set contains given partition.
         *
         * @param part Partition to check.
         * @return {@code True} if partition set contains given partition.
         */
        private boolean contains(int part) {
            return parts.contains(part);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "PartSet [nodeId=" + node.id() + ", size=" + parts.size() + ", parts=" + parts + ']';
        }
    }
}
