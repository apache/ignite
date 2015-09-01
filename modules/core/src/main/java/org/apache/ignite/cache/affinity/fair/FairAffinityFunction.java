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
import org.apache.ignite.cache.affinity.AffinityCentralizedFunction;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Fair affinity function which tries to ensure that all nodes get equal number of partitions with
 * minimum amount of reassignments between existing nodes.
 * <p>
 * Cache affinity can be configured for individual caches via
 * {@link CacheConfiguration#setAffinity(AffinityFunction)} method.
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

    /** */
    private final int parts;

    /**
     * Creates fair affinity with default partition count.
     */
    public FairAffinityFunction() {
        this(DFLT_PART_CNT);
    }

    /**
     * @param parts Number of partitions.
     */
    public FairAffinityFunction(int parts) {
        this.parts = parts;
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext ctx) {
        List<ClusterNode> topSnapshot = ctx.currentTopologySnapshot();

        if (topSnapshot.size() == 1) {
            ClusterNode primary = topSnapshot.get(0);

            return Collections.nCopies(parts, Collections.singletonList(primary));
        }

        List<List<ClusterNode>> assignment = createCopy(ctx);

        int tiers = Math.min(ctx.backups() + 1, topSnapshot.size());

        // Per tier pending partitions.
        Map<Integer, Queue<Integer>> pendingParts = new HashMap<>();

        FullAssignmentMap fullMap = new FullAssignmentMap(tiers, assignment, topSnapshot);

        for (int tier = 0; tier < tiers; tier++) {
            // Check if this is a new tier and add pending partitions.
            Queue<Integer> pending = pendingParts.get(tier);

            for (int part = 0; part < parts; part++) {
                if (fullMap.assignments.get(part).size() < tier + 1) {
                    if (pending == null) {
                        pending = new LinkedList<>();

                        pendingParts.put(tier, pending);
                    }

                    if (!pending.contains(part))
                        pending.add(part);

                }
            }

            // Assign pending partitions, if any.
            assignPending(tier, pendingParts, fullMap, topSnapshot);

            // Balance assignments.
            balance(tier, pendingParts, fullMap, topSnapshot);
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
     */
    private void assignPending(int tier, Map<Integer, Queue<Integer>> pendingMap, FullAssignmentMap fullMap,
        List<ClusterNode> topSnapshot) {
        Queue<Integer> pending = pendingMap.get(tier);

        if (F.isEmpty(pending))
            return;

        int idealPartCnt = parts / topSnapshot.size();

        Map<UUID, PartitionSet> tierMapping = fullMap.tierMapping(tier);

        PrioritizedPartitionMap underloadedNodes = filterNodes(tierMapping, idealPartCnt, false);

        // First iterate over underloaded nodes.
        assignPendingToUnderloaded(tier, pendingMap, fullMap, underloadedNodes, topSnapshot, false);

        if (!pending.isEmpty() && !underloadedNodes.isEmpty()) {
            // Same, forcing updates.
            assignPendingToUnderloaded(tier, pendingMap, fullMap, underloadedNodes, topSnapshot, true);
        }

        if (!pending.isEmpty())
            assignPendingToNodes(tier, pendingMap, fullMap, topSnapshot);

        assert pending.isEmpty();

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
     */
    private void assignPendingToUnderloaded(
        int tier,
        Map<Integer, Queue<Integer>> pendingMap,
        FullAssignmentMap fullMap,
        PrioritizedPartitionMap underloadedNodes,
        Collection<ClusterNode> topSnapshot,
        boolean force) {
        Iterator<Integer> it = pendingMap.get(tier).iterator();

        int ideal = parts / topSnapshot.size();

        while (it.hasNext()) {
            int part = it.next();

            for (PartitionSet set : underloadedNodes.assignments()) {
                ClusterNode node = set.node();

                assert node != null;

                if (fullMap.assign(part, tier, node, force, pendingMap)) {
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
     */
    private void assignPendingToNodes(int tier, Map<Integer, Queue<Integer>> pendingMap,
        FullAssignmentMap fullMap, List<ClusterNode> topSnapshot) {
        Iterator<Integer> it = pendingMap.get(tier).iterator();

        int idx = 0;

        while (it.hasNext()) {
            int part = it.next();

            int i = idx;

            boolean assigned = false;

            do {
                ClusterNode node = topSnapshot.get(i);

                if (fullMap.assign(part, tier, node, false, pendingMap)) {
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

                    if (fullMap.assign(part, tier, node, true, pendingMap)) {
                        it.remove();

                        assigned = true;
                    }

                    i = (i + 1) % topSnapshot.size();

                    if (assigned)
                        idx = i;
                } while (i != idx);
            }

            if (!assigned)
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
     */
    private void balance(int tier, Map<Integer, Queue<Integer>> pendingParts, FullAssignmentMap fullMap,
        Collection<ClusterNode> topSnapshot) {
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
                        if (fullMap.assign(part, tier, underloaded.node(), false, pendingParts)) {
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
                            if (fullMap.assign(part, tier, underloaded.node(), true, pendingParts)) {
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
     * @return Assignment copy and per node partition map.
     */
    private List<List<ClusterNode>> createCopy(AffinityFunctionContext ctx) {
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
            else {
                if (leftNodeId == null) {
                    partNodesCp = new ArrayList<>(partNodes.size() + 1); // Node joined.

                    partNodesCp.addAll(partNodes);
                }
                else {
                    partNodesCp = new ArrayList<>(partNodes.size());

                    for (ClusterNode affNode : partNodes) {
                        if (!affNode.id().equals(leftNodeId))
                            partNodesCp.add(affNode);
                    }
                }
            }

            cp.add(partNodesCp);
        }

        return cp;
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
     * Constructs assignment map for specified tier.
     *
     * @param tier Tier number, -1 for all tiers altogether.
     * @param assignment Assignment to construct map from.
     * @param topSnapshot Topology snapshot.
     * @return Assignment map.
     */
    private static Map<UUID, PartitionSet> assignments(int tier, List<List<ClusterNode>> assignment,
        Collection<ClusterNode> topSnapshot) {
        Map<UUID, PartitionSet> tmp = new LinkedHashMap<>();

        for (int part = 0; part < assignment.size(); part++) {
            List<ClusterNode> nodes = assignment.get(part);

            assert nodes instanceof RandomAccess;

            if (nodes.size() <= tier)
                continue;

            int start = tier < 0 ? 0 : tier;
            int end = tier < 0 ? nodes.size() : tier + 1;

            for (int i = start; i < end; i++) {
                ClusterNode n = nodes.get(i);

                PartitionSet set = tmp.get(n.id());

                if (set == null) {
                    set = new PartitionSet(n);

                    tmp.put(n.id(), set);
                }

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

    /**
     * Full assignment map. Auxiliary data structure which maintains resulting assignment and temporary
     * maps consistent.
     */
    @SuppressWarnings("unchecked")
    private static class FullAssignmentMap {
        /** Per-tier assignment maps. */
        private Map<UUID, PartitionSet>[] tierMaps;

        /** Full assignment map. */
        private Map<UUID, PartitionSet> fullMap;

        /** Resulting assignment. */
        private List<List<ClusterNode>> assignments;

        /**
         * @param tiers Number of tiers.
         * @param assignments Assignments to modify.
         * @param topSnapshot Topology snapshot.
         */
        private FullAssignmentMap(int tiers, List<List<ClusterNode>> assignments, Collection<ClusterNode> topSnapshot) {
            this.assignments = assignments;

            tierMaps = new Map[tiers];

            for (int tier = 0; tier < tiers; tier++)
                tierMaps[tier] = assignments(tier, assignments, topSnapshot);

            fullMap = assignments(-1, assignments, topSnapshot);
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
         * @param force Force flag.
         * @param pendingParts per tier pending partitions map.
         * @return {@code True} if assignment succeeded.
         */
        boolean assign(int part, int tier, ClusterNode node, boolean force, Map<Integer, Queue<Integer>> pendingParts) {
            UUID nodeId = node.id();

            if (!fullMap.get(nodeId).contains(part)) {
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

                        if (pending == null) {
                            pending = new LinkedList<>();

                            pendingParts.put(t, pending);
                        }

                        pending.add(part);

                        return true;
                    }
                }

                throw new IllegalStateException("Unable to assign partition to node while force is true.");
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