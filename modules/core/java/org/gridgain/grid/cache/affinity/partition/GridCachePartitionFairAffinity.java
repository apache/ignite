// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.affinity.partition;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.logger.log4j.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Fair affinity function which tries to ensure that all nodes get equal number of partitions with
 * minimum amount of reassignments between existing nodes.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCachePartitionFairAffinity implements GridCacheAffinityFunction {
    /** Ascending comparator. */
    private static final Comparator<PartitionSet> ASC_CMP = new PartitionSetComparator(false);

    /** Descending comparator. */
    private static final Comparator<PartitionSet> DESC_CMP = new PartitionSetComparator(true);

    /** */
    private int parts;

    private static final GridLogger log = new GridLog4jLogger();

    public GridCachePartitionFairAffinity(int parts) {
        this.parts = parts;
    }

    /** {@inheritDoc} */
    @Override public List<List<GridNode>> assignPartitions(GridCacheAffinityFunctionContext ctx) {
        List<GridNode> topSnapshot = ctx.currentTopologySnapshot();
        //        if (prevAssignment != null)
//            U.debug(log, "Assigning partitions: " + Arrays.asList(prevAssignment) + ", topSnapshot=" + topSnapshot);

        if (topSnapshot.size() == 1) {
            GridNode primary = topSnapshot.get(0);

            List<List<GridNode>> assignments = new ArrayList<>(parts);

            for (int i = 0; i < parts; i++)
                assignments.add(Collections.singletonList(primary));

            return assignments;
        }

        GridBiTuple<List<List<GridNode>>, Map<UUID, PartitionSet>> cp = createCopy(ctx, topSnapshot);

        List<List<GridNode>> assignment = cp.get1();

        int tiers = Math.min(ctx.backups() + 1, topSnapshot.size());

        // Per tier pending partitions.
        Map<Integer, Queue<Integer>> pendingParts = new HashMap<>();

        FullAssignmentMap fullMap = new FullAssignmentMap(tiers, assignment, topSnapshot);

        for (int tier = 0; tier < tiers; tier++) {
//            U.debug(log, "Assigning tier: " + tier);

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

//            U.debug(log, "Assigning pending partitions for tier [tier=" + tier + ", pending=" + pendingParts + ']');

            // Assign pending partitions, if any.
            assignPending(tier, pendingParts, fullMap, topSnapshot);

//            U.debug(log, "Balancing parititions: " + Arrays.asList(fullMap.assignments) +
//                ", tier0=" + fullMap.tierMapping(0) + ", tier1=" + fullMap.tierMapping(1));

            // Balance assignments.
            balance(tier, pendingParts, fullMap, topSnapshot);
        }

//        U.debug(log, ">>>>> Assigned partitions: " + Arrays.asList(fullMap.assignments));

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
        return U.safeAbs(key.hashCode()) % parts;
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        // No-op.
    }

    private void assignPending(int tier, Map<Integer, Queue<Integer>> pendingMap, FullAssignmentMap fullMap,
        List<GridNode> topSnapshot) {
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

        if (!pending.isEmpty()) {
            assignPendingToNodes(tier, pendingMap, fullMap, topSnapshot);
        }

        assert pending.isEmpty();

        pendingMap.remove(tier);
    }

    private void assignPendingToUnderloaded(
        int tier,
        Map<Integer, Queue<Integer>> pendingMap,
        FullAssignmentMap fullMap,
        PrioritizedPartitionMap underloadedNodes,
        Collection<GridNode> topSnapshot,
        boolean force) {
        Iterator<Integer> it = pendingMap.get(tier).iterator();

        int ideal = parts / topSnapshot.size();

        while (it.hasNext()) {
            int part = it.next();

            for (PartitionSet set : underloadedNodes.assignments()) {
                GridNode node = set.node();

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

    private void assignPendingToNodes(int tier, Map<Integer, Queue<Integer>> pendingMap,
        FullAssignmentMap fullMap, List<GridNode> topSnapshot) {
        Iterator<Integer> it = pendingMap.get(tier).iterator();

        int idx = 0;

        while (it.hasNext()) {
            int part = it.next();

//            U.debug(log, "Trying to assign partition: " + part);

            int i = idx;

            boolean assigned = false;

            do {
                GridNode node = topSnapshot.get(i);

//                U.debug(log, "Inspecting node: " + node.id());

                if (fullMap.assign(part, tier, node, false, pendingMap)) {
                    it.remove();

                    assigned = true;
                }

                i = (i + 1) % topSnapshot.size();

                if (assigned)
                    idx = i;
            } while (i != idx);

            if (!assigned) {
//                U.debug(log, "Switching to force");

                do {
                    GridNode node = topSnapshot.get(i);

//                    U.debug(log, "Inspecting node: " + node.id());

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

     private Map<UUID, PartitionSet> perNodeAssignments(List<GridNode>[] assignments, int tier) {
        Map<UUID, PartitionSet> res = new HashMap<>();

        for (int part = 0; part < assignments.length; part++) {
            if (tier == -1) {
                for (GridNode node : assignments[part]) {
                    PartitionSet parts = res.get(node.id());

                    if (parts == null) {
                        parts = new PartitionSet(node);

                        res.put(node.id(), parts);
                    }

                    parts.add(part);
                }
            }
            else {
                GridNode node = assignments[part].get(tier);

                PartitionSet parts = res.get(node.id());

                if (parts == null) {
                    parts = new PartitionSet(node);

                    res.put(node.id(), parts);
                }

                parts.add(part);
            }
        }

        return res;
    }

    private void balance(int tier, Map<Integer, Queue<Integer>> pendingParts, FullAssignmentMap fullMap,
        List<GridNode> topSnapshot) {
        int idealPartCnt = parts / topSnapshot.size();

        Map<UUID, PartitionSet> mapping = fullMap.tierMapping(tier);

        PrioritizedPartitionMap underloadedNodes = filterNodes(mapping, idealPartCnt, false);
        PrioritizedPartitionMap overloadedNodes = filterNodes(mapping, idealPartCnt, true);

        do {
            boolean retry = false;

            for (PartitionSet overloaded : overloadedNodes.assignments()) {
//                U.debug(log, "Stealing partitions from overloaded set: " + overloaded);

                for (Integer part : overloaded.partitions()) {
//                    U.debug(log, "Trying to move partition: " + part);

                    boolean assigned = false;

                    for (PartitionSet underloaded : underloadedNodes.assignments()) {
//                        U.debug(log, "Trying to move partition to underloaded set: " + underloaded);

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
                        else {
//                            U.debug(log, "Failed to assign: " + part);
                        }
                    }

                    if (!assigned) {
                        for (PartitionSet underloaded : underloadedNodes.assignments()) {
//                            U.debug(log, "Trying to forcibly move partition to underloaded set: " + underloaded);

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
                            else {
//                                U.debug(log, "Failed to assign: " + part);
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

    private PrioritizedPartitionMap filterNodes(Map<UUID, PartitionSet> mapping, int idealPartCnt, boolean overloaded) {
        assert mapping != null;

        PrioritizedPartitionMap res = new PrioritizedPartitionMap(overloaded ? DESC_CMP : ASC_CMP);

        for (PartitionSet set : mapping.values()) {
            if ((overloaded && set.size() > idealPartCnt) || (!overloaded && set.size() < idealPartCnt))
               res.add(set);
        }

        return res;
    }

    @SuppressWarnings("unchecked")
    private GridBiTuple<List<List<GridNode>>, Map<UUID, PartitionSet>> createCopy(
        GridCacheAffinityFunctionContext ctx, Iterable<GridNode> topSnapshot) {
        GridDiscoveryEvent discoEvt = ctx.discoveryEvent();

        UUID leftNodeId = discoEvt.type() == GridEventType.EVT_NODE_JOINED ? null : discoEvt.eventNodeId();

        List<List<GridNode>> cp = new ArrayList<>(parts);

        Map<UUID, PartitionSet> parts = new HashMap<>();

        for (int part = 0; part < this.parts; part++) {
            List<GridNode> partNodes = ctx.previousAssignment(part);

            List<GridNode> partNodesCp = new ArrayList<>(partNodes.size());

            for (GridNode affNode : partNodes) {
                if (!affNode.id().equals(leftNodeId)) {
                    partNodesCp.add(affNode);

                    PartitionSet partSet = parts.get(affNode.id());

                    if (partSet == null) {
                        partSet = new PartitionSet(affNode);

                        parts.put(affNode.id(), partSet);
                    }

                    partSet.add(part);
                }
            }

            cp.add(partNodesCp);
        }

        if (leftNodeId == null) {
            // Node joined, find it and add empty set to mapping.
            GridNode joinedNode = null;

            for (GridNode node : topSnapshot) {
                if (node.id().equals(discoEvt.eventNodeId())) {
                    joinedNode = node;

                    break;
                }
            }

            assert joinedNode != null;

            parts.put(joinedNode.id(), new PartitionSet(joinedNode));
        }

        return F.t(cp, parts);
    }

    private Map<UUID, GridNode> groupByNodeId(Collection<GridNode> snap) {
        Map<UUID, GridNode> res = new HashMap<>(snap.size(), 1.0f);

        for (GridNode n : snap)
            res.put(n.id(), n);

        return res;
    }

    @SuppressWarnings("unchecked")
    private List<GridNode>[] createEmpty(int affNodes) {
        List<GridNode>[] assigns = new List[parts];

        for (int i = 0; i < assigns.length; i++)
            assigns[i] = new ArrayList<>(affNodes);

        return assigns;
    }

    /**
     *
     */
    private static class PartitionSetComparator implements Comparator<PartitionSet>, Serializable {
        /** */
        private boolean descending;

        /**
         * @param descending {@code True} if comparator should be descending.
         */
        private PartitionSetComparator(boolean descending) {
            this.descending = descending;
        }

        /** {@inheritDoc} */
        @Override public int compare(PartitionSet o1, PartitionSet o2) {
            int res = o1.parts.size() < o2.parts.size() ? -1 : o1.parts.size() > o2.parts.size() ? 1 : 0;

            return descending ? -res : res;
        }
    }

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

        public void remove(UUID uuid) {
            PartitionSet rmv = assignmentMap.remove(uuid);

            assignmentList.remove(rmv);
        }

        public boolean isEmpty() {
            return assignmentList.isEmpty();
        }
    }

    private static Map<UUID, PartitionSet> assignments(int tier, List<List<GridNode>> prevAssignment,
        List<GridNode> topSnapshot) {
        Map<UUID, PartitionSet> tmp = new LinkedHashMap<>();

        for (int part = 0; part < prevAssignment.size(); part++) {
            List<GridNode> nodes = prevAssignment.get(part);

            assert nodes instanceof RandomAccess;

            if (nodes.size() <= tier)
                continue;

            int start = tier < 0 ? 0 : tier;
            int end = tier < 0 ? nodes.size() : tier + 1;

            for (int i = start; i < end; i++) {
                GridNode n = nodes.get(i);

                PartitionSet set = tmp.get(n.id());

                if (set == null) {
                    set = new PartitionSet(n);

                    tmp.put(n.id(), set);
                }

                set.add(part);
            }
        }

        if (tmp.size() < topSnapshot.size()) {
            for (GridNode node : topSnapshot) {
                if (!tmp.containsKey(node.id()))
                    tmp.put(node.id(), new PartitionSet(node));
            }
        }

        return tmp;
    }

    @SuppressWarnings("unchecked")
    private static class FullAssignmentMap {
        private Map<UUID, PartitionSet>[] tierMaps;

        private Map<UUID, PartitionSet> fullMap;

        private List<List<GridNode>> assignments;

        private FullAssignmentMap(int tiers, List<List<GridNode>> assignments, List<GridNode> topSnapshot) {
            this.assignments = assignments;

            tierMaps = new Map[tiers];

            for (int tier = 0; tier < tiers; tier++)
                tierMaps[tier] = assignments(tier, assignments, topSnapshot);

            fullMap = assignments(-1, assignments, topSnapshot);
        }

        boolean assign(int part, int tier, GridNode node, boolean force, Map<Integer, Queue<Integer>> pendingParts) {
//            U.debug(log, "Assigning partition to node [part=" + part + ", tier=" + tier + ", nodeId=" + node.id() +
//                ", force=" + force + ", pending=" + pendingParts + ']');

            UUID nodeId = node.id();

            if (!fullMap.get(nodeId).contains(part)) {
                tierMaps[tier].get(nodeId).add(part);

                fullMap.get(nodeId).add(part);

                List<GridNode> assignment = assignments.get(part);

                if (assignment.size() <= tier)
                    assignment.add(node);
                else {
                    GridNode oldNode = assignment.set(tier, node);

                    if (oldNode != null) {
                        UUID oldNodeId = oldNode.id();

                        tierMaps[tier].get(oldNodeId).remove(part);
                        fullMap.get(oldNodeId).remove(part);
                    }
                }

//                U.debug(log, "Assigned");

                return true;
            }
            else if (force) {
                assert !tierMaps[tier].get(nodeId).contains(part);

                // Check previous tiers first.
                for (int t = 0; t < tier; t++) {
//                    U.debug(log, "Check1: " + t);

                    if (tierMaps[t].get(nodeId).contains(part)) {
//                        U.debug(log, "Cannot assign partition to node since partition is present on tier [tier=" + t
//                            + ']');

                        return false;
                    }
                }

//                U.debug(log, "tierMaps.length=" + tierMaps.length);

                // Partition is on some lower tier, switch it.
                for (int t = tier + 1; t < tierMaps.length; t++) {
//                    U.debug(log, "Check2: " + t + ", " + tierMaps[t].get(nodeId) + ", nodeId=" + nodeId);

                    if (tierMaps[t].get(nodeId).contains(part)) {
//                        U.debug(log, "Contains!");

                        GridNode oldNode = assignments.get(part).get(tier);

                        assert oldNode != null;

                        // Move partition from level t to tier.
                        assignments.get(part).set(tier, node);
                        assignments.get(part).set(t, null);

                        tierMaps[tier].get(oldNode.id()).remove(part);
                        tierMaps[tier].get(nodeId).add(part);
                        fullMap.get(oldNode.id()).remove(part);
                        tierMaps[t].get(nodeId).remove(part);

                        Queue<Integer> pending = pendingParts.get(t);

                        if (pending == null) {
                            pending = new LinkedList<>();

                            pendingParts.put(t, pending);
                        }

                        pending.add(part);

//                        U.debug(log, "Added partition to pending set: " + pending + ", t=" + t +
//                            ", pending=" + pending);

                        return true;
                    }
                    else {
//                        U.debug(log, "Does not contain part: " + part);
                    }
                }

                throw new IllegalStateException("Unable to assign partition to node while force is true.");
            }

            // !force.
//            U.debug(log, "Failed to assign partition to node since partition is present in full map.");

            return false;
        }

        public Map<UUID, PartitionSet> tierMapping(int tier) {
            return tierMaps[tier];
        }
    }

    @SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
    private static class PartitionSet {
        /** */
        private GridNode node;

        /** Partitions. */
        private LinkedList<Integer> parts = new LinkedList<>();

        /** Iterator. */
        private Iterator<Integer> it;

        /**
         * @param node Node.
         */
        private PartitionSet(GridNode node) {
            this.node = node;
        }

        public GridNode node() {
            return node;
        }

        public UUID nodeId() {
            return node.id();
        }

        public int size() {
            return parts.size();
        }

        public int next() {
            if (it == null)
                it = parts.iterator();

            return it.hasNext() ? it.next() : -1;
        }

        public void remove() {
            if (it == null)
                throw new IllegalStateException();

            it.remove();
        }

        public void shift(int shiftIdx) {
            for (int i = 0; i < shiftIdx; i++)
                parts.add(parts.pollFirst());
        }

        public boolean add(int part) {
            if (it != null)
                throw new IllegalStateException();

            if (!parts.contains(part)) {
                parts.add(part);

                return true;
            }

            return false;
        }

        public void remove(Integer part) {
            if (it != null)
                throw new IllegalStateException();

            parts.remove(part); // Remove object, not index.
        }

        public Collection<Integer> partitions() {
            return parts;
        }

        public boolean contains(int part) {
            return parts.contains(part);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "PartSet [nodeId=" + node.id() + ", size=" + parts.size() + ", parts=" + parts + ']';
        }
    }
}
