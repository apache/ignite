// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.affinity.partition;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * FIXDOC: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCachePartitionFairAffinity {
    private int parts;

    private int keyBackups;

    public GridCachePartitionFairAffinity(int parts, int keyBackups) {
        this.parts = parts;
        this.keyBackups = keyBackups;
    }

    public List<GridNode>[] assignPartitions(List<GridNode>[] prevAssignment,
        Collection<GridNode> topSnapshot, GridDiscoveryEvent evt) {
        assert prevAssignment != null || topSnapshot.size() == 1;
        assert prevAssignment == null || prevAssignment.length == parts;

        List<GridNode>[] assignments = createEmpty();

        if (prevAssignment == null) {
            GridNode n = F.first(topSnapshot);

            for (int i = 0; i < parts; i++)
                assignments[i].add(n);

            return assignments;
        }

        Map<UUID, GridNode> nodes = groupByNodeId(topSnapshot);

        if (evt.type() == GridEventType.EVT_NODE_JOINED) {
            GridNode evtNode = nodes.get(evt.eventNodeId());

            assert evtNode != null : "Added node is not present in topology";

            int actualBackups = Math.min(keyBackups, topSnapshot.size());

            int shiftIdx = 0;

            for (int tier = 0; tier < actualBackups; tier++) {
                // Must be linked hash map since order is important.
                LinkedHashMap<UUID, PartitionSet> perNodeAssignments = assignments(tier, prevAssignment, shiftIdx,
                    topSnapshot.size(), actualBackups);

                if (perNodeAssignments == null) {
                    assert tier == actualBackups - 1;

                    // Special case, node adds tier to the assignment table.
                    // There is only one node that can be assigned as a backup.
                    for (List<GridNode> assignment : assignments) {
                        GridNode lastBackup = remaining(topSnapshot, assignment);

                        assignment.add(lastBackup);
                    }
                }
                else {
                    assert !perNodeAssignments.containsKey(evt.eventNodeId());

                    int idealPartCnt = Math.round((float)parts / topSnapshot.size());

                    assert idealPartCnt != 0;

                    PartitionSet moving = new PartitionSet(evt.eventNodeId());

                    while (moving.size() != idealPartCnt) {
                        for (Map.Entry<UUID, PartitionSet> entry : perNodeAssignments.entrySet()) {
                            PartitionSet parts = entry.getValue();

                            while (true) {
                                int part = parts.next();

                                if (part == -1)
                                    break; // while.

                                if (assignableBackup(part, evtNode, tier, assignments)) {
                                    parts.remove();

                                    moving.add(part);

                                    break;
                                }
                            }

                            if (moving.size() == idealPartCnt)
                                break;
                        }
                    }

                    perNodeAssignments.put(evt.eventNodeId(), moving);

                    for (Map.Entry<UUID, PartitionSet> entry : perNodeAssignments.entrySet()) {
                        GridNode node = nodes.get(entry.getKey());

                        assert node != null;

                        for (int part : entry.getValue().partitions())
                            assignments[part].add(node);
                    }
                }

                // Check last tier distribution.
                // If distribution is uneven, fallback to first tier and try with different shift index.
                if (!checkDistribution(assignments, tier)) {
                    if (shiftIdx == 0)
                        shiftIdx = parts + 1;
                    else
                        shiftIdx--;

                    U.debug("Distribution check failed, will retry with shift: " + shiftIdx);

                    assignments = createEmpty();

                    tier = -1;
                }
            }

        }
        else {
            // TODO
            return null;
        }

        return assignments;
    }

    private boolean checkDistribution(List<GridNode>[] assignments, int tier) {
//        U.debug("Will do intermediate assignments check: " + Arrays.asList(assignments));

        Map<UUID, Collection<Integer>> nodeMap = new HashMap<>();

        for (int part = 0; part < assignments.length; part++) {
            GridNode n = assignments[part].get(tier);

            Collection<Integer> parts = nodeMap.get(n.id());

            if (parts == null) {
                parts = new HashSet<>();

                nodeMap.put(n.id(), parts);
            }

            boolean added = parts.add(part);

            assert added : "Duplicate partition: " + part;
        }

        int max = -1, min = Integer.MAX_VALUE;

        for (Collection<Integer> parts : nodeMap.values()) {
            max = Math.max(max, parts.size());
            min = Math.min(min, parts.size());
        }

//        U.debug("Checked distribution for tier [tier=" + tier + ", max=" + max + ", min=" + min + ']');

        boolean verified = max - min <= 2;

        if (!verified) {
            System.out.println("Verification failed, will retry: max=" + max + ", min=" + min);

            return verified;
        }

        nodeMap.clear();

        for (int part = 0; part < assignments.length; part++) {
            for (GridNode n : assignments[part]) {
                Collection<Integer> parts = nodeMap.get(n.id());

                if (parts == null) {
                    parts = new HashSet<>();

                    nodeMap.put(n.id(), parts);
                }

                boolean added = parts.add(part);

                assert added : "Duplicate partition: " + part;
            }
        }

        max = -1;
        min = Integer.MAX_VALUE;

        for (Collection<Integer> parts : nodeMap.values()) {
            max = Math.max(max, parts.size());
            min = Math.min(min, parts.size());
        }

//        U.debug("Checked distribution for assignment [max=" + max + ", min=" + min + ']');

        verified = max - min <= keyBackups + 1;

        if (!verified) {
            System.out.println("Verification failed, will retry222: max=" + max + ", min=" + min);

            return verified;
        }

        return true;
    }

    private GridNode remaining(Iterable<GridNode> topSnapshot, Collection<GridNode> assigned) {
        for (GridNode node : topSnapshot) {
            if (!assigned.contains(node))
                return node;
        }

        throw new IllegalStateException("Failed to find remaining backup node (all topology nodes were assigned) " +
            "[topSnapshot=" + topSnapshot + ", assigned=" + assigned + ']');
    }

    private boolean assignableBackup(int part, GridNode evtNode, int tier, List<GridNode>[] assignments) {
        List<GridNode> assignedNodes = assignments[part];

        for (int i = 0; i < tier; i++) {
            if (evtNode.equals(assignedNodes.get(i)))
                return false;
        }

        return true;
    }

    private Map<UUID, GridNode> groupByNodeId(Collection<GridNode> snap) {
        Map<UUID, GridNode> res = new HashMap<>(snap.size(), 1.0f);

        for (GridNode n : snap)
            res.put(n.id(), n);

        return res;
    }

    private LinkedHashMap<UUID, PartitionSet> assignments(int tier, List<GridNode>[] prevAssignment, int shiftIdx,
        int topSize, int totalTiers) {
        Map<UUID, PartitionSet> tmp = new LinkedHashMap<>(topSize, 1.0f);

        for (int part = 0; part < prevAssignment.length; part++) {
            List<GridNode> nodes = prevAssignment[part];

            assert nodes instanceof RandomAccess;

            if (nodes.size() <= tier)
                return null;

            GridNode n = nodes.get(tier);

            PartitionSet set = tmp.get(n.id());

            if (set == null) {
                set = new PartitionSet(n.id());

                tmp.put(n.id(), set);
            }

            set.add(part);
        }

        // Sort partition sets by size in descending order.
        List<PartitionSet> cp = new ArrayList<>(tmp.size());

        cp.addAll(tmp.values());

        Collections.sort(cp);

//        U.debug("Sorted partition sets: " + cp);

        assert cp.size() == topSize - 1 : "cp.size=" + cp.size() + ", topSize=" + topSize;

        LinkedHashMap<UUID, PartitionSet> res = new LinkedHashMap<>();

        int tierBase = shiftIdx / totalTiers;

        int tierAdjust = (shiftIdx % totalTiers) > tier ? 1 : 0;

        shiftIdx = tierBase + tierAdjust;

        int base = shiftIdx / topSize;

        for (int i = 0; i < cp.size(); i++) {
            PartitionSet set = cp.get(i);

            int adjust = (shiftIdx % topSize) > i ? 1 : 0;

            int shift = base + adjust;

            set.shift(shift);

//            U.debug("Set shifted partitions [nodeId=" + set.nodeId() + ", parts=" + set.partitions() +
//                ", shift=" + shift + ", base=" + base + ", topSize=" + topSize + ", adjust=" + adjust +
//                ", i=" + i + ']');

            res.put(set.nodeId(), set);
        }

        return res;
    }

    @SuppressWarnings("unchecked")
    private List<GridNode>[] createEmpty() {
        List<GridNode>[] assigns = new List[parts];

        for (int i = 0; i < assigns.length; i++)
            assigns[i] = new ArrayList<>(); // TODO size.

        return assigns;
    }

    @SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
    private static class PartitionSet implements Comparable<PartitionSet> {
        /** */
        private UUID nodeId;

        /** Partitions. */
        private LinkedList<Integer> parts = new LinkedList<>();

        /** Iterator. */
        private Iterator<Integer> it;

        /**
         * @param nodeId Node ID.
         */
        private PartitionSet(UUID nodeId) {
            this.nodeId = nodeId;
        }

        public UUID nodeId() {
            return nodeId;
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

        public void add(int part) {
            if (it != null)
                throw new IllegalStateException();

            parts.add(part);
        }

        public Collection<Integer> partitions() {
            return parts;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(PartitionSet o) {
            return o.parts.size() < parts.size() ? -1 : o.parts.size() > parts.size() ? 1 : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "PartSet [nodeId=" + nodeId + ", size=" + parts.size() + ']';
        }
    }
}
