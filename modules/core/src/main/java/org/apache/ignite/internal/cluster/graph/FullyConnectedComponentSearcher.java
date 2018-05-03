package org.apache.ignite.internal.cluster.graph;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;

public class FullyConnectedComponentSearcher {
    /** The maximal number of nodes when brutforce algorithm will be used. */
    private static final int BRUT_FORCE_THRESHOULD = 24;

    private final int totalNodesCnt;

    private final BitSet[] connections;

    public FullyConnectedComponentSearcher(BitSet[] connections) {
        this.connections = connections;
        totalNodesCnt = connections.length;
    }

    /**
     * Find largest fully connected component from presented set of the nodes {@code where}.
     *
     * @param where Set of nodes where fully connected component must be found.
     * @return Set of nodes forming fully connected component.
     */
    public BitSet findLargest(BitSet where) {
        int nodes = where.cardinality();

        if (nodes <= BRUT_FORCE_THRESHOULD)
            return brutforce(nodes, where);

        // Return best of the 2 euristics.
        BitSet e1 = heuristic1(where);
        BitSet e2 = heuristic2(where);

        return e1.cardinality() > e2.cardinality() ? e1 : e2;
    }


    private BitSet heuristic1(BitSet set) {
        int nodes = set.cardinality();
        Integer[] indexes = extractNodeIndexes(nodes, set);

        Arrays.sort(indexes, new BitSetComparator(totalNodesCnt));

        return greedy(nodes, indexes);
    }


    private BitSet heuristic2(BitSet set) {
        int nodes = set.cardinality();
        Integer[] indexes = extractNodeIndexes(nodes, set);

        Arrays.sort(indexes, new BitSetComparator(totalNodesCnt).reversed());

        return greedy(nodes, indexes);
    }

    private BitSet greedy(int nodes, Integer[] indexes) {
        // Set of the nodes which can be used to form fully connected component.
        BitSet canUse = new BitSet(nodes);
        for (int i = 0; i < nodes; i++)
            canUse.set(i);

        BitSet bestResult = null;

        while (!canUse.isEmpty()) {
            // Even if we pick all possible nodes, their size will not be greater that current best result.
            // No needs to run next greedy iteration in this case.
            if (bestResult != null && canUse.cardinality() <= bestResult.cardinality())
                break;

            BitSet currentResult = new BitSet(nodes);

            Iterator<Integer> it = new BitSetIterator(canUse);

            while (it.hasNext()) {
                /* Try to add node to the current set that forms fully connected component.
                   Node will be skipped if after adding, current set loose fully connectivity. */
                int pendingIdx = it.next();

                boolean fullyConnected = true;

                Iterator<Integer> existedNodes = new BitSetIterator(currentResult);
                while (existedNodes.hasNext()) {
                    int existedIdx = existedNodes.next();

                    // If no connection between existing node and pending node, skip pending node.
                    if (!connections[indexes[pendingIdx]].get(indexes[existedIdx])) {
                        fullyConnected = false;

                        break;
                    }
                }

                if (fullyConnected)
                    currentResult.set(pendingIdx);
            }

            if (bestResult == null || currentResult.cardinality() > bestResult.cardinality())
                bestResult = currentResult;
        }

        // Replace relative node indexes (used in indexes) to absolute node indexes (used in whole graph connections).
        BitSet reindexedBestResult = new BitSet(totalNodesCnt);
        Iterator<Integer> it = new BitSetIterator(bestResult);
        while (it.hasNext())
            reindexedBestResult.set(indexes[it.next()]);

        return reindexedBestResult;
    }

    /**
     * Simple brutforce implementation which works in O(2^N * N^2) in worst case.
     *
     * @param nodes Nodes count.
     * @param set Set of nodes.
     * @return Subset of given {@code set} of nodes which forms fully connected component.
     */
    private BitSet brutforce(int nodes, BitSet set) {
        Integer[] indexes = extractNodeIndexes(nodes, set);

        int resultMask = -1;
        int maxCardinality = -1;

        // Iterate over all possible combinations of used nodes.
        for (int mask = (1 << nodes) - 1; mask > 0; mask--) {
            int cardinality = Integer.bitCount(mask);

            if (cardinality <= maxCardinality)
                continue;

            // Check that selected set of nodes forms fully connected component.
            boolean fullyConnected = true;

            for (int i = 0; i < nodes && fullyConnected; i++)
                if ((mask & (1 << i)) != 0)
                    for (int j = 0; j < nodes; j++)
                        if ((mask & (1 << j)) != 0) {
                            boolean connected = connections[indexes[i]].get(indexes[j]);

                            if (!connected) {
                                fullyConnected = false;

                                break;
                            }
                        }

            if (fullyConnected) {
                resultMask = mask;
                maxCardinality = cardinality;
            }
        }

        BitSet resultSet = new BitSet(nodes);

        for (int i = 0; i < nodes; i++) {
            if ((resultMask & (1 << i)) != 0) {
                int idx = indexes[i];

                assert set.get(idx)
                    : "Result contains node which is not presented in income set [nodeIdx" + idx + ", set=" + set + "]";

                resultSet.set(idx);
            }
        }

        assert resultSet.cardinality() > 0
            : "No nodes selected as fully connected component [set=" + set + "]";

        return resultSet;
    }

    private Integer[] extractNodeIndexes(int nodes, BitSet set) {
        Integer[] indexes = new Integer[nodes];
        Iterator<Integer> it = new BitSetIterator(set);
        int i = 0;

        while (it.hasNext())
            indexes[i++] = it.next();

        assert i == indexes.length
            : "Extracted not all indexes [nodes=" + nodes + ", extracted=" + i + ", set=" + set + "]";

        return indexes;
    }


    class BitSetComparator implements Comparator<Integer> {
        private final long[][] cached;

        BitSetComparator(int allNodes) {
            cached = new long[allNodes][];
        }

        private long[] connectionsAsLong(int idx) {
            if (cached[idx] != null)
                return cached[idx];

            return cached[idx] = connections[idx].toLongArray();
        }

        @Override public int compare(Integer o1, Integer o2) {
            long[] conn1 = connectionsAsLong(o1);
            long[] conn2 = connectionsAsLong(o2);

            int len = Math.min(conn1.length, conn2.length);
            for (int i = 0; i < len; i++) {
                int res = Long.compare(conn1[i], conn2[i]);

                if (res != 0)
                    return res;
            }

            return conn1.length - conn2.length;
        }
    }
}
