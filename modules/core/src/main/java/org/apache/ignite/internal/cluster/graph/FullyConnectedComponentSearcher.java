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

package org.apache.ignite.internal.cluster.graph;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Class to find (possibly) largest fully-connected component (also can be called as <b>complete subgraph</b>) in graph.
 * This problem is also known as <b>Clique problem</b> which is NP-complete.
 *
 * For small number of nodes simple brute-force algorithm is used which finds such component guaranteed.
 * For large number of nodes some sort of greedy heuristic is used which works well for real-life scenarios
 * but doesn't guarantee to find largest component, however very close to ideal result.
 */
public class FullyConnectedComponentSearcher {
    /** The maximal number of nodes when bruteforce algorithm will be used. */
    private static final int BRUTE_FORCE_THRESHOULD = 24;

    /** Number of nodes in connections graph. */
    private final int totalNodesCnt;

    /** Adjacency matrix. */
    private final BitSet[] connections;

    /**
     * Constructor.
     *
     * @param connections Adjacency matrix.
     */
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
        int nodesCnt = where.cardinality();

        if (nodesCnt <= BRUTE_FORCE_THRESHOULD)
            return bruteforce(nodesCnt, where);

        // Return best of the 2 heuristics.
        BitSet e1 = heuristic1(where);
        BitSet e2 = heuristic2(where);

        return e1.cardinality() > e2.cardinality() ? e1 : e2;
    }

    /**
     * Extract node indexes (set bits) from given {@code selectedSet} to integer array.
     *
     * @param selectedNodesCnt Number of nodes.
     * @param selectedSet Set of nodes.
     * @return Arrays which contains node indexes.
     */
    private Integer[] extractNodeIndexes(int selectedNodesCnt, BitSet selectedSet) {
        Integer[] indexes = new Integer[selectedNodesCnt];
        Iterator<Integer> it = new BitSetIterator(selectedSet);
        int i = 0;

        while (it.hasNext())
            indexes[i++] = it.next();

        assert i == indexes.length
            : "Extracted not all indexes [nodesCnt=" + selectedNodesCnt + ", extracted=" + i + ", set=" + selectedSet + "]";

        return indexes;
    }

    /**
     * Sorts nodes using {@link ConnectionsComparator}
     * and runs greedy algorithm {@link #greedyIterative(int, Integer[])} on it.
     *
     * @param selectedSet Set of nodes used to form fully-connected component.
     * @return Subset of given {@code selectedSet} which forms fully connected component.
     */
    private BitSet heuristic1(BitSet selectedSet) {
        int selectedNodesCnt = selectedSet.cardinality();
        Integer[] nodeIndexes = extractNodeIndexes(selectedNodesCnt, selectedSet);

        Arrays.sort(nodeIndexes, new ConnectionsComparator(totalNodesCnt));

        return greedyIterative(selectedNodesCnt, nodeIndexes);
    }

    /**
     * Exactly the same thing as in {@link #heuristic1(BitSet)} but using reversed {@link ConnectionsComparator}.
     *
     * @param selectedSet Set of nodes used to form fully-connected component.
     * @return Subset of given {@code selectedSet} which forms fully connected component.
     */
    private BitSet heuristic2(BitSet selectedSet) {
        int selectedNodesCnt = selectedSet.cardinality();
        Integer[] nodeIndexes = extractNodeIndexes(selectedNodesCnt, selectedSet);

        Arrays.sort(nodeIndexes, new ConnectionsComparator(totalNodesCnt).reversed());

        return greedyIterative(selectedNodesCnt, nodeIndexes);
    }

    /**
     * Finds fully-connected component between given {@code nodeIndexes} and tries to maximize size of it.
     *
     * The main idea of the algorithm is that after specific sorting,
     * nodes able to form fully-connected will be placed closer to each other in given {@code nodeIndexes} array.
     * While nodes not able to form will be placed further.
     *
     * At the begging of algorithm we form global set of nodes can be used to form fully-connected component.
     * We iterate over this set and try to add each node to current fully-connected component, which is empty at the beginning.
     *
     * When we add node to the component we need to check that after adding new component is also fully-connected.
     * See {@link #joinNode(BitSet, int, Integer[])}.
     *
     * After end of iteration we exclude nodes which formed fully-connected from the global set and run iteration again and again
     * on remaining nodes, while the global set will not be empty.
     *
     * Complexity is O(N^2), where N is number of nodes.
     *
     * @param selectedNodesCnt Number of nodes.
     * @param nodeIndexes Node indexes used to form fully-connected component.
     * @return Subset of given {@code nodeIndexes} which forms fully connected component.
     */
    private BitSet greedyIterative(int selectedNodesCnt, Integer[] nodeIndexes) {
        // Set of the nodes which can be used to form fully connected component.
        BitSet canUse = new BitSet(selectedNodesCnt);
        for (int i = 0; i < selectedNodesCnt; i++)
            canUse.set(i);

        BitSet bestRes = null;

        while (!canUse.isEmpty()) {
            // Even if we pick all possible nodes, their size will not be greater than current best result.
            // No needs to run next iteration in this case.
            if (bestRes != null && canUse.cardinality() <= bestRes.cardinality())
                break;

            BitSet currRes = new BitSet(selectedNodesCnt);

            Iterator<Integer> canUseIter = new BitSetIterator(canUse);
            while (canUseIter.hasNext()) {
                /* Try to add node to the current set that forms fully connected component.
                   Node will be skipped if after adding, current set loose fully connectivity. */
                int pickedIdx = canUseIter.next();

                if (joinNode(currRes, pickedIdx, nodeIndexes)) {
                    currRes.set(pickedIdx);
                    canUse.set(pickedIdx, false);
                }
            }

            if (bestRes == null || currRes.cardinality() > bestRes.cardinality())
                bestRes = currRes;
        }

        // Try to improve our best result, if it was formed on second or next iteration.
        for (int nodeIdx = 0; nodeIdx < selectedNodesCnt; nodeIdx++)
            if (!bestRes.get(nodeIdx) && joinNode(bestRes, nodeIdx, nodeIndexes))
                bestRes.set(nodeIdx);

        // Replace relative node indexes (used in indexes) to absolute node indexes (used in whole graph connections).
        BitSet reindexedBestRes = new BitSet(totalNodesCnt);
        Iterator<Integer> it = new BitSetIterator(bestRes);
        while (it.hasNext())
            reindexedBestRes.set(nodeIndexes[it.next()]);

        return reindexedBestRes;
    }

    /**
     * Checks that given {@code nodeIdx} can be joined to current fully-connected component,
     * so after join result component will be also fully-connected.
     *
     * @param currComponent Current fully-connected component.
     * @param nodeIdx Node relative index.
     * @param nodeIndexes Node absolute indexes.
     * @return {@code True} if given node can be joined to {@code currentComponent}.
     */
    private boolean joinNode(BitSet currComponent, int nodeIdx, Integer[] nodeIndexes) {
        boolean fullyConnected = true;

        Iterator<Integer> alreadyUsedIter = new BitSetIterator(currComponent);
        while (alreadyUsedIter.hasNext()) {
            int existedIdx = alreadyUsedIter.next();

            // If no connection between existing node and picked node, skip picked node.
            if (!connections[nodeIndexes[nodeIdx]].get(nodeIndexes[existedIdx])) {
                fullyConnected = false;

                break;
            }
        }

        return fullyConnected;
    }

    /**
     * Simple bruteforce implementation which works in O(2^N * N^2), where N is number of nodes.
     *
     * @param selectedNodesCnt Nodes count.
     * @param selectedSet Set of nodes.
     * @return Subset of given {@code set} of nodes which forms fully connected component.
     */
    private BitSet bruteforce(int selectedNodesCnt, BitSet selectedSet) {
        Integer[] indexes = extractNodeIndexes(selectedNodesCnt, selectedSet);

        int resMask = -1;
        int maxCardinality = -1;

        // Iterate over all possible combinations of used nodes.
        for (int mask = (1 << selectedNodesCnt) - 1; mask > 0; mask--) {
            int cardinality = Integer.bitCount(mask);

            if (cardinality <= maxCardinality)
                continue;

            // Check that selected set of nodes forms fully connected component.
            boolean fullyConnected = true;

            for (int i = 0; i < selectedNodesCnt && fullyConnected; i++)
                if ((mask & (1 << i)) != 0)
                    for (int j = 0; j < selectedNodesCnt; j++)
                        if ((mask & (1 << j)) != 0) {
                            boolean connected = connections[indexes[i]].get(indexes[j]);

                            if (!connected) {
                                fullyConnected = false;

                                break;
                            }
                        }

            if (fullyConnected) {
                resMask = mask;
                maxCardinality = cardinality;
            }
        }

        BitSet resSet = new BitSet(selectedNodesCnt);

        for (int i = 0; i < selectedNodesCnt; i++) {
            if ((resMask & (1 << i)) != 0) {
                int idx = indexes[i];

                assert selectedSet.get(idx)
                    : "Result contains node which is not presented in income set [nodeIdx" + idx + ", set=" + selectedSet + "]";

                resSet.set(idx);
            }
        }

        assert resSet.cardinality() > 0
            : "No nodes selected as fully connected component [set=" + selectedSet + "]";

        return resSet;
    }

    /**
     * Comparator which sorts nodes by their connections array.
     *
     * Suppose you have connections matrix:
     *
     * 1111
     * 1101
     * 1010
     * 1101
     *
     * Each connection row associated with some node.
     * Comparator will sort node indexes using their connection rows as very big binary numbers, as in example:
     *
     * 1111
     * 1101
     * 1101
     * 1011
     *
     * Note: Comparator sorts only node indexes, actual connection rows swapping will be not happened.
     */
    private class ConnectionsComparator implements Comparator<Integer> {
        /** Cache each connection long array representation, to avoid creating new object for each comparison. */
        private final long[][] cachedConnRows;

        /**
         * Constructor
         * @param totalNodesCnt Total number of nodes in the cluster.
         */
        ConnectionsComparator(int totalNodesCnt) {
            cachedConnRows = new long[totalNodesCnt][];
        }

        /**
         * Returns long array representation of connection row for given node {@code idx}.
         *
         * @param idx Node index.
         * @return Long array connection row representation.
         */
        private long[] connectionRow(int idx) {
            if (cachedConnRows[idx] != null)
                return cachedConnRows[idx];

            return cachedConnRows[idx] = connections[idx].toLongArray();
        }

        /** {@inheritDoc} */
        @Override public int compare(Integer node1, Integer node2) {
            long[] conn1 = connectionRow(node1);
            long[] conn2 = connectionRow(node2);

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
