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

package org.apache.ignite.internal.cluster;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Random;
import org.apache.ignite.internal.cluster.graph.FullyConnectedComponentSearcher;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Class to test correctness of fully-connected component searching algorithm.
 */
@RunWith(Parameterized.class)
public class FullyConnectedComponentSearcherTest {
    /** Per test timeout */
    @Rule
    public Timeout globalTimeout = new Timeout((int) GridTestUtils.DFLT_TEST_TIMEOUT);

    /** Adjacency matrix provider for each test. */
    private AdjacencyMatrixProvider provider;

    /** Minimul acceptable result of size of fully-connected component for each test. */
    private int minAcceptableRes;

    /**
     * @param provider Adjacency matrix.
     * @param minAcceptableRes Expected result.
     */
    public FullyConnectedComponentSearcherTest(AdjacencyMatrixProvider provider, int minAcceptableRes) {
        this.provider = provider;
        this.minAcceptableRes = minAcceptableRes;
    }

    /**
     *
     */
    @Test
    public void testFind() {
        BitSet[] matrix = provider.provide();

        int nodes = matrix.length;

        BitSet all = new BitSet(nodes);
        for (int i = 0; i < nodes; i++)
            all.set(i);

        FullyConnectedComponentSearcher searcher = new FullyConnectedComponentSearcher(matrix);

        BitSet res = searcher.findLargest(all);
        int size = res.cardinality();

        Assert.assertTrue("Actual = " + size + ", Expected = " + minAcceptableRes,
            size >= minAcceptableRes);
    }

    /**
     * @return Test dataset.
     */
    @Parameterized.Parameters(name = "{index}: search({0}) >= {1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
            {new StaticMatrix(new String[] {
                "100",
                "010",
                "001",
            }), 1},
            {new StaticMatrix(new String[] {
                "101",
                "010",
                "101",
            }), 2},
            {new StaticMatrix(new String[] {
                "1101",
                "1111",
                "0110",
                "1101",
            }), 3},
            {new StaticMatrix(new String[] {
                "1111001",
                "1111000",
                "1111000",
                "1111000",
                "0000111",
                "0000111",
                "1000111",
            }), 4},
            {new AlmostSplittedMatrix(30, 100, 200), 200},
            {new AlmostSplittedMatrix(500, 1000, 2000), 2000},
            {new AlmostSplittedMatrix(1000, 2000, 3000), 3000},
            {new AlmostSplittedMatrix(30, 22, 25, 33, 27), 33},
            {new AlmostSplittedMatrix(1000, 400, 1000, 800), 1000},
            {new SeveralConnectionsAreLostMatrix(200, 10), 190},
            {new SeveralConnectionsAreLostMatrix(2000, 100), 1900},
            {new SeveralConnectionsAreLostMatrix(2000, 500), 1500},
            {new SeveralConnectionsAreLostMatrix(4000, 2000), 2000}
        });
    }

    /**
     * Provider for adjacency matrix for each test.
     */
    interface AdjacencyMatrixProvider {
        /**
         * @return Adjacency matrix.
         */
        BitSet[] provide();
    }

    /**
     * Static graph represented as array of strings. Each cell (i, j) in such matrix means that there is connection
     * between node(i) and node(j). Needed mostly to test bruteforce algorithm implementation.
     */
    static class StaticMatrix implements AdjacencyMatrixProvider {
        /** Matrix. */
        private final BitSet[] matrix;

        /**
         * @param strMatrix String matrix.
         */
        public StaticMatrix(@NotNull String[] strMatrix) {
            A.ensure(strMatrix.length > 0, "Matrix should not be empty");
            for (int i = 0; i < strMatrix.length; i++)
                A.ensure(strMatrix[i].length() == strMatrix.length,
                    "Matrix should be quadratic. Problem row: " + i);

            int nodes = strMatrix.length;

            matrix = init(nodes);

            for (int i = 0; i < nodes; i++)
                for (int j = 0; j < nodes; j++)
                    matrix[i].set(j, strMatrix[i].charAt(j) == '1');
        }

        /** {@inheritDoc} */
        @Override public BitSet[] provide() {
            return matrix;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "StaticMatrix{" +
                "matrix=" + Arrays.toString(matrix) +
                '}';
        }
    }

    /**
     * A graph splitted on several isolated fully-connected components,
     * but each of such component have some connections to another to reach graph connectivity.
     * Answer is this case should be the size max(Pi), where Pi size of each fully-connected component.
     */
    static class AlmostSplittedMatrix implements AdjacencyMatrixProvider {
        /** Partition sizes. */
        private final int[] partSizes;

        /** Connections between parts. */
        private final int connectionsBetweenParts;

        /** Matrix. */
        private final BitSet[] matrix;

        /**
         * @param connectionsBetweenParts Connections between parts.
         * @param partSizes Partition sizes.
         */
        public AlmostSplittedMatrix(int connectionsBetweenParts, int... partSizes) {
            A.ensure(connectionsBetweenParts >= 1 + partSizes.length, "There should be at least 1 connection between parts");
            A.ensure(partSizes.length >= 2, "The should be at least 2 parts of cluster");
            for (int i = 0; i < partSizes.length; i++)
                A.ensure(partSizes[i] > 0, "Part size " + (i + 1) + " shouldn't be empty");

            this.partSizes = partSizes.clone();
            this.connectionsBetweenParts = connectionsBetweenParts;

            int nodes = 0;
            for (int i = 0; i < partSizes.length; i++)
                nodes += partSizes[i];

            matrix = init(nodes);

            int[] startIdx = new int[partSizes.length];

            for (int i = 0, total = 0; i < partSizes.length; i++) {
                startIdx[i] = total;

                fillAll(matrix, total, total + partSizes[i]);

                total += partSizes[i];
            }

            Random random = new Random(777);

            for (int i = 0, part1 = 0; i < connectionsBetweenParts; i++) {
                int part2 = (part1 + 1) % partSizes.length;

                // Pick 2 random nodes from 2 parts and add connection between them.
                int idx1 = random.nextInt(partSizes[part1]) + startIdx[part1];
                int idx2 = random.nextInt(partSizes[part2]) + startIdx[part2];

                matrix[idx1].set(idx2);
                matrix[idx2].set(idx1);
            }
        }

        /** {@inheritDoc} */
        @Override public BitSet[] provide() {
            return matrix;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "AlmostSplittedGraph{" +
                "partSizes=" + Arrays.toString(partSizes) +
                ", connectionsBetweenParts=" + connectionsBetweenParts +
                '}';
        }
    }

    /**
     * Complete graph with several connections lost choosen randomly.
     * In worst case each lost connection decreases potential size of maximal fully-connected component.
     * So answer in this test case should be at least N - L, where N - nodes, L - lost connections.
     */
    static class SeveralConnectionsAreLostMatrix implements AdjacencyMatrixProvider {
        /** Nodes. */
        private final int nodes;

        /** Lost connections. */
        private final int lostConnections;

        /** Matrix. */
        private final BitSet[] matrix;

        /**
         * @param nodes Nodes.
         * @param lostConnections Lost connections.
         */
        public SeveralConnectionsAreLostMatrix(int nodes, int lostConnections) {
            A.ensure(nodes > 0, "There should be at least 1 node");

            this.nodes = nodes;
            this.lostConnections = lostConnections;

            this.matrix = init(nodes);

            fillAll(matrix, 0, nodes);

            Random random = new Random(777);

            for (int i = 0; i < lostConnections; i++) {
                int idx1 = random.nextInt(nodes);
                int idx2 = random.nextInt(nodes);

                if (idx1 == idx2)
                    continue;

                matrix[idx1].set(idx2, false);
                matrix[idx2].set(idx1, false);
            }
        }

        /** {@inheritDoc} */
        @Override public BitSet[] provide() {
            return matrix;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "SeveralConnectionsAreLost{" +
                "nodes=" + nodes +
                ", lostConnections=" + lostConnections +
                '}';
        }
    }

    /**
     * Utility method to pre-create adjacency matrix.
     *
     * @param nodes Nodes in graph.
     * @return Adjacency matrix.
     */
    private static BitSet[] init(int nodes) {
        BitSet[] matrix = new BitSet[nodes];
        for (int i = 0; i < nodes; i++)
            matrix[i] = new BitSet(nodes);

        return matrix;
    }

    /**
     * Utility method to fill all connections between all nodes from {@code fromIdx} and {@code endIdx} exclusive.
     *
     * @param matrix Adjacency matrix.
     * @param fromIdx Lower bound node index inclusive.
     * @param endIdx Upper bound node index exclusive.
     */
    private static void fillAll(BitSet[] matrix, int fromIdx, int endIdx) {
        for (int i = fromIdx; i < endIdx; i++)
            for (int j = fromIdx; j < endIdx; j++) {
                matrix[i].set(j);
                matrix[j].set(i);
            }
    }
}
