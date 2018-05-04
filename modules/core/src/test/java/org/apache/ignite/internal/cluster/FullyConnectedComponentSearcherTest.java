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
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class FullyConnectedComponentSearcherTest {

    private AdjacencyMatrixProvider provider;

    private int minAcceptableResult;

    public FullyConnectedComponentSearcherTest(AdjacencyMatrixProvider provider, int minAcceptableResult) {
        this.provider = provider;
        this.minAcceptableResult = minAcceptableResult;
    }

    @Test
    public void testFind() {
        BitSet[] matrix = provider.provide();

        int nodes = matrix.length;

        BitSet all = new BitSet(nodes);
        for (int i = 0; i < nodes; i++)
            all.set(i);

        FullyConnectedComponentSearcher searcher = new FullyConnectedComponentSearcher(matrix);

        BitSet result = searcher.findLargest(all);
        int size = result.cardinality();

        Assert.assertTrue("Actual = " + size + ", Expected = " + minAcceptableResult,
            size >= minAcceptableResult);
    }

    @Parameterized.Parameters(name = "{index}: search({0}) >= {1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
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

    interface AdjacencyMatrixProvider {
        BitSet[] provide();
    }

    static class StaticMatrix implements AdjacencyMatrixProvider {

        private final BitSet[] matrix;

        public StaticMatrix(@NotNull String[] stringMatrix) {
            A.ensure(stringMatrix.length > 0, "Matrix should not be empty");
            for (int i = 0; i < stringMatrix.length; i++)
                A.ensure(stringMatrix[i].length() == stringMatrix.length,
                    "Matrix should be quadratic. Problem row: " + i);

            int nodes = stringMatrix.length;

            matrix = init(nodes);

            for (int i = 0; i < nodes; i++)
                for (int j = 0; j < nodes; j++)
                    matrix[i].set(j, stringMatrix[i].charAt(j) == '1');
        }

        @Override public BitSet[] provide() {
            return matrix;
        }
    }

    static class AlmostSplittedMatrix implements AdjacencyMatrixProvider {

        private final int[] partSizes;

        private final int connectionsBetweenParts;

        private final BitSet[] matrix;

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

        @Override public BitSet[] provide() {
            return matrix;
        }

        @Override
        public String toString() {
            return "AlmostSplittedGraph{" +
                "partSizes=" + Arrays.toString(partSizes) +
                ", connectionsBetweenParts=" + connectionsBetweenParts +
                '}';
        }
    }

    static class SeveralConnectionsAreLostMatrix implements AdjacencyMatrixProvider {

        private final int nodes;

        private final int lostConnections;

        private final BitSet[] matrix;

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

        @Override public BitSet[] provide() {
            return matrix;
        }

        @Override
        public String toString() {
            return "SeveralConnectionsAreLost{" +
                "nodes=" + nodes +
                ", lostConnections=" + lostConnections +
                '}';
        }
    }

    private static void fillAll(BitSet[] matrix, int fromIdx, int endIdx) {
        for (int i = fromIdx; i < endIdx; i++)
            for (int j = fromIdx; j < endIdx; j++) {
                matrix[i].set(j);
                matrix[j].set(i);
            }
    }

    private static BitSet[] init(int nodes) {
        BitSet[] matrix = new BitSet[nodes];
        for (int i = 0; i < nodes; i++)
            matrix[i] = new BitSet(nodes);

        return matrix;
    }
}
