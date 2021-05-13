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

package org.apache.ignite.internal.affinity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static java.util.Objects.nonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for affinity function.
 */
public class RendezvousAffinityFunctionTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(RendezvousAffinityFunctionTest.class);

    /** Affinity deviation ratio. */
    public static final double AFFINITY_DEVIATION_RATIO = 0.2;

    @Test
    public void testPartitionDistribution() {
        int nodes = 50;

        int parts = 10_000;

        int replicas = 4;

        ArrayList<ClusterNode> clusterNodes = prepareNetworkTopology(nodes);

        assertTrue(parts > nodes, "Partitions should be more that nodes");

        int ideal = (parts * replicas) / nodes;

        List<List<ClusterNode>> assignment = RendezvousAffinityFunction.assignPartitions(
            clusterNodes,
            parts,
            replicas,
            false,
            null
        );

        HashMap<ClusterNode, ArrayList<Integer>> assignmentByNode = new HashMap<>(nodes);

        int part = 0;

        for (List<ClusterNode> partNodes : assignment) {
            for (ClusterNode node : partNodes) {
                ArrayList<Integer> nodeParts = assignmentByNode.get(node);

                if (nodeParts == null)
                    assignmentByNode.put(node, nodeParts = new ArrayList<>());

                nodeParts.add(part);
            }

            part++;
        }

        for (ClusterNode node : clusterNodes) {
            ArrayList<Integer> nodeParts = assignmentByNode.get(node);

            assertNotNull(nodeParts);

            assertTrue(nodeParts.size() > ideal * (1 - AFFINITY_DEVIATION_RATIO)
                    && nodeParts.size() < ideal * (1 + AFFINITY_DEVIATION_RATIO),
                "Partition distribution is too far from ideal [node=" + node
                    + ", size=" + nodeParts.size()
                    + ", idealSize=" + ideal
                    + ", parts=" + compact(nodeParts) + ']');
        }
    }

    @NotNull private ArrayList<ClusterNode> prepareNetworkTopology(int nodes) {
        ArrayList<ClusterNode> clusterNodes = new ArrayList<>(nodes);

        for (int i = 0; i < nodes; i++)
            clusterNodes.add(new ClusterNode(UUID.randomUUID().toString(), "Node " + i, "127.0.0.1", 121212));
        return clusterNodes;
    }

    @Test
    public void serializeAssignment() {
        int nodes = 50;

        int parts = 10_000;

        int replicas = 4;

        ArrayList<ClusterNode> clusterNodes = prepareNetworkTopology(nodes);

        assertTrue(parts > nodes, "Partitions should be more that nodes");

        List<List<ClusterNode>> assignment = RendezvousAffinityFunction.assignPartitions(
            clusterNodes,
            parts,
            replicas,
            false,
            null
        );

        byte[] assignmentBytes = ByteUtils.toBytes(assignment);

        assertNotNull(assignment);

        LOG.info("Assignment is serialized successfully [bytes=" + assignmentBytes.length + ']');

        List<List<ClusterNode>> deserializedAssignment = (List<List<ClusterNode>>)ByteUtils.fromBytes(assignmentBytes);

        assertNotNull(deserializedAssignment);

        assertEquals(assignment, deserializedAssignment);
    }

    /**
     * Returns sorted and compacted string representation of given {@code col}. Two nearby numbers with difference at
     * most 1 are compacted to one continuous segment. E.g. collection of [1, 2, 3, 5, 6, 7, 10] will be compacted to
     * [1-3, 5-7, 10].
     *
     * @param col Collection of integers.
     * @return Compacted string representation of given collections.
     */
    public static String compact(Collection<Integer> col) {
        return compact(col, i -> i + 1);
    }

    /**
     * Returns sorted and compacted string representation of given {@code col}. Two nearby numbers are compacted to one
     * continuous segment. E.g. collection of [1, 2, 3, 5, 6, 7, 10] with {@code nextValFun = i -> i + 1} will be
     * compacted to [1-3, 5-7, 10].
     *
     * @param col Collection of numbers.
     * @param nextValFun Function to get nearby number.
     * @return Compacted string representation of given collections.
     */
    public static <T extends Number & Comparable<? super T>> String compact(
        Collection<T> col,
        Function<T, T> nextValFun
    ) {
        assert nonNull(col);
        assert nonNull(nextValFun);

        if (col.isEmpty())
            return "[]";

        StringBuffer sb = new StringBuffer();
        sb.append('[');

        List<T> l = new ArrayList<>(col);
        Collections.sort(l);

        T left = l.get(0), right = left;
        for (int i = 1; i < l.size(); i++) {
            T val = l.get(i);

            if (right.compareTo(val) == 0 || nextValFun.apply(right).compareTo(val) == 0) {
                right = val;
                continue;
            }

            if (left.compareTo(right) == 0)
                sb.append(left);
            else
                sb.append(left).append('-').append(right);

            sb.append(',').append(' ');

            left = right = val;
        }

        if (left.compareTo(right) == 0)
            sb.append(left);
        else
            sb.append(left).append('-').append(right);

        sb.append(']');

        return sb.toString();
    }
}
