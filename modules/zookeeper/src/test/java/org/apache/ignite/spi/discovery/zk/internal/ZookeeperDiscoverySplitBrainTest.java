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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Zookeeper SPI discovery.
 */
public class ZookeeperDiscoverySplitBrainTest extends ZookeeperDiscoverySpiTestBase {
    /**
     * A simple split-brain test, where cluster spliited on 2 parts of server nodes (2 and 3).
     * There is also client which sees both parts of splitted cluster.
     *
     * Result cluster should be: 3 server nodes + 1 client.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleSplitBrain() throws Exception {
        failCommSpi = true;

        startGridsMultiThreaded(5);

        startClientGridsMultiThreaded(5, 3);

        awaitPartitionMapExchange();

        List<ClusterNode> all = G.allGrids().stream()
            .map(g -> g.cluster().localNode())
            .collect(Collectors.toList());

        List<ClusterNode> part1 = all.subList(0, 3);
        List<ClusterNode> part2 = all.subList(3, all.size());

        ConnectionsFailureMatrix matrix = ConnectionsFailureMatrix.buildFrom(part1, part2);

        ClusterNode lastClient = startGrid(8).cluster().localNode();

        awaitPartitionMapExchange();

        // Make last client connected to other nodes.
        for (ClusterNode node : all) {
            if (node.id().equals(lastClient.id()))
                continue;

            matrix.addConnection(lastClient, node);
            matrix.addConnection(node, lastClient);
        }

        PeerToPeerCommunicationFailureSpi.fail(matrix);

        waitForTopology(4);
    }

    /**
     * A simple not actual split-brain test, where some connections between server nodes are lost.
     * Server nodes: 5.
     * Client nodes: 5.
     * Lost connections between server nodes: 2.
     *
     * Result cluster should be: 3 server nodes + 5 clients.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotActualSplitBrain() throws Exception {
        failCommSpi = true;

        startGridsMultiThreaded(5);

        List<ClusterNode> srvNodes = G.allGrids().stream()
            .map(g -> g.cluster().localNode())
            .collect(Collectors.toList());

        Assert.assertEquals(5, srvNodes.size());

        startClientGridsMultiThreaded(5, 3);

        awaitPartitionMapExchange();

        ConnectionsFailureMatrix matrix = new ConnectionsFailureMatrix();

        List<ClusterNode> allNodes = G.allGrids().stream().map(g -> g.cluster().localNode()).collect(Collectors.toList());

        matrix.addAll(allNodes);

        // Remove 2 connections between server nodes.
        matrix.removeConnection(srvNodes.get(0), srvNodes.get(1));
        matrix.removeConnection(srvNodes.get(1), srvNodes.get(0));
        matrix.removeConnection(srvNodes.get(2), srvNodes.get(3));
        matrix.removeConnection(srvNodes.get(3), srvNodes.get(2));

        PeerToPeerCommunicationFailureSpi.fail(matrix);

        waitForTopology(8);
    }

    /**
     * Almost split-brain test, server nodes splitted on 2 parts and there are some connections between these 2 parts.
     * Server nodes: 5.
     * Client nodes: 5.
     * Splitted on: 3 servers + 2 clients and 3 servers + 2 clients.
     * Extra connections between server nodes: 3.
     *
     * Result cluster should be: 3 server nodes + 2 clients.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAlmostSplitBrain() throws Exception {
        failCommSpi = true;

        startGridsMultiThreaded(6);

        List<ClusterNode> srvNodes = G.allGrids().stream()
            .map(g -> g.cluster().localNode())
            .collect(Collectors.toList());

        Assert.assertEquals(6, srvNodes.size());

        List<ClusterNode> srvPart1 = srvNodes.subList(0, 3);
        List<ClusterNode> srvPart2 = srvNodes.subList(3, srvNodes.size());

        startClientGridsMultiThreaded(6, 5);

        awaitPartitionMapExchange();

        List<ClusterNode> clientNodes = G.allGrids().stream()
            .map(g -> g.cluster().localNode())
            .filter(ClusterNode::isClient)
            .collect(Collectors.toList());

        Assert.assertEquals(5, clientNodes.size());

        List<ClusterNode> clientPart1 = clientNodes.subList(0, 2);
        List<ClusterNode> clientPart2 = clientNodes.subList(2, 4);

        List<ClusterNode> splittedPart1 = new ArrayList<>();
        splittedPart1.addAll(srvPart1);
        splittedPart1.addAll(clientPart1);

        List<ClusterNode> splittedPart2 = new ArrayList<>();
        splittedPart2.addAll(srvPart2);
        splittedPart2.addAll(clientPart2);

        ConnectionsFailureMatrix matrix = new ConnectionsFailureMatrix();

        matrix.addAll(splittedPart1);
        matrix.addAll(splittedPart2);

        matrix.addConnection(srvPart1.get(0), srvPart2.get(1));
        matrix.addConnection(srvPart2.get(1), srvPart1.get(0));

        matrix.addConnection(srvPart1.get(1), srvPart2.get(2));
        matrix.addConnection(srvPart2.get(2), srvPart1.get(1));

        matrix.addConnection(srvPart1.get(2), srvPart2.get(0));
        matrix.addConnection(srvPart2.get(0), srvPart1.get(2));

        PeerToPeerCommunicationFailureSpi.fail(matrix);

        waitForTopology(5);
    }

    /**
     * Class represents available connections between cluster nodes.
     * This is needed to simulate network problems in {@link PeerToPeerCommunicationFailureSpi}.
     */
    static class ConnectionsFailureMatrix {
        /** Available connections per each node id. */
        private Map<UUID, Set<UUID>> availableConnections = new HashMap<>();

        /**
         * @param from Cluster node 1.
         * @param to Cluster node 2.
         * @return {@code True} if there is connection between nodes {@code from} and {@code to}.
         */
        public boolean hasConnection(ClusterNode from, ClusterNode to) {
            return availableConnections.getOrDefault(from.id(), Collections.emptySet()).contains(to.id());
        }

        /**
         * Adds connection between nodes {@code from} and {@code to}.
         * @param from Cluster node 1.
         * @param to Cluster node 2.
         */
        public void addConnection(ClusterNode from, ClusterNode to) {
            availableConnections.computeIfAbsent(from.id(), s -> new HashSet<>()).add(to.id());
        }

        /**
         * Removes connection between nodes {@code from} and {@code to}.
         * @param from Cluster node 1.
         * @param to Cluster node 2.
         */
        public void removeConnection(ClusterNode from, ClusterNode to) {
            availableConnections.getOrDefault(from.id(), Collections.emptySet()).remove(to.id());
        }

        /**
         * Adds connections between all nodes presented in given {@code nodeSet}.
         *
         * @param nodeSet Set of the cluster nodes.
         */
        public void addAll(List<ClusterNode> nodeSet) {
            for (int i = 0; i < nodeSet.size(); i++) {
                for (int j = 0; j < nodeSet.size(); j++) {
                    if (i == j)
                        continue;

                    addConnection(nodeSet.get(i), nodeSet.get(j));
                }
            }
        }

        /**
         * Builds connections failure matrix from two part of the cluster nodes.
         * Each part has all connections inside, but hasn't any connection to another part.
         *
         * @param part1 Part 1.
         * @param part2 Part 2.
         * @return Connections failure matrix.
         */
        static ConnectionsFailureMatrix buildFrom(List<ClusterNode> part1, List<ClusterNode> part2) {
            ConnectionsFailureMatrix matrix = new ConnectionsFailureMatrix();
            matrix.addAll(part1);
            matrix.addAll(part2);
            return matrix;
        }
    }
}
