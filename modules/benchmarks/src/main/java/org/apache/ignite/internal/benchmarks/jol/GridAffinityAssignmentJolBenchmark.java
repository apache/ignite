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

package org.apache.ignite.internal.benchmarks.jol;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentV2;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.processors.affinity.HistoryAffinityAssignment;
import org.apache.ignite.internal.processors.affinity.HistoryAffinityAssignmentImpl;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.openjdk.jol.info.GraphLayout;

/**
 *
 */
public class GridAffinityAssignmentJolBenchmark {
    /**  */
    private static DiscoveryMetricsProvider metrics = new DiscoveryMetricsProvider() {
        @Override public ClusterMetrics metrics() {
            return null;
        }

        @Override public Map<Integer, CacheMetrics> cacheMetrics() {
            return null;
        }
    };

    /** */
    private static IgniteProductVersion ver = new IgniteProductVersion();

    /** */
    private static Field field;

    /** */
    public static void main(String[] args) throws Exception {
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(true, 65000);

        int[] parts = new int[] {1024, 8192, 32768, 65000};

        int[] nodes = new int[] {1, 16, 160, 600};

        // We need to implement compressed bitsets https://issues.apache.org/jira/browse/IGNITE-4554.
        // On 65k partitions and nodes > 700 HashSet take advantage over BitSet.
        // After implementation need to check consumption on big clusters.
        for (int part : parts)
            for (int node : nodes) {
                measure(aff, part, node, 0);

                measure(aff, part, node, 3);

                measure(aff, part, node, node);
            }

        // Measure history assignment for normal and huge partition count.
        // Nodes count doesn't affect heap occupation.
        // Best result is achieved when running one measure at a time with large enough size of new region
        // (to avoid object relocation).
        measureHistory(1024, 32, 0);
        measureHistory(1024, 32, 1);
        measureHistory(1024, 32, 2);
        measureHistory(1024, 32, Integer.MAX_VALUE);
        measureHistory(32768, 32, 0);
        measureHistory(32768, 32, 1);
        measureHistory(32768, 32, 2);
        measureHistory(32768, 32, Integer.MAX_VALUE);
    }

    /**
     * @param disabled Disabled.
     */
    private static void setOptimization(boolean disabled) throws NoSuchFieldException, IllegalAccessException {
        if (field == null) {
            field = AffinityAssignment.class.getDeclaredField("IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION");

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            field.setAccessible(true);
        }

        field.set(null, disabled);
    }

    /**
     * @param aff Aff.
     * @param parts Parts.
     * @param nodeCnt Node count.
     * @param backups Backups.
     */
    private static void measure(
        RendezvousAffinityFunction aff,
        int parts,
        int nodeCnt,
        int backups
    ) throws Exception {
        List<ClusterNode> nodes = new ArrayList<>();

        for (int i = 0; i < nodeCnt; i++) {
            ClusterNode node = node(i);
            nodes.add(node);
        }

        AffinityFunctionContext ctx = new GridAffinityFunctionContextImpl(
            nodes,
            new ArrayList<>(),
            new DiscoveryEvent(),
            new AffinityTopologyVersion(),
            backups
        );

        List<List<ClusterNode>> assignment = aff.assignPartitions(ctx);

        setOptimization(false);

        GridAffinityAssignmentV2 ga = new GridAffinityAssignmentV2(
            new AffinityTopologyVersion(1, 0),
            assignment,
            new ArrayList<>()
        );

        System.gc();

        long totalSize = GraphLayout.parseInstance(ga).totalSize();

        System.out.println("Optimized, parts " + parts
            + " nodeCount " + nodeCnt
            + " backups " + backups
            + " " + totalSize);

        setOptimization(true);

        GridAffinityAssignmentV2 ga2 = new GridAffinityAssignmentV2(
            new AffinityTopologyVersion(1, 0),
            assignment,
            new ArrayList<>()
        );

        System.gc();

        long totalSize2 = GraphLayout.parseInstance(ga2).totalSize();

        System.out.println("Deoptimized, parts " + parts
            + " nodeCount " + nodeCnt
            + " backups " + backups
            + " " + totalSize2);

        if (totalSize > totalSize2)
            throw new Exception("Optimized AffinityAssignment size " + totalSize + " is more than deoptimized " + totalSize2);
    }

    /**
     * @param parts Parts.
     * @param nodes Nodes.
     */
    private static void measureHistory(int parts, int nodes, int backups) throws Exception {
        System.gc();

        long deopt = measureHistory0(parts, nodes, true, backups);

        System.gc();

        long opt = measureHistory0(parts, nodes, false, backups);

        if (opt > deopt)
            throw new Exception("Optimized HistoryAffinityAssignment size " + opt + " is more than deoptimized " + deopt);

        float rate = deopt / (float)opt;

        System.out.println("Optimization: optimized=" + opt + ", deoptimized=" + deopt + " rate: " + ((int)(rate * 1000)) / 1000. );
    }

    /**
     * @param parts Parts.
     * @param nodeCnt Node count.
     * @param disableOptimization Disable optimization.
     */
    private static long measureHistory0(int parts, int nodeCnt, boolean disableOptimization, int backups) throws Exception {
        System.gc();

        setOptimization(disableOptimization);

        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(true, parts);

        List<ClusterNode> nodes = new ArrayList<>(nodeCnt);

        nodes.add(node(0));

        Map<AffinityTopologyVersion, HistoryAffinityAssignment> affCache = new ConcurrentSkipListMap<>();

        List<List<ClusterNode>> prevAssignment = new ArrayList<>();

        prevAssignment = aff.assignPartitions(context(new ArrayList<>(nodes), prevAssignment, 1));

        for (int i = 1; i < nodeCnt; i++) {
            ClusterNode newNode = node(i);

            nodes.add(newNode);

            List<List<ClusterNode>> idealAssignment = aff.assignPartitions(context(new ArrayList<>(nodes), prevAssignment, backups));

            List<List<ClusterNode>> lateAssignmemnt = new ArrayList<>(parts);

            for (int j = 0; j < idealAssignment.size(); j++) {
                List<ClusterNode> ideal0 = idealAssignment.get(j);
                List<ClusterNode> prev = prevAssignment.get(j);

                ClusterNode curPrimary = prev.get(0);

                if (!curPrimary.equals(ideal0.get(0))) {
                    List<ClusterNode> cpy = new ArrayList<>(ideal0);

                    cpy.remove(curPrimary);
                    cpy.add(0, curPrimary);

                    lateAssignmemnt.add(cpy);
                }
                else
                    lateAssignmemnt.add(ideal0);
            }

            AffinityTopologyVersion topVer = new AffinityTopologyVersion(i + 1, 0);
            GridAffinityAssignmentV2 a = new GridAffinityAssignmentV2(topVer, lateAssignmemnt, idealAssignment);
            HistoryAffinityAssignment h = new HistoryAffinityAssignmentImpl(a, backups);

            if (!lateAssignmemnt.equals(h.assignment()))
                throw new RuntimeException();

            if (!idealAssignment.equals(h.idealAssignment()))
                throw new RuntimeException();

            affCache.put(topVer, h);

            AffinityTopologyVersion topVer0 = new AffinityTopologyVersion(i + 1, 1);

            List<List<ClusterNode>> assignment = new ArrayList<>(parts);

            for (int j = 0; j < idealAssignment.size(); j++) {
                List<ClusterNode> clusterNodes = idealAssignment.get(j);

                assignment.add(clusterNodes);
            }

            GridAffinityAssignmentV2 a0 = new GridAffinityAssignmentV2(topVer0, assignment, idealAssignment);
            HistoryAffinityAssignment h0 = new HistoryAffinityAssignmentImpl(a0, backups);

            if (!assignment.equals(h0.assignment()))
                throw new RuntimeException();

            if (!idealAssignment.equals(h0.idealAssignment()))
                throw new RuntimeException();

            affCache.put(topVer0, h0);

            prevAssignment = idealAssignment;
        }

        System.gc();

        GraphLayout l = GraphLayout.parseInstance(affCache);

        // Exclude nodes from estimation.
        GraphLayout l2 = GraphLayout.parseInstance(nodes.toArray(new Object[nodes.size()]));

        GraphLayout l3 = l.subtract(l2);

        System.out.println("Heap usage [optimized=" + !disableOptimization + ", parts=" + parts
            + ", nodeCnt=" + nodeCnt
            + ", backups=" + backups
            + ", " + l3.toFootprint() + ']');

        return l3.totalSize();
    }

    /**
     * @param nodes Nodes.
     * @param prevAssignment Prev assignment.
     * @param backups Backups.
     */
    private static AffinityFunctionContext context(
        List<ClusterNode> nodes,
        List<List<ClusterNode>> prevAssignment,
        int backups) {
        return new GridAffinityFunctionContextImpl(
            nodes,
            prevAssignment,
            new DiscoveryEvent(),
            new AffinityTopologyVersion(),
            backups
        );
    }

    /**
     * @return New test node.
     */
    private static ClusterNode node(int idx) {
        TcpDiscoveryNode node = new TcpDiscoveryNode(
            UUID.randomUUID(),
            Collections.singletonList("127.0.0.1"),
            Collections.singletonList("127.0.0.1"),
            0,
            metrics,
            ver,
            "Node_" + idx
        );
        node.setAttributes(Collections.emptyMap());

        return node;
    }
}
