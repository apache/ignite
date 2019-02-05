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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
            TcpDiscoveryNode node = new TcpDiscoveryNode(
                UUID.randomUUID(),
                Collections.singletonList("127.0.0.1"),
                Collections.singletonList("127.0.0.1"),
                0,
                metrics,
                ver,
                i
            );
            node.setAttributes(new HashMap<>());
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
            +" nodeCount " + nodeCnt
            +" backups " + backups
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
            +" nodeCount " + nodeCnt
            +" backups " + backups
            + " " + totalSize2);

        if (totalSize > totalSize2)
            throw new Exception("Optimized AffinityAssignment size " + totalSize +" is more than deoptimized" + totalSize2);
    }
}
