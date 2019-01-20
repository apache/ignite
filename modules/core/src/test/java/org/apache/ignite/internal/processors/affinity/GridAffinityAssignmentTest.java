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

package org.apache.ignite.internal.processors.affinity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import junit.framework.TestCase;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.BitSetIntSet;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

/**
 * Tests for {@link GridAffinityAssignment}.
 */
public class GridAffinityAssignmentTest extends TestCase {
    /**  */
    protected DiscoveryMetricsProvider metrics = new DiscoveryMetricsProvider() {
        @Override public ClusterMetrics metrics() {
            return null;
        }

        @Override public Map<Integer, CacheMetrics> cacheMetrics() {
            return null;
        }
    };

    /** */
    protected IgniteProductVersion ver = new IgniteProductVersion();

    /**
     * Test GridAffinityAssignment logic when backup threshold is not reached.
     */
    @Test
    public void testPrimaryBackupPartitions() {
        ClusterNode clusterNode1 = node(metrics, ver, "1");
        ClusterNode clusterNode2 = node(metrics, ver, "2");
        ClusterNode clusterNode3 = node(metrics, ver, "3");
        ClusterNode clusterNode4 = node(metrics, ver, "4");
        ClusterNode clusterNode5 = node(metrics, ver, "5");
        ClusterNode clusterNode6 = node(metrics, ver, "6");

        List<ClusterNode> clusterNodes = new ArrayList<ClusterNode>() {{
            add(clusterNode1);
            add(clusterNode2);
            add(clusterNode3);
            add(clusterNode4);
            add(clusterNode5);
            add(clusterNode6);
        }};

        GridAffinityAssignment gridAffinityAssignment = new GridAffinityAssignment(
            new AffinityTopologyVersion(1, 0),
            new ArrayList<List<ClusterNode>>() {{
                add(new ArrayList<ClusterNode>() {{
                    add(clusterNode1);
                    add(clusterNode2);
                    add(clusterNode3);
                    add(clusterNode4);
                }});
                add(new ArrayList<ClusterNode>() {{
                    add(clusterNode1);
                    add(clusterNode2);
                    add(clusterNode3);
                    add(clusterNode4);
                }});
                add(new ArrayList<ClusterNode>() {{
                    add(clusterNode5);
                    add(clusterNode6);
                }});
            }},
            new ArrayList<>()
        );

        List<Integer> parts = new ArrayList<Integer>() {{
            add(0);
            add(1);
        }};

        assertTrue(gridAffinityAssignment.primaryPartitions(clusterNode1.id()).containsAll(parts));
        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode1.id()).contains(2));
        assertTrue(gridAffinityAssignment.backupPartitions(clusterNode1.id()).isEmpty());

        for (int i = 1; i < 4; i++) {
            Set<Integer> primary = gridAffinityAssignment.primaryPartitions(clusterNodes.get(i).id());

            assertTrue(primary.isEmpty());

            Set<Integer> backup = gridAffinityAssignment.backupPartitions(clusterNodes.get(i).id());

            assertTrue(backup.containsAll(parts));
            assertFalse(backup.contains(2));
        }

        assertTrue(gridAffinityAssignment.primaryPartitions(clusterNode5.id()).contains(2));
        assertTrue(gridAffinityAssignment.backupPartitions(clusterNode5.id()).isEmpty());

        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode6.id()).contains(2));
        assertTrue(gridAffinityAssignment.backupPartitions(clusterNode6.id()).contains(2));

        assertEquals(4, gridAffinityAssignment.getIds(0).size());

        for (int i = 0; i < 4; i++)
            assertTrue(gridAffinityAssignment.getIds(0).contains(clusterNodes.get(i).id()));

        if (AffinityAssignment.IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION)
            assertSame(gridAffinityAssignment.getIds(0), gridAffinityAssignment.getIds(0));
        else
            assertNotSame(gridAffinityAssignment.getIds(0), gridAffinityAssignment.getIds(0));

        try {
            gridAffinityAssignment.primaryPartitions(clusterNode1.id()).add(1000);

            fail("Unmodifiable exception expected");
        }
        catch (UnsupportedOperationException ignored) {
            // Ignored.
        }

        try {
            gridAffinityAssignment.backupPartitions(clusterNode1.id()).add(1000);

            fail("Unmodifiable exception expected");
        }
        catch (UnsupportedOperationException ignored) {
            // Ignored.
        }

        Set<Integer> unwrapped = (Set<Integer>)Whitebox.getInternalState(
            gridAffinityAssignment.primaryPartitions(clusterNode1.id()),
            "c"
        );

        if (AffinityAssignment.IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION)
            assertTrue(unwrapped instanceof HashSet);
        else
            assertTrue(unwrapped instanceof BitSetIntSet);
    }

    /**
     * Test GridAffinityAssignment logic when backup threshold is reached. Basically partitioned cache case.
     */
    @Test
    public void testBackupsMoreThanThreshold() {
        List<ClusterNode> nodes = new ArrayList<>();

        for(int i = 0; i < 10; i++)
            nodes.add(node(metrics, ver, "1" + i));

        GridAffinityAssignment gridAffinityAssignment = new GridAffinityAssignment(
            new AffinityTopologyVersion(1, 0),
            new ArrayList<List<ClusterNode>>() {{
                add(nodes);
            }},
            new ArrayList<>()
        );

        assertSame(gridAffinityAssignment.getIds(0), gridAffinityAssignment.getIds(0));
    }

    /**
     *
     * @param metrics Metrics.
     * @param v Version.
     * @param consistentId ConsistentId.
     * @return TcpDiscoveryNode.
     */
    protected TcpDiscoveryNode node(DiscoveryMetricsProvider metrics, IgniteProductVersion v, String consistentId) {
        return new TcpDiscoveryNode(
            UUID.randomUUID(),
            Collections.singletonList("127.0.0.1"),
            Collections.singletonList("127.0.0.1"),
            0,
            metrics,
            v,
            consistentId
        );
    }
}
