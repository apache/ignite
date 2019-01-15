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
import java.util.UUID;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

/**
 * Tests for {@link GridAffinityAssignment}.
 */
@RunWith(JUnit4.class)
public class GridAffinityAssignmentTest {
    /**
     *
     */
    private DiscoveryMetricsProvider metrics = new DiscoveryMetricsProvider() {
        @Override public ClusterMetrics metrics() {
            return null;
        }

        @Override public Map<Integer, CacheMetrics> cacheMetrics() {
            return null;
        }
    };

    /**
     *
     */
    private IgniteProductVersion version = new IgniteProductVersion();

    /**
     * Test GridAffinityAssignment logic when backup threshold is not reached.
     */
    @Test
    public void testPrimaryBackupPartitions() {
        ClusterNode clusterNode1 = node(metrics, version, "1");
        ClusterNode clusterNode2 = node(metrics, version, "2");
        ClusterNode clusterNode3 = node(metrics, version, "3");
        ClusterNode clusterNode4 = node(metrics, version, "4");
        ClusterNode clusterNode5 = node(metrics, version, "5");
        ClusterNode clusterNode6 = node(metrics, version, "6");

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
            new ArrayList<>(),
            4
        );

        assertTrue(gridAffinityAssignment.primaryPartitions(clusterNode1.id()).contains(0));
        assertTrue(gridAffinityAssignment.primaryPartitions(clusterNode1.id()).contains(1));
        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode1.id()).contains(2));

        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode2.id()).contains(0));
        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode2.id()).contains(1));
        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode2.id()).contains(2));

        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode3.id()).contains(0));
        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode3.id()).contains(1));
        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode3.id()).contains(2));

        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode4.id()).contains(0));
        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode4.id()).contains(1));
        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode4.id()).contains(2));

        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode5.id()).contains(0));
        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode5.id()).contains(1));
        assertTrue(gridAffinityAssignment.primaryPartitions(clusterNode5.id()).contains(2));
        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode6.id()).contains(0));
        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode6.id()).contains(1));
        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode6.id()).contains(2));

        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode1.id()).contains(0));
        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode1.id()).contains(1));
        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode1.id()).contains(2));

        assertTrue(gridAffinityAssignment.backupPartitions(clusterNode2.id()).contains(0));
        assertTrue(gridAffinityAssignment.backupPartitions(clusterNode2.id()).contains(1));
        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode2.id()).contains(2));

        assertTrue(gridAffinityAssignment.backupPartitions(clusterNode3.id()).contains(0));
        assertTrue(gridAffinityAssignment.backupPartitions(clusterNode3.id()).contains(1));
        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode3.id()).contains(2));

        assertTrue(gridAffinityAssignment.backupPartitions(clusterNode4.id()).contains(0));
        assertTrue(gridAffinityAssignment.backupPartitions(clusterNode4.id()).contains(1));
        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode4.id()).contains(2));

        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode5.id()).contains(0));
        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode5.id()).contains(1));
        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode5.id()).contains(2));
        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode5.id()).contains(0));
        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode5.id()).contains(1));
        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode5.id()).contains(2));

        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode6.id()).contains(0));
        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode6.id()).contains(1));
        assertTrue(gridAffinityAssignment.backupPartitions(clusterNode6.id()).contains(2));
        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode6.id()).contains(0));
        assertFalse(gridAffinityAssignment.backupPartitions(clusterNode6.id()).contains(1));
        assertTrue(gridAffinityAssignment.backupPartitions(clusterNode6.id()).contains(2));

        assertEquals(4, gridAffinityAssignment.getIds(0).size());
        assertTrue(gridAffinityAssignment.getIds(0).contains(clusterNode1.id()));
        assertTrue(gridAffinityAssignment.getIds(0).contains(clusterNode2.id()));
        assertTrue(gridAffinityAssignment.getIds(0).contains(clusterNode3.id()));
        assertTrue(gridAffinityAssignment.getIds(0).contains(clusterNode4.id()));

        assertEquals(4, gridAffinityAssignment.getIds(1).size());
        assertTrue(gridAffinityAssignment.getIds(1).contains(clusterNode1.id()));
        assertTrue(gridAffinityAssignment.getIds(1).contains(clusterNode2.id()));
        assertTrue(gridAffinityAssignment.getIds(1).contains(clusterNode3.id()));
        assertTrue(gridAffinityAssignment.getIds(1).contains(clusterNode4.id()));

        assertEquals(2, gridAffinityAssignment.getIds(2).size());
        assertTrue(gridAffinityAssignment.getIds(2).contains(clusterNode5.id()));
        assertTrue(gridAffinityAssignment.getIds(2).contains(clusterNode6.id()));

        assertNotSame(gridAffinityAssignment.getIds(0), gridAffinityAssignment.getIds(0));
        assertNotSame(gridAffinityAssignment.getIds(1), gridAffinityAssignment.getIds(1));
        assertNotSame(gridAffinityAssignment.getIds(2), gridAffinityAssignment.getIds(2));

        assertFalse(gridAffinityAssignment.getIds(0) instanceof HashSet);
        assertFalse(gridAffinityAssignment.getIds(1) instanceof HashSet);
        assertFalse(gridAffinityAssignment.getIds(2) instanceof HashSet);

        try {
            gridAffinityAssignment.primaryPartitions(clusterNode1.id()).add(1000);

            fail("Unmodifiable exception expected");
        } catch (UnsupportedOperationException ignored) {

        }

        try {
            gridAffinityAssignment.backupPartitions(clusterNode1.id()).add(1000);

            fail("Unmodifiable exception expected");
        } catch (UnsupportedOperationException ignored) {

        }
    }

    /**
     * Test GridAffinityAssignment logic when backup threshold is reached. Basically partitioned cache case.
     */
    @Test
    public void testBackupsMoreThanThreshold() {
        ClusterNode clusterNode1 = node(metrics, version, "1");
        ClusterNode clusterNode2 = node(metrics, version, "2");
        ClusterNode clusterNode3 = node(metrics, version, "3");
        ClusterNode clusterNode4 = node(metrics, version, "4");
        ClusterNode clusterNode5 = node(metrics, version, "5");
        ClusterNode clusterNode6 = node(metrics, version, "6");

        GridAffinityAssignment gridAffinityAssignment = new GridAffinityAssignment(
            new AffinityTopologyVersion(1, 0),
            new ArrayList<List<ClusterNode>>() {{
                add(new ArrayList<ClusterNode>() {{
                    add(clusterNode1);
                    add(clusterNode2);
                    add(clusterNode3);
                    add(clusterNode4);
                    add(clusterNode5);
                    add(clusterNode5);
                    add(clusterNode6);
                }});
            }},
            new ArrayList<>(),
            10
        );

        assertSame(gridAffinityAssignment.getIds(0), gridAffinityAssignment.getIds(0));
        assertTrue(gridAffinityAssignment.getIds(0) instanceof HashSet);
    }

    /**
     *
     * @param metrics Metrics.
     * @param v Version.
     * @param consistentId ConsistentId.
     * @return TcpDiscoveryNode.
     */
    private TcpDiscoveryNode node(DiscoveryMetricsProvider metrics, IgniteProductVersion v, String consistentId) {
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
