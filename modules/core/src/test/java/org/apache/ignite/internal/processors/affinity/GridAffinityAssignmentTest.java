package org.apache.ignite.internal.processors.affinity;

import java.util.ArrayList;
import java.util.Collections;
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
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class GridAffinityAssignmentTest {

    @Test
    public void testPrimaryBackupPartitions() {
        DiscoveryMetricsProvider metrics = new DiscoveryMetricsProvider() {
            @Override public ClusterMetrics metrics() {
                return null;
            }

            @Override public Map<Integer, CacheMetrics> cacheMetrics() {
                return null;
            }
        };

        IgniteProductVersion v = new IgniteProductVersion();

        ClusterNode clusterNode1 = node(metrics, v, "1");
        ClusterNode clusterNode2 = node(metrics, v, "2");
        ClusterNode clusterNode3 = node(metrics, v, "3");
        ClusterNode clusterNode4 = node(metrics, v, "4");
        ClusterNode clusterNode5 = node(metrics, v, "5");
        ClusterNode clusterNode6 = node(metrics, v, "6");

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

        try {
            gridAffinityAssignment.primaryPartitions(clusterNode1.id()).add(1000);

            fail("Unmodifiable exception");
        } catch (UnsupportedOperationException ignored) {

        }

        try {
            gridAffinityAssignment.backupPartitions(clusterNode1.id()).add(1000);

            fail("Unmodifiable exception");
        } catch (UnsupportedOperationException ignored) {

        }
    }

    private TcpDiscoveryNode node(DiscoveryMetricsProvider metrics, IgniteProductVersion v, String a) {
        return new TcpDiscoveryNode(
            UUID.randomUUID(),
            Collections.singletonList("127.0.0.1"),
            Collections.singletonList("127.0.0.1"),
            0,
            metrics,
            v,
            a
        );
    }

}
