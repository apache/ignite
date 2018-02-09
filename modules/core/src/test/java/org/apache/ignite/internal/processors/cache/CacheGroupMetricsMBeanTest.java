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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.CacheGroupMetricsMXBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Cache group JMX metrics test.
 */
public class CacheGroupMetricsMBeanTest extends GridCommonAbstractTest implements Serializable {
    /** */
    private static class RoundRobinVariableSizeAffinityFunction implements AffinityFunction {
        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return 10;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return key.hashCode() % partitions();
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<ClusterNode> nodes = affCtx.currentTopologySnapshot();

            List<List<ClusterNode>> assignmentParts = new ArrayList<>(partitions());

            for (int part = 0; part < partitions(); part++) {
                int backups = part % nodes.size() + 1;

                List<ClusterNode> assignmentNodes = new ArrayList<>(backups);

                for (int backup = 0; backup < backups; backup++)
                    assignmentNodes.add(nodes.get((part + part / nodes.size() + backup) % nodes.size()));

                assignmentParts.add(assignmentNodes);
            }

            return assignmentParts;
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // No-op
        }

    }

    /**
     * Partition assignment for cache1 with given affinity function:
     *
     *  P/N 0 1 2
     *  ---------
     *  0 | P
     *  1 |   P B
     *  2 | B B P
     *  3 |   P
     *  4 | B   P
     *  5 | P B B
     *  6 |     P
     *  7 | P B
     *  8 | B P B
     *  9 | P
     *
     */
    private static final int [][] assignmentMapArr =
        new int[][] {{0}, {1, 2}, {2, 0, 1}, {1}, {2, 0}, {0, 1, 2}, {2}, {0, 1}, {1, 2, 0}, {0}};

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cCfg1 = new CacheConfiguration()
            .setName("cache1")
            .setGroupName("group1")
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(3)
            .setAffinity(new RoundRobinVariableSizeAffinityFunction());

        CacheConfiguration cCfg2 = new CacheConfiguration()
            .setName("cache2")
            .setGroupName("group2")
            .setCacheMode(CacheMode.REPLICATED);

        CacheConfiguration cCfg3 = new CacheConfiguration()
            .setName("cache3")
            .setGroupName("group2")
            .setCacheMode(CacheMode.REPLICATED);

        CacheConfiguration cCfg4 = new CacheConfiguration()
            .setName("cache4")
            .setCacheMode(CacheMode.PARTITIONED);

        cfg.setCacheConfiguration(cCfg1, cCfg2, cCfg3, cCfg4);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Gets CacheGroupMetricsMXBean for given node and group name.
     *
     * @param nodeIdx Node index.
     * @param cacheOrGrpName Cache group name.
     * @return MBean instance.
     */
    private CacheGroupMetricsMXBean mxBean(int nodeIdx, String cacheOrGrpName) throws MalformedObjectNameException {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(nodeIdx), "Cache groups", cacheOrGrpName);

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, CacheGroupMetricsMXBean.class,
            true);
    }

    /**
     * Converts array, containing partitions allocation to map from partitions to set of nodes.
     *
     * @param arr Array.
     * @return Map from partitions to set of nodes.
     */
    private Map<Integer, Set<String>> arrayToAllocationMap(int[][] arr) {
        Map<Integer, Set<String>> res = new LinkedHashMap<>();

        for (int part = 0; part < arr.length; part++) {
            Set<String> nodeSet = new HashSet<>();

            if (arr[part] != null)
                for (int node = 0; node < arr[part].length; node++)
                    nodeSet.add(grid(arr[part][node]).localNode().id().toString());

            res.put(part, nodeSet);
        }

        return res;
    }

    /**
     * Converts array, containing affinity assignment to map from partitions to list of nodes.
     *
     * @param arr Array.
     * @return Map from partitions to list of nodes.
     */
    private Map<Integer, List<String>> arrayToAssignmentMap(int[][] arr) {
        Map<Integer, List<String>> res = new LinkedHashMap<>();

        for (int part = 0; part < arr.length; part++) {
            List<String> nodeList = new ArrayList<>();

            if (arr[part] != null)
                for (int node = 0; node < arr[part].length; node++)
                    nodeList.add(grid(arr[part][node]).localNode().id().toString());

            res.put(part, nodeList);
        }

        return res;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheGroupMetrics() throws Exception {
        startGrid(0);
        startGrid(1);
        startGrid(2);

        awaitPartitionMapExchange(true, false, null);

        CacheGroupMetricsMXBean mxBean0Grp1 = mxBean(0, "group1");
        CacheGroupMetricsMXBean mxBean0Grp2 = mxBean(0, "group2");
        CacheGroupMetricsMXBean mxBean0Grp3 = mxBean(0, "cache4");
        CacheGroupMetricsMXBean mxBean1Grp1 = mxBean(1, "group1");
        CacheGroupMetricsMXBean mxBean2Grp1 = mxBean(2, "group1");

        assertEquals("group1", mxBean0Grp1.getGroupName());
        assertEquals(null, mxBean0Grp3.getGroupName());

        assertEquals(3, mxBean0Grp1.getBackups());

        assertEquals(10, mxBean0Grp1.getPartitions());

        assertEquals(1, mxBean0Grp1.getMinimumNumberOfPartitionCopies());
        assertEquals(3, mxBean0Grp1.getMaximumNumberOfPartitionCopies());

        assertEquals(0, mxBean0Grp1.getClusterMovingPartitionsCount());
        assertEquals(19, mxBean0Grp1.getClusterOwningPartitionsCount());

        assertEquals(7, mxBean0Grp1.getLocalNodeOwningPartitionsCount());
        assertEquals(6, mxBean1Grp1.getLocalNodeOwningPartitionsCount());
        assertEquals(6, mxBean2Grp1.getLocalNodeOwningPartitionsCount());

        assertEquals(F.asList("cache1"), mxBean0Grp1.getCaches());
        assertEquals(F.asList("cache2", "cache3"), mxBean0Grp2.getCaches());
        assertEquals(F.asList("cache4"), mxBean0Grp3.getCaches());

        assertEquals(arrayToAssignmentMap(assignmentMapArr), mxBean0Grp1.getAffinityPartitionsAssignmentMap());
        assertEquals(arrayToAllocationMap(assignmentMapArr), mxBean0Grp1.getOwningPartitionsAllocationMap());
        assertEquals(arrayToAllocationMap(new int[10][]), mxBean0Grp1.getMovingPartitionsAllocationMap());

        try (IgniteDataStreamer<Integer, Integer> st = grid(0).dataStreamer("cache1")) {
            for (int i = 0; i < 50_000; i++)
                st.addData(i, i);
        }

        stopGrid(2);

        // Check moving partitions while rebalancing.
        assertFalse(arrayToAllocationMap(new int[10][]).equals(mxBean0Grp1.getMovingPartitionsAllocationMap()));

        assertTrue(mxBean0Grp1.getLocalNodeMovingPartitionsCount() > 0);
        assertTrue(mxBean0Grp1.getClusterMovingPartitionsCount() > 0);

    }
}
