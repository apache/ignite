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

package org.apache.ignite.internal;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.ClusterMetricsMXBean;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Baseline nodes metrics self test.
 */
@GridCommonTest(group = "Kernal Self")
@RunWith(JUnit4.class)
public class ClusterBaselineNodesMetricsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineNodes() throws Exception {
        // Start 2 server nodes.
        IgniteEx ignite0 = startGrid(0);
        startGrid(1);

        // Cluster metrics.
        ClusterMetricsMXBean mxBeanCluster = mxBean(0, ClusterMetricsMXBeanImpl.class);

        ignite0.cluster().active(true);

        // Added 2 server nodes to baseline.
        resetBlt();

        // Add server node outside of the baseline.
        startGrid(2);

        // Start client node.
        Ignition.setClientMode(true);
        startGrid(3);
        Ignition.setClientMode(false);

        Collection<BaselineNode> baselineNodes;

        // State #0: 3 server nodes (2 total baseline nodes, 2 active baseline nodes), 1 client node
        log.info(String.format(">>> State #0: topology version = %d", ignite0.cluster().topologyVersion()));

        assertEquals(3, mxBeanCluster.getTotalServerNodes());
        assertEquals(1, mxBeanCluster.getTotalClientNodes());
        assertEquals(2, mxBeanCluster.getTotalBaselineNodes());
        assertEquals(2, mxBeanCluster.getActiveBaselineNodes());
        assertEquals(2, (baselineNodes = ignite0.cluster().currentBaselineTopology()) != null
            ? baselineNodes.size()
            : 0);

        stopGrid(1, true);

        // State #1: 2 server nodes (2 total baseline nodes, 1 active baseline node), 1 client node
        log.info(String.format(">>> State #1: topology version = %d", ignite0.cluster().topologyVersion()));

        assertEquals(2, mxBeanCluster.getTotalServerNodes());
        assertEquals(1, mxBeanCluster.getTotalClientNodes());
        assertEquals(2, mxBeanCluster.getTotalBaselineNodes());
        assertEquals(1, mxBeanCluster.getActiveBaselineNodes());
        assertEquals(2, (baselineNodes = ignite0.cluster().currentBaselineTopology()) != null
            ? baselineNodes.size()
            : 0);

        startGrid(1);

        ClusterMetricsMXBean mxBeanLocalNode1 = mxBean(1, ClusterLocalNodeMetricsMXBeanImpl.class);

        // State #2: 3 server nodes (2 total baseline nodes, 2 active baseline nodes), 1 client node
        log.info(String.format(">>> State #2: topology version = %d", ignite0.cluster().topologyVersion()));

        assertEquals(3, mxBeanCluster.getTotalServerNodes());
        assertEquals(1, mxBeanCluster.getTotalClientNodes());
        assertEquals(2, mxBeanCluster.getTotalBaselineNodes());
        assertEquals(2, mxBeanCluster.getActiveBaselineNodes());
        assertEquals(1, mxBeanLocalNode1.getTotalBaselineNodes());
        assertEquals(2, (baselineNodes = ignite0.cluster().currentBaselineTopology()) != null
            ? baselineNodes.size()
            : 0);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        String storePath = getClass().getSimpleName().toLowerCase() + "/" + getName();

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setStoragePath(storePath)
                .setWalPath(storePath + "/wal")
                .setWalArchivePath(storePath + "/archive")
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(2L * 1024 * 1024 * 1024)
                )
        );

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    private void resetBlt() throws Exception {
        resetBaselineTopology();

        awaitPartitionMapExchange();
    }

    /**
     * Gets ClusterMetricsMXBean for given node.
     *
     * @param nodeIdx Node index.
     * @param clazz Class of ClusterMetricsMXBean implementation.
     * @return MBean instance.
     */
    private ClusterMetricsMXBean mxBean(int nodeIdx, Class<? extends ClusterMetricsMXBean> clazz)
        throws MalformedObjectNameException {

        ObjectName mbeanName = U.makeMBeanName(
            getTestIgniteInstanceName(nodeIdx),
            "Kernal",
            clazz.getSimpleName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, ClusterMetricsMXBean.class, true);
    }
}
