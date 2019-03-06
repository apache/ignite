/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;

/**
 * Baseline nodes metrics self test.
 */
@GridCommonTest(group = "Kernal Self")
public class ClusterBaselineNodesMetricsSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED, "false");

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED);
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
