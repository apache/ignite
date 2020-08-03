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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.GridTestTask;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.messaging.MessagingListenActor;
import org.apache.ignite.mxbean.ClusterMetricsMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CLIENT_MODE;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;

/**
 * Grid node metrics self test.
 */
@GridCommonTest(group = "Kernal Self")
public class ClusterNodeMetricsSelfTest extends GridCommonAbstractTest {
    /** Test message size. */
    private static final int MSG_SIZE = 1024;

    /** Number of messages. */
    private static final int MSG_CNT = 3;

    /** Size of value in bytes. */
    private static final int VAL_SIZE = 512 * 1024;

    /** Amount of cache entries. */
    private static final int MAX_VALS_AMOUNT = 400;

    /** Cache name. */
    private final String CACHE_NAME = "cache1";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration();
        cfg.setMetricsUpdateFrequency(500);

        CacheConfiguration<Integer, Object> ccfg = defaultCacheConfiguration();
        ccfg.setName(CACHE_NAME);
        ccfg.setStatisticsEnabled(true);

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxMemorySize(MAX_VALS_AMOUNT * VAL_SIZE);
        plc.setMaxSize(0);

        ccfg.setEvictionPolicy(plc);
        ccfg.setOnheapCacheEnabled(true);

        return cfg.setCacheConfiguration(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAllocatedMemory() throws Exception {
        IgniteEx ignite = grid();

        final IgniteCache cache = ignite.getOrCreateCache(CACHE_NAME);

        DataRegion dataRegion = getDefaultDataRegion(ignite);

        DataRegionMetricsImpl memMetrics = dataRegion.memoryMetrics();

        memMetrics.enableMetrics();

        int pageSize = getPageSize(ignite);

        assertEquals(dataRegion.pageMemory().loadedPages(), memMetrics.getTotalAllocatedPages());

        fillCache(cache);

        assertTrue(memMetrics.getTotalAllocatedPages() * pageSize > MAX_VALS_AMOUNT * VAL_SIZE);
    }

    /**
     * @param ignite Ignite instance.
     */
    private DataRegion getDefaultDataRegion(IgniteEx ignite) throws IgniteCheckedException {
        return ignite.context().cache().context().database().dataRegion(null);
    }

    /**
     * @param ignite Ignite instance.
     */
    private int getPageSize(IgniteEx ignite) {
        return ignite.context().cache().context().database().pageSize();
    }

    /**
     * Fill cache with values.
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void fillCache(final IgniteCache<Integer, Object> cache) throws Exception {
        final byte[] val = new byte[VAL_SIZE];

        for (int i = 0; i < MAX_VALS_AMOUNT * 4; i++)
            cache.put(i, val);

        // Let metrics update twice.
        awaitMetricsUpdate(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSingleTaskMetrics() throws Exception {
        Ignite ignite = grid();

        final CountDownLatch taskLatch = new CountDownLatch(1);
        ignite.compute().executeAsync(new GridTestTask(taskLatch), "testArg");

        // Let metrics update twice.
        awaitMetricsUpdate(2);

        taskLatch.countDown();

        awaitMetricsUpdate(1);

        ClusterMetrics metrics = ignite.cluster().localNode().metrics();

        info("Node metrics: " + metrics);

        assert metrics.getAverageActiveJobs() > 0;
        assert metrics.getAverageCancelledJobs() == 0;
        assert metrics.getAverageJobExecuteTime() >= 0;
        assert metrics.getAverageJobWaitTime() >= 0;
        assert metrics.getAverageRejectedJobs() == 0;
        assert metrics.getAverageWaitingJobs() == 0;
        assert metrics.getCurrentActiveJobs() == 0;
        assert metrics.getCurrentCancelledJobs() == 0;
        assert metrics.getCurrentJobExecuteTime() > 0;
        assert metrics.getCurrentJobWaitTime() == 0;
        assert metrics.getCurrentWaitingJobs() == 0;
        assert metrics.getMaximumActiveJobs() == 1;
        assert metrics.getMaximumCancelledJobs() == 0;
        assert metrics.getMaximumJobExecuteTime() >= 0;
        assert metrics.getMaximumJobWaitTime() >= 0;
        assert metrics.getMaximumRejectedJobs() == 0;
        assert metrics.getMaximumWaitingJobs() == 0;
        assert metrics.getTotalCancelledJobs() == 0;
        assert metrics.getTotalExecutedJobs() == 1;
        assert metrics.getTotalRejectedJobs() == 0;
        assert metrics.getTotalExecutedTasks() == 1;
        assert metrics.getTotalJobsExecutionTime() > 0;

        assertTrue("MaximumJobExecuteTime=" + metrics.getMaximumJobExecuteTime() +
            " is less than AverageJobExecuteTime=" + metrics.getAverageJobExecuteTime(),
            metrics.getMaximumJobExecuteTime() >= metrics.getAverageJobExecuteTime());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInternalTaskMetrics() throws Exception {
        Ignite ignite = grid();

        // Visor task is internal and should not affect metrics.
        ignite.compute().withName("visor-test-task").execute(new TestInternalTask(), "testArg");

        // Let metrics update twice.
        awaitMetricsUpdate(2);

        ClusterMetrics metrics = ignite.cluster().localNode().metrics();

        info("Node metrics: " + metrics);

        assert metrics.getAverageActiveJobs() == 0;
        assert metrics.getAverageCancelledJobs() == 0;
        assert metrics.getAverageJobExecuteTime() == 0;
        assert metrics.getAverageJobWaitTime() == 0;
        assert metrics.getAverageRejectedJobs() == 0;
        assert metrics.getAverageWaitingJobs() == 0;
        assert metrics.getCurrentActiveJobs() == 0;
        assert metrics.getCurrentCancelledJobs() == 0;
        assert metrics.getCurrentJobExecuteTime() == 0;
        assert metrics.getCurrentJobWaitTime() == 0;
        assert metrics.getCurrentWaitingJobs() == 0;
        assert metrics.getMaximumActiveJobs() == 0;
        assert metrics.getMaximumCancelledJobs() == 0;
        assert metrics.getMaximumJobExecuteTime() == 0;
        assert metrics.getMaximumJobWaitTime() == 0;
        assert metrics.getMaximumRejectedJobs() == 0;
        assert metrics.getMaximumWaitingJobs() == 0;
        assert metrics.getTotalCancelledJobs() == 0;
        assert metrics.getTotalExecutedJobs() == 0;
        assert metrics.getTotalRejectedJobs() == 0;
        assert metrics.getTotalExecutedTasks() == 0;
        assert metrics.getTotalJobsExecutionTime() == 0;

        assertTrue("MaximumJobExecuteTime=" + metrics.getMaximumJobExecuteTime() +
            " is less than AverageJobExecuteTime=" + metrics.getAverageJobExecuteTime(),
            metrics.getMaximumJobExecuteTime() >= metrics.getAverageJobExecuteTime());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIoMetrics() throws Exception {
        Ignite ignite0 = grid();
        Ignite ignite1 = startGrid(1);

        Object msg = new TestMessage();

        int size = ignite0.configuration().getMarshaller().marshal(msg).length;

        assert size > MSG_SIZE;

        final CountDownLatch latch = new CountDownLatch(MSG_CNT);

        ignite0.message().localListen(null, new MessagingListenActor<TestMessage>() {
            @Override protected void receive(UUID nodeId, TestMessage rcvMsg) throws Throwable {
                latch.countDown();
            }
        });

        ignite1.message().localListen(null, new MessagingListenActor<TestMessage>() {
            @Override protected void receive(UUID nodeId, TestMessage rcvMsg) throws Throwable {
                respond(rcvMsg);
            }
        });

        for (int i = 0; i < MSG_CNT; i++)
            message(ignite0.cluster().forRemotes()).send(null, msg);

        latch.await();

        ClusterMetrics metrics = ignite0.cluster().localNode().metrics();

        info("Node 0 metrics: " + metrics);

        // Time sync messages are being sent.
        assert metrics.getSentMessagesCount() >= MSG_CNT;
        assert metrics.getSentBytesCount() > size * MSG_CNT;
        assert metrics.getReceivedMessagesCount() >= MSG_CNT;
        assert metrics.getReceivedBytesCount() > size * MSG_CNT;

        metrics = ignite1.cluster().localNode().metrics();

        info("Node 1 metrics: " + metrics);

        // Time sync messages are being sent.
        assert metrics.getSentMessagesCount() >= MSG_CNT;
        assert metrics.getSentBytesCount() > size * MSG_CNT;
        assert metrics.getReceivedMessagesCount() >= MSG_CNT;
        assert metrics.getReceivedBytesCount() > size * MSG_CNT;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterNodeMetrics() throws Exception {
        final Ignite ignite0 = grid();
        final Ignite ignite1 = startGrid(1);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ignite0.cluster().nodes().size() == 2 && ignite1.cluster().nodes().size() == 2;
            }
        }, 3000L);

        ClusterMetrics metrics0 = ignite0.cluster().localNode().metrics();

        ClusterMetrics nodesMetrics =
            ignite0.cluster().forNode(ignite0.cluster().localNode(), ignite1.cluster().localNode()).metrics();

        assertEquals(metrics0.getTotalCpus(), nodesMetrics.getTotalCpus());
        assertEquals(1, metrics0.getTotalNodes());
        assertEquals(2, nodesMetrics.getTotalNodes());

        assert metrics0.getHeapMemoryUsed() > 0;
        assert metrics0.getHeapMemoryTotal() > 0;
    }

    /**
     * Test JMX metrics.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testJmxClusterMetrics() throws Exception {
        Ignite node = grid();

        Ignite node1 = startGrid(1);
        Ignite node2 = startClientGrid(2);

        waitForDiscovery(node2, node1, node);

        UUID nodeId0 = node.cluster().localNode().id();
        UUID nodeId1 = node1.cluster().localNode().id();
        UUID nodeId2 = node2.cluster().localNode().id();

        Set<UUID> srvNodes = new HashSet<>(Arrays.asList(nodeId0, nodeId1));
        Set<UUID> clientNodes = Collections.singleton(nodeId2);
        Set<UUID> allNodes = new HashSet<>(Arrays.asList(nodeId0, nodeId1, nodeId2));

        // ClusterMetricsMXBeanImpl test.
        JmxClusterMetricsHelper helperCluster = new JmxClusterMetricsHelper(node.configuration(),
            ClusterMetricsMXBeanImpl.class);

        assertEquals(node.cluster().topologyVersion(), helperCluster.attr("TopologyVersion"));

        assertEquals(2, helperCluster.attr("TotalServerNodes"));
        assertEquals(1, helperCluster.attr("TotalClientNodes"));

        assertEquals(allNodes, helperCluster.nodeIdsForAttribute(ATTR_BUILD_VER, VER_STR, true, true));
        assertEquals(srvNodes, helperCluster.nodeIdsForAttribute(ATTR_BUILD_VER, VER_STR, true, false));
        assertEquals(clientNodes, helperCluster.nodeIdsForAttribute(ATTR_BUILD_VER, VER_STR, false, true));
        assertEquals(Collections.emptySet(), helperCluster.nodeIdsForAttribute(ATTR_BUILD_VER, VER_STR, false, false));

        assertEquals(srvNodes, helperCluster.nodeIdsForAttribute(ATTR_CLIENT_MODE, "false", true, true));
        assertEquals(Collections.emptySet(), helperCluster.nodeIdsForAttribute(ATTR_CLIENT_MODE, "false", false,
            false));
        assertEquals(clientNodes, helperCluster.nodeIdsForAttribute(ATTR_CLIENT_MODE, "true", true, true));

        assertTrue(helperCluster.attributeNames().containsAll(node.cluster().localNode().attributes().keySet()));
        assertTrue(helperCluster.attributeNames().containsAll(node1.cluster().localNode().attributes().keySet()));
        assertTrue(helperCluster.attributeNames().containsAll(node2.cluster().localNode().attributes().keySet()));

        assertEquals(new HashSet<>(Arrays.asList("true", "false")), helperCluster.attributeValues(ATTR_CLIENT_MODE));
        assertEquals(Collections.emptySet(), helperCluster.attributeValues("NO_SUCH_ATTRIBUTE"));

        // ClusterLocalNodeMetricsMXBeanImpl test.
        JmxClusterMetricsHelper helperNode0 = new JmxClusterMetricsHelper(node.configuration(),
            ClusterLocalNodeMetricsMXBeanImpl.class);
        JmxClusterMetricsHelper helperNode2 = new JmxClusterMetricsHelper(node2.configuration(),
            ClusterLocalNodeMetricsMXBeanImpl.class);

        // For server node.
        assertEquals(1, helperNode0.attr("TotalServerNodes"));
        assertEquals(0, helperNode0.attr("TotalClientNodes"));

        assertEquals(node.cluster().topologyVersion(), helperNode0.attr("TopologyVersion"));

        assertEquals(node.cluster().localNode().attributes().keySet(), helperNode0.attributeNames());

        assertEquals(Collections.singleton("false"), helperNode0.attributeValues(ATTR_CLIENT_MODE));
        assertEquals(Collections.emptySet(), helperNode0.attributeValues("NO_SUCH_ATTRIBUTE"));

        assertEquals(Collections.singleton(nodeId0), helperNode0.nodeIdsForAttribute(ATTR_BUILD_VER, VER_STR, true,
            true));
        assertEquals(Collections.singleton(nodeId0), helperNode0.nodeIdsForAttribute(ATTR_BUILD_VER, VER_STR, true,
            false));
        assertEquals(Collections.emptySet(), helperNode0.nodeIdsForAttribute(ATTR_BUILD_VER, VER_STR, false, true));
        assertEquals(Collections.emptySet(), helperNode0.nodeIdsForAttribute(ATTR_BUILD_VER, VER_STR, false, false));

        // For client node.
        assertEquals(0, helperNode2.attr("TotalServerNodes"));
        assertEquals(1, helperNode2.attr("TotalClientNodes"));

        assertEquals(node.cluster().topologyVersion(), helperNode2.attr("TopologyVersion"));

        assertEquals(node2.cluster().localNode().attributes().keySet(), helperNode2.attributeNames());

        assertEquals(Collections.singleton("true"), helperNode2.attributeValues(ATTR_CLIENT_MODE));
        assertEquals(Collections.emptySet(), helperNode2.attributeValues("NO_SUCH_ATTRIBUTE"));

        assertEquals(Collections.singleton(nodeId2), helperNode2.nodeIdsForAttribute(ATTR_BUILD_VER, VER_STR, true,
            true));
        assertEquals(Collections.emptySet(), helperNode2.nodeIdsForAttribute(ATTR_BUILD_VER, VER_STR, true, false));
        assertEquals(Collections.singleton(nodeId2), helperNode2.nodeIdsForAttribute(ATTR_BUILD_VER, VER_STR, false,
            true));
        assertEquals(Collections.emptySet(), helperNode2.nodeIdsForAttribute(ATTR_BUILD_VER, VER_STR, false, false));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCpuLoadMetric() throws Exception {
        Ignite ignite = grid();

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            try {
                assertTrue(GridTestUtils.waitForCondition(
                    () -> ignite.cluster().localNode().metrics().getCurrentCpuLoad() > 0, 10000L));
            }
            catch (IgniteInterruptedCheckedException e) {
                e.printStackTrace();
            }
        });

        final IgniteCache cache = ignite.getOrCreateCache(new CacheConfiguration<>("TEST")
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.REPLICATED));

        for (int i = 0; i < MAX_VALS_AMOUNT * 1000; i++)
            cache.put(i, i);

        fut.get();
    }

    /**
     * Test message.
     */
    private static class TestMessage implements Serializable {
        /** */
        private final byte[] arr = new byte[MSG_SIZE];
    }

    /**
     * Test internal task.
     */
    @GridInternal
    private static class TestInternalTask extends GridTestTask {
        // No-op.
    }

    /**
     * Helper class to simplify ClusterMetricsMXBean testing.
     */
    private static class JmxClusterMetricsHelper {
        /** MBean server. */
        private final MBeanServer mbeanSrv;

        /** ClusterMetrics MX bean name. */
        private final ObjectName mbean;

        /**
         * @param cfg Ignite configuration.
         * @throws MalformedObjectNameException Thrown in case of any errors.
         */
        private JmxClusterMetricsHelper(IgniteConfiguration cfg, Class<? extends ClusterMetricsMXBean> clazz) throws MalformedObjectNameException {
            this.mbeanSrv = cfg.getMBeanServer();

            this.mbean = U.makeMBeanName(cfg.getIgniteInstanceName(), "Kernal", clazz.getSimpleName());
        }

        /**
         * Get MBean attribute through MBean server.
         *
         * @param name Attribute name.
         * @return Current value of attribute.
         * @throws Exception If failed.
         */
        private Object attr(String name) throws Exception {
            return mbeanSrv.getAttribute(mbean, name);
        }

        /**
         * Get distinct attribute names for given nodes projection.
         */
        public Set<String> attributeNames() throws Exception {
            String[] signature = {};
            Object[] params = {};

            return (Set<String>)mbeanSrv.invoke(mbean, "attributeNames", params, signature);
        }

        /**
         * Get distinct attribute values for given nodes projection.
         *
         * @param attrName Attribute name.
         */
        public Set<String> attributeValues(String attrName) throws Exception {
            String[] signature = {"java.lang.String"};
            Object[] params = {attrName};

            return (Set<String>)mbeanSrv.invoke(mbean, "attributeValues", params, signature);
        }

        /**
         * Get node IDs with the given attribute value.
         *
         * @param attrName Attribute name.
         * @param attrVal Attribute value.
         * @param includeSrvs Include server nodes.
         * @param includeClients Include client nodes.
         */
        public Set<UUID> nodeIdsForAttribute(String attrName, String attrVal, boolean includeSrvs,
            boolean includeClients) throws Exception {
            String[] signature = {"java.lang.String", "java.lang.String", "boolean", "boolean"};
            Object[] params = {attrName, attrVal, includeSrvs, includeClients};

            return (Set<UUID>)mbeanSrv.invoke(mbean, "nodeIdsForAttribute", params, signature);
        }
    }
}
