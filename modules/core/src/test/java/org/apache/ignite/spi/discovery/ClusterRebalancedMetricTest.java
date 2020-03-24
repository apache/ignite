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

package org.apache.ignite.spi.discovery;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CLUSTER_REBALANCED;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.REBALANCE_METRICS;

/**
 * Tests {@link GridMetricManager#CLUSTER_REBALANCED} metric.
 */
public class ClusterRebalancedMetricTest extends GridCommonAbstractTest {
    /** Test cache name. */
    private static final String TEST_CACHE = "TEST_CACHE";

    /**
     * @param idx Index of the node to be started.
     * @param persistenceEnabled Whether node native persistence is enabled.
     */
    protected IgniteConfiguration getConfiguration(int idx, boolean persistenceEnabled) throws Exception {
        DataRegionConfiguration drCfg = new DataRegionConfiguration()
            .setPersistenceEnabled(persistenceEnabled);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(drCfg);

        return getConfiguration(getTestIgniteInstanceName(idx))
            .setDataStorageConfiguration(dsCfg);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("rawtypes")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        CacheConfiguration cCfg = new CacheConfiguration(TEST_CACHE)
            .setBackups(2)
            .setCacheMode(PARTITIONED)
            .setAtomicityMode(TRANSACTIONAL);

        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(cCfg)
            .setCommunicationSpi(commSpi)
            .setClusterStateOnStart(INACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /**
     * Tests {@link GridMetricManager#CLUSTER_REBALANCED} metric in case of in-memory cluster.
     */
    @Test
    public void testInMemoryClusterRebalancedMetric() throws Exception {
        checkClusterRebalancedMetric(false);
    }

    /**
     * Tests {@link GridMetricManager#CLUSTER_REBALANCED} metric in case of cluster with native persistence enabled.
     */
    @Test
    public void testPersistenceClusterRebalancedMetric() throws Exception {
        checkClusterRebalancedMetric(true);
    }

    /**
     * @param persistenceEnabled Whether native persistence is enabled.
     */
    public void checkClusterRebalancedMetric(boolean persistenceEnabled) throws Exception {
        IgniteEx ignite = startGrid(0, persistenceEnabled);

        startClientGrid(1, persistenceEnabled);

        assertClusterRebalancedMetricOnAllNodes(false);

        ignite.cluster().state(ACTIVE);

        awaitPmeAndAssertRebalancedMetricOnAllNodes(true);

        ignite.cache(TEST_CACHE).put("key", "val");

        startClientGrid(2, persistenceEnabled);

        awaitPmeAndAssertRebalancedMetricOnAllNodes(true);

        TestRecordingCommunicationSpi spi = startGridWithRebalanceBlocked(3, persistenceEnabled);

        if (persistenceEnabled) {
            awaitPartitionMapExchange(true, true, null, false);

            assertClusterRebalancedMetricOnAllNodes(true);

            ignite.cluster().setBaselineTopology(ignite.cluster().forServers().nodes());
        }

        spi.waitForBlocked();

        assertClusterRebalancedMetricOnAllNodes(false);

        spi.stopBlock();

        awaitPmeAndAssertRebalancedMetricOnAllNodes(true);
    }

    /**
     * @param exp Expected value of {@link GridMetricManager#CLUSTER_REBALANCED} metric.
     */
    private void awaitPmeAndAssertRebalancedMetricOnAllNodes(boolean exp) throws Exception {
        awaitPartitionMapExchange(true, true, null, true);

        assertClusterRebalancedMetricOnAllNodes(exp);
    }

    /**
     * Checks that {@link GridMetricManager#CLUSTER_REBALANCED} metric is set to {@code exp} on all cluster nodes.
     */
    private void assertClusterRebalancedMetricOnAllNodes(boolean exp) {
        G.allGrids().stream().allMatch(ignite -> {
            BooleanMetric rebalancedMetric = ((IgniteEx) ignite)
                .context()
                .metric()
                .registry(REBALANCE_METRICS)
                .findMetric(CLUSTER_REBALANCED);

            return exp == rebalancedMetric.value();
        });
    }

    /**
     * @param idx Index of the node to be started.
     * @param persistenceEnabled Whether native persistence is enabled.
     */
    protected TestRecordingCommunicationSpi startGridWithRebalanceBlocked(
        int idx,
        boolean persistenceEnabled
    ) throws Exception {
        IgniteConfiguration cfg = getConfiguration(idx, persistenceEnabled);

        TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi) cfg.getCommunicationSpi();

        spi.blockMessages((node, msg) ->  {
            if (!(msg instanceof GridDhtPartitionDemandMessage))
                return false;

            GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage) msg;

            return CU.cacheId(TEST_CACHE) == demandMsg.groupId();
        });

        startGrid(cfg);

        return spi;
    }

    /**
     * @param idx Index of the node to be started.
     * @param persistenceEnabled Whether native persistence is enabled.
     */
    private IgniteEx startGrid(int idx, boolean persistenceEnabled) throws Exception {
        return startGrid(getConfiguration(idx, persistenceEnabled));
    }

    /**
     * @param idx Index of the client node to be started.
     * @param persistenceEnabled Whether native persistence is enabled.
     */
    private IgniteEx startClientGrid(int idx, boolean persistenceEnabled) throws Exception {
        return startClientGrid(getConfiguration(idx, persistenceEnabled));
    }
}
