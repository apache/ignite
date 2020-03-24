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
    /** Whether node starts with persistence enabled. */
    private boolean persistenceEnabled;

    /** {@inheritDoc} */
    @SuppressWarnings("rawtypes")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistenceEnabled)
                .setMaxSize(10L * 1024 * 1024)
            ));

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setBackups(1)
            .setCacheMode(PARTITIONED)
            .setAtomicityMode(TRANSACTIONAL));

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        cfg.setClusterStateOnStart(INACTIVE);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
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
        checkClusterRebalancedMetric();
    }

    /**
     * Tests {@link GridMetricManager#CLUSTER_REBALANCED} metric in case of cluster with native persistence enabled.
     */
    @Test
    public void testPersistenceClusterRebalancedMetric() throws Exception {
        persistenceEnabled = true;

        checkClusterRebalancedMetric();
    }

    /**
     * Checks {@link GridMetricManager#CLUSTER_REBALANCED} metric value.
     */
    public void checkClusterRebalancedMetric() throws Exception {
        IgniteEx ignite = startGrid(0);

        startClientGrid(1);

        assertClusterRebalancedMetricOnAllNodes(false);

        ignite.cluster().state(ACTIVE);

        awaitPmeAndAssertRebalancedMetricOnAllNodes(true);

        ignite.cache(DEFAULT_CACHE_NAME).put("key", "val");

        startClientGrid(2);

        awaitPmeAndAssertRebalancedMetricOnAllNodes(true);

        TestRecordingCommunicationSpi spi = startGridWithRebalanceBlocked(3);

        if (persistenceEnabled) {
            awaitPmeAndAssertRebalancedMetricOnAllNodes(true);

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
        awaitPartitionMapExchange(true, true, null, false);

        assertClusterRebalancedMetricOnAllNodes(exp);
    }

    /**
     * Checks that {@link GridMetricManager#CLUSTER_REBALANCED} metric is set to {@code exp} on all cluster nodes.
     */
    private void assertClusterRebalancedMetricOnAllNodes(boolean exp) {
        assertTrue(G.allGrids().stream().allMatch(ignite -> {
            BooleanMetric rebalancedMetric = ((IgniteEx)ignite)
                .context()
                .metric()
                .registry(REBALANCE_METRICS)
                .findMetric(CLUSTER_REBALANCED);

            return exp == rebalancedMetric.value();
        }));
    }

    /**
     * @param idx Index of the node to be started.
     */
    protected TestRecordingCommunicationSpi startGridWithRebalanceBlocked(int idx) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx));

        TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi) cfg.getCommunicationSpi();

        spi.blockMessages((node, msg) ->  {
            if (!(msg instanceof GridDhtPartitionDemandMessage))
                return false;

            GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage) msg;

            return CU.cacheId(DEFAULT_CACHE_NAME) == demandMsg.groupId();
        });

        startGrid(cfg);

        return spi;
    }
}
