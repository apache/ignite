/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 * Test checks historical rebalance in various situations.
 */
@WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
public class HistoricalReservationTest extends GridCommonAbstractTest {

    /** Count all cluster nodes. */
    public static final int CLUSTER_NODES = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setBackups(1))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setCheckpointFrequency(300_000)
                .setWalCompactionEnabled(true)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(200 * 1024 * 1024)
                    .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * Check deactivate cluster before historical rebalance.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivated() throws Exception {
        historicalRebalance(() -> {
            G.allGrids().get(0).active(false);
        });
    }

    /**
     * Check whole cluster restart before historical rebalance.
     * Wherein rebalanced node is comming to not active cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestartGrid() throws Exception {
        historicalRebalance(() -> {
            try {
                stopAllGrids();

                startGrids(CLUSTER_NODES);
            }
            catch (Exception e) {
                fail(e.getMessage());
            }
        });
    }

    /**
     * Check deactivate and whole cluster restart before historical rebalance.
     * Wherein rebalanced node is comming to not active cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateAndRestartGrid() throws Exception {
        historicalRebalance(() -> {
            try {
                G.allGrids().get(0).active(false);

                stopAllGrids();

                startGrids(CLUSTER_NODES);
            }
            catch (Exception e) {
                fail(e.getMessage());
            }
        });
    }

    /**
     * Check deactivate and whole cluster restart before historical rebalance.
     * Wherein rebalanced node is comming to active cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateAndRestartGridActive() throws Exception {
        historicalRebalance(() -> {
            try {
                G.allGrids().get(0).active(false);

                stopAllGrids();

                startGrids(CLUSTER_NODES);

                G.allGrids().get(0).active(true);
            }
            catch (Exception e) {
                fail(e.getMessage());
            }
        });
    }

    /**
     * Execute some action before historical rebalance.
     *
     * @param someAction Some action, executing before rebalance.
     * @throws Exception If faild.
     */
    public void historicalRebalance(IgniteRunnable someAction) throws Exception {
        String rebalancedNodeName = getTestIgniteInstanceName(CLUSTER_NODES) + "_rebalance";

        IgniteEx ignite0 = startGrids(CLUSTER_NODES);
        IgniteEx rebalancedNode = startGrid(rebalancedNodeName);

        ignite0.cluster().active(true);

        preloadData(ignite0, 0, 100);

        forceCheckpoint();

        awaitPartitionMapExchange();

        rebalancedNode.close();

        preloadData(ignite0, 100, 200);

        awaitPartitionMapExchange();

        someAction.run();

        AtomicBoolean hasFullRebalance = new AtomicBoolean();

        IgniteConfiguration cfg = getConfiguration(rebalancedNodeName);

        TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

        spi.record((node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage)msg;

                if (!F.isEmpty(demandMsg.partitions().fullSet()))
                    hasFullRebalance.compareAndSet(false, true);
            }

            return false;
        });

        rebalancedNode = startGrid(cfg);

        rebalancedNode.cluster().active(true);

        awaitPartitionMapExchange();

        assertFalse("Full rebalance appeared where only the  historical one was expected.", hasFullRebalance.get());

        assertPartitionsSame(idleVerify(rebalancedNode, DEFAULT_CACHE_NAME));
    }

    /**
     * Load data in range specified.
     *
     * @param ignite Ignite.
     * @param from Firs index loading data.
     * @param to Last index loading data.
     */
    private void preloadData(IgniteEx ignite, int from, int to) {
        try (IgniteDataStreamer streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = from; i < to; i++)
                streamer.addData(i, "Val " + i);
        }
    }
}
