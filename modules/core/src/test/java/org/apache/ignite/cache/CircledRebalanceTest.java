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

package org.apache.ignite.cache;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Tests of circled historical rebalance.
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
public class CircledRebalanceTest extends GridCommonAbstractTest {

    /** Count of restart iterations. */
    public static final int ITERATIONS = 10;

    /** Count of backup for default cache. */
    private int backups = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setShutdownPolicy(ShutdownPolicy.GRACEFUL)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setCheckpointFrequency(6_000)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(200L * 1024 * 1024)
                    .setPersistenceEnabled(true)))
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setAffinity(new RendezvousAffinityFunction(false, 64))
                .setBackups(backups));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        IgnitionEx.stopAll(true, ShutdownPolicy.IMMEDIATE);

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * Restart two nodes in a cicle.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTwoNodesRestart() throws Exception {
        testCircledNodesRestart(2, 4);
    }

    /**
     * Restart one node in a cicle.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOneNodeRestart() throws Exception {
        testCircledNodesRestart(1, 4);
    }

    /**
     * Test restarts in a circle all backup nodes and check that always historical rebalance happens.
     *
     * @param backups Count of backups of default cache.
     * @param nodes Count of nodes.
     * @throws Exception If failed.
     */
    public void testCircledNodesRestart(int backups, int nodes) throws Exception {
        this.backups = backups;

        int realBackups = Math.min(nodes - 1, backups);

        IgniteEx ignite0 = startGrids(nodes);

        ignite0.cluster().active(true);

        loadData(true);

        awaitPartitionMapExchange();

        AtomicBoolean hasFullRebalance = new AtomicBoolean();

        for (int i = 0; i < ITERATIONS; i++) {
            int[] nodesToRestart = new int[realBackups];

            for (int j = 0; j < realBackups; j++)
                nodesToRestart[j] = (i + j) % nodes;

            info("Iter: " + i + " restart nodes: " + Arrays.toString(nodesToRestart));

            for (int j = 0; j < realBackups; j++)
                stopGrid(nodesToRestart[j]);

            loadData(false);

            for (int j = 0; j < realBackups; j++)
                startNodeAndRecordDemandMsg(hasFullRebalance, nodesToRestart[j]);

            for (int j = 0; j < realBackups; j++)
                TestRecordingCommunicationSpi.spi(grid(nodesToRestart[j])).waitForRecorded();

            assertFalse("Assert on iter " + i, hasFullRebalance.get());
        }
    }

    /**
     * Start node matched by number of parameter and
     *
     * @param hasFullRebalance Has full rebalance flag.
     * @throws Exception If failed.
     */
    @NotNull private void startNodeAndRecordDemandMsg(AtomicBoolean hasFullRebalance, int nodeNum) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(nodeNum));

        TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

        spi.record((node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage)msg;

                hasFullRebalance.compareAndSet(false, !F.isEmpty(demandMsg.partitions().fullSet()));

                return true;
            }

            return false;
        });

        startGrid(cfg);
    }

    /**
     * Load some data to default cache.
     *
     * @param sequentially True for loading sequential keys, false for random.
     */
    public void loadData(boolean sequentially) {
        Random random = new Random();

        Ignite ignite = G.allGrids().get(0);

        try (IgniteDataStreamer streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < 100; i++) {
                Integer ranDomKey = random.nextInt(10_000);

                streamer.addData(sequentially ? i : ranDomKey, "Val " + ranDomKey);
            }
        }
    }
}
