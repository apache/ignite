/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;

/**
 *
 */
public class GridCacheRebalancingWithAsyncClearingTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final int PARTITIONS_CNT = 32;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(300L * 1024 * 1024))
        );

        cfg.setCacheConfiguration(new CacheConfiguration<>(CACHE_NAME)
                .setAtomicityMode(atomicityMode())
                .setBackups(2)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setIndexedTypes(Integer.class, Integer.class)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
                .setAffinity(new RendezvousAffinityFunction(false, PARTITIONS_CNT))
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    /**
     * Test that partition clearing doesn't block partitions map exchange.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE, value = "1")
    public void testPartitionClearingNotBlockExchange() throws Exception {
        IgniteEx ig = (IgniteEx) startGrids(3);
        ig.cluster().active(true);

        // High number of keys triggers long partition eviction.
        final int keysCnt = SF.applyLB(300_000, 10_000);

        try (IgniteDataStreamer<Integer, Integer> ds = ig.dataStreamer(CACHE_NAME)) {
            log.info("Writing initial data...");

            ds.allowOverwrite(true);

            for (int k = 1; k <= keysCnt; k++) {
                ds.addData(k, k);

                if (k % 10_000 == 0)
                    log.info("Written " + k + " entities.");
            }

            log.info("Writing initial data finished.");
        }

        stopGrid(2);

        awaitPartitionMapExchange();

        try (IgniteDataStreamer<Integer, Integer> ds = ig.dataStreamer(CACHE_NAME)) {
            log.info("Writing external data...");

            ds.allowOverwrite(true);

            for (int k = 1; k <= keysCnt; k++) {
                ds.addData(k, 2 * k);

                if (k % 10_000 == 0)
                    log.info("Written " + k + " entities.");
            }

            log.info("Writing external data finished.");
        }

        IgniteCache<Integer, Integer> cache = ig.cache(CACHE_NAME);

        forceCheckpoint();

        GridCachePartitionExchangeManager exchangeManager = ig.cachex(CACHE_NAME).context().shared().exchange();

        long topVer = exchangeManager.lastTopologyFuture().topologyVersion().topologyVersion();

        startGrid(2);

        // Check that exchange future is completed and version is incremented
        GridDhtPartitionsExchangeFuture fut1 = exchangeManager.lastTopologyFuture();

        fut1.get();

        Assert.assertEquals(topVer + 1, fut1.topologyVersion().topologyVersion());

        // Check that additional exchange didn't influence on asynchronous partitions eviction.
        boolean asyncClearingIsRunning = false;
        for (int p = 0; p < PARTITIONS_CNT; p++) {
            GridDhtLocalPartition part = grid(2).cachex(CACHE_NAME).context().topology().localPartition(p);
            if (part != null && part.state() == GridDhtPartitionState.MOVING && part.isClearing()) {
                asyncClearingIsRunning = true;
                break;
            }
        }

        Assert.assertTrue("Async clearing is not running at the moment", asyncClearingIsRunning);

        // Check that stopping & starting node didn't break rebalance process.
        stopGrid(1);

        startGrid(1);

        // Wait for rebalance on all nodes.
        for (Ignite ignite : G.allGrids())
            ignite.cache(CACHE_NAME).rebalance().get();

        // Check no data loss.
        for (int k = 1; k <= keysCnt; k++) {
            Integer val = cache.get(k);

            Assert.assertNotNull("Value for " + k + " is null", val);

            Assert.assertEquals("Check failed for " + k + " " + val, 2 * k, (int)val);
        }
    }

    /**
     * Test that partitions belong to affinity in state RENTING or EVICTED are correctly rebalanced.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorrectRebalancingCurrentlyRentingPartitions() throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().active(true);

        // High number of keys triggers long partition eviction.
        final int keysCnt = SF.applyLB(300_000, 10_000);

        try (IgniteDataStreamer<Integer, Integer> ds = ignite.dataStreamer(CACHE_NAME)) {
            log.info("Writing initial data...");

            for (int k = 1; k <= keysCnt; k++) {
                ds.addData(k, k);

                if (k % 50_000 == 0)
                    log.info("Written " + k + " entities.");
            }

            log.info("Writing initial data finished.");
        }

        startGrid(3);

        // Trigger partition eviction from other nodes.
        resetBaselineTopology();

        stopGrid(3);

        // Trigger evicting partitions rebalancing.
        resetBaselineTopology();

        // Emulate stopping grid during partition eviction.
        stopGrid(1);

        // Started node should have partition in RENTING or EVICTED state.
        startGrid(1);

        awaitPartitionMapExchange(true, true, null, true);

        // Check no data loss.
        for (int k = 1; k <= keysCnt; k++) {
            Integer val = (Integer) ignite.cache(CACHE_NAME).get(k);
            Assert.assertNotNull("Value for " + k + " is null", val);
            Assert.assertEquals("Check failed for " + k + " = " + val, k, (int)val);
        }
    }
}
