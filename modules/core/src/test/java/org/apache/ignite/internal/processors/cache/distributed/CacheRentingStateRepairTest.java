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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Contains several test scenarios related to partition state transitions during it's lifecycle.
 */
public class CacheRentingStateRepairTest extends GridCommonAbstractTest {
    /** */
    public static final int PARTS = 1024;

    /** */
    private static final String CLIENT = "client";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS).setPartitions(64));

        ccfg.setOnheapCacheEnabled(false);

        ccfg.setBackups(1);

        ccfg.setRebalanceBatchSize(100);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setCacheConfiguration(ccfg);

        cfg.setActiveOnStart(false);

        cfg.setConsistentId(igniteInstanceName);

        long sz = 100 * 1024 * 1024;

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setPageSize(1024)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true).setInitialSize(sz).setMaxSize(sz))
            .setWalSegmentSize(8 * 1024 * 1024)
            .setWalMode(WALMode.LOG_ONLY).setCheckpointFrequency(24L * 60 * 60 * 1000);

        cfg.setDataStorageConfiguration(memCfg);

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
    @Override public String getTestIgniteInstanceName(int idx) {
        return "node" + idx;
    }

    /**
     * Tests partition is properly evicted when node is restarted in the middle of the eviction.
     */
    @Test
    public void testRentingStateRepairAfterRestart() throws Exception {
        try {
            IgniteEx g0 = startGrid(0);

            g0.cluster().baselineAutoAdjustEnabled(false);
            startGrid(1);

            g0.cluster().active(true);

            awaitPartitionMapExchange();

            List<Integer> parts = evictingPartitionsAfterJoin(g0, g0.cache(DEFAULT_CACHE_NAME), 20);

            int delayEvictPart = parts.get(0);

            int k = 0;

            while (g0.affinity(DEFAULT_CACHE_NAME).partition(k) != delayEvictPart)
                k++;

            g0.cache(DEFAULT_CACHE_NAME).put(k, k);

            GridDhtPartitionTopology top = dht(g0.cache(DEFAULT_CACHE_NAME)).topology();

            GridDhtLocalPartition part = top.localPartition(delayEvictPart);

            assertNotNull(part);

            // Prevent eviction.
            part.reserve();

            startGrid(2);

            g0.cluster().setBaselineTopology(3);

            // Wait until all is evicted except first partition.
            assertTrue("Failed to wait for partition eviction: reservedPart=" + part.id() + ", otherParts=" +
                top.localPartitions().stream().map(p -> "[id=" + p.id() + ", state=" + p.state() + ']').collect(Collectors.toList()),
                waitForCondition(() -> {
                for (int i = 0; i < parts.size(); i++) {
                    if (delayEvictPart == i)
                        continue; // Skip reserved partition.

                    Integer p = parts.get(i);

                    @Nullable GridDhtLocalPartition locPart = top.localPartition(p);

                    assertNotNull(locPart);

                    if (locPart.state() != GridDhtPartitionState.EVICTED)
                        return false;
                }

                return true;
            }, 5000));

            /**
             * Force renting state before node stop.
             * This also could be achieved by stopping node just after RENTING state is set.
             */
            part.setState(GridDhtPartitionState.RENTING);

            assertEquals(GridDhtPartitionState.RENTING, part.state());

            stopGrid(0);

            g0 = startGrid(0);

            awaitPartitionMapExchange();

            part = dht(g0.cache(DEFAULT_CACHE_NAME)).topology().localPartition(delayEvictPart);

            assertNotNull(part);

            final GridDhtLocalPartition finalPart = part;

            CountDownLatch clearLatch = new CountDownLatch(1);

            part.onClearFinished(fut -> {
                assertEquals(GridDhtPartitionState.EVICTED, finalPart.state());

                clearLatch.countDown();
            });

            assertTrue("Failed to wait for partition eviction after restart",
                clearLatch.await(5_000, TimeUnit.MILLISECONDS));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Tests the partition is not cleared when rebalanced.
     */
    @Test
    public void testRebalanceRentingPartitionAndServerNodeJoin() throws Exception {
        testRebalanceRentingPartitionAndNodeJoin(false, 0);
    }

    /**
     * Tests the partition is not cleared when rebalanced.
     */
    @Test
    public void testRebalanceRentingPartitionAndClientNodeJoin() throws Exception {
        testRebalanceRentingPartitionAndNodeJoin(true, 0);
    }

    /**
     * Tests the partition is not cleared when rebalanced.
     */
    @Test
    public void testRebalanceRentingPartitionAndServerNodeJoinWithDelay() throws Exception {
        testRebalanceRentingPartitionAndNodeJoin(false, 5_000);
    }

    /**
     * Tests the partition is not cleared when rebalanced.
     */
    @Test
    public void testRebalanceRentingPartitionAndClientNodeJoinWithDelay() throws Exception {
        testRebalanceRentingPartitionAndNodeJoin(true, 5_000);
    }

    /**
     * @param client {@code True} for client node join.
     * @param delay Delay.
     *
     * @throws Exception if failed.
     */
    private void testRebalanceRentingPartitionAndNodeJoin(boolean client, long delay) throws Exception {
        try {
            IgniteEx g0 = startGrids(2);

            g0.cluster().baselineAutoAdjustEnabled(false);
            g0.cluster().active(true);

            awaitPartitionMapExchange();

            List<Integer> parts = evictingPartitionsAfterJoin(g0, g0.cache(DEFAULT_CACHE_NAME), 20);

            int delayEvictPart = parts.get(0);

            List<Integer> keys = partitionKeys(g0.cache(DEFAULT_CACHE_NAME), delayEvictPart, 2_000, 0);

            for (Integer key : keys)
                g0.cache(DEFAULT_CACHE_NAME).put(key, key);

            GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl)dht(g0.cache(DEFAULT_CACHE_NAME)).topology();

            GridDhtLocalPartition part = top.localPartition(delayEvictPart);

            assertNotNull(part);

            // Wait for eviction. Same could be achieved by calling awaitPartitionMapExchange(true, true, null, true);
            part.reserve();

            startGrid(2);

            resetBaselineTopology();

            part.release();

            part.rent(false).get();

            CountDownLatch l1 = new CountDownLatch(1);
            CountDownLatch l2 = new CountDownLatch(1);

            // Create race between processing of final supply message and partition clearing.
            // Evicted partition will be recreated using supplied factory.
            top.partitionFactory((ctx, grp, id) -> id != delayEvictPart ? new GridDhtLocalPartition(ctx, grp, id, false) :
                new GridDhtLocalPartition(ctx, grp, id, false) {
                    @Override public void beforeApplyBatch(boolean last) {
                        if (last) {
                            l1.countDown();

                            U.awaitQuiet(l2);

                            if (delay > 0) // Delay rebalance finish to enforce race with clearing.
                                doSleep(delay);
                        }
                    }
                });

            stopGrid(2);

            resetBaselineTopology(); // Trigger rebalance for delayEvictPart after eviction.

            IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
                @Override public void run() {
                    try {
                        l1.await();

                        // Trigger partition clear on next topology version.
                        if (client)
                            startClientGrid(CLIENT);
                        else
                            startGrid(2);

                        l2.countDown(); // Finish partition rebalance after initiating clear.
                    }
                    catch (Exception e) {
                        fail(X.getFullStackTrace(e));
                    }
                }
            }, 1);

            fut.get();

            awaitPartitionMapExchange(true, true, null, true);

            assertPartitionsSame(idleVerify(g0));
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }
}
