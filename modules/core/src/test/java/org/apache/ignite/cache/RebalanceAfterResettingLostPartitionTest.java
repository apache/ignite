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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class RebalanceAfterResettingLostPartitionTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache" + UUID.randomUUID().toString();

    /** Cache size */
    public static final int CACHE_SIZE = 10_000;

    /** Stop all grids and cleanup persistence directory. */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** Stop all grids and cleanup persistence directory. */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setRebalanceBatchSize(100);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration().setWalSegmentSize(4 * 1024 * 1024);

        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(500L * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg);

        cfg.setCacheConfiguration(new CacheConfiguration()
            .setName(CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1));

        return cfg;
    }

    /**
     * Test to restore lost partitions and rebalance data on working grid with two nodes.
     *
     * @throws Exception if fail.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testRebalanceAfterPartitionsWereLost() throws Exception {
        startGrids(2);

        grid(0).cluster().active(true);

        for (int j = 0; j < CACHE_SIZE; j++)
            grid(0).cache(CACHE_NAME).put(j, "Value" + j);

        String g1Name = grid(1).name();

        // Stopping the the second node.
        stopGrid(1);

        // Cleaning the persistence for second node.
        cleanPersistenceDir(g1Name);

        AtomicInteger msgCntr = new AtomicInteger();

        TestRecordingCommunicationSpi.spi(ignite(0)).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                if (msg instanceof GridDhtPartitionSupplyMessage &&
                    ((GridCacheGroupIdMessage)msg).groupId() == CU.cacheId(CACHE_NAME)) {
                    if (msgCntr.get() > 3)
                        return true;
                    else
                        msgCntr.incrementAndGet();
                }

                return false;
            }
        });

        // Starting second node again(with the same consistent id).
        startGrid(1);

        // Waitting for rebalance.
        TestRecordingCommunicationSpi.spi(ignite(0)).waitForBlocked();

        // Killing the first node at the moment of rebalancing.
        stopGrid(0);

        // Returning first node to the cluster.
        IgniteEx g0 = startGrid(0);

        assertTrue(Objects.requireNonNull(
            grid(0).cachex(CACHE_NAME)).context().topology().localPartitions().stream().allMatch(
            p -> p.state() == GridDhtPartitionState.LOST));

        // Verify that partition loss is detected.
        assertTrue(Objects.requireNonNull(
            grid(1).cachex(CACHE_NAME)).context().topology().localPartitions().stream().allMatch(
            p -> p.state() == GridDhtPartitionState.LOST));

        // Reset lost partitions and wait for PME.
        grid(1).resetLostPartitions(Arrays.asList(CACHE_NAME, "ignite-sys-cache"));

        awaitPartitionMapExchange();

        assertTrue(Objects.requireNonNull(
            grid(0).cachex(CACHE_NAME)).context().topology().localPartitions().stream().allMatch(
            p -> p.state() == GridDhtPartitionState.OWNING));

        // Verify that partitions are in owning state.
        assertTrue(Objects.requireNonNull(
            grid(1).cachex(CACHE_NAME)).context().topology().localPartitions().stream().allMatch(
            p -> p.state() == GridDhtPartitionState.OWNING));

        // Verify that data was successfully rebalanced.
        for (int i = 0; i < CACHE_SIZE; i++)
            assertEquals("Value" + i, grid(0).cache(CACHE_NAME).get(i));

        for (int i = 0; i < CACHE_SIZE; i++)
            assertEquals("Value" + i, grid(1).cache(CACHE_NAME).get(i));
    }
}
