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
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.WALMode.LOG_ONLY;

/**
 *
 */
public class ResetLostPartitionTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String[] CACHE_NAMES = {"cacheOne", "cacheTwo", "cacheThree"};

    /** Cache size */
    public static final int CACHE_SIZE = 100000 / CACHE_NAMES.length;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.setPageSize(1024).setWalMode(LOG_ONLY).setWalSegmentSize(4 * 1024 * 1024);

        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(500L * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg);

        CacheConfiguration[] ccfg = new CacheConfiguration[] {
            cacheConfiguration(CACHE_NAMES[0], CacheAtomicityMode.ATOMIC),
            cacheConfiguration(CACHE_NAMES[1], CacheAtomicityMode.ATOMIC),
            cacheConfiguration(CACHE_NAMES[2], CacheAtomicityMode.TRANSACTIONAL)
        };

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @param mode Cache atomicity mode.
     * @return Configured cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String cacheName, CacheAtomicityMode mode) {
        return new CacheConfiguration<>(cacheName)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(mode)
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
            .setAffinity(new RendezvousAffinityFunction(false, 64))
            .setIndexedTypes(String.class, String.class);
    }

    /**
     * Test to restore lost partitions after grid reactivation.
     *
     * @throws Exception if fail.
     */
    @Test
    public void testReactivateGridBeforeResetLostPartitions() throws Exception {
        doRebalanceAfterPartitionsWereLost(true);
    }

    /**
     * Test to restore lost partitions on working grid.
     *
     * @throws Exception if fail.
     */
    @Test
    public void testResetLostPartitions() throws Exception {
        doRebalanceAfterPartitionsWereLost(false);
    }

    /**
     * @param reactivateGridBeforeResetPart Reactive grid before try to reset lost partitions.
     * @throws Exception if fail.
     */
    private void doRebalanceAfterPartitionsWereLost(boolean reactivateGridBeforeResetPart) throws Exception {
        startGrids(3);

        grid(0).cluster().active(true);

        for (String cacheName : CACHE_NAMES) {
            try (IgniteDataStreamer<Object, Object> st = grid(0).dataStreamer(cacheName)) {
                for (int j = 0; j < CACHE_SIZE; j++)
                    st.addData(j, "Value" + j);
            }
        }

        String g1Name = grid(1).name();

        stopGrid(1);

        cleanPersistenceDir(g1Name);

        //Here we have two from three data nodes and cache with 1 backup. So there is no data loss expected.
        assertEquals(CACHE_NAMES.length * CACHE_SIZE, averageSizeAroundAllNodes());

        //Start node 2 with empty PDS. Rebalance will be started.
        startGrid(1);

        //During rebalance stop node 3. Rebalance will be stopped.
        stopGrid(2);

        //Start node 3.
        startGrid(2);

        // Data loss is expected because rebalance to node 1 have not finished and node 2 was stopped.
        assertTrue(CACHE_NAMES.length * CACHE_SIZE > averageSizeAroundAllNodes());

        // Check all nodes report same lost partitions.
        for (String cacheName : CACHE_NAMES) {
            Collection<Integer> lost = null;

            for (Ignite grid : G.allGrids()) {
                if (lost == null)
                    lost = grid.cache(cacheName).lostPartitions();
                else
                    assertEquals(lost, grid.cache(cacheName).lostPartitions());
            }

            assertTrue(lost != null && !lost.isEmpty());
        }

        if (reactivateGridBeforeResetPart) {
            grid(0).cluster().active(false);
            grid(0).cluster().active(true);
        }

        // Try to reset lost partitions.
        grid(2).resetLostPartitions(Arrays.asList(CACHE_NAMES));

        awaitPartitionMapExchange();

        // Check all nodes report same lost partitions.
        for (String cacheName : CACHE_NAMES) {
            for (Ignite grid : G.allGrids())
                assertTrue(grid.cache(cacheName).lostPartitions().isEmpty());
        }

        // All data was back.
        assertEquals(CACHE_NAMES.length * CACHE_SIZE, averageSizeAroundAllNodes());

        //Stop node 2 for checking rebalance correctness from this node.
        stopGrid(2);

        //Rebalance should be successfully finished.
        assertEquals(CACHE_NAMES.length * CACHE_SIZE, averageSizeAroundAllNodes());
    }

    /**
     * @param gridNumber Grid number.
     * @param cacheName Cache name.
     * @return Partitions states for given cache name.
     */
    private List<GridDhtPartitionState> getPartitionsStates(int gridNumber, String cacheName) {
        CacheGroupContext cgCtx = grid(gridNumber).context().cache().cacheGroup(CU.cacheId(cacheName));

        GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl)cgCtx.topology();

        return top.localPartitions().stream()
            .map(GridDhtLocalPartition::state)
            .collect(Collectors.toList());
    }

    /**
     * Checks that all nodes see the correct size.
     */
    private int averageSizeAroundAllNodes() {
        int totalSize = 0;

        for (Ignite ignite : IgnitionEx.allGrids()) {
            for (String cacheName : CACHE_NAMES) {
                totalSize += ignite.cache(cacheName).size();
            }
        }

        return totalSize / IgnitionEx.allGrids().size();
    }
}
