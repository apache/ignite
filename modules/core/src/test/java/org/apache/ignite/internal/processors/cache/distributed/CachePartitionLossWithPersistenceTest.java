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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;

/**
 *
 */
public class CachePartitionLossWithPersistenceTest extends GridCommonAbstractTest {
    /** */
    public static final int WAIT = 2_000;

    /** */
    private static final int PARTS_CNT = 32;

    /** */
    private PartitionLossPolicy lossPlc;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setConsistentId(igniteInstanceName);
        cfg.setClientMode(igniteInstanceName.startsWith("client"));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setWalSegmentSize(4 * 1024 * 1024)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(100L * 1024 * 1024))
        );

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
            setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC).
            setPartitionLossPolicy(lossPlc).
            setBackups(1).
            setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT)));

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests activation on partial baseline with lost partitions.
     */
    @Test
    public void testResetOnLesserTopologyAfterRestart() throws Exception {
        IgniteEx crd = startGrids(5);
        crd.cluster().active(true);

        stopAllGrids();

        crd = startGrids(2);
        crd.cluster().active(true);

        resetBaselineTopology();

        assertFalse(grid(0).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());
        assertFalse(grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions().isEmpty());

        crd.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange();
    }

    /**
     *
     */
    @Test
    @Ignore("https://ggsystems.atlassian.net/browse/GG-28521")
    public void testConsistencyAfterResettingLostPartitions_1() throws Exception {
        doTestConsistencyAfterResettingLostPartitions(0, false);
    }

    /**
     * Lagging node cannot be rebalanced from joining node.
     */
    @Test
    public void testConsistencyAfterResettingLostPartitions_2() throws Exception {
        doTestConsistencyAfterResettingLostPartitions(1, true);
    }

    /**
     *
     */
    @Test
    public void testConsistencyAfterResettingLostPartitions_3() throws Exception {
        doTestConsistencyAfterResettingLostPartitions(2, false);
    }

    /**
     * Test scenario: two nodes are left causing data loss and later returned to topology in various order.
     * <p>
     * Expected result: after resetting lost state partitions are synced and no data loss occured. No assertions happened
     * due to updates going to moving partitions.
     *
     * @param partResetMode Reset mode:
     * <ul>
     *     <li>0 - reset then only lagging node is returned.</li>
     *     <li>1 - reset then both nodes are returned.</li>
     *     <li>2 - reset then both nodes are returned and new node is added to baseline causing partition movement.</li>
     * </ul>
     * @param testPutToMovingPart {@code True} to try updating moving partition.
     */
    private void doTestConsistencyAfterResettingLostPartitions(int partResetMode, boolean testPutToMovingPart) throws Exception {
        lossPlc = READ_WRITE_SAFE;

        IgniteEx crd = startGrids(2);
        crd.cluster().active(true);

        startGrid(2);
        resetBaselineTopology();
        awaitPartitionMapExchange();

        // Find a lost partition which is primary for g1.
        int part = IntStream.range(0, PARTS_CNT).boxed().filter(new Predicate<Integer>() {
            @Override public boolean test(Integer p) {
                final List<ClusterNode> nodes = new ArrayList<>(crd.affinity(DEFAULT_CACHE_NAME).mapPartitionToPrimaryAndBackups(p));

                return nodes.get(0).equals(grid(1).localNode()) && nodes.get(1).equals(grid(2).localNode());
            }
        }).findFirst().orElseThrow(AssertionError::new);

        stopGrid(1);

        final IgniteInternalCache<Object, Object> cachex = crd.cachex(DEFAULT_CACHE_NAME);

        for (int p = 0; p < PARTS_CNT; p++)
            cachex.put(p, 0);

        stopGrid(2); // g1 now lags behind g2.

        final Collection<Integer> lostParts = crd.cache(DEFAULT_CACHE_NAME).lostPartitions();

        assertEquals(PARTS_CNT, cachex.context().topology().localPartitions().size() + lostParts.size());

        assertTrue(lostParts.contains(part));

        // Start lagging node first.
        final IgniteEx g1 = startGrid(1);

        final Collection<Integer> g1LostParts = g1.cache(DEFAULT_CACHE_NAME).lostPartitions();

        assertEquals(lostParts, g1LostParts);

        if (testPutToMovingPart) {
            // Block rebalancing from g2 to g1 to ensure a primary partition is in moving state.
            TestRecordingCommunicationSpi.spi(g1).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return msg instanceof GridDhtPartitionDemandMessage;
                }
            });
        }

        if (partResetMode == 0)
            crd.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        final IgniteEx g2 = startGrid(2);

        final Collection<Integer> g2LostParts = g2.cache(DEFAULT_CACHE_NAME).lostPartitions();

        if (partResetMode != 0)
            assertEquals(lostParts, g2LostParts);

        if (partResetMode == 1)
            crd.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        if (testPutToMovingPart) {
            TestRecordingCommunicationSpi.spi(g1).waitForBlocked();

            /** Try put to moving partition. Due to forced reassignment g2 should be a primary for the partition. */
            cachex.put(part, 0);

            TestRecordingCommunicationSpi.spi(g1).stopBlock();
        }

        if (partResetMode == 2) {
            final IgniteEx g3 = startGrid(3);

            resetBaselineTopology();

            crd.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));
        }

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));

        // Read validation.
        for (int p = 0; p < PARTS_CNT; p++) {
            for (Ignite ignite : G.allGrids())
                assertEquals("Partition " + p, 0, ignite.cache(DEFAULT_CACHE_NAME).get(p));
        }
    }
}
