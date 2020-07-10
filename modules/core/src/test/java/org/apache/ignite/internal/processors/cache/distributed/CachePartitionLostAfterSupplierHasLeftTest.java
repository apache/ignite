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
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
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
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test scenario: last supplier has left while a partition on demander is cleared before sending first demand request.
 */
public class CachePartitionLostAfterSupplierHasLeftTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS_CNT = 64;

    /** */
    private PartitionLossPolicy lossPlc;

    /** */
    private boolean persistence;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setActiveOnStart(false);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setWalSegmentSize(4 * 1024 * 1024)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(persistence)
                        .setMaxSize(100L * 1024 * 1024))
        );

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setAtomicityMode(TRANSACTIONAL).
            setCacheMode(PARTITIONED).
            setBackups(1).
            setPartitionLossPolicy(lossPlc).
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
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnCrd() throws Exception {
        lossPlc = PartitionLossPolicy.READ_WRITE_SAFE;
        persistence = true;

        doTestPartitionLostWhileClearing(2, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnCrd_Volatile() throws Exception {
        lossPlc = PartitionLossPolicy.READ_WRITE_SAFE;
        persistence = false;

        doTestPartitionLostWhileClearing(2, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnFullMessage() throws Exception {
        lossPlc = PartitionLossPolicy.READ_WRITE_SAFE;
        persistence = true;

        doTestPartitionLostWhileClearing(3, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnFullMessage_Volatile() throws Exception {
        lossPlc = PartitionLossPolicy.READ_WRITE_SAFE;
        persistence = false;

        doTestPartitionLostWhileClearing(3, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnCrd_Unsafe() throws Exception {
        lossPlc = PartitionLossPolicy.IGNORE; // READ_WRITE_SAFE is used instead of IGNORE.
        persistence = true;

        doTestPartitionLostWhileClearing(2, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnCrd_Unsafe_Volatile() throws Exception {
        lossPlc = PartitionLossPolicy.IGNORE; // READ_WRITE_SAFE is used instead of IGNORE.
        persistence = false;

        doTestPartitionLostWhileClearing(2, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnFullMessage_Unsafe() throws Exception {
        lossPlc = PartitionLossPolicy.IGNORE; // READ_WRITE_SAFE is used instead of IGNORE.
        persistence = true;

        doTestPartitionLostWhileClearing(3, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_FailOnFullMessage_Unsafe_Volatile() throws Exception {
        lossPlc = PartitionLossPolicy.IGNORE; // READ_WRITE_SAFE is used instead of IGNORE.
        persistence = false;

        doTestPartitionLostWhileClearing(3, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLostWhileClearing_Restart() throws Exception {
        lossPlc = PartitionLossPolicy.READ_WRITE_SAFE;
        persistence = true;

        doTestPartitionLostWhileClearing(2, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-13054")
    public void testPartitionLostWhileClearing_Restart_2() throws Exception {
        lossPlc = PartitionLossPolicy.READ_WRITE_SAFE;
        persistence = true;

        doTestPartitionLostWhileClearing(2, 2);
    }

    /**
     *
     */
    @Test
    public void testPartitionConsistencyOnSupplierRestart() throws Exception {
        lossPlc = PartitionLossPolicy.READ_ONLY_SAFE;
        persistence = true;

        int entryCnt = PARTS_CNT * 200;

        IgniteEx crd = (IgniteEx)startGridsMultiThreaded(2);

        crd.cluster().active(true);

        IgniteCache<Integer, String> cache0 = crd.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < entryCnt / 2; i++)
            cache0.put(i, String.valueOf(i));

        forceCheckpoint();

        stopGrid(1);

        for (int i = entryCnt / 2; i < entryCnt; i++)
            cache0.put(i, String.valueOf(i));

        final IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(1));

        final TestRecordingCommunicationSpi spi1 = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

        spi1.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtPartitionDemandMessage) {
                    GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage)msg;

                    return msg0.groupId() == CU.cacheId(DEFAULT_CACHE_NAME);
                }

                return false;
            }
        });

        startGrid(cfg);

        spi1.waitForBlocked(1);

        // Will cause a cancellation of rebalancing because a supplier has left.
        stopGrid(0);

        awaitPartitionMapExchange();

        final Collection<Integer> lostParts = grid(1).cache(DEFAULT_CACHE_NAME).lostPartitions();

        assertEquals(PARTS_CNT, lostParts.size());

        final IgniteEx g0 = startGrid(0);

        final Collection<Integer> lostParts2 = g0.cache(DEFAULT_CACHE_NAME).lostPartitions();

        assertEquals(PARTS_CNT, lostParts2.size());

        spi1.stopBlock();

        g0.resetLostPartitions(Collections.singletonList(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange();

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }

    /**
     * Test scenario: last supplier has left while a partition on demander is cleared before sending first demand request.
     *
     * @param cnt Nodes count.
     * @param mode Test mode: 0 - reset while clearing, 1 - restart while clearing and activate all, 2 - restart while
     *             clearing and activate in wrong order.
     * @throws Exception If failed.
     */
    private void doTestPartitionLostWhileClearing(int cnt, int mode) throws Exception {
        IgniteEx crd = startGrids(cnt);
        crd.cluster().baselineAutoAdjustEnabled(false);
        crd.cluster().active(true);

        int partId = -1;
        int idx0 = 0;
        int idx1 = 1;

        for (int p = 0; p < PARTS_CNT; p++) {
            List<ClusterNode> nodes = new ArrayList<>(crd.affinity(DEFAULT_CACHE_NAME).mapPartitionToPrimaryAndBackups(p));

            if (grid(nodes.get(0)) == grid(idx0) && grid(nodes.get(1)) == grid(idx1)) {
                partId = p;

                break;
            }
        }

        assertTrue(partId >= 0);

        final int keysCnt = 10_010;

        List<Integer> keys = partitionKeys(grid(idx0).cache(DEFAULT_CACHE_NAME), partId, keysCnt, 0);

        load(grid(idx0), DEFAULT_CACHE_NAME, keys.subList(0, keysCnt - 10));

        stopGrid(idx1);

        load(grid(idx0), DEFAULT_CACHE_NAME, keys.subList(keysCnt - 10, keysCnt));

        TestRecordingCommunicationSpi.spi(grid(0)).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                if (msg instanceof GridDhtPartitionSupplyMessage) {
                    GridDhtPartitionSupplyMessage msg0 = (GridDhtPartitionSupplyMessage) msg;

                    return msg0.groupId() == CU.cacheId(DEFAULT_CACHE_NAME);
                }

                return false;
            }
        });

        IgniteEx g1 = startGrid(idx1);

        stopGrid(idx0); // Stop supplier in the middle of rebalancing.

        final GridDhtLocalPartition part = g1.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(partId);

        assertEquals(GridDhtPartitionState.LOST, part.state());
        assertTrue(g1.cachex(DEFAULT_CACHE_NAME).lostPartitions().contains(partId));

        if (mode != 0) {
            stopAllGrids();

            if (mode == 1) {
                crd = startGrids(cnt);
                crd.cluster().active(true);
            }
            else if (mode == 2) {
                crd = startGrid(idx1);
                crd.cluster().active(true);

                startGrid(idx0);
            }
            else
                fail("Mode: " + mode);

            awaitPartitionMapExchange();

            assertPartitionsSame(idleVerify(crd, DEFAULT_CACHE_NAME));
        }
        else {
            // Will own a clearing partition.
            g1.resetLostPartitions(Collections.singletonList(DEFAULT_CACHE_NAME));

            awaitPartitionMapExchange();

            // Expecting partition in OWNING state.
            final PartitionUpdateCounter cntr = counter(partId, DEFAULT_CACHE_NAME, g1.name());
            assertNotNull(cntr);

            // Counter must be reset.
            assertEquals(0, cntr.get());

            // Puts done concurrently with clearing after reset should not be lost.
            g1.cache(DEFAULT_CACHE_NAME).putAll(keys.stream().collect(Collectors.toMap(k -> k, v -> -1)));

            g1.context().cache().context().evict().awaitFinishAll();

            for (Integer key : keys)
                assertEquals("key=" + key.toString(), -1, g1.cache(DEFAULT_CACHE_NAME).get(key));
        }
    }

    /**
     * @param ignite Ignite.
     * @param cache Cache.
     * @param keys Keys.
     */
    private void load(IgniteEx ignite, String cache, List<Integer> keys) {
        try (IgniteDataStreamer<Object, Object> s = ignite.dataStreamer(cache)) {
            for (Integer key : keys)
                s.addData(key, key);
        }
    }
}
