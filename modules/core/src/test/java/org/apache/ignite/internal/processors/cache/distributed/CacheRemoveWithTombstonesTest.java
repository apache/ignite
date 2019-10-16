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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;

/**
 *
 */
@RunWith(Parameterized.class)
public class CacheRemoveWithTombstonesTest extends GridCommonAbstractTest {
    /** Test parameters. */
    @Parameterized.Parameters(name = "persistenceEnabled={0}, historicalRebalance={1}")
    public static Collection parameters() {
        List<Object[]> res = new ArrayList<>();

        for (boolean persistenceEnabled : new boolean[] {false, true}) {
            for (boolean histRebalance : new boolean[] {false, true}) {
                if (!persistenceEnabled && histRebalance)
                    continue;

                res.add(new Object[]{persistenceEnabled, histRebalance});
            }
        }

        return res;
    }

    /** */
    @Parameterized.Parameter(0)
    public boolean persistence;

    /** */
    @Parameterized.Parameter(1)
    public boolean histRebalance;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setConsistentId(gridName);

        cfg.setCommunicationSpi(commSpi);

        if (persistence) {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                            new DataRegionConfiguration()
                                .setInitialSize(256L * 1024 * 1024)
                                .setMaxSize(256L * 1024 * 1024)
                                .setPersistenceEnabled(true)
                    )
                    .setWalMode(WALMode.LOG_ONLY);

            cfg.setDataStorageConfiguration(dsCfg);
        }

        return cfg;
    }

    /**
     *
     */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        if (histRebalance)
            System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");
    }

    /**
     *
     */
    @After
    public void after() throws Exception {
        if (histRebalance)
            System.clearProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAndRebalanceRaceTx() throws Exception {
        testRemoveAndRebalanceRace(TRANSACTIONAL, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAndRebalanceRaceAtomic() throws Exception {
        testRemoveAndRebalanceRace(ATOMIC, false);
    }

    /**
     * @throws Exception If failed.
     * @param expTombstone {@code True} if tombstones should be created.
     */
    private void testRemoveAndRebalanceRace(CacheAtomicityMode atomicityMode, boolean expTombstone) throws Exception {
        IgniteEx ignite0 = startGrid(0);

        if (histRebalance)
            startGrid(1);

        if (persistence)
            ignite0.cluster().active(true);

        IgniteCache<Object, Object> cache0 = ignite0.createCache(cacheConfiguration(atomicityMode));

        final int KEYS = histRebalance ? 1024 : 1024 * 256;

        if (histRebalance) {
            // Preload initial data to have start point for WAL rebalance.
            try (IgniteDataStreamer<Object, Object> streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME)) {
                streamer.allowOverwrite(true);

                for (int i = 0; i < KEYS; i++)
                    streamer.addData(-i, 0);
            }

            forceCheckpoint();

            stopGrid(1);
        }

        // This data will be rebalanced.
        try (IgniteDataStreamer<Object, Object> streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < KEYS; i++)
                streamer.addData(i, i);
        }

        blockRebalance(ignite0);

        IgniteEx ignite1 = GridTestUtils.runAsync(() -> startGrid(1)).get(10, TimeUnit.SECONDS);

        if (persistence) {
            ignite0.cluster().baselineAutoAdjustEnabled(false);

            ignite0.cluster().setBaselineTopology(2);
        }

        TestRecordingCommunicationSpi.spi(ignite0).waitForBlocked();

        Set<Integer> keysWithTombstone = new HashSet<>();

        // Do removes while rebalance is in progress.
        // All keys are removed during historical rebalance.
        for (int i = 0, step = histRebalance ? 1 : 64; i < KEYS; i += step) {
            keysWithTombstone.add(i);

            cache0.remove(i);
        }

        final LongMetric tombstoneMetric0 = ignite0.context().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric("Tombstones");

        final LongMetric tombstoneMetric1 = ignite1.context().metric().registry(
            MetricUtils.cacheGroupMetricsRegistryName(DEFAULT_CACHE_NAME)).findMetric("Tombstones");

        // On first node there should not be tombstones.
        assertEquals(0, tombstoneMetric0.value());

        if (expTombstone)
            assertEquals(keysWithTombstone.size(), tombstoneMetric1.value());
        else
            assertEquals(0, tombstoneMetric1.value());

        // Update some of removed keys, this should remove tombstones.
        for (int i = 0; i < KEYS; i += 128) {
            keysWithTombstone.remove(i);

            cache0.put(i, i);
        }

        assertTrue("Keys with tombstones should exist", !keysWithTombstone.isEmpty());

        assertEquals(0, tombstoneMetric0.value());

        if (expTombstone)
            assertEquals(keysWithTombstone.size(), tombstoneMetric1.value());
        else
            assertEquals(0, tombstoneMetric1.value());

        TestRecordingCommunicationSpi.spi(ignite0).stopBlock();

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache1 = ignite(1).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < KEYS; i++) {
            if (keysWithTombstone.contains(i))
                assertNull(cache1.get(i));
            else
                assertEquals((Object)i, cache1.get(i));
        }

        // Tombstones should be removed after once rebalance is completed.
        GridTestUtils.waitForCondition(() -> tombstoneMetric1.value() == 0, 30_000);

        assertEquals(0, tombstoneMetric1.value());
    }

    /**
     *
     */
    private static void blockRebalance(IgniteEx node) {
        final int grpId = groupIdForCache(node, DEFAULT_CACHE_NAME);

        TestRecordingCommunicationSpi.spi(node).blockMessages((node0, msg) ->
            (msg instanceof GridDhtPartitionSupplyMessage)
            && ((GridCacheGroupIdMessage)msg).groupId() == grpId
        );
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(CacheAtomicityMode atomicityMode) {
        return new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(atomicityMode)
            .setCacheMode(PARTITIONED)
            .setBackups(2)
            .setRebalanceMode(ASYNC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 64));
    }
}
