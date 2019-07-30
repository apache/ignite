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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

/**
 *
 */
public class CacheRemoveWithTombstonesTest extends GridCommonAbstractTest {
    /** */
    private boolean persistence;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        if (persistence) {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                            new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024).setPersistenceEnabled(true))
                    .setWalMode(WALMode.LOG_ONLY);

            cfg.setDataStorageConfiguration(dsCfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
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
     */
    @Test
    public void testRemoveAndRebalanceRaceTxWithPersistence() throws Exception {
        persistence = true;

        testRemoveAndRebalanceRaceTx();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAndRebalanceRaceAtomicWithPersistence() throws Exception {
        persistence = true;

        testRemoveAndRebalanceRaceAtomic();
    }

    /**
     * @throws Exception If failed.
     * @param expTombstone {@code True} if tombstones should be created.
     */
    private void testRemoveAndRebalanceRace(CacheAtomicityMode atomicityMode, boolean expTombstone) throws Exception {
        IgniteEx ignite0 = startGrid(0);

        if (persistence)
            ignite0.cluster().active(true);

        IgniteCache<Integer, Integer> cache0 = ignite0.createCache(cacheConfiguration(atomicityMode));

        if (MvccFeatureChecker.forcedMvcc())
            expTombstone = false;

        LongMetric tombstoneMetric0 = ignite0.context().metric().registry(
                cacheMetricsRegistryName(DEFAULT_CACHE_NAME, false)).findMetric("Tombstones");

        Map<Integer, Integer> map = new HashMap<>();

        final int KEYS = 1024;

        for (int i = 0; i < KEYS; i++)
            map.put(i, i);

        cache0.putAll(map);

        blockRebalance(ignite0);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                return startGrid(1);
            }
        });

        IgniteEx ignite1 = (IgniteEx)fut.get(30_000);

        if (persistence) {
            ignite0.cluster().baselineAutoAdjustEnabled(false);

            ignite0.cluster().setBaselineTopology(2);
        }

        Set<Integer> removed = new HashSet<>();

        // Do removes while rebalance is in progress.
        for (int i = 0; i < KEYS; i++) {
            if (i % 2 == 0) {
                removed.add(i);

                cache0.remove(i);
            }
        }

        final LongMetric tombstoneMetric1 =  ignite1.context().metric().registry(
                cacheMetricsRegistryName(DEFAULT_CACHE_NAME, false)).findMetric("Tombstones");

        // On first node there should not be tombstones.
        assertEquals(0, tombstoneMetric0.get());

        if (expTombstone)
            assertEquals(removed.size(), tombstoneMetric1.get());
        else
            assertEquals(0, tombstoneMetric1.get());

        // Update some of removed keys, this should remove tombstones.
        for (int i = 0; i < KEYS; i++) {
            if (i % 4 == 0) {
                removed.remove(i);

                cache0.put(i, i);
            }
        }

        assert !removed.isEmpty();

        assertEquals(0, tombstoneMetric0.get());

        if (expTombstone)
            assertEquals(removed.size(), tombstoneMetric1.get());
        else
            assertEquals(0, tombstoneMetric1.get());

        TestRecordingCommunicationSpi.spi(ignite0).stopBlock();

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache1 = ignite(1).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < KEYS; i++) {
            if (removed.contains(i))
                assertNull(cache1.get(i));
            else
                assertEquals((Object)i, cache1.get(i));
        }

        // Tombstones should be removed after once rebalance is completed.
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return tombstoneMetric1.get() == 0;
            }
        }, 30_000);

        assertEquals(0, tombstoneMetric1.get());
    }

    /**
     *
     */
    private void blockRebalance(Ignite node) {
        final int grpId = groupIdForCache(ignite(0), DEFAULT_CACHE_NAME);

        TestRecordingCommunicationSpi.spi(node).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                return (msg instanceof GridDhtPartitionSupplyMessage)
                        && ((GridCacheGroupIdMessage)msg).groupId() == grpId;
            }
        });
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(2);
        ccfg.setRebalanceMode(ASYNC);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        return ccfg;
    }
}
