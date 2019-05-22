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

package org.apache.ignite.internal.processors.cache.persistence.preload;

import java.util.Collections;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheDataStoreEx;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED;

/**
 * Test cases for checking cancellation rebalancing process if some events occurs.
 */
public class GridCachePersistenceRebalanceSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_PART_COUNT = 8;

    /** */
    private static final int TEST_SIZE = GridTestUtils.SF.applyLB(100_000, 10_000);

    /** */
    @Before
    public void setBefore() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @After
    public void setAfter() {
        System.setProperty(IGNITE_PERSISTENCE_REBALANCE_ENABLED, "false");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(true))
                .setWalMode(WALMode.LOG_ONLY))
        .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /**
     * @param ignite The ignite instance to check.
     * @param check The mode to check.
     * @return The number of pending switch requests.
     */
    private int getPendingSwitchRequests(IgniteEx ignite, CacheDataStoreEx.StorageMode check) {
        return ignite.context()
            .cache()
            .context()
            .preloadMgr()
            .switcher()
            .pendingRequests(new Predicate<CacheDataStoreEx.StorageMode>() {
                @Override public boolean test(CacheDataStoreEx.StorageMode mode) {
                    return mode == check;
                }
            });
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_JVM_PAUSE_DETECTOR_DISABLED, value = "true")
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IGNITE_PERSISTENCE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testPersistenceRebalanceBase() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ignite0.getOrCreateCache(
            new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false)
                    .setPartitions(CACHE_PART_COUNT)));

        loadData(ignite0, DEFAULT_CACHE_NAME, TEST_SIZE);

        assertTrue(!ignite0.cluster().isBaselineAutoAdjustEnabled());

        IgniteEx ignite1 = startGrid(1);

        ignite1.cluster().setBaselineTopology(ignite1.cluster().nodes());

        GridTestUtils.waitForCondition(
            new GridAbsPredicate() {
                @Override public boolean apply() {
                    return CACHE_PART_COUNT == getPendingSwitchRequests(ignite1, CacheDataStoreEx.StorageMode.FULL);
                }
            }, 15_000);

        assertEquals(getPendingSwitchRequests(ignite1, CacheDataStoreEx.StorageMode.FULL), CACHE_PART_COUNT);

        forceCheckpoint(ignite1);

        awaitPartitionMapExchange(true,
            true,
            Collections.singleton(ignite1.localNode()), true);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_JVM_PAUSE_DETECTOR_DISABLED, value = "true")
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IGNITE_PERSISTENCE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testPersistenceRebalanceManualCache() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ignite0.getOrCreateCache(
            new CacheConfiguration<Integer, byte[]>("manual")
                .setCacheMode(CacheMode.PARTITIONED)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(1)
                .setRebalanceDelay(-1)
                .setAffinity(new RendezvousAffinityFunction(false)
                    .setPartitions(8)));

        loadData(ignite0, "manual", TEST_SIZE);

        assertTrue(!ignite0.cluster().isBaselineAutoAdjustEnabled());

        IgniteEx ignite1 = startGrid(1);

        ignite1.cluster().setBaselineTopology(ignite1.cluster().nodes());

        printPartitionState("manual", 0);

        cache.put(TEST_SIZE, new byte[1000]);

        awaitPartitionMapExchange(true, true, Collections.singleton(ignite1.localNode()), true);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_JVM_PAUSE_DETECTOR_DISABLED, value = "true")
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IGNITE_PERSISTENCE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testPersistenceRebalanceAsyncUpdates() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ignite0.getOrCreateCache(
            new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false)
                    .setPartitions(8)));

        loadData(ignite0, DEFAULT_CACHE_NAME, TEST_SIZE);

        assertTrue(!ignite0.cluster().isBaselineAutoAdjustEnabled());

        IgniteEx ignite1 = startGrid(1);

        TestRecordingCommunicationSpi.spi(ignite1)
            .blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return msg instanceof GridPartitionBatchDemandMessage;
                }
            });

        ignite1.cluster().setBaselineTopology(ignite1.cluster().nodes());

        TestRecordingCommunicationSpi.spi(ignite1).waitForBlocked();

        cache.put(TEST_SIZE, new byte[1000]);

        awaitPartitionMapExchange(true, true, Collections.singleton(ignite1.localNode()), true);
    }
}
