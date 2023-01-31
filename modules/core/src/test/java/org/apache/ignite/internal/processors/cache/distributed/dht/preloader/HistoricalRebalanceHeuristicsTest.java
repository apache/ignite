/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 * Checks that way of rebalancing is selected based on a heuristic:
 * if number of updates to rebalance is greater than partition size, full rebalance should be used.
 */
@WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
@RunWith(Parameterized.class)
public class HistoricalRebalanceHeuristicsTest extends GridCommonAbstractTest {
    /** Initial keys. */
    private static final int INITIAL_KEYS = 5000;

    /** Atomicity mode. */
    @Parameterized.Parameter()
    public boolean historical;

    /** Cache recreate. */
    @Parameterized.Parameter(value = 1)
    public boolean cacheRecreate;

    /** Limited WAL. */
    @Parameterized.Parameter(value = 2)
    public boolean limitedWal;

    /** Full rebalancing happened flag. */
    private final AtomicBoolean fullRebalancingHappened = new AtomicBoolean(false);

    /** Historical rebalancing happened flag. */
    private final AtomicBoolean historicalRebalancingHappened = new AtomicBoolean(false);

    /**
     * @return List of versions pairs to test.
     */
    @Parameterized.Parameters(name = "historical = {0}, cacheRecreate={1}, limitedWal = {2}")
    public static Collection<Object[]> testData() {
        List<Object[]> res = new ArrayList<>();

        res.add(new Object[] {true, false, false});
        res.add(new Object[] {false, false, false});
        res.add(new Object[] {true, true, false});

        // Case when earliest checkpoint in the WAL history contains information for the initial cache (before the drop)
        // and update counters in it greater than the actual update counters for the existing cache.
        res.add(new Object[] {true, true, true});

        return res;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                        .setDefaultDataRegionConfiguration(
                                new DataRegionConfiguration()
                                        .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                                        .setPersistenceEnabled(true)
                        )
        );

        if (limitedWal)
            cfg.getDataStorageConfiguration()
                    .setWalSegmentSize(512 * 1024)
                    .setMaxWalArchiveSize(5 * 1024 * 1024);

        cfg.setCacheConfiguration(cacheConfiguration());

        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();

        spi.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage)msg;

                if (demandMsg.groupId() == CU.cacheId(DEFAULT_CACHE_NAME)) {
                    if (demandMsg.partitions().hasFull())
                        fullRebalancingHappened.set(true);

                    if (demandMsg.partitions().hasHistorical())
                        historicalRebalancingHappened.set(true);
                }
            }

            return false;
        });

        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration() {
        return new CacheConfiguration<Integer, Integer>()
                .setAffinity(new RendezvousAffinityFunction(false, 8))
                .setBackups(1)
                .setName(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        fullRebalancingHappened.set(false);
        historicalRebalancingHappened.set(false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Checks that heuristic (see class header doc) works correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testHistoricalRebalanceHeuristics() throws Exception {
        IgniteEx grid = startGrids(2);

        grid.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = grid.cache(DEFAULT_CACHE_NAME);

        if (cacheRecreate) {
            for (int i = 0; i < INITIAL_KEYS * 2; i++)
                cache.put(i, i);

            cache.destroy();

            cache = grid.getOrCreateCache(cacheConfiguration());

            forceCheckpoint();
        }

        for (int i = 0; i < INITIAL_KEYS; i++)
            cache.put(i, i);

        forceCheckpoint();

        stopGrid(1);

        int limit = historical ? INITIAL_KEYS * 3 / 2 : INITIAL_KEYS * 5 / 2;

        for (int i = INITIAL_KEYS; i < limit; i++)
            cache.put(i % INITIAL_KEYS, i);

        startGrid(1);

        awaitPartitionMapExchange(true, true, null);

        assertEquals(historical, historicalRebalancingHappened.get());

        assertEquals(!historical, fullRebalancingHappened.get());
    }
}
