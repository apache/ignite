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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.spi.checkpoint.noop.NoopCheckpointSpi;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgnitePdsWholeClusterRestartTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = SF.applyLB(5, 3);

    /** */
    private static final int ENTRIES_COUNT = SF.apply(1_000);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration ccfg1 = defaultCacheConfiguration();

        ccfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg1.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg1.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg1.setBackups(2);

        cfg.setActiveOnStart(false);

        // To avoid hostname lookup on start.
        cfg.setCheckpointSpi(new NoopCheckpointSpi());

        cfg.setCacheConfiguration(ccfg1);

        cfg.setConsistentId(gridName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRestarts() throws Exception {
        startGrids(GRID_CNT);

        ignite(0).active(true);

        awaitPartitionMapExchange();

        try (IgniteDataStreamer<Object, Object> ds = ignite(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < ENTRIES_COUNT; i++)
                ds.addData(i, i);
        }

        stopAllGrids();

        List<Integer> idxs = new ArrayList<>();

        for (int i = 0; i < GRID_CNT; i++)
            idxs.add(i);

        for (int r = 0; r < SF.applyLB(10, 3); r++) {
            Collections.shuffle(idxs);

            info("Will start in the following order: " + idxs);

            for (Integer idx : idxs)
                startGrid(idx);

            try {
                ignite(0).active(true);

                for (int g = 0; g < GRID_CNT; g++) {
                    Ignite ig = ignite(g);

                    for (int k = 0; k < ENTRIES_COUNT; k++)
                        assertEquals("Failed to read [g=" + g + ", part=" + ig.affinity(DEFAULT_CACHE_NAME).partition(k) +
                                ", nodes=" + ig.affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(k) + ']',
                            k, ig.cache(DEFAULT_CACHE_NAME).get(k));
                }
            }
            finally {
                stopAllGrids();
            }
        }
    }
}
