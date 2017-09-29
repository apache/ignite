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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.checkpoint.noop.NoopCheckpointSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class IgnitePdsWholeClusterRestartTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 5;

    /** */
    private static final int ENTRIES_COUNT = 1_000;

    /** */
    public static final String CACHE_NAME = "cache1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        MemoryConfiguration dbCfg = new MemoryConfiguration();

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();

        memPlcCfg.setName("dfltMemPlc");
        memPlcCfg.setInitialSize(100 * 1024 * 1024);
        memPlcCfg.setMaxSize(100 * 1024 * 1024);

        dbCfg.setMemoryPolicies(memPlcCfg);
        dbCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        cfg.setMemoryConfiguration(dbCfg);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setName(CACHE_NAME);
        ccfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg1.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg1.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg1.setBackups(2);

        cfg.setActiveOnStart(false);

        // To avoid hostname lookup on start.
        cfg.setCheckpointSpi(new NoopCheckpointSpi());

        cfg.setCacheConfiguration(ccfg1);

        cfg.setPersistentStoreConfiguration(
            new PersistentStoreConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
        );

        cfg.setConsistentId(gridName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /**
     * @throws Exception if failed.
     */
    public void testRestarts() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5741");

        startGrids(GRID_CNT);

        ignite(0).active(true);

        awaitPartitionMapExchange();

        try (IgniteDataStreamer<Object, Object> ds = ignite(0).dataStreamer(CACHE_NAME)) {
            for (int i = 0; i < ENTRIES_COUNT; i++)
                ds.addData(i, i);
        }

        stopAllGrids();

        List<Integer> idxs = new ArrayList<>();

        for (int i = 0; i < GRID_CNT; i++)
            idxs.add(i);

        for (int r = 0; r < 10; r++) {
            Collections.shuffle(idxs);

            info("Will start in the following order: " + idxs);

            for (Integer idx : idxs)
                startGrid(idx);

            try {
                ignite(0).active(true);

                for (int g = 0; g < GRID_CNT; g++) {
                    Ignite ig = ignite(g);

                    for (int k = 0; k < ENTRIES_COUNT; k++)
                        assertEquals("Failed to read [g=" + g + ", part=" + ig.affinity(CACHE_NAME).partition(k) +
                            ", nodes=" + ig.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(k) + ']',
                            k, ig.cache(CACHE_NAME).get(k));
                }
            }
            finally {
                stopAllGrids();
            }
        }
    }
}
