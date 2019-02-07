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

import java.util.concurrent.Callable;
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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgnitePdsPageEvictionDuringPartitionClearTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 128))
            .setRebalanceMode(CacheRebalanceMode.SYNC)
            .setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        // Intentionally set small page cache size.
        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(70L * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 20 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(GridCacheDatabaseSharedManager.IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC, "true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.clearProperty(GridCacheDatabaseSharedManager.IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPageEvictionOnNodeStart() throws Exception {
        for (int r = 0; r < 3; r++) {
            cleanPersistenceDir();

            startGrids(2);

            try {
                Ignite ig = ignite(0);

                ig.active(true);

                IgniteDataStreamer<Object, Object> streamer = ig.dataStreamer(CACHE_NAME);

                for (int i = 0; i < 300_000; i++) {
                    streamer.addData(i, new TestValue(i));

                    if (i > 0 && i % 10_000 == 0)
                        info("Done: " + i);
                }

                streamer.flush();

                IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        IgniteEx ig = startGrid(2);

                        info(">>>>>>>>>>>");
                        info(">>>>>>>>>>>");
                        info(">>>>>>>>>>>");

                        return ig;
                    }
                });

                for (int i = 500_000; i < 1_000_000; i++) {
                    streamer.addData(i, new TestValue(i));

                    if (i > 0 && i % 10_000 == 0) {
                        info("Done: " + i);

                        U.sleep(1000);
                    }
                }

                streamer.close();

                fut.get();
            }
            finally {
                stopAllGrids();

                cleanPersistenceDir();
            }
        }
    }

    /**
     *
     */
    private static class TestValue {
        /** */
        private int id;

        /** */
        private byte[] payload = new byte[512];

        /**
         * @param id ID.
         */
        private TestValue(int id) {
            this.id = id;
        }
    }
}
