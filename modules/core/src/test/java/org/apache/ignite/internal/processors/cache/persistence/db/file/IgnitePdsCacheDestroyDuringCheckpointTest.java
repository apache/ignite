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

package org.apache.ignite.internal.processors.cache.persistence.db.file;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for cache creation/deletion with frequent checkpoints.
 */
public class IgnitePdsCacheDestroyDuringCheckpointTest extends GridCommonAbstractTest {
    /** */
    private static final String NAME_PREFIX = "CACHE-";

    /** */
    private static final int NUM_ITERATIONS = SF.applyLB(5, 3);

    /** */
    private static final int NUM_CACHES = SF.applyLB(10, 3);

    /** */
    private static final int NUM_ENTRIES_PER_CACHE = 200;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDataStorageConfiguration(createDbConfig());

        return cfg;
    }

    /**
     * @return DB config.
     */
    private DataStorageConfiguration createDbConfig() {
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.setCheckpointFrequency(300);

        storageCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
        );

        return storageCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If fail.
     */
    @Test
    public void testCacheCreatePutCheckpointDestroy() throws Exception {
        IgniteEx ig = startGrid(0);
        ig.active(true);

        for (int j = 0; j < NUM_ITERATIONS; j++) {
            Ignite client = startClientGrid(1);

            for (int i = 0; i < NUM_CACHES; i++) {
                IgniteCache<?, ?> cache = ig.cache(NAME_PREFIX + i);
                if (cache != null)
                    cache.destroy();
            }

            populateCache(client);
            checkCacheSizes(client);

            client.close();
        }
    }

    /** */
    private void populateCache(Ignite client) {
        for (int i = 0; i < NUM_CACHES; i++) {
            CacheConfiguration cfg = new CacheConfiguration();
            cfg.setName(NAME_PREFIX + i).setAtomicityMode(CacheAtomicityMode.ATOMIC)
                    .setBackups(1).setStatisticsEnabled(true).setManagementEnabled(true);
            client.getOrCreateCache(cfg);

            IgniteDataStreamer<Object, Object> streamer = client.dataStreamer(NAME_PREFIX + i);

            for (int j = 0; j < NUM_ENTRIES_PER_CACHE; j++) {
                String bo = i + "|" + j + "|WHATEVER";
                streamer.addData(j, bo);
            }

            streamer.close();
            log.info("Streamer closed");
        }
    }

    /** */
    private void checkCacheSizes(Ignite client) {
        for (int i = 0; i < NUM_CACHES; i++) {
            IgniteCache<Object, Object> cache = client.getOrCreateCache(NAME_PREFIX + i);

            int size = cache.size(CachePeekMode.ALL);

            if (NUM_ENTRIES_PER_CACHE != size) {
                for (Object o : cache) {
                    log.info("O " + o);
                }
                assertTrue(false);
            }
        }
    }
}
