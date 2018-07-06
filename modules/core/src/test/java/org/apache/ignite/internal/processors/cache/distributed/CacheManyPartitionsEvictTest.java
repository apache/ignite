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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC;

/**
 *
 */
public class CacheManyPartitionsEvictTest extends GridCommonAbstractTest {
    /** */
    public static final int PARTS = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSystemThreadPoolSize(256);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setBackups(1);
        ccfg.setGroupName("CacheManyPartitionsEvictTest");

        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS));

        cfg.setCacheConfiguration(ccfg, new CacheConfiguration(ccfg).setName(DEFAULT_CACHE_NAME + "_2"));

        cfg.setActiveOnStart(false);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().
                                setCheckpointPageBufferSize(5L * 1024 * 1024).
                                setPersistenceEnabled(true).
                                setInitialSize(2048L * 1024 * 1024).
                                setMaxSize(2048L * 1024 * 1024)).
                        setWriteThrottlingEnabled(true)
                .setWalMode(WALMode.NONE)
                .setCheckpointFrequency(1_000)
                ;

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    public void testEvict() throws Exception {
        System.setProperty(IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC, "true");

        try {
            IgniteEx grid0 = startGrid(0);

            startGrid(1);

            grid(0).cluster().active(true);

            final int cnt = 209715;

            IgniteCache<Object, Object> cache = grid0.getOrCreateCache(DEFAULT_CACHE_NAME + "_2");

            for (int i = 0; i < PARTS; i++) {
                cache.put(i, i);
            }


            try(IgniteDataStreamer<Object, Object> dataStreamer = grid0.dataStreamer(DEFAULT_CACHE_NAME)) {
                for (int i = 0; i < cnt; i++) {
                    dataStreamer.addData(i, new byte[10 * 1024]);

                    if (i % 1000 == 0) {
                        log.info("Progress: " + i + " of " + cnt);

                        dataStreamer.flush();
                    }
                }
            }

            stopGrid(1);

            try(IgniteDataStreamer<Object, Object> dataStreamer = grid0.dataStreamer(DEFAULT_CACHE_NAME)) {
                for (int i = cnt; i < cnt + PARTS; i++) {
                    dataStreamer.addData(i, new byte[1024 * 1024]);

                    if (i % 10 == 0)
                        log.info("Progress: " + i + " of " + cnt);
                }
            }

            startGrid(1);

//            grid0.cluster().setBaselineTopology(grid0.cluster().topologyVersion());

            waitForRebalancing();

            awaitPartitionMapExchange();

            Thread.sleep(20_000);
        }
        finally {
            stopAllGrids();
        }
    }


    @Override
    protected long getTestTimeout() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();

        stopAllGrids();
    }
}