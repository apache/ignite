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

package org.apache.ignite.internal.processors.datastreamer;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;

/**
 * Data streamer performance test.
 */
public class IgniteDataStreamerPerformanceTest extends GridCommonAbstractTest {
    /** */
    private static final int GRIDS = 3;

    /** */
    private static final int BACKUPS = 2;

    /** */
    private static final int DATA_REGION_SIZE_MB = 512;

    /** */
    private static final int ENTRIES_TO_LOAD_PER_ROUND = 20_000;

    /** */
    private static final int ROUNDS = 5;

    /** */
    private static final int HEATING = 3;

    /** */
    private final String[] vals = new String[1024];

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 600_000_000;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setWalMode(WALMode.NONE)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(DATA_REGION_SIZE_MB * 1024L * 1024L)
                .setPersistenceEnabled(true))
            .setCheckpointFrequency(3000)
            .setPageSize(DFLT_PAGE_SIZE));

        CacheConfiguration<?, ?> cc = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        cc.setCacheMode(PARTITIONED);
        cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cc.setBackups(BACKUPS);
        cc.setWriteSynchronizationMode(PRIMARY_SYNC);

        cfg.setCacheConfiguration(cc);

        return cfg;
    }

    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * Test loading from client and default receiver.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientDefault() throws Exception {
       doTest(true, null);
    }

    /**
     * Test loading from server node and default receiver.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testServerDefault() throws Exception {
        doTest(true, null);
    }

    /**
     * Test loading from client and batched sorted receiver.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientBatchedSorted() throws Exception {
        doTest(true, DataStreamerCacheUpdaters.batchedSorted());
    }

    /**
     * Test loading from client and batched receiver.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientBatched() throws Exception {
        doTest(true, DataStreamerCacheUpdaters.batched());
    }

    /**
     * Test loading from client and individual receiver.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientIndividual() throws Exception {
        doTest(true, DataStreamerCacheUpdaters.individual());
    }

    /**
     * Test loading from server node and individual receiver.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testServerIndividual() throws Exception {
        doTest(false, DataStreamerCacheUpdaters.individual());
    }

    /** */
    private void doTest(boolean client, @Nullable StreamReceiver<Integer, Object> receiver) throws Exception {
        long total = 0;

        try {
            startGridsMultiThreaded(GRIDS);

            try (Ignite ldr = client ? startClientGrid(GRIDS) : startGrid(GRIDS)) {
                for (int r = 0; r < ROUNDS + HEATING; ++r) {
                    assert grid(0).cache(DEFAULT_CACHE_NAME).size() == 0;

                    long start;

                    try (IgniteDataStreamer<Integer, Object> streamer = ldr.dataStreamer(DEFAULT_CACHE_NAME)) {
                        if (receiver != null)
                            streamer.receiver(receiver);

                        randomizeLoadData();

                        start = U.currentTimeMillis();

                        if (r < HEATING)
                            info("Heatings left: " + (HEATING - r));
                        else
                            info("Rounds left: " + (ROUNDS - r + HEATING));

                        for (int e = 1; e <= ENTRIES_TO_LOAD_PER_ROUND; ++e)
                            streamer.addData(e, vals[e % vals.length]);
                    }

                    forceCheckpoint(G.allGrids());

                    long end = U.currentTimeMillis();

                    assert grid(0).cache(DEFAULT_CACHE_NAME).size() == ENTRIES_TO_LOAD_PER_ROUND;

                    if (r >= HEATING) {
                        long loaded = (ENTRIES_TO_LOAD_PER_ROUND * 1000) / (end - start);

                        total += loaded;

                        info("Adds/sec: " + loaded);
                    }

                    grid(0).cache(DEFAULT_CACHE_NAME).clear();
                }

                info("Average add rate: " + (total / ROUNDS) + " / sec.");
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private void randomizeLoadData(){
        for (int i = 0; i < vals.length; i++) {
            int valLen = ThreadLocalRandom.current().nextInt(4, 7 * 1024);

            StringBuilder sb = new StringBuilder();

            for (int j = 0; j < valLen; j++)
                sb.append('a' + ThreadLocalRandom.current().nextInt(20));

            vals[i] = sb.toString();
        }
    }
}
