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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class IgnitePdsContinuousRestartTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static final int ENTRIES_COUNT = 10_000;

    /** */
    public static final String CACHE_NAME = "cache1";

    /** Checkpoint delay. */
    private volatile int checkpointDelay = -1;

    /** */
    private boolean cancel = false;

    /**
     * Default constructor.
     */
    public IgnitePdsContinuousRestartTest() {
    }

    /**
     * @param cancel Cancel.
     */
    public IgnitePdsContinuousRestartTest(boolean cancel) {
        this.cancel = cancel;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        MemoryConfiguration memCfg = new MemoryConfiguration();

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();

        memPlcCfg.setName("dfltMemPlc");
        memPlcCfg.setMaxSize(400 * 1024 * 1024);
        memPlcCfg.setInitialSize(400 * 1024 * 1024);

        memCfg.setMemoryPolicies(memPlcCfg);
        memCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        cfg.setMemoryConfiguration(memCfg);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setName(CACHE_NAME);
        ccfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg1.setAffinity(new RendezvousAffinityFunction(false, 128));
        ccfg1.setBackups(2);

        cfg.setCacheConfiguration(ccfg1);

        cfg.setPersistentStoreConfiguration(
            new PersistentStoreConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setCheckpointingFrequency(checkpointDelay)
        );

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
    public void testRebalancingDuringLoad_1000_500_1_1() throws Exception {
        checkRebalancingDuringLoad(1000, 500, 1, 1);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalancingDuringLoad_8000_500_1_1() throws Exception {
        checkRebalancingDuringLoad(8000, 500, 1, 1);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalancingDuringLoad_1000_20000_1_1() throws Exception {
        checkRebalancingDuringLoad(1000, 20000, 1, 1);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalancingDuringLoad_8000_8000_1_1() throws Exception {
        checkRebalancingDuringLoad(8000, 8000, 1, 1);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalancingDuringLoad_1000_500_8_1() throws Exception {
        checkRebalancingDuringLoad(1000, 500, 8, 1);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalancingDuringLoad_8000_500_8_1() throws Exception {
        checkRebalancingDuringLoad(8000, 500, 8, 1);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalancingDuringLoad_1000_20000_8_1() throws Exception {
        checkRebalancingDuringLoad(1000, 20000, 8, 1);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalancingDuringLoad_8000_8000_8_1() throws Exception {
        checkRebalancingDuringLoad(8000, 8000, 8, 1);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalancingDuringLoad_1000_500_8_16() throws Exception {
        checkRebalancingDuringLoad(1000, 500, 8, 16);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalancingDuringLoad_8000_500_8_16() throws Exception {
        checkRebalancingDuringLoad(8000, 500, 8, 16);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalancingDuringLoad_1000_20000_8_16() throws Exception {
        checkRebalancingDuringLoad(1000, 20000, 8, 16);
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalancingDuringLoad_8000_8000_8_16() throws Exception {
        checkRebalancingDuringLoad(8000, 8000, 8, 16);
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testRebalncingDuringLoad_10_10_1_1() throws Exception {
        checkRebalancingDuringLoad(10, 10, 1, 1);
    }

    /**
     *
     * @throws Exception if failed.
     */
    public void testRebalncingDuringLoad_10_500_8_16() throws Exception {
        checkRebalancingDuringLoad(10, 500, 8, 16);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TimeUnit.MINUTES.toMillis(3);
    }

    /**
     * @throws Exception if failed.
     */
    private void checkRebalancingDuringLoad(
        int restartDelay,
        int checkpointDelay,
        int threads,
        final int batch
    ) throws Exception {
        this.checkpointDelay = checkpointDelay;

        startGrids(GRID_CNT);

        final Ignite load = ignite(0);

        load.active(true);

        try (IgniteDataStreamer<Object, Object> s = load.dataStreamer(CACHE_NAME)) {
            s.allowOverwrite(true);

            for (int i = 0; i < ENTRIES_COUNT; i++)
                s.addData(i, i);
        }

        final AtomicBoolean done = new AtomicBoolean(false);

        IgniteInternalFuture<?> busyFut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                IgniteCache<Object, Object> cache = load.cache(CACHE_NAME);
                Random rnd = ThreadLocalRandom.current();

                while (!done.get()) {
                    Map<Integer, Integer> map = new TreeMap<>();

                    for (int i = 0; i < batch; i++)
                        map.put(rnd.nextInt(ENTRIES_COUNT), rnd.nextInt());

                    cache.putAll(map);
                }

                return null;
            }
        }, threads, "updater");

        long end = System.currentTimeMillis() + 90_000;

        Random rnd = ThreadLocalRandom.current();

        while (System.currentTimeMillis() < end) {
            int idx = rnd.nextInt(GRID_CNT - 1) + 1;

            stopGrid(idx, cancel);

            U.sleep(restartDelay);

            startGrid(idx);

            U.sleep(restartDelay);
        }

        done.set(true);

        busyFut.get();
    }
}
