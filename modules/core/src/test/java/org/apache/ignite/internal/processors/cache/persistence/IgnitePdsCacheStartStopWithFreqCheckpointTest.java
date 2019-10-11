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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class IgnitePdsCacheStartStopWithFreqCheckpointTest extends GridCommonAbstractTest {
    /** Caches. */
    private static final int CACHES = SF.applyLB(10, 3);

    /** Cache name. */
    private static final String CACHE_NAME = "test";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(1000)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(512 * 1024 * 1024)
                    .setPersistenceEnabled(true)
            );

        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration[] ccfgs = new CacheConfiguration[CACHES];

        for (int i = 0; i < ccfgs.length; i++)
            ccfgs[i] = cacheConfiguration(i);

        cfg.setCacheConfiguration(ccfgs);

        return cfg;
    }

    /** */
    private CacheConfiguration cacheConfiguration(int cacheIdx) {
        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME + cacheIdx)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.REPLICATED)
            .setBackups(0);

        if (MvccFeatureChecker.forcedMvcc())
            ccfg.setRebalanceDelay(Long.MAX_VALUE);
        else
            ccfg.setRebalanceMode(CacheRebalanceMode.NONE);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test checkpoint deadlock during caches start/stop process and frequent checkpoints is set.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCheckpointDeadlock() throws Exception {
        IgniteEx crd = startGrid(0);

        crd.cluster().active(true);

        for (int cacheId = 0; cacheId < CACHES; cacheId++) {
            IgniteCache<Object, Object> cache = crd.getOrCreateCache(CACHE_NAME + cacheId);

            for (int key = 0; key < 4096; key++)
                cache.put(key, key);
        }

        forceCheckpoint();

        final AtomicBoolean stopFlag = new AtomicBoolean();

        IgniteInternalFuture<?> cacheStartStopFut = GridTestUtils.runAsync(() -> {
            while (!stopFlag.get()) {
                List<String> cacheNames = new ArrayList<>();
                for (int i = 0; i < CACHES / 2; i++)
                    cacheNames.add(CACHE_NAME + i);

                try {
                    // Stop cache without destroy.
                    crd.context().cache().dynamicDestroyCaches(cacheNames, false,false).get();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to destroy cache", e);
                }

                List<CacheConfiguration> cachesToStart = new ArrayList<>();
                for (int i = 0; i < CACHES / 2; i++)
                    cachesToStart.add(cacheConfiguration(i));

                crd.getOrCreateCaches(cachesToStart);
            }
        });

        U.sleep(SF.applyLB(60_000, 10_000));

        log.info("Stopping caches start/stop process.");

        stopFlag.set(true);

        try {
            cacheStartStopFut.get(30, TimeUnit.SECONDS);
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            U.dumpThreads(log);

            log.warning("Caches start/stop future hangs. Interrupting checkpointer...");

            interruptCheckpointer(crd);

            // Should succeed.
            cacheStartStopFut.get();

            Assert.assertTrue("Checkpoint and exchange is probably in deadlock (see thread dump above for details).", false);
        }
    }

    /**
     * Interrupts checkpoint thread for given node.
     *
     * @param node Node.
     */
    private void interruptCheckpointer(IgniteEx node) {
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager) node.context().cache().context().database();

        dbMgr.checkpointerThread().interrupt();
    }
}
