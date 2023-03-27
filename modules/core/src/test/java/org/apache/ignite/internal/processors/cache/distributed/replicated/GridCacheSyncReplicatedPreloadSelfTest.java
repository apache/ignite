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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;

/**
 * Multithreaded tests for replicated cache preloader.
 */
public class GridCacheSyncReplicatedPreloadSelfTest extends GridCommonAbstractTest {
    /**
     * Constructs test.
     */
    public GridCacheSyncReplicatedPreloadSelfTest() {
        super(false /* don't start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setNearConfiguration(null);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        // This property is essential for this test.
        cacheCfg.setRebalanceMode(SYNC);

        cacheCfg.setRebalanceBatchSize(10000);

        cfg.setCacheConfiguration(cacheCfg);
        cfg.setDeploymentMode(CONTINUOUS);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    @Test
    public void testNodeRestart() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-10082", MvccFeatureChecker.forcedMvcc());

        int keyCnt = 1000;
        int retries = 20;

        Ignite g0 = startGrid(0);
        Ignite g1 = startGrid(1);

        for (int i = 0; i < keyCnt; i++)
            g0.cache(DEFAULT_CACHE_NAME).put(i, i);

        assertEquals(keyCnt, ((IgniteKernal)g0).internalCache(DEFAULT_CACHE_NAME).size());
        assertEquals(keyCnt, ((IgniteKernal)g1).internalCache(DEFAULT_CACHE_NAME).size());

        for (int n = 0; n < retries; n++) {
            info("Starting additional grid node...");

            Ignite g2 = startGrid(2);

            assertEquals(keyCnt, ((IgniteKernal)g2).internalCache(DEFAULT_CACHE_NAME).size());

            info("Stopping additional grid node...");

            stopGrid(2);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    @Test
    public void testNodeRestartMultithreaded() throws Exception {
        final int keyCnt = 1000;
        final int retries = 50;
        int threadCnt = 5;

        Ignite g0 = startGrid(0);
        Ignite g1 = startGrid(1);

        for (int i = 0; i < keyCnt; i++)
            g0.cache(DEFAULT_CACHE_NAME).put(i, i);

        assertEquals(keyCnt, ((IgniteKernal)g0).internalCache(DEFAULT_CACHE_NAME).size());
        assertEquals(keyCnt, ((IgniteKernal)g1).internalCache(DEFAULT_CACHE_NAME).size());

        final AtomicInteger cnt = new AtomicInteger();

        multithreaded(
            new Callable() {
                @Nullable @Override public Object call() throws Exception {
                    while (true) {
                        int c = cnt.incrementAndGet();

                        if (c > retries)
                            break;

                        int idx = c + 1;

                        info("Starting additional grid node with index: " + idx);

                        startGrid(idx);

                        info("Stopping additional grid node with index: " + idx);

                        stopGrid(idx);
                    }

                    return null;
                }
            },
            threadCnt);
    }
}
