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

package org.apache.ignite.internal.processors.cache.datastructures;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SystemDataRegionConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Tests behavior of volatile data structures and regular caches
 * when {@link IgniteOutOfMemoryException} is thrown.
 */
public class OutOfMemoryVolatileRegionTest extends GridCommonAbstractTest {
    /** Minimal region size. */
    private static final long DATA_REGION_SIZE = 15L * 1024 * 1024;

    /** */
    private static final int ATTEMPTS_NUM = 3;

    /** Failure handler triggered. */
    private static volatile boolean failure;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setPageSize(4096)
            .setSystemDataRegionConfiguration(
                    new SystemDataRegionConfiguration()
                            .setInitialSize(DATA_REGION_SIZE)
                            .setMaxSize(DATA_REGION_SIZE)
            )
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMetricsEnabled(true)));

        cfg.setFailureHandler(new AbstractFailureHandler() {
            /** {@inheritDoc} */
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                failure = true;

                // Do not invalidate a node context.
                return false;
            }
        });

        cfg.setCacheConfiguration(cacheConfiguration(ATOMIC), cacheConfiguration(TRANSACTIONAL));

        return cfg;
    }

    /**
     * Creates a new cache configuration with the given cache atomicity mode.
     *
     * @param mode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CacheAtomicityMode mode) {
        return new CacheConfiguration(mode.name())
            .setAtomicityMode(mode)
            .setAffinity(new RendezvousAffinityFunction(false, 32));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        startGrid(0);
        startGrid(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadAndClearAtomicCache() throws Exception {
        loadAndClearCache(ATOMIC, ATTEMPTS_NUM);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadAndClearTransactionalCache() throws Exception {
        loadAndClearCache(TRANSACTIONAL, ATTEMPTS_NUM);
    }

    /**
     * Creates a new cache with the given atomicity node and tries to load & clear it in a loop.
     * It is assumed that {@link IgniteOutOfMemoryException} is thrown during loading the cache,
     * however {@link IgniteCache#clear()} should return the cache to the operable state.
     *
     * @param mode Cache atomicity mode.
     * @param attempts Number of attempts to load and clear the cache.
     */
    private void loadAndClearCache(CacheAtomicityMode mode, int attempts) {
        grid(0).cluster().state(ClusterState.ACTIVE);

        failure = false;

        IgniteCache<Object, Object> cache = grid(0).cache(mode.name());

        for (int i = 0; i < attempts; ++i) {
            for (int key = 0; key < 5_000; ++key)
                cache.put(key, new byte[40]);

            cache.clear();
        }

        assertFalse("Failure handler should not be notified", failure);

        try {
            for (int j = 0; j < 100000; j++) {
                grid(0).reentrantLock("l" + getClass().getName() + j,
                    j % 2 == 0, j % 3 == 0, true);
                grid(1).semaphore("s" + getClass().getName() + j,
                    1 + (j % 7), j % 3 == 0, true);
                grid(0).countDownLatch("c" + getClass().getName() + j,
                    1 + (j % 13), j % 2 == 0, true);
            }

            fail("OutOfMemoryException hasn't been thrown");
        }
        catch (Exception e) {
            if (!X.hasCause(e, IgniteOutOfMemoryException.class))
                fail("Unexpected exception" + e);

            log.info("Expected exception, n: " + e);
        }

        assertTrue("Failure handler wasn't notified", failure);

        for (int i = 0; i < attempts; ++i) {
            for (int key = 0; key < 5_000; ++key)
                cache.put(key, new byte[40]);

            cache.clear();
        }
    }
}
