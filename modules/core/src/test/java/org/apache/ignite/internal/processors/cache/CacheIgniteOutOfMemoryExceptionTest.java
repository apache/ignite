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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.configuration.DataPageEvictionMode.DISABLED;

/**
 * Tests behavior of IgniteCache when {@link IgniteOutOfMemoryException} is thrown.
 */
public class CacheIgniteOutOfMemoryExceptionTest extends GridCommonAbstractTest {
    /** */
    private static final long DATA_REGION_SIZE = 10L * 1024 * 1024;

    /** */
    private static final int ATTEMPTS_NUM = 3;

    /** Node failure occurs. */
    private static final AtomicBoolean failure = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(DATA_REGION_SIZE)
                    .setPageEvictionMode(DISABLED)
                    .setPersistenceEnabled(false)));

        cfg.setFailureHandler(new AbstractFailureHandler() {
            /** {@inheritDoc} */
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                failure.set(true);

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
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
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
        IgniteCache<Object, Object> cache = grid(0).cache(mode.name());

        for (int i = 0; i < attempts; ++i) {
            try {
                for (int key = 0; key < 500_000; ++key)
                    cache.put(key, new byte[4000]);

                fail("OutOfMemoryException hasn't been thrown");
            }
            catch (Exception e) {
                assertTrue(
                    "Exception has been thrown, but the exception type is unexpected [exc=" + e + ']',
                    X.hasCause(e, IgniteOutOfMemoryException.class));

                assertTrue("Failure handler should be called due to IOOM.", failure.get());
            }

            // Let's check that the cache can be cleared without any errors.
            failure.set(false);

            try {
                cache.clear();
            }
            catch (Exception e) {
                fail("Clearing the cache should not trigger any exception [exc=" + e + ']');
            }

            assertFalse("Failure handler should not be called during clearing the cache.", failure.get());
        }
    }
}
