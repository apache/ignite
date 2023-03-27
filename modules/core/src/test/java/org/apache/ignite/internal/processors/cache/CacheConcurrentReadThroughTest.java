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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.Test;

/**
 * Test was added to check fix for IGNITE-4465.
 */
public class CacheConcurrentReadThroughTest extends GridCommonAbstractTest {
    /** */
    private static final int SYS_THREADS = 16;

    /** */
    @Before
    public void beforeCacheConcurrentReadThroughTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (!cfg.isClientMode()) {
            cfg.setPublicThreadPoolSize(SYS_THREADS);
            cfg.setSystemThreadPoolSize(SYS_THREADS);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentReadThrough() throws Exception {
        startGrid(0);

        Ignite client = startClientGrid(1);

        assertTrue(client.configuration().isClientMode());

        for (int iter = 0; iter < 10; iter++) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            final String cacheName = "test-" + iter;

            ccfg.setName(cacheName);
            ccfg.setReadThrough(true);
            ccfg.setCacheStoreFactory(new TestStoreFactory());
            ccfg.setStatisticsEnabled(true);

            client.createCache(ccfg);

            final Integer key = 1;

            TestCacheStore.loadCnt.set(0);

            Collection<IgniteFuture<?>> futs = new ArrayList<>();

            for (int i = 0; i < SYS_THREADS * 3; i++) {
                futs.add(client.compute().runAsync(new IgniteRunnable() {
                    @IgniteInstanceResource
                    private transient Ignite ignite;

                    @Override public void run() {
                        assertFalse(ignite.configuration().isClientMode());

                        Object v = ignite.<Integer, Integer>cache(cacheName).get(key);

                        if (v == null)
                            throw new IgniteException("Failed to get value");
                    }
                }));
            }

            for (IgniteFuture<?> fut : futs)
                fut.get();

            log.info("Iteration [iter=" + iter + ']');

            assertTrue(GridTestUtils.waitForCondition(() -> {
                int loadCnt = TestCacheStore.loadCnt.get();

                long misses = ignite(1).cache(cacheName).metrics().getCacheMisses();

                log.info("Iteration [loadCnt=" + loadCnt + ", misses=" + misses + ']');

                return (loadCnt > 0 && loadCnt <= SYS_THREADS) && (misses > 0 && misses <= SYS_THREADS);
            }, 5000));

            client.destroyCache(cacheName);
        }
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<TestCacheStore> {
        /** {@inheritDoc} */
        @Override public TestCacheStore create() {
            return new TestCacheStore();
        }
    }

    /**
     *
     */
    private static class TestCacheStore extends CacheStoreAdapter<Integer, Integer> {
        /** */
        private static final AtomicInteger loadCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            loadCnt.incrementAndGet();

            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            return key;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    }
}
