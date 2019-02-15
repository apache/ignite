/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    private static boolean client;

    /** */
    @Before
    public void beforeCacheConcurrentReadThroughTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(client);

        if (!client) {
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

        client = true;

        Ignite client = startGrid(1);

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

            int loadCnt = TestCacheStore.loadCnt.get();

            long misses = ignite(1).cache(cacheName).metrics().getCacheMisses();

            log.info("Iteration [iter=" + iter + ", loadCnt=" + loadCnt + ", misses=" + misses + ']');

            assertTrue("Unexpected loadCnt: " + loadCnt, loadCnt > 0 && loadCnt <= SYS_THREADS);
            assertTrue("Unexpected misses: " + misses, misses > 0 && misses <= SYS_THREADS);

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
