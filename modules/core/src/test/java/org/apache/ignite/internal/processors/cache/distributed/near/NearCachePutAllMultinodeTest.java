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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreaded;

/**
 *
 */
public class NearCachePutAllMultinodeTest extends GridCommonAbstractTest {
    /** Number of grids to start. */
    private static final int GRID_CNT = 3;

    /** Number of transactions. */
    private static final int TX_CNT = 10_000;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30_000;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected final IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        if (!c.isClientMode()) {
            CacheConfiguration cc = defaultCacheConfiguration();

            cc.setCacheMode(PARTITIONED);
            cc.setWriteSynchronizationMode(FULL_SYNC);
            cc.setAtomicityMode(TRANSACTIONAL);
            cc.setBackups(1);

            // Set store to disable one-phase commit.
            cc.setCacheStoreFactory(new TestFactory());

            c.setCacheConfiguration(cc);
        }

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(GRID_CNT - 2);

        Ignite grid = startClientGrid(GRID_CNT - 2);

        grid.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration());

        grid = startClientGrid(GRID_CNT - 1);

        grid.cache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11877")
    @Test
    public void testMultithreadedPutAll() throws Exception {
        final AtomicInteger idx = new AtomicInteger();

        runMultiThreaded(new Callable<Object>() {
            private final Random rnd = new Random();

            @Override public Object call() throws Exception {
                int threadIdx = idx.getAndIncrement();

                int node = threadIdx % 2 + (GRID_CNT - 2);

                IgniteCache<Object, Object> cache = jcache(node);

                for (int i = 0; i < TX_CNT; i++) {
                    Map<Integer, String> map = new TreeMap<>();

                    for (int j = 0; j < 3; j++)
                        map.put(rnd.nextInt(10), "value");

                    cache.putAll(map);

                    if (i % 100 == 0)
                        log.info("Iteration: " + i + " " + node);
                }

                return null;
            }
        }, 8, "putAll");
    }

    /**
     *
     */
    static class TestFactory implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new CacheStoreAdapter() {
                @Override public Object load(Object key) {
                    return null;
                }

                @Override public void write(Cache.Entry entry) {
                    // No-op.
                }

                @Override public void delete(Object key) {
                    // No-op.
                }
            };
        }
    }
}
