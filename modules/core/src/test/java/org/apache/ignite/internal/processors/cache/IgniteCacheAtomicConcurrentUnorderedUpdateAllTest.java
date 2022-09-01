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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/** Test concurrent putAll/removeAll operations with unordered set of keys on atomic caches. */
@RunWith(Parameterized.class)
public class IgniteCacheAtomicConcurrentUnorderedUpdateAllTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 3;

    /** */
    private static final int THREADS_CNT = 20;

    /** */
    private static final String CACHE_NAME = "test-cache";

    /** */
    private static final int CACHE_SIZE = 1_000;

    /** Parameters. */
    @Parameterized.Parameters(name = "cacheMode={0}, writeThrough={1}, near={2}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[] {CacheMode.PARTITIONED, Boolean.FALSE, Boolean.FALSE},
            new Object[] {CacheMode.PARTITIONED, Boolean.TRUE, Boolean.FALSE},
            new Object[] {CacheMode.PARTITIONED, Boolean.FALSE, Boolean.TRUE},
            new Object[] {CacheMode.REPLICATED, Boolean.FALSE, Boolean.FALSE},
            new Object[] {CacheMode.REPLICATED, Boolean.TRUE, Boolean.FALSE}
        );
    }

    /** Cache mode. */
    @Parameterized.Parameter()
    public CacheMode cacheMode;

    /** Write through. */
    @Parameterized.Parameter(1)
    public Boolean writeThrough;

    /** Near cache. */
    @Parameterized.Parameter(2)
    public Boolean near;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentUpdateAll() throws Exception {
        Ignite ignite = startGridsMultiThreaded(NODES_CNT);

        Factory<CacheStore<Object, Object>> cacheStoreFactory = writeThrough ?
                new MapCacheStoreStrategy.MapStoreFactory() : null;

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>(CACHE_NAME)
            .setWriteThrough(writeThrough).setCacheStoreFactory(cacheStoreFactory)
            .setNearConfiguration(near ? new NearCacheConfiguration<>() : null)
            .setCacheMode(cacheMode).setAtomicityMode(ATOMIC).setBackups(1));

        CyclicBarrier barrier = new CyclicBarrier(THREADS_CNT);

        AtomicInteger threadCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(() -> {
            int threadIdx = threadCnt.incrementAndGet();

            IgniteCache<Object, Object> cache0 = grid(ThreadLocalRandom.current().nextInt(NODES_CNT)).cache(CACHE_NAME);

            Map<Object, Object> map = new LinkedHashMap<>();

            if (threadIdx % 2 == 0) {
                for (int i = 0; i < CACHE_SIZE; i++)
                    map.put(i, i);
            }
            else {
                for (int i = CACHE_SIZE - 1; i >= 0; i--)
                    map.put(i, i);
            }

            for (int i = 0; i < 20; i++) {
                try {
                    barrier.await();
                }
                catch (Exception e) {
                    fail(e.getMessage());
                }

                cache0.putAll(map);

                cache0.invokeAll(map.keySet(), (k, v) -> v);

                cache0.removeAll(map.keySet());

                log.info("Thread " + threadIdx + " iteration " + i + " finished");
            }
        }, THREADS_CNT, "update-all-runner");

        assertEquals(0, cache.size());
    }
}
