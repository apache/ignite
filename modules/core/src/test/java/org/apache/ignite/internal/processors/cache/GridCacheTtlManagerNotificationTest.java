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
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 *
 */
public class GridCacheTtlManagerNotificationTest extends GridCommonAbstractTest {
    /** Count of caches in multi caches test. */
    private static final int CACHES_CNT = 10;

    /** Prefix for cache name fir multi caches test. */
    private static final String CACHE_PREFIX = "cache-";

    /** Test cache mode. */
    protected CacheMode cacheMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration[] ccfgs = new CacheConfiguration[CACHES_CNT + 1];

        ccfgs[0] = createCacheConfiguration(DEFAULT_CACHE_NAME);

        for (int i = 0; i < CACHES_CNT; i++)
            ccfgs[i + 1] = createCacheConfiguration(CACHE_PREFIX + i);

        cfg.setCacheConfiguration(ccfgs);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration createCacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setEagerTtl(true);
        ccfg.setName(name);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testThatNotificationWorkAsExpected() throws Exception {
        try (final Ignite g = startGrid(0)) {
            final BlockingArrayQueue<Event> queue = new BlockingArrayQueue<>();

            g.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    queue.add(evt);

                    return true;
                }
            }, EventType.EVT_CACHE_OBJECT_EXPIRED);

            final String key = "key";

            IgniteCache<Object, Object> cache = g.cache(DEFAULT_CACHE_NAME);

            ExpiryPolicy plc1 = new CreatedExpiryPolicy(new Duration(MILLISECONDS, 100_000));

            cache.withExpiryPolicy(plc1).put(key + 1, 1);

            Thread.sleep(1_000); // Cleaner should see entry.

            ExpiryPolicy plc2 = new CreatedExpiryPolicy(new Duration(MILLISECONDS, 1000));

            cache.withExpiryPolicy(plc2).put(key + 2, 1);

            assertNotNull(queue.poll(5, SECONDS)); // We should receive event about second entry expiration.
        }
    }

    /**
     * Adds in several threads value to cache with different expiration policy.
     * Waits for expiration of keys with small expiration duration.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testThatNotificationWorkAsExpectedInMultithreadedMode() throws Exception {
        final CyclicBarrier barrier = new CyclicBarrier(21);
        final AtomicInteger keysRangeGen = new AtomicInteger();
        final AtomicInteger evtCnt = new AtomicInteger();
        final int cnt = 1_000;

        try (final Ignite g = startGrid(0)) {
            final IgniteCache<Object, Object> cache = g.cache(DEFAULT_CACHE_NAME);

            g.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    evtCnt.incrementAndGet();

                    return true;
                }
            }, EventType.EVT_CACHE_OBJECT_EXPIRED);

            int smallDuration = 2000;

            int threadCnt = 10;

            GridTestUtils.runMultiThreadedAsync(
                new CacheFiller(cache, 100_000, barrier, keysRangeGen, cnt),
                threadCnt, "");

            GridTestUtils.runMultiThreadedAsync(
                new CacheFiller(cache, smallDuration, barrier, keysRangeGen, cnt),
                threadCnt, "");

            barrier.await();

            Thread.sleep(1_000); // Cleaner should see at least one entry.

            barrier.await();

            assertEquals(2 * threadCnt * cnt, cache.size());

            Thread.sleep(2 * smallDuration);

            assertEquals(threadCnt * cnt, cache.size());
            assertEquals(threadCnt * cnt, evtCnt.get());
        }
    }

    /**
     * Adds in several threads value to several caches with different expiration policy.
     * Waits for expiration of keys with small expiration duration.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testThatNotificationWorkAsExpectedManyCaches() throws Exception {
        final int smallDuration = 4_000;

        final int cnt = 1_000;
        final int cacheCnt = CACHES_CNT;
        final int threadCnt = 2;

        final CyclicBarrier barrier = new CyclicBarrier(2 * threadCnt * cacheCnt + 1);
        final AtomicInteger keysRangeGen = new AtomicInteger();
        final AtomicInteger evtCnt = new AtomicInteger(0);
        final List<IgniteCache<Object, Object>> caches = new ArrayList<>(cacheCnt);

        try (final Ignite g = startGrid(0)) {
            for (int i = 0; i < cacheCnt; i++) {
                IgniteCache<Object, Object> cache = g.cache("cache-" + i);

                caches.add(cache);
            }

            g.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    evtCnt.incrementAndGet();

                    return true;
                }
            }, EventType.EVT_CACHE_OBJECT_EXPIRED);

            for (int i = 0; i < cacheCnt; i++) {
                GridTestUtils.runMultiThreadedAsync(
                    new CacheFiller(caches.get(i), 100_000, barrier, keysRangeGen, cnt),
                    threadCnt,
                    "put-large-duration");

                GridTestUtils.runMultiThreadedAsync(
                    new CacheFiller(caches.get(i), smallDuration, barrier, keysRangeGen, cnt),
                    threadCnt,
                    "put-small-duration");
            }

            barrier.await();

            Thread.sleep(1_000);

            barrier.await();

            for (int i = 0; i < cacheCnt; i++)
                assertEquals("Unexpected size of " + CACHE_PREFIX + i, 2 * threadCnt * cnt, caches.get(i).size());

            Thread.sleep(2 * smallDuration);

            for (int i = 0; i < cacheCnt; i++)
                assertEquals("Unexpected size of " + CACHE_PREFIX + i, threadCnt * cnt, caches.get(i).size());

            assertEquals("Unexpected count of expired entries", threadCnt * CACHES_CNT * cnt, evtCnt.get());
        }
    }

    /** */
    private static class CacheFiller implements Runnable {
        /** Barrier. */
        private final CyclicBarrier barrier;

        /** Keys range generator. */
        private final AtomicInteger keysRangeGenerator;

        /** Count. */
        private final int cnt;

        /** Cache. */
        private final IgniteCache<Object, Object> cache;

        /** Expiration duration. */
        private final int expirationDuration;

        /**
         * @param cache Cache.
         * @param expirationDuration Expiration duration.
         * @param barrier Barrier.
         * @param keysRangeGenerator Keys.
         * @param cnt Count.
         */
        CacheFiller(IgniteCache<Object, Object> cache, int expirationDuration, CyclicBarrier barrier,
            AtomicInteger keysRangeGenerator, int cnt) {
            this.expirationDuration = expirationDuration;
            this.barrier = barrier;
            this.keysRangeGenerator = keysRangeGenerator;
            this.cnt = cnt;
            this.cache = cache;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                barrier.await();

                ExpiryPolicy plc1 = new CreatedExpiryPolicy(new Duration(MILLISECONDS, expirationDuration));

                int keyStart = keysRangeGenerator.getAndIncrement() * cnt;

                for (int i = keyStart; i < keyStart + cnt; i++)
                    cache.withExpiryPolicy(plc1).put("key" + i, 1);

                barrier.await();
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }
    }
}
