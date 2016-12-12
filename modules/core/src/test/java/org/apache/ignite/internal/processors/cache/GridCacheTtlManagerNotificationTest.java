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

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.BlockingArrayQueue;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 *
 */
public class GridCacheTtlManagerNotificationTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Test cache mode. */
    protected CacheMode cacheMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(cacheMode);
        ccfg.setEagerTtl(true);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
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

            IgniteCache<Object, Object> cache = g.cache(null);

            ExpiryPolicy plc1 = new CreatedExpiryPolicy(new Duration(MILLISECONDS, 100_000));

            cache.withExpiryPolicy(plc1).put(key + 1, 1);

            Thread.sleep(1_000); // Cleaner should see entry.

            ExpiryPolicy plc2 = new CreatedExpiryPolicy(new Duration(MILLISECONDS, 1000));

            cache.withExpiryPolicy(plc2).put(key + 2, 1);

            assertNotNull(queue.poll(5, SECONDS)); // We should receive event about second entry expiration.
        }
    }

    /**
     * Add in several threads value to cache with different expiration policy.
     * Wait for expiration of keys with small expiration duration.
     */
    public void testThatNotificationWorkAsExpectedInMultithreadedMode() throws Exception {
        final CyclicBarrier barrier = new CyclicBarrier(21);
        final AtomicInteger keysRangeGen = new AtomicInteger();
        final AtomicInteger evtCnt = new AtomicInteger();
        final int cnt = 1_000;

        try (final Ignite g = startGrid(0)) {
            final IgniteCache<Object, Object> cache = g.cache(null);

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
                e.printStackTrace();
            }
        }
    }
}