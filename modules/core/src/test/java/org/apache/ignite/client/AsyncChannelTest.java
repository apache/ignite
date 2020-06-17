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

package org.apache.ignite.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.thin.AbstractThinClientTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Async channel tests.
 */
public class AsyncChannelTest extends AbstractThinClientTest {
    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** Threads count. */
    private static final int THREADS_CNT = 25;

    /** Cache name. */
    private static final String CACHE_NAME = "tx_cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCacheConfiguration(
            new CacheConfiguration(CACHE_NAME).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODES_CNT);

        awaitPartitionMapExchange();
    }

    /**
     * Test that client channel works in async mode.
     */
    @Test
    public void testAsyncRequests() throws Exception {
        try (IgniteClient client = startClient(0)) {
            Ignite ignite = grid(0);

            IgniteCache<Integer, Integer> igniteCache = ignite.cache(CACHE_NAME);
            ClientCache<Integer, Integer> clientCache = client.cache(CACHE_NAME);

            clientCache.clear();

            Lock keyLock = igniteCache.lock(0);

            IgniteInternalFuture fut;

            keyLock.lock();

            try {
                CountDownLatch latch = new CountDownLatch(1);

                fut = GridTestUtils.runAsync(() -> {
                    latch.countDown();

                    // This request is blocked until we explicitly unlock key in another thread.
                    clientCache.put(0, 0);

                    clientCache.put(1, 1);

                    assertEquals(10, clientCache.size(CachePeekMode.PRIMARY));
                });

                latch.await();

                for (int i = 2; i < 10; i++) {
                    clientCache.put(i, i);

                    assertEquals((Integer)i, igniteCache.get(i));
                    assertEquals((Integer)i, clientCache.get(i));
                }

                // Parallel thread must be blocked on key 0.
                assertFalse(clientCache.containsKey(1));
            }
            finally {
                keyLock.unlock();
            }

            fut.get();

            assertTrue(clientCache.containsKey(1));
        }
    }

    /**
     * Test multiple concurrent async requests.
     */
    @Test
    public void testConcurrentRequests() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> clientCache = client.cache(CACHE_NAME);

            clientCache.clear();

            AtomicInteger keyCnt = new AtomicInteger();

            CyclicBarrier barrier = new CyclicBarrier(THREADS_CNT);

            GridTestUtils.runMultiThreaded(() -> {
                try {
                    barrier.await();
                }
                catch (Exception e) {
                    fail();
                }

                for (int i = 0; i < 100; i++) {
                    int key = keyCnt.incrementAndGet();

                    clientCache.put(key, key);

                    assertEquals(key, (long)clientCache.get(key));
                }

            }, THREADS_CNT, "thin-client-thread");
        }
    }

    /**
     * Test multiple concurrent async queries.
     */
    @Test
    public void testConcurrentQueries() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> clientCache = client.cache(CACHE_NAME);

            clientCache.clear();

            for (int i = 0; i < 10; i++)
                clientCache.put(i, i);

            CyclicBarrier barrier = new CyclicBarrier(THREADS_CNT);

            GridTestUtils.runMultiThreaded(() -> {
                try {
                    barrier.await();
                }
                catch (Exception e) {
                    fail();
                }

                for (int i = 0; i < 10; i++) {
                    Query<Cache.Entry<Integer, String>> qry = new ScanQuery<Integer, String>().setPageSize(1);

                    try (QueryCursor<Cache.Entry<Integer, String>> cur = clientCache.query(qry)) {
                        int cacheSize = clientCache.size(CachePeekMode.PRIMARY);
                        int curSize = cur.getAll().size();

                        assertEquals(cacheSize, curSize);
                    }
                }
            }, THREADS_CNT, "thin-client-thread");
        }
    }
}
