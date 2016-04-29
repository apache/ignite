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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test to reproduce https://issues.apache.org/jira/browse/IGNITE-3073
 */
public class IgniteCacheStarvationOnRebalanceTest extends GridCacheAbstractSelfTest {
    /** Grid count. */
    private static final int GRID_CNT = 4;
    /** Timeout for put tasks. */
    private static final long PUT_TASK_TIMEOUT = 1 * 60 * 1000;
    /** Test timeout. */
    private static final long TEST_TIMEOUT = 5 * 60 * 1000;
    /** Use small system thread pool to reproduce the issue. */
    private static final int IGNITE_THREAD_POOL_SIZE = 5;
    /** Use many threads to put values into cache. */
    private static final int PUT_THREAD_POOL_SIZE = IGNITE_THREAD_POOL_SIZE * 10;

    /**
     * {@inheritDoc}
     */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Use small system thread pool to reproduce the issue.
        cfg.setSystemThreadPoolSize(IGNITE_THREAD_POOL_SIZE);
        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected Class<?>[] indexedTypes() {
        return new Class<?>[] {Integer.class, CacheValue.class};
    }

    /**
     *  {@inheritDoc}
     */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadSystemWithPutAndStartRebalancing() throws Exception {
        final AtomicInteger cnt = new AtomicInteger(0);
        final IgniteCache<Integer, CacheValue> cache = grid(0).cache(null);
        final long endTaskTime = System.currentTimeMillis() + PUT_TASK_TIMEOUT;

        Collection<Future> futures = new ArrayList<>();
        ExecutorService executorSrvc = Executors.newFixedThreadPool(20);

        for (int i = 0; i < PUT_THREAD_POOL_SIZE; ++i) {
            futures.add(executorSrvc.submit(new Runnable() {
                @Override public void run() {

                    // Put values to cache for 1 min.
                    while (System.currentTimeMillis() < endTaskTime) {
                        int key = cnt.getAndIncrement();
                        cache.put(key, new CacheValue(key));
                        if (key % 50 == 0)
                            System.out.println("put " + key);
                    }
                }
            }));
        }

        Thread.sleep(500);
        info("Initial set of keys is loaded");

        info("Starting new node...");
        startGrid(GRID_CNT + 1);
        info("New node is started");

        // Wait for put tasks. If put() is blocked the test is timed out.
        for (Future f : futures)
            f.get();
    }

    /**
     * Test cache value.
     */
    private static class CacheValue {
        @QuerySqlField(index = true)
        private final int val;

        /**
         * @param val Value.
         */
        CacheValue(int val) {
            this.val = val;
        }

        /**
         */
        int value() {
            return val;
        }

        /**
         * {@inheritDoc}
         */
        @Override public String toString() {
            return S.toString(CacheValue.class, this);
        }
    }
}