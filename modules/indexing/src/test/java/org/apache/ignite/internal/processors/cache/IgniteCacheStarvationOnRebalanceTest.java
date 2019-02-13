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

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test to reproduce https://issues.apache.org/jira/browse/IGNITE-3073.
 */
public class IgniteCacheStarvationOnRebalanceTest extends GridCacheAbstractSelfTest {
    /** Grid count. */
    private static final int GRID_CNT = 4;

    /** Test timeout. */
    private static final long TEST_TIMEOUT = 3 * 60 * 1000;

    /** Use small system thread pool to reproduce the issue. */
    private static final int IGNITE_THREAD_POOL_SIZE = 5;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Use small system thread pool to reproduce the issue.
        cfg.setSystemThreadPoolSize(IGNITE_THREAD_POOL_SIZE);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected Class<?>[] indexedTypes() {
        return new Class<?>[] {Integer.class, CacheValue.class};
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadSystemWithPutAndStartRebalancing() throws Exception {
        final IgniteCache<Integer, CacheValue> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        final long endTime = System.currentTimeMillis() + SF.applyLB((int)TEST_TIMEOUT - 60_000, 5_000);

        int iter = 0;

        while (System.currentTimeMillis() < endTime) {
            info("Iteration: " + iter++);

            final AtomicBoolean stop = new AtomicBoolean();

            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!stop.get() && System.currentTimeMillis() < endTime) {
                        int key = rnd.nextInt(100_000);

                        cache.put(key, new CacheValue(key));
                    }

                    return null;
                }
            }, IGNITE_THREAD_POOL_SIZE * 4, "put-thread");

            try {
                Thread.sleep(500);

                info("Initial set of keys is loaded.");

                info("Starting new node...");

                startGrid(GRID_CNT + 1);

                info("New node is started.");

                Thread.sleep(500);
            }
            finally {
                stop.set(true);
            }

            // Wait for put tasks. If put() is blocked the test is timed out.
            fut.get();

            stopGrid(GRID_CNT + 1);
        }
    }

    /**
     * Test cache value.
     */
    private static class CacheValue {
        /** */
        @QuerySqlField(index = true)
        private final int val;

        /**
         * @param val Value.
         */
        CacheValue(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheValue.class, this);
        }
    }
}
