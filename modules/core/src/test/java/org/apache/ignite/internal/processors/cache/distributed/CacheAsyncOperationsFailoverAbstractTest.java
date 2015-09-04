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

package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public abstract class CacheAsyncOperationsFailoverAbstractTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int NODE_CNT = 4;

    /** */
    private static final long TEST_TIME = 60_000;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return NODE_CNT;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIME + 60_000;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setCacheStoreFactory(null);
        ccfg.setReadThrough(false);
        ccfg.setWriteThrough(false);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllAsyncFailover() throws Exception {
        putAllAsyncFailover(5, 10);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllAsyncFailoverManyThreads() throws Exception {
        putAllAsyncFailover(ignite(0).configuration().getSystemThreadPoolSize() * 2, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsyncFailover() throws Exception {
        IgniteCache<TestKey, TestValue> cache = ignite(0).<TestKey, TestValue>cache(null).withAsync();

        int ops = cache.getConfiguration(CacheConfiguration.class).getMaxConcurrentAsyncOperations();

        log.info("Max concurrent async operations: " + ops);

        assertTrue(ops > 0);

        // Start/stop one node.
        for (int i = 0; i < 2; i++) {
            log.info("Iteration: " + i);

            startGrid(NODE_CNT);

            List<IgniteFuture<?>> futs = startAsyncOperations(ops, cache);

            stopGrid(NODE_CNT);

            for (IgniteFuture<?> fut : futs)
                fut.get();

            log.info("Iteration done: " + i);
        }

        // Start all nodes except one.
        try {
            List<IgniteFuture<?>> futs = startAsyncOperations(ops, cache);

            for (int i = 1; i < NODE_CNT; i++)
                stopGrid(i);

            for (IgniteFuture<?> fut : futs)
                fut.get();
        }
        finally {
            for (int i = 1; i < NODE_CNT; i++)
                startGrid(i);
        }
    }

    /**
     * @param ops Number of operations.
     * @param cache Cache.
     * @return Futures.
     * @throws Exception If failed.
     */
    private List<IgniteFuture<?>> startAsyncOperations(final int ops, final IgniteCache<TestKey, TestValue> cache)
        throws Exception
    {
        final List<IgniteFuture<?>> futs = Collections.synchronizedList(new ArrayList<IgniteFuture<?>>(ops));

        final AtomicInteger left = new AtomicInteger(ops);

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                List<IgniteFuture<?>> futs0 = new ArrayList<>();

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (left.getAndDecrement() > 0) {
                    TreeMap<TestKey, TestValue> map = new TreeMap<>();

                    int keys = 50;

                    for (int k = 0; k < keys; k++)
                        map.put(new TestKey(rnd.nextInt(10_000)), new TestValue(k));

                    cache.putAll(map);

                    IgniteFuture<?> fut = cache.future();

                    assertNotNull(fut);

                    futs0.add(fut);
                }

                futs.addAll(futs0);

                return null;
            }
        }, 10, "put-thread");

        assertEquals(ops, futs.size());

        return futs;
    }

    /**
     * @param threads Number of threads.
     * @param opsPerThread Number of concurrent async operations per thread.
     * @throws Exception If failed.
     */
    private void putAllAsyncFailover(final int threads, final int opsPerThread) throws Exception {
        log.info("Start test [threads=" + threads + ", opsPerThread=" + opsPerThread + ']');

        final AtomicBoolean finished = new AtomicBoolean();

        final long endTime = System.currentTimeMillis() + TEST_TIME;

        IgniteInternalFuture<Object> restartFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Thread.currentThread().setName("restart-thread");

                while (!finished.get() && System.currentTimeMillis() < endTime) {
                    startGrid(NODE_CNT);

                    U.sleep(500);

                    stopGrid(NODE_CNT);
                }

                return null;
            }
        });

        try {
            final IgniteCache<TestKey, TestValue> cache = ignite(0).<TestKey, TestValue>cache(null).withAsync();

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    int iter = 0;

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    long time;

                    long lastInfo = 0;

                    while ((time = System.currentTimeMillis()) < endTime) {
                        if (time - lastInfo > 5000)
                            log.info("Starting operations [iter=" + iter + ']');

                        List<IgniteFuture<?>> futs = new ArrayList<>(opsPerThread);

                        for (int i = 0; i < opsPerThread; i++) {
                            TreeMap<TestKey, TestValue> map = new TreeMap<>();

                            int keys = rnd.nextInt(1, 50);

                            for (int k = 0; k < keys; k++)
                                map.put(new TestKey(rnd.nextInt(10_000)), new TestValue(iter));

                            cache.putAll(map);

                            IgniteFuture<?> fut = cache.future();

                            assertNotNull(fut);

                            futs.add(fut);
                        }

                        if (time - lastInfo > 5000) {
                            log.info("Waiting for futures [iter=" + iter + ']');

                            lastInfo = time;
                        }

                        for (IgniteFuture<?> fut : futs)
                            fut.get();

                        iter++;
                    }

                    return null;
                }
            }, threads, "update-thread");

            finished.set(true);

            restartFut.get();
        }
        finally {
            finished.set(true);
        }
    }

    /**
     *
     */
    private static class TestKey implements Serializable, Comparable<TestKey> {
        /** */
        private long key;

        /**
         * @param key Key.
         */
        public TestKey(long key) {
            this.key = key;
        }

        /**
         * @return Key.
         */
        public long key() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull TestKey other) {
            return ((Long)key).compareTo(other.key);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey other = (TestKey)o;

            return key == other.key;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return (int)(key ^ (key >>> 32));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestKey.class, this);
        }
    }

    /**
     *
     */
    private static class TestValue implements Serializable {
        /** */
        private long val;

        /**
         * @param val Value.
         */
        public TestValue(long val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public long value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestValue other = (TestValue)o;

            return val == other.val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }
    }
}