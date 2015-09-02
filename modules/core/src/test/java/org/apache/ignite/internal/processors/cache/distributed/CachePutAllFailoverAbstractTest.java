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
import java.util.Collection;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
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
public abstract class CachePutAllFailoverAbstractTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int NODE_CNT = 2;

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
    public void testPutAllFailover() throws Exception {
        testPutAllFailover(Test.PUT_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverPessimisticTx() throws Exception {
        if (atomicityMode() == CacheAtomicityMode.ATOMIC)
            return;

        testPutAllFailover(Test.PUT_ALL_PESSIMISTIC_TX);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllFailoverAsync() throws Exception {
        testPutAllFailover(Test.PUT_ALL_ASYNC);
    }

    /**
     * @param test Test type
     * @throws Exception If failed.
     */
    private void testPutAllFailover(final Test test) throws Exception {
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
            IgniteCache<TestKey, TestValue> cache0 = ignite(0).cache(null);

            final IgniteCache<TestKey, TestValue> cache = test == Test.PUT_ALL_ASYNC ? cache0.withAsync() : cache0;

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    int iter = 0;

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    long time;

                    long lastInfo = 0;

                    while ((time = System.currentTimeMillis()) < endTime) {
                        if (time - lastInfo > 5000) {
                            log.info("Do putAll [iter=" + iter + ']');

                            lastInfo = time;
                        }

                        switch (test) {
                            case PUT_ALL: {
                                TreeMap<TestKey, TestValue> map = new TreeMap<>();

                                for (int k = 0; k < 100; k++)
                                    map.put(new TestKey(rnd.nextInt(200)), new TestValue(iter));

                                cache.putAll(map);

                                break;
                            }

                            case PUT_ALL_ASYNC: {
                                Collection<IgniteFuture<?>> futs = new ArrayList<>();

                                for (int i = 0 ; i < 10; i++) {
                                    TreeMap<TestKey, TestValue> map = new TreeMap<>();

                                    for (int k = 0; k < 100; k++)
                                        map.put(new TestKey(rnd.nextInt(200)), new TestValue(iter));

                                    cache.putAll(map);

                                    IgniteFuture<?> fut = cache.future();

                                    assertNotNull(fut);

                                    futs.add(fut);
                                }

                                for (IgniteFuture<?> fut : futs)
                                    fut.get();

                                break;
                            }

                            case PUT_ALL_PESSIMISTIC_TX: {
                                final TreeMap<TestKey, TestValue> map = new TreeMap<>();

                                for (int k = 0; k < 100; k++)
                                    map.put(new TestKey(rnd.nextInt(200)), new TestValue(iter));

                                doInTransaction(ignite(0), new Callable<Object>() {
                                    @Override public Object call() throws Exception {
                                        for (TestKey key : map.keySet())
                                            cache.get(key);

                                        cache.putAll(map);

                                        return null;
                                    }
                                });

                                break;
                            }

                            default:
                                assert false;
                        }

                        iter++;
                    }

                    return null;
                }
            }, 2, "update-thread");

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

    /**
     *
     */
    private enum Test {
        /** */
        PUT_ALL,

        /** */
        PUT_ALL_ASYNC,

        /** */
        PUT_ALL_PESSIMISTIC_TX
    }
}
