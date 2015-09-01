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

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Multithreaded update test with off heap enabled.
 */
public abstract class GridCacheOffHeapMultiThreadedUpdateAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    protected static volatile boolean failed;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setMemoryMode(OFFHEAP_TIERED);
        ccfg.setOffHeapMaxMemory(1024 * 1024);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3 * 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        failed = false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransform() throws Exception {
        testTransform(keyForNode(0));

        if (gridCount() > 1)
            testTransform(keyForNode(1));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void testTransform(final Integer key) throws Exception {
        final IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        cache.put(key, 0);

        final int THREADS = 5;
        final int ITERATIONS_PER_THREAD = 10_000;

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                for (int i = 0; i < ITERATIONS_PER_THREAD && !failed; i++) {
                    if (i % 500 == 0)
                        log.info("Iteration " + i);

                    cache.invoke(key, new IncProcessor());
                }

                return null;
            }
        }, THREADS, "transform");

        for (int i = 0; i < gridCount(); i++) {
            Integer val = (Integer)grid(i).cache(null).get(key);

            assertEquals("Unexpected value for grid " + i, (Integer)(ITERATIONS_PER_THREAD * THREADS), val);
        }

        assertFalse(failed);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        testPut(keyForNode(0));

        if (gridCount() > 1)
            testPut(keyForNode(1));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void testPut(final Integer key) throws Exception {
        final IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        cache.put(key, 0);

        final int THREADS = 5;
        final int ITERATIONS_PER_THREAD = iterations();

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                for (int i = 0; i < ITERATIONS_PER_THREAD && !failed; i++) {
                    if (i % 500 == 0)
                        log.info("Iteration " + i);

                    Integer val = cache.getAndPut(key, i);

                    assertNotNull(val);
                }

                return null;
            }
        }, THREADS, "put");

        for (int i = 0; i < gridCount(); i++) {
            Integer val = (Integer)grid(i).cache(null).get(key);

            assertNotNull("Unexpected value for grid " + i, val);
        }

        assertFalse(failed);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutxIfAbsent() throws Exception {
        testPutxIfAbsent(keyForNode(0));

        if (gridCount() > 1)
            testPutxIfAbsent(keyForNode(1));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void testPutxIfAbsent(final Integer key) throws Exception {
        final IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        cache.put(key, 0);

        final int THREADS = 5;
        final int ITERATIONS_PER_THREAD = iterations();

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                for (int i = 0; i < ITERATIONS_PER_THREAD && !failed; i++) {
                    if (i % 500 == 0)
                        log.info("Iteration " + i);

                    assertFalse(cache.putIfAbsent(key, 100));
                }

                return null;
            }
        }, THREADS, "putxIfAbsent");

        for (int i = 0; i < gridCount(); i++) {
            Integer val = (Integer)grid(i).cache(null).get(key);

            assertEquals("Unexpected value for grid " + i, (Integer)0, val);
        }

        assertFalse(failed);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGet() throws Exception {
        testPutGet(keyForNode(0));

        if (gridCount() > 1)
            testPutGet(keyForNode(1));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void testPutGet(final Integer key) throws Exception {
        final IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        cache.put(key, 0);

        final int THREADS = 5;
        final int ITERATIONS_PER_THREAD = iterations();

        IgniteInternalFuture<Long> putFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
                    if (i % 1000 == 0)
                        log.info("Put iteration " + i);

                    cache.put(key, i);
                }

                return null;
            }
        }, THREADS, "put");

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<Long> getFut;

        try {
            getFut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int cnt = 0;

                    while (!stop.get()) {
                        if (++cnt % 5000 == 0)
                            log.info("Get iteration " + cnt);

                        assertNotNull(cache.get(key));
                    }

                    return null;
                }
            }, THREADS, "get");

            putFut.get();
        }
        finally {
            stop.set(true);
        }

        getFut.get();

        for (int i = 0; i < gridCount(); i++) {
            Integer val = (Integer)grid(i).cache(null).get(key);

            assertNotNull("Unexpected value for grid " + i, val);
        }
    }

    /**
     * @param idx Node index.
     * @return Primary key for node.
     */
    protected Integer keyForNode(int idx) {
        Integer key0 = null;

        for (int i = 0; i < 10_000; i++) {
            if (grid(0).affinity(null).isPrimary(grid(idx).localNode(), i)) {
                key0 = i;

                break;
            }
        }

        assertNotNull(key0);

        return key0;
    }

    /**
     * @return Number of iterations.
     */
    protected int iterations() {
        return 10_000;
    }

    /**
     *
     */
    protected static class IncProcessor implements EntryProcessor<Integer, Integer, Void>, Serializable {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Integer> e, Object... args) {
            Integer val = e.getValue();

            if (val == null) {
                failed = true;

                System.out.println(Thread.currentThread() + " got null in processor: " + val);

                return null;
            }

            e.setValue(val + 1);

            return null;
        }
    }

    /**
     *
     */
    protected static class TestFilter implements IgnitePredicate<Cache.Entry<Integer, Integer>> {
        /** {@inheritDoc} */
        @Override public boolean apply(Cache.Entry<Integer, Integer> e) {
            if (e == null) {
                failed = true;

                System.out.println(Thread.currentThread() + " got null entry in filter: " + e);

                return false;
            }
            else if (e.getValue() == null) {
                failed = true;

                System.out.println(Thread.currentThread() + " got null value in filter: " + e);

                return false;
            }

            return true;
        }
    }
}