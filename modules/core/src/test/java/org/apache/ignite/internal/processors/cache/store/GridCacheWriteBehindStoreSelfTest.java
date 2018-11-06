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

package org.apache.ignite.internal.processors.cache.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.processors.cache.GridCacheTestStore;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jsr166.ConcurrentLinkedHashMap;

/**
 * This class provides basic tests for {@link org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore}.
 */
public class GridCacheWriteBehindStoreSelfTest extends GridCacheWriteBehindStoreAbstractSelfTest {
    /**
     * Tests correct store (with write coalescing) shutdown when underlying store fails.
     *
     * @throws Exception If failed.
     */
    public void testShutdownWithFailureWithCoalescing() throws Exception {
        testShutdownWithFailure(true);
    }

    /**
     * Tests correct store (without write coalescing) shutdown when underlying store fails.
     *
     * @throws Exception If failed.
     */
    public void testShutdownWithFailureWithoutCoalescing() throws Exception {
        testShutdownWithFailure(false);
    }

    /**
     * Tests correct store shutdown when underlying store fails.
     *
     * @param writeCoalescing Write coalescing flag.
     * @throws Exception If failed.
     */
    private void testShutdownWithFailure(final boolean writeCoalescing) throws Exception {
        final AtomicReference<Exception> err = new AtomicReference<>();

        multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    delegate.setShouldFail(true);

                    initStore(2, writeCoalescing);

                    try {
                        store.write(new CacheEntryImpl<>(1, "val1"));
                        store.write(new CacheEntryImpl<>(2, "val2"));
                    }
                    finally {
                        shutdownStore();

                        delegate.setShouldFail(false);
                    }
                }
                catch (Exception e) {
                    err.set(e);
                }
            }
        }, 1).get();

        if (err.get() != null)
            throw err.get();
    }

    /**
     * Simple store (with write coalescing) test.
     *
     * @throws Exception If failed.
     */
    public void testSimpleStoreWithCoalescing() throws Exception {
        testSimpleStore(true);
    }

    /**
     * Simple store (without write coalescing) test.
     *
     * @throws Exception If failed.
     */
    public void testSimpleStoreWithoutCoalescing() throws Exception {
        testSimpleStore(false);
    }

    /**
     * Checks that write behind cache flush frequency was correctly adjusted to nanos expecting putAllCnt to be
     * less or equal than elapsed time divided by flush frequency.
     *
     * @throws Exception If failed.
     */
    public void testSimpleStoreFlushFrequencyWithoutCoalescing() throws Exception {
        initStore(1, false);

        long writeBehindFlushFreqNanos = FLUSH_FREQUENCY * 1000 * 1000;

        int threshold = store.getWriteBehindStoreBatchSize() / 10;

        try {
            long start = System.nanoTime();

            for (int i = 0; i < threshold / 2; i++)
                store.write(new CacheEntryImpl<>(i, "v" + i));

            U.sleep(FLUSH_FREQUENCY + 300);

            for (int i = threshold / 2; i < threshold; i++)
                store.write(new CacheEntryImpl<>(i, "v" + i));

            long elapsed = System.nanoTime() - start;

            U.sleep(FLUSH_FREQUENCY + 300);

            int expFlushOps = (int)(1 + elapsed / writeBehindFlushFreqNanos);

            assertTrue(delegate.getPutAllCount() <= expFlushOps);
        }
        finally {
            shutdownStore();
        }
    }

    /**
     * Simple store test.
     *
     * @param writeCoalescing Write coalescing flag.
     * @throws Exception If failed.
     */
    private void testSimpleStore(boolean writeCoalescing) throws Exception {
        initStore(2, writeCoalescing);

        try {
            store.write(new CacheEntryImpl<>(1, "v1"));
            store.write(new CacheEntryImpl<>(2, "v2"));

            assertEquals("v1", store.load(1));
            assertEquals("v2", store.load(2));
            assertNull(store.load(3));

            store.delete(1);

            assertNull(store.load(1));
            assertEquals("v2", store.load(2));
            assertNull(store.load(3));
        }
        finally {
            shutdownStore();
        }
    }

    /**
     * Check that all values written to the store with coalescing will be in underlying store after timeout
     * or due to size limits.
     *
     * @throws Exception If failed.
     */
    public void testValuePropagationWithCoalescing() throws Exception {
        testValuePropagation(true);
    }

    /**
     * Check that all values written to the store without coalescing will be in underlying store after timeout
     * or due to size limits.
     *
     * @throws Exception If failed.
     */
    public void testValuePropagationWithoutCoalescing() throws Exception {
        testValuePropagation(false);
    }

    /**
     * Check that all values written to the store will be in underlying store after timeout or due to size limits.
     *
     * @param writeCoalescing Write coalescing flag
     * @throws Exception If failed.
     */
    @SuppressWarnings({"NullableProblems"})
    private void testValuePropagation(boolean writeCoalescing) throws Exception {
        // Need to test size-based write.
        initStore(1, writeCoalescing);

        try {
            for (int i = 0; i < CACHE_SIZE * 2; i++)
                store.write(new CacheEntryImpl<>(i, "val" + i));

            U.sleep(200);

            for (int i = 0; i < CACHE_SIZE; i++) {
                String val = delegate.load(i);

                assertNotNull("Value for [key= " + i + "] was not written in store", val);
                assertEquals("Invalid value [key=" + i + "]", "val" + i, val);
            }

            U.sleep(FLUSH_FREQUENCY + 300);

            for (int i = CACHE_SIZE; i < CACHE_SIZE * 2; i++) {
                String val = delegate.load(i);

                assertNotNull("Value for [key= " + i + "] was not written in store", val);
                assertEquals("Invalid value [key=" + i + "]", "val" + i, val);
            }
        }
        finally {
            shutdownStore();
        }
    }

    /**
     * Tests store with write coalescing behaviour under continuous put of the same key with different values.
     *
     * @throws Exception If failed.
     */
    public void testContinuousPutWithCoalescing() throws Exception {
        testContinuousPut(true);
    }

    /**
     * Tests store without write coalescing behaviour under continuous put of the same key with different values.
     *
     * @throws Exception If failed.
     */
    public void testContinuousPutWithoutCoalescing() throws Exception {
        testContinuousPut(false);
    }

    /**
     * Tests store behaviour under continuous put of the same key with different values.
     *
     * @param writeCoalescing Write coalescing flag for cache.
     * @throws Exception If failed.
     */
    private void testContinuousPut(boolean writeCoalescing) throws Exception {
        initStore(2, writeCoalescing);

        try {
            final AtomicBoolean running = new AtomicBoolean(true);

            final AtomicInteger actualPutCnt = new AtomicInteger();

            IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
                @SuppressWarnings({"NullableProblems"})
                @Override public void run() {
                    try {
                        while (running.get()) {
                            for (int i = 0; i < CACHE_SIZE; i++) {
                                store.write(new CacheEntryImpl<>(i, "val-0"));

                                actualPutCnt.incrementAndGet();

                                store.write(new CacheEntryImpl<>(i, "val" + i));

                                actualPutCnt.incrementAndGet();
                            }
                        }
                    }
                    catch (Exception e) {
                        error("Unexpected exception in put thread", e);

                        assert false;
                    }
                }
            }, 1, "put");

            U.sleep(FLUSH_FREQUENCY * 2 + 500);
            running.set(false);
            U.sleep(FLUSH_FREQUENCY * 2 + 500);

            int delegatePutCnt = delegate.getPutAllCount();

            fut.get();

            log().info(">>> [putCnt = " + actualPutCnt.get() + ", delegatePutCnt=" + delegatePutCnt + "]");

            assertTrue("No puts were made to the underlying store", delegatePutCnt > 0);
            if (store.getWriteCoalescing()) {
                assertTrue("Too many puts were made to the underlying store", delegatePutCnt < actualPutCnt.get() / 10);
            }
            else {
                assertTrue("Too few puts cnt=" + actualPutCnt.get() + " << storePutCnt=" + delegatePutCnt, delegatePutCnt > actualPutCnt.get() / 2);
            }
        }
        finally {
            shutdownStore();
        }

        // These checks must be done after the store shut down
        assertEquals("Invalid store size", CACHE_SIZE, delegate.getMap().size());

        for (int i = 0; i < CACHE_SIZE; i++)
            assertEquals("Invalid value stored", "val" + i, delegate.getMap().get(i));
    }

    /**
     * Tests that all values were put into the store with write coalescing will be written to the underlying store
     * after shutdown is called.
     *
     * @throws Exception If failed.
     */
    public void testShutdownWithCoalescing() throws Exception {
        testShutdown(true);
    }

    /**
     * Tests that all values were put into the store without write coalescing will be written to the underlying store
     * after shutdown is called.
     *
     * @throws Exception If failed.
     */
    public void testShutdownWithoutCoalescing() throws Exception {
        testShutdown(false);
    }

    /**
     * Tests that all values were put into the store will be written to the underlying store
     * after shutdown is called.
     *
     * @param writeCoalescing Write coalescing flag.
     * @throws Exception If failed.
     */
    private void testShutdown(boolean writeCoalescing) throws Exception {
        initStore(2, writeCoalescing);

        try {
            final AtomicBoolean running = new AtomicBoolean(true);

            IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
                @SuppressWarnings({"NullableProblems"})
                @Override public void run() {
                    try {
                        while (running.get()) {
                            for (int i = 0; i < CACHE_SIZE; i++) {
                                store.write(new CacheEntryImpl<>(i, "val-0"));

                                store.write(new CacheEntryImpl<>(i, "val" + i));
                            }
                        }
                    }
                    catch (Exception e) {
                        error("Unexpected exception in put thread", e);

                        assert false;
                    }
                }
            }, 1, "put");

            U.sleep(300);

            running.set(false);

            fut.get();
        }
        finally {
            shutdownStore();
        }

        // These checks must be done after the store shut down
        assertEquals("Invalid store size", CACHE_SIZE, delegate.getMap().size());

        for (int i = 0; i < CACHE_SIZE; i++)
            assertEquals("Invalid value stored", "val" + i, delegate.getMap().get(i));
    }

    /**
     * Tests that all values will be written to the underlying store
     * right in the same order as they were put into the store with coalescing.
     *
     * @throws Exception If failed.
     */
    public void testBatchApplyWithCoalescing() throws Exception {
        testBatchApply(true);
    }

    /**
     * Tests that all values will be written to the underlying store
     * right in the same order as they were put into the store without coalescing.
     *
     * @throws Exception If failed.
     */
    public void testBatchApplyWithoutCoalescing() throws Exception {
        testBatchApply(false);
    }

    /**
     * Tests that all values will be written to the underlying store
     * right in the same order as they were put into the store.
     *
     * @param writeCoalescing Write coalescing flag.
     * @throws Exception If failed.
     */
    private void testBatchApply(boolean writeCoalescing) throws Exception {
        delegate = new GridCacheTestStore(new ConcurrentLinkedHashMap<Integer, String>() {
            @Override public void clear() { }
        });

        initStore(1, writeCoalescing);

        List<Integer> intList = new ArrayList<>(CACHE_SIZE);

        try {
            for (int i = 0; i < CACHE_SIZE; i++) {
                store.write(new CacheEntryImpl<>(i, "val" + i));

                intList.add(i);
            }
        }
        finally {
            shutdownStore();
        }

        Map<Integer, String> underlyingMap = delegate.getMap();

        assertTrue("Store map key set: " + underlyingMap.keySet(), F.eqOrdered(underlyingMap.keySet(), intList));
    }
}
