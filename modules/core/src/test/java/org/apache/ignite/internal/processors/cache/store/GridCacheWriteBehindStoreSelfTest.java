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
     * Tests correct store shutdown when underlying store fails,
     *
     * @throws Exception If failed.
     */
    public void testShutdownWithFailure() throws Exception {
        final AtomicReference<Exception> err = new AtomicReference<>();

        multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    delegate.setShouldFail(true);

                    initStore(2);

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
     * @throws Exception If failed.
     */
    public void testSimpleStore() throws Exception {
        initStore(2);

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
     * Check that all values written to the store will be in underlying store after timeout or due to size limits.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"NullableProblems"})
    public void testValuePropagation() throws Exception {
        // Need to test size-based write.
        initStore(1);

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
     * Tests store behaviour under continuous put of the same key with different values.
     *
     * @throws Exception If failed
     */
    public void testContinuousPut() throws Exception {
        initStore(2);

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

            int delegatePutCnt = delegate.getPutAllCount();

            running.set(false);

            fut.get();

            log().info(">>> [putCnt = " + actualPutCnt.get() + ", delegatePutCnt=" + delegatePutCnt + "]");

            assertTrue("No puts were made to the underlying store", delegatePutCnt > 0);
            assertTrue("Too many puts were made to the underlying store", delegatePutCnt < actualPutCnt.get() / 10);
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
     * Tests that all values were put into the store will be written to the underlying store
     * after shutdown is called.
     *
     * @throws Exception If failed.
     */
    public void testShutdown() throws Exception {
        initStore(2);

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
     * right in the same order as they were put into the store.
     *
     * @throws Exception If failed.
     */
    public void testBatchApply() throws Exception {
        delegate = new GridCacheTestStore(new ConcurrentLinkedHashMap<Integer, String>());

        initStore(1);

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