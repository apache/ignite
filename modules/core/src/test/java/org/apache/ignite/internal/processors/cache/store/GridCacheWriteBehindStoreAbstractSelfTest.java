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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.processors.cache.GridCacheTestStore;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Harness for {@link GridCacheWriteBehindStore} tests.
 */
public abstract class GridCacheWriteBehindStoreAbstractSelfTest extends GridCommonAbstractTest {
    /** Write cache size. */
    public static final int CACHE_SIZE = 1024;

    /** Value dump interval. */
    public static final int FLUSH_FREQUENCY = 1000;

    /** Underlying store. */
    protected GridCacheTestStore delegate = new GridCacheTestStore();

    /** Tested store. */
    protected GridCacheWriteBehindStore<Integer, String> store;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        delegate = null;
        store = null;

        super.afterTestsStopped();
    }

    /**
     * Initializes store.
     *
     * @param flushThreadCnt Count of flush threads
     * @throws Exception If failed.
     */
    protected void initStore(int flushThreadCnt) throws Exception {
        store = new GridCacheWriteBehindStore<>(null, "", "", log, delegate);

        store.setFlushFrequency(FLUSH_FREQUENCY);

        store.setFlushSize(CACHE_SIZE);

        store.setFlushThreadCount(flushThreadCnt);

        delegate.reset();

        store.start();
    }

    /**
     * Shutdowns store.
     *
     * @throws Exception If failed.
     */
    protected void shutdownStore() throws Exception {
        store.stop();

        assertTrue("Store cache must be empty after shutdown", store.writeCache().isEmpty());
    }

    /**
     * Performs multiple put, get and remove operations in several threads on a store. After
     * all threads finished their operations, returns the total set of keys that should be
     * in underlying store.
     *
     * @param threadCnt Count of threads that should update keys.
     * @param keysPerThread Count of unique keys assigned to a thread.
     * @return Set of keys that was totally put in store.
     * @throws Exception If failed.
     */
    protected Set<Integer> runPutGetRemoveMultithreaded(int threadCnt, final int keysPerThread) throws Exception {
        final ConcurrentMap<String, Set<Integer>> perThread = new ConcurrentHashMap<>();

        final AtomicBoolean running = new AtomicBoolean(true);

        final AtomicInteger cntr = new AtomicInteger();

        final AtomicInteger operations = new AtomicInteger();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @SuppressWarnings({"NullableProblems"})
            @Override public void run() {
                // Initialize key set for this thread.
                Set<Integer> set = new HashSet<>();

                Set<Integer> old = perThread.putIfAbsent(Thread.currentThread().getName(), set);

                if (old != null)
                    set = old;

                List<Integer> original = new ArrayList<>();

                Random rnd = new Random();

                for (int i = 0; i < keysPerThread; i++)
                    original.add(cntr.getAndIncrement());

                try {
                    while (running.get()) {
                        int op = rnd.nextInt(3);
                        int idx = rnd.nextInt(keysPerThread);

                        int key = original.get(idx);

                        switch (op) {
                            case 0:
                                store.write(new CacheEntryImpl<>(key, "val" + key));
                                set.add(key);

                                operations.incrementAndGet();

                                break;

                            case 1:
                                store.delete(key);
                                set.remove(key);

                                operations.incrementAndGet();

                                break;

                            case 2:
                            default:
                                store.write(new CacheEntryImpl<>(key, "broken"));

                                String val = store.load(key);

                                assertEquals("Invalid intermediate value: " + val, "broken", val);

                                store.write(new CacheEntryImpl<>(key, "val" + key));

                                set.add(key);

                                // 2 put operations performed here.
                                operations.incrementAndGet();
                                operations.incrementAndGet();
                                operations.incrementAndGet();

                                break;
                        }
                    }
                }
                catch (Exception e) {
                    error("Unexpected exception in put thread", e);

                    assert false;
                }
            }
        }, threadCnt, "put");

        U.sleep(10000);

        running.set(false);

        fut.get();

        log().info(">>> " + operations + " operations performed totally");

        Set<Integer> total = new HashSet<>();

        for (Set<Integer> threadVals : perThread.values()) {
            total.addAll(threadVals);
        }

        return total;
    }
}