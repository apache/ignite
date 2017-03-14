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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.TestMemoryMode;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/**
 *
 */
public class IgniteCachePutRetryTransactionalSelfTest extends IgniteCachePutRetryAbstractSelfTest {
    /** */
    private static final int FACTOR = 1000;

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLongRetries() throws Exception {
        final AtomicBoolean finished = new AtomicBoolean();

        IgniteAtomicLong atomic = ignite(0).atomicLong("TestAtomic", 0, true);

        IgniteInternalFuture<Object> fut = runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    stopGrid(3);

                    U.sleep(300);

                    startGrid(3);
                }

                return null;
            }
        });

        final int keysCnt = 20_000;

        try {
            for (int i = 0; i < keysCnt; i++)
                atomic.incrementAndGet();

            finished.set(true);

            fut.get();
        }
        finally {
            finished.set(true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitTransactionRetriesSingleValue() throws Exception {
        checkRetry(Test.TX_PUT, TestMemoryMode.HEAP, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitTransactionRetriesSingleValueStoreEnabled() throws Exception {
        checkRetry(Test.TX_PUT, TestMemoryMode.HEAP, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitTransactionRetries() throws Exception {
        explicitTransactionRetries(TestMemoryMode.HEAP, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitTransactionRetriesSingleOperation() throws Exception {
        explicitTransactionRetries(TestMemoryMode.HEAP, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitTransactionRetriesStoreEnabled() throws Exception {
        explicitTransactionRetries(TestMemoryMode.HEAP, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitTransactionRetriesOffheapSwap() throws Exception {
        explicitTransactionRetries(TestMemoryMode.OFFHEAP_EVICT_SWAP, false);
    }

    /**
     * @param memMode Memory mode.
     * @param store If {@code true} uses cache with store.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void explicitTransactionRetries(TestMemoryMode memMode, boolean store) throws Exception {
        ignite(0).createCache(cacheConfiguration(memMode, store));

        final AtomicInteger idx = new AtomicInteger();
        int threads = 8;

        final AtomicReferenceArray<Exception> err = new AtomicReferenceArray<>(threads);

        IgniteInternalFuture<Long> fut = runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int th = idx.getAndIncrement();
                int base = th * FACTOR;

                Ignite ignite = ignite(0);
                final IgniteCache<Object, Object> cache = ignite.cache(null);

                try {
                    for (int i = 0; i < FACTOR; i++) {
                        doInTransaction(ignite, new ProcessCallable(cache, base, i));

                        if (i > 0 && i % 500 == 0)
                            info("Done: " + i);
                    }
                } catch (Exception e) {
                    err.set(th, e);
                }

                return null;
            }
        }, threads, "tx-runner");

        while (!fut.isDone()) {
            int stopIdx = ThreadLocalRandom.current().nextInt(2, 4); // Random in [2, 3].

            stopGrid(stopIdx);

            U.sleep(500);

            startGrid(stopIdx);
        }

        for (int i = 0; i < threads; i++) {
            Exception error = err.get(i);

            if (error != null)
                throw error;
        }

        // Verify contents of the cache.
        for (int g = 0; g < GRID_CNT; g++) {
            IgniteCache<Object, Object> cache = ignite(g).cache(null);

            for (int th = 0; th < threads; th++) {
                int base = th * FACTOR;

                String key = "key-" + base;

                Set<String> set = (Set<String>)cache.get(key);

                assertNotNull("Missing set for key: " + key, set);
                assertEquals(FACTOR, set.size());

                for (int i = 0; i < FACTOR; i++) {
                    assertEquals("value-" + i, cache.get("key-" + base + "-" + i));
                    assertTrue(set.contains("value-" + i));
                }
            }
        }
    }

    /**
     *
     */
    public void testOriginatingNodeFailureForcesOnePhaseCommitDataCleanup() throws Exception {
        ignite(0).createCache(cacheConfiguration(TestMemoryMode.HEAP, false));

        final AtomicBoolean finished = new AtomicBoolean();

        final int keysCnt = keysCount();

        IgniteInternalFuture<Object> fut = runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = new Random();

                while (!finished.get()) {
                    stopGrid(0);

                    U.sleep(300);

                    startGrid(0);

                    if (rnd.nextBoolean()) // OPC possible only when there is no migration from one backup to another.
                        awaitPartitionMapExchange();
                }

                return null;
            }
        });

        IgniteInternalFuture<Object> fut2 = runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int iter = 0;

                while (!finished.get()) {
                    try {
                        IgniteCache<Integer, Integer> cache = ignite(0).cache(null);

                        Integer val = ++iter;

                        for (int i = 0; i < keysCnt; i++)
                            cache.invoke(i, new SetEntryProcessor(val));
                    }
                    catch (Exception ignored) {
                        // No-op.
                    }
                }

                return null;
            }
        });

        try {
            U.sleep(DURATION);
        }
        finally {
            finished.set(true);

            fut.get();
            fut2.get();
        }

        checkOnePhaseCommitReturnValuesCleaned();
    }

    /**
     * Callable to process inside transaction.
     */
    private static class ProcessCallable implements Callable<Void> {
        /** */
        private IgniteCache cache;

        /** */
        private int base;

        /** */
        private int i;

        /**
         * @param cache Cache.
         * @param base Base index.
         * @param i Iteration index.
         */
        private ProcessCallable(IgniteCache<Object, Object> cache, int base, int i) {
            this.cache = cache;
            this.base = base;
            this.i = i;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public Void call() throws Exception {
            String key1 = "key-" + base + "-" + i;
            String key2 = "key-" + base;

            assert key1.compareTo(key2) > 0;

            ((IgniteCache<String, String>)cache).put(key1, "value-" + i);

            ((IgniteCache<String, Set<String>>)cache).invoke(key2, new AddEntryProcessor("value-" + i));

            return null;
        }
    }

    /**
     *
     */
    private static class AddEntryProcessor implements CacheEntryProcessor<String, Set<String>, Void> {
        /** */
        private String addVal;

        /**
         * @param addVal Value to add.
         */
        private AddEntryProcessor(String addVal) {
            this.addVal = addVal;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<String, Set<String>> entry, Object... arguments) throws EntryProcessorException {
            Set<String> set = entry.getValue();

            if (set == null)
                set = new HashSet<>();

            set.add(addVal);

            entry.setValue(set);

            return null;
        }
    }
}
