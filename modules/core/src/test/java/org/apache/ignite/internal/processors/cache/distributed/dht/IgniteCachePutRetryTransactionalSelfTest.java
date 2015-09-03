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
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

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

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLongRetries() throws Exception {
        final AtomicBoolean finished = new AtomicBoolean();

        IgniteAtomicLong atomic = ignite(0).atomicLong("TestAtomic", 0, true);

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
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

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    public void testExplicitTransactionRetries() throws Exception {
        final AtomicInteger idx = new AtomicInteger();
        int threads = 8;

        final AtomicReferenceArray<Exception> err = new AtomicReferenceArray<>(threads);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
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
                }
                catch (Exception e) {
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
        for (int g = 0; g < gridCount(); g++) {
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