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

package org.apache.ignite.cache.store;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.Cache;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheStoreBalancingWrapper;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Store test.
 */
public class GridCacheBalancingStoreSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testLoads() throws Exception {
        final int range = 300;

        final AtomicInteger cycles = new AtomicInteger();
        final AtomicReference<Exception> err = new AtomicReference<>();

        final CacheStoreBalancingWrapper<Integer, Integer> w =
            new CacheStoreBalancingWrapper<>(new VerifyStore(range));

        final AtomicBoolean finish = new AtomicBoolean();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                try {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!finish.get()) {
                        int cnt = rnd.nextInt(CacheStoreBalancingWrapper.DFLT_LOAD_ALL_THRESHOLD) + 1;

                        if (cnt == 1) {
                            int key = rnd.nextInt(range);

                            assertEquals((Integer)key, w.load(key));
                        }
                        else {
                            Collection<Integer> keys = new HashSet<>(cnt);

                            for (int i = 0; i < cnt; i++)
                                keys.add(rnd.nextInt(range));

                            final Map<Integer, Integer> loaded = new HashMap<>();

                            w.loadAll(keys, new CI2<Integer, Integer>() {
                                @Override public void apply(Integer k, Integer v) {
                                    loaded.put(k, v);
                                }
                            });

                            for (Integer key : keys)
                                assertEquals(key, loaded.get(key));
                        }

                        int c = cycles.incrementAndGet();

                        if (c > 0 && c % 2_000_000 == 0)
                            info("Finished cycles: " + c);
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();

                    err.compareAndSet(null, e);
                }

                return null;
            }
        }, 10, "test");

        try {
            Thread.sleep(30_000);
        }
        finally {
            finish.set(true);
        }

        fut.get();

        if (err.get() != null)
            throw err.get();

        info("Total: " + cycles.get());
    }

    /**
     *
     */
    private static class VerifyStore implements CacheStore<Integer, Integer> {
        /** */
        private Lock[] locks;

        /**
         * @param range Range.
         */
        private VerifyStore(int range) {
            locks = new Lock[range];

            for (int i = 0; i < locks.length; i++)
                locks[i] = new ReentrantLock();
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer load(Integer key) {
            boolean res = locks[key].tryLock();

            if (res) {
                try {
                    return key;
                }
                finally {
                    locks[key].unlock();
                }
            }
            else
                fail("Failed to acquire lock for key: " + key);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Integer> clo, @Nullable Object... args) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, Integer> loadAll(Iterable<? extends Integer> keys) {
            Map<Integer, Integer> loaded = new HashMap<>();

            for (Integer key : keys) {
                boolean res = locks[key].tryLock();

                if (res) {
                    try {
                        loaded.put(key, key);
                    }
                    finally {
                        locks[key].unlock();
                    }
                }
                else
                    fail("Failed to acquire lock for key: " + key);
            }

            return loaded;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends Integer>> entries) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) {
            // No-op.
        }
    }
}