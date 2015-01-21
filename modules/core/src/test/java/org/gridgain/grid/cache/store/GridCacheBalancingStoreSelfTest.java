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

package org.gridgain.grid.cache.store;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

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

        final GridCacheStoreBalancingWrapper<Integer, Integer> w =
            new GridCacheStoreBalancingWrapper<>(new VerifyStore(range));

        final AtomicBoolean finish = new AtomicBoolean();

        IgniteFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new IgniteCallable<Void>() {
            @Override public Void call() throws Exception {
                try {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!finish.get()) {
                        int cnt = rnd.nextInt(GridCacheStoreBalancingWrapper.DFLT_LOAD_ALL_THRESHOLD) + 1;

                        if (cnt == 1) {
                            int key = rnd.nextInt(range);

                            assertEquals((Integer)key, w.load(null, key));
                        }
                        else {
                            Collection<Integer> keys = new HashSet<>(cnt);

                            for (int i = 0; i < cnt; i++)
                                keys.add(rnd.nextInt(range));

                            final Map<Integer, Integer> loaded = new HashMap<>();

                            w.loadAll(null, keys, new CI2<Integer, Integer>() {
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
    private static class VerifyStore implements GridCacheStore<Integer, Integer> {
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
        @Nullable @Override public Integer load(@Nullable IgniteTx tx, Integer key) throws IgniteCheckedException {
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
        @Override public void loadCache(IgniteBiInClosure<Integer, Integer> clo, @Nullable Object... args)
            throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void loadAll(@Nullable IgniteTx tx, Collection<? extends Integer> keys,
            IgniteBiInClosure<Integer, Integer> c) throws IgniteCheckedException {
            for (Integer key : keys) {
                boolean res = locks[key].tryLock();

                if (res) {
                    try {
                        c.apply(key, key);
                    }
                    finally {
                        locks[key].unlock();
                    }
                }
                else
                    fail("Failed to acquire lock for key: " + key);
            }
        }

        /** {@inheritDoc} */
        @Override public void put(@Nullable IgniteTx tx, Integer key, Integer val) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void putAll(@Nullable IgniteTx tx, Map<? extends Integer, ? extends Integer> map)
            throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable IgniteTx tx, Integer key) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void removeAll(@Nullable IgniteTx tx, Collection<? extends Integer> keys)
            throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void txEnd(IgniteTx tx, boolean commit) throws IgniteCheckedException {
            // No-op.
        }
    }
}
