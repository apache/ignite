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

package org.apache.ignite.internal.processors.cache.eviction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Swap benchmark.
 */
@SuppressWarnings("BusyWait")
public class GridCacheBatchEvictUnswapSelfTest extends GridCacheAbstractSelfTest {
    /** Eviction policy size. */
    public static final int EVICT_PLC_SIZE = 100000;

    /** Keys count. */
    public static final int KEYS_CNT = 100000;

    /** Batch size. */
    private static final int BATCH_SIZE = 200;

    /** Number of threads for concurrent benchmark + concurrency level. */
    private static final int N_THREADS = 8;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        // Let this test run 2 minutes as it runs for 20 seconds locally.
        return 2 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cacheCfg = super.cacheConfiguration(gridName);

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        CacheStore store = new CacheStoreAdapter<Long, String>() {
            @Nullable @Override public String load(Long key) {
                return null;
            }

            @Override public void loadCache(final IgniteBiInClosure<Long, String> c,
                @Nullable Object... args) {
                for (int i = 0; i < KEYS_CNT; i++)
                    c.apply((long)i, String.valueOf(i));
            }

            @Override public void write(javax.cache.Cache.Entry<? extends Long, ? extends String> val) {
                // No-op.
            }

            @Override public void delete(Object key) {
                // No-op.
            }
        };

        cacheCfg.setCacheStoreFactory(singletonFactory(store));
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxSize(EVICT_PLC_SIZE);

        cacheCfg.setEvictionPolicy(plc);
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setEvictSynchronized(false);
        cacheCfg.setNearConfiguration(null);

        return cacheCfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentEvictions() throws Exception {
        runConcurrentTest(grid(0), KEYS_CNT, BATCH_SIZE);
    }

    /**
     * @param g Grid instance.
     * @param keysCnt Number of keys to swap and promote.
     * @param batchSize Size of batch to swap/promote.
     * @throws Exception If failed.
     */
    private void runConcurrentTest(Ignite g, final int keysCnt, final int batchSize) throws Exception {
        assert keysCnt % batchSize == 0;

        final AtomicInteger evictedKeysCnt = new AtomicInteger();

        final IgniteCache<Object, Object> cache = g.cache(null);

        cache.loadCache(null, 0);

        info("Finished load cache.");

        IgniteInternalFuture<?> evictFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                Collection<Long> keys = new ArrayList<>(batchSize);

                int evictedBatches = 0;

                for (long i = 0; i < keysCnt; i++) {
                    keys.add(i);

                    if (keys.size() == batchSize) {
                        for (Long key : keys)
                            cache.localEvict(Collections.<Object>singleton(key));

                        evictedKeysCnt.addAndGet(batchSize);

                        keys.clear();

                        evictedBatches++;

                        if (evictedBatches % 100 == 0 && evictedBatches > 0)
                            info("Evicted " + (evictedBatches * batchSize) + " entries.");
                    }
                }
            }
        }, N_THREADS, "evict");

        final AtomicInteger unswappedKeys = new AtomicInteger();

        IgniteInternalFuture<?> unswapFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    Collection<Long> keys = new ArrayList<>(batchSize);

                    int unswappedBatches = 0;

                    for (long i = 0; i < keysCnt; i++) {
                        keys.add(i);

                        if (keys.size() == batchSize) {
                            for (Long key : keys)
                                cache.localPromote(Collections.singleton(key));

                            unswappedKeys.addAndGet(batchSize);

                            keys.clear();

                            unswappedBatches++;

                            if (unswappedBatches % 100 == 0 && unswappedBatches > 0)
                                info("Unswapped " + (unswappedBatches * batchSize) + " entries.");
                        }
                    }
                }
                catch (IgniteException e) {
                    e.printStackTrace();
                }
            }
        }, N_THREADS, "promote");

        evictFut.get();

        unswapFut.get();

        info("Clearing cache.");

        for (long i = 0; i < KEYS_CNT; i++)
            cache.remove(i);
    }
}