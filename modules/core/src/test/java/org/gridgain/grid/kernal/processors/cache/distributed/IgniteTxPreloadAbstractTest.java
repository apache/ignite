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

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import javax.cache.processor.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.transactions.IgniteTxConcurrency.*;

/**
 * Tests transaction during cache preloading.
 */
public abstract class IgniteTxPreloadAbstractTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 6;

    /** */
    private static volatile boolean keyNotLoaded;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        keyNotLoaded = false;

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteTxPreloading() throws Exception {
        IgniteCache<String, Integer> cache = jcache(0);

        for (int i = 0; i < 10000; i++)
            cache.put(String.valueOf(i), 0);

        final AtomicInteger gridIdx = new AtomicInteger(1);

        IgniteFuture<?> fut = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    int idx = gridIdx.getAndIncrement();

                    startGrid(idx);

                    return null;
                }
            },
            GRID_CNT - 1,
            "grid-starter-" + getName()
        );

        waitForRemoteNodes(grid(0), 2);

        Set<String> keys = new HashSet<>();

        for (int i = 0; i < 10; i++)
            keys.add(String.valueOf(i * 1000));

        cache.invokeAll(keys, new EntryProcessor<String, Integer, Void>() {
            @Override public Void process(MutableEntry<String, Integer> e, Object... args) {
                Integer val = e.getValue();

                if (val == null) {
                    keyNotLoaded = true;

                    e.setValue(1);

                    return null;
                }

                e.setValue(val + 1);

                return null;
            }
        });

        assertFalse(keyNotLoaded);

        fut.get();

        for (int i = 0; i < GRID_CNT; i++)
            // Wait for preloader.
            cache(i).forceRepartition().get();

        for (int i = 0; i < GRID_CNT; i++) {
            for (String key : keys)
                assertEquals("Unexpected value for cache " + i, (Integer)1, cache(i).get(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalTxPreloadingOptimistic() throws Exception {
        testLocalTxPreloading(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalTxPreloadingPessimistic() throws Exception {
        testLocalTxPreloading(PESSIMISTIC);
    }

    /**
     * Tries to execute transaction doing transform when target key is not yet preloaded.
     *
     * @param txConcurrency Transaction concurrency;
     * @throws Exception If failed.
     */
    private void testLocalTxPreloading(IgniteTxConcurrency txConcurrency) throws Exception {
        Map<String, Integer> map = new HashMap<>();

        for (int i = 0; i < 10000; i++)
            map.put(String.valueOf(i), 0);

        IgniteCache<String, Integer> cache0 = jcache(0);

        cache0.putAll(map);

        final String TX_KEY = "9000";

        int expVal = 0;

        for (int i = 1; i < GRID_CNT; i++) {
            assertEquals((Integer)expVal, cache0.get(TX_KEY));

            startGrid(i);

            IgniteCache<String, Integer> cache = jcache(i);

            IgniteTransactions txs = ignite(i).transactions();

            try (IgniteTx tx = txs.txStart(txConcurrency, IgniteTxIsolation.READ_COMMITTED)) {
                cache.invoke(TX_KEY, new EntryProcessor<String, Integer, Void>() {
                    @Override public Void process(MutableEntry<String, Integer> e, Object... args) {
                        Integer val = e.getValue();

                        if (val == null) {
                            keyNotLoaded = true;

                            e.setValue(1);

                            return null;
                        }

                        e.setValue(val + 1);

                        return null;
                    }
                });

                tx.commit();
            }

            assertFalse(keyNotLoaded);

            expVal++;

            assertEquals((Integer)expVal, cache.get(TX_KEY));
        }

        for (int i = 0; i < GRID_CNT; i++)
            assertEquals("Unexpected value for cache " + i, (Integer)expVal, cache(i).get(TX_KEY));
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setPreloadMode(GridCachePreloadMode.ASYNC);

        cfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);

        cfg.setStore(null);

        return cfg;
    }
}
