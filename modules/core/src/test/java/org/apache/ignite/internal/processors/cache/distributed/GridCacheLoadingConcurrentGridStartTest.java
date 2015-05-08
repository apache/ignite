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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.integration.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Tests for cache data loading during simultaneous grids start.
 */
public class GridCacheLoadingConcurrentGridStartTest extends GridCommonAbstractTest {
    /** Grids count */
    private static int GRIDS_CNT = 5;

    /** Keys count */
    private static int KEYS_CNT = 1_000_000;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setBackups(1);

        CacheStore<Integer, String> store = new CacheStoreAdapter<Integer, String>() {
            @Override public void loadCache(IgniteBiInClosure<Integer, String> f, Object... args) {
                for (int i = 0; i < KEYS_CNT; i++)
                    f.apply(i, Integer.toString(i));
            }

            @Nullable @Override public String load(Integer i) throws CacheLoaderException {
                return null;
            }

            @Override public void write(Cache.Entry<? extends Integer, ? extends String> entry) throws CacheWriterException {
                // No-op.
            }

            @Override public void delete(Object o) throws CacheWriterException {
                // No-op.
            }
        };

        ccfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(store));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception if failed
     */
    public void testLoadCacheWithDataStreamer() throws Exception {
        IgniteInClosure<Ignite> f = new IgniteInClosure<Ignite>() {
            @Override public void apply(Ignite grid) {
                try (IgniteDataStreamer<Integer, String> dataStreamer = grid.dataStreamer(null)) {
                    for (int i = 0; i < KEYS_CNT; i++)
                        dataStreamer.addData(i, Integer.toString(i));
                }
            }
        };

        loadCache(f);
    }

    /**
     * @throws Exception if failed
     */
    public void testLoadCacheFromStore() throws Exception {
        loadCache(new IgniteInClosure<Ignite>() {
            @Override public void apply(Ignite grid) {
                grid.cache(null).loadCache(null);
            }
        });
    }

    /**
     * Loads cache using closure and asserts cache size.
     *
     * @param f cache loading closure
     * @throws Exception if failed
     */
    private void loadCache(IgniteInClosure<Ignite> f) throws Exception {
        Ignite g0 = startGrid(0);

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Ignite>() {
            @Override public Ignite call() throws Exception {
                return startGridsMultiThreaded(1, GRIDS_CNT - 1);
            }
        });

        try {
            f.apply(g0);
        }
        finally {
            fut.get();
        }

        assertCacheSize();
    }

    /** Asserts cache size. */
    private void assertCacheSize() {
        IgniteCache<Integer, String> cache = grid(0).cache(null);

        assertEquals(KEYS_CNT, cache.size(CachePeekMode.PRIMARY));

        int total = 0;

        for (int i = 0; i < GRIDS_CNT; i++)
            total += grid(i).cache(null).localSize(CachePeekMode.PRIMARY);

        assertEquals(KEYS_CNT, total);
    }
}
