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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Basic store test.
 */
public abstract class GridCacheBasicStoreMultithreadedAbstractTest extends GridCommonAbstractTest {
    /** Cache store. */
    private GridCacheStore<Integer, Integer> store;

    /**
     *
     */
    protected GridCacheBasicStoreMultithreadedAbstractTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridCache<?, ?> cache = cache();

        if (cache != null)
            cache.clearAll();

        stopAllGrids();
    }

    /**
     * @return Caching mode.
     */
    protected abstract GridCacheMode cacheMode();

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setSwapEnabled(false);

        cc.setStore(store);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentGet() throws Exception {
        final AtomicInteger cntr = new AtomicInteger();

        store = new GridCacheStoreAdapter<Integer, Integer>() {
            @Override public Integer load(@Nullable IgniteTx tx, Integer key) {
                return cntr.incrementAndGet();
            }

            /** {@inheritDoc} */
            @Override public void put(IgniteTx tx, Integer key, @Nullable Integer val) {
                assert false;
            }

            /** {@inheritDoc} */
            @Override public void remove(IgniteTx tx, Integer key) {
                assert false;
            }
        };

        startGrid();

        final GridCache<Integer, Integer> cache = cache();

        int threads = 2;

        final CyclicBarrier barrier = new CyclicBarrier(threads);

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                barrier.await();

                cache.get(1);

                return null;
            }
        }, threads, "concurrent-get-worker");

        assertEquals(1, cntr.get());
    }
}
