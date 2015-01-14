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

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Load cache test.
 */
public class GridCachePartitionedLoadCacheSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grids count. */
    private static final int GRID_CNT = 3;

    /** Puts count. */
    private static final int PUT_CNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setStore(new TestStore());
        cache.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalLoadCache() throws Exception {
        try {
            startGridsMultiThreaded(GRID_CNT);

            IgniteCache<Integer, String> cache = jcache(0);

            cache.localLoadCache(null, PUT_CNT);

            GridCache<Integer, String> cache0 = cache(0);

            GridCacheAffinity aff = cache0.affinity();

            int[] parts = aff.allPartitions(grid(0).localNode());

            int cnt1 = 0;

            for (int i = 0; i < PUT_CNT; i++) {
                if (U.containsIntArray(parts, aff.partition(i)))
                    cnt1++;
            }

            info("Number of keys to load: " + cnt1);

            int cnt2 = 0;

            for (GridCacheEntry<Integer, String> e : cache0.entrySet()) {
                assert e.primary() || e.backup();

                cnt2++;
            }

            info("Number of keys loaded: " + cnt2);

            assertEquals(cnt1, cnt2);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test store.
     */
    private static class TestStore extends GridCacheStoreAdapter<Integer, String> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, String> clo,
            @Nullable Object... args) throws IgniteCheckedException {
            assert clo != null;
            assert args != null;

            Integer cnt = (Integer)args[0];

            assert cnt != null;

            for (int i = 0; i < cnt; i++)
                clo.apply(i, "val" + i);
        }

        /** {@inheritDoc} */
        @Override public String load(IgniteTx tx, Integer key) throws IgniteCheckedException {
            // No-op.

            return null;
        }

        /** {@inheritDoc} */
        @Override public void put(IgniteTx tx, Integer key, String val) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(IgniteTx tx, Integer key) throws IgniteCheckedException {
            // No-op.
        }
    }
}
