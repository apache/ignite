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

package org.apache.ignite.internal.processors.cache;

import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Cache iterator performance test.
 */
public class GridCacheIteratorPerformanceTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Large entry count. */
    private static final int LARGE_ENTRY_CNT = 100000;

    /** Small entry count. */
    private static final int SMALL_ENTRY_CNT = 10000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid();
    }

    /**
     * Iterates over cache.
     *
     * @param cache Projection.
     * @param c Visitor closure.
     */
    private void iterate(IgniteCache<Integer, Integer> cache, IgniteInClosure<Cache.Entry<Integer, Integer>> c) {
        for (Cache.Entry<Integer, Integer> entry : cache.localEntries())
            c.apply(entry);
    }

    /**
     * @return Empty filter.
     */
    private IgniteInClosure<Cache.Entry<Integer, Integer>> emptyFilter() {
        return new CI1<Cache.Entry<Integer, Integer>>() {
            @Override public void apply(Cache.Entry<Integer, Integer> e) {
                // No-op
            }
        };
    }

    /**
     * @throws Exception If failed.
     */
    public void testSmall() throws Exception {
        IgniteCache<Integer, Integer> cache = grid().cache(null);

        for (int i = 0; i < SMALL_ENTRY_CNT; i++)
            cache.put(i, i);

        assert cache.size() == SMALL_ENTRY_CNT;

        IgniteInClosure<Cache.Entry<Integer, Integer>> c = emptyFilter();

        // Warmup.
        for (int i = 0; i < 10; i ++)
            iterate(cache, c);

        long start = System.currentTimeMillis();

        iterate(cache, c);

        long time = System.currentTimeMillis() - start;

        X.println(">>>");
        X.println(">>> Iterated over " + cache.size() + " entries.");
        X.println(">>> Iteration time: " + time + "ms.");
        X.println(">>>");
    }

    /**
     * @throws Exception If failed.
     */
    public void testLarge() throws Exception {
        IgniteCache<Integer, Integer> cache = grid().cache(null);

        for (int i = 0; i < LARGE_ENTRY_CNT; i++)
            cache.put(i, i);

        assert cache.size() == LARGE_ENTRY_CNT;

        IgniteInClosure<Cache.Entry<Integer, Integer>> c = emptyFilter();

        // Warmup.
        for (int i = 0; i < 3; i++)
            iterate(cache, c);

        long start = System.currentTimeMillis();

        iterate(cache, c);

        long time = System.currentTimeMillis() - start;

        X.println(">>>");
        X.println(">>> Iterated over " + cache.size() + " entries.");
        X.println(">>> Iteration time: " + time + "ms.");
        X.println(">>>");
    }

    /**
     * @throws Exception If failed.
     */
    public void testFiltered() throws Exception {
        IgniteCache<Integer, Integer> cache = grid().cache(null);

        for (int i = 0; i < LARGE_ENTRY_CNT; i++)
            cache.put(i, i);

        assert cache.size() == LARGE_ENTRY_CNT;

        final BoxedInt cnt = new BoxedInt();

        IgniteInClosure<Cache.Entry<Integer, Integer>> c = new CI1<Cache.Entry<Integer, Integer>>() {
            @Override public void apply(Cache.Entry<Integer, Integer> t) {
                if (t.getValue() < SMALL_ENTRY_CNT)
                    cnt.increment();
            }
        };

        assert cache.size() == LARGE_ENTRY_CNT;

        // Warmup.
        for (int i = 0; i < 3; i++)
            iterate(cache, c);

        cnt.reset();

        long start = System.currentTimeMillis();

        iterate(cache, c);

        long time = System.currentTimeMillis() - start;

        X.println(">>>");
        X.println(">>> Iterated over " + cache.size() + " entries, accepted " + cnt.get() + " entries.");
        X.println(">>> Iteration time: " + time + "ms.");
        X.println(">>>");
    }

    /**
     * Boxed integer.
     */
    private static class BoxedInt {
        /** */
        private int i;

        /**
         * @param i Integer.
         */
        BoxedInt(int i) {
            this.i = i;
        }

        /**
         * Default constructor.
         */
        BoxedInt() {
            // No-op.
        }

        /**
         * @return Integer.
         */
        int increment() {
            return ++i;
        }

        /**
         * @return Integer.
         */
        int get() {
            return i;
        }

        /**
         * Resets integer.
         */
        void reset() {
            i = 0;
        }
    }
}