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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;

/**
 * Test for cache swap.
 */
public class GridCacheOffHeapSelfTest extends GridCommonAbstractTest {
    /** Saved versions. */
    private final Map<Integer, Object> versions = new HashMap<>();

    /** */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setNetworkTimeout(2000);

        CacheConfiguration<?,?> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setIndexedTypes(Integer.class, CacheValue.class);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setDeploymentMode(SHARED);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        versions.clear();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffHeapIterator() throws Exception {
        try {
            startGrids(1);

            grid(0);

            IgniteCache<Integer, Integer> cache = grid(0).cache(null);

            for (int i = 0; i < 100; i++) {
                info("Putting: " + i);

                cache.put(i, i);
            }

            int i = 0;

            for (Cache.Entry<Integer, Integer> e : cache.localEntries(CachePeekMode.OFFHEAP)) {
                Integer key = e.getKey();

                info("Key: " + key);

                i++;

                cache.remove(e.getKey());

                assertNull(cache.get(key));
            }

            assertEquals(100, i);

            assert cache.localSize() == 0;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class CacheValue {
        /** Value. */
        @QuerySqlField
        private final int val;

        /**
         * @param val Value.
         */
        private CacheValue(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheValue.class, this);
        }
    }
}