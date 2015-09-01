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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;

/**
 * Test that store is called correctly on puts.
 */
public class GridCachePartitionedStorePutSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final AtomicInteger CNT = new AtomicInteger(0);

    /** */
    private IgniteCache<Integer, Integer> cache1;

    /** */
    private IgniteCache<Integer, Integer> cache2;

    /** */
    private IgniteCache<Integer, Integer> cache3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(discoverySpi());
        cfg.setCacheConfiguration(cacheConfiguration());
        cfg.setUserAttributes(F.asMap(IDX_ATTR, CNT.getAndIncrement()));

        return cfg;
    }

    /**
     * @return Discovery SPI.
     */
    private DiscoverySpi discoverySpi() {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        return spi;
    }

    /**
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setCacheStoreFactory(singletonFactory(new TestStore()));
        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);
        cfg.setLoadPreviousValue(true);
        cfg.setAffinity(new GridCacheModuloAffinityFunction(3, 1));
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cache1 = startGrid(1).cache(null);
        cache2 = startGrid(2).cache(null);
        cache3 = startGrid(3).cache(null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutx() throws Throwable {
        info("Putting to the first node.");

        cache1.put(0, 1);

        info("Putting to the second node.");

        cache2.put(0, 2);

        info("Putting to the third node.");

        cache3.put(0, 3);
    }

    /**
     * Test store.
     */
    private static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(javax.cache.Cache.Entry<? extends Object, ? extends Object> e) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op
        }
    }
}