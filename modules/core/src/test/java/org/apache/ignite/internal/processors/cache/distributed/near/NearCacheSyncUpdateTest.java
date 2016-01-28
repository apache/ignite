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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK;
import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class NearCacheSyncUpdateTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearCacheSyncUpdateAtomic1() throws Exception {
        nearCacheSyncUpdateTx(ATOMIC, CLOCK);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearCacheSyncUpdateAtomic2() throws Exception {
        nearCacheSyncUpdateTx(ATOMIC, PRIMARY);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearCacheSyncUpdateTx() throws Exception {
        nearCacheSyncUpdateTx(TRANSACTIONAL, null);
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @param writeOrderMode Write order mode.
     * @throws Exception If failed.
     */
    private void nearCacheSyncUpdateTx(CacheAtomicityMode atomicityMode,
        CacheAtomicWriteOrderMode writeOrderMode) throws Exception {
        final IgniteCache<Integer, Integer> cache =
            ignite(0).createCache(cacheConfiguration(atomicityMode, writeOrderMode));

        try {
            final AtomicInteger idx = new AtomicInteger();

            final int KEYS_PER_THREAD = 5000;

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int idx0 = idx.getAndIncrement();

                    int startKey = KEYS_PER_THREAD * idx0;

                    for (int i = startKey; i < startKey + KEYS_PER_THREAD; i++) {
                        cache.put(i, i);

                        assertEquals(i, (Object)cache.localPeek(i));

                        cache.remove(i);

                        assertNull(cache.get(i));
                    }

                    final int BATCH_SIZE = 50;

                    Map<Integer, Integer> map = new TreeMap<>();

                    for (int i = startKey; i < startKey + KEYS_PER_THREAD; i++) {
                        map.put(i, i);

                        if (map.size() == BATCH_SIZE) {
                            cache.putAll(map);

                            for (Integer key : map.keySet())
                                assertEquals(key, cache.localPeek(key));

                            cache.removeAll(map.keySet());

                            for (Integer key : map.keySet())
                                assertNull(cache.get(key));

                            map.clear();
                        }
                    }

                    return null;
                }
            }, 10, "update-thread");
        }
        finally {
            ignite(0).destroyCache(null);
        }
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @param writeOrderMode Write order mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(CacheAtomicityMode atomicityMode,
        CacheAtomicWriteOrderMode writeOrderMode) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setAtomicWriteOrderMode(writeOrderMode);
        ccfg.setNearConfiguration(new NearCacheConfiguration<Integer, Integer>());

        return ccfg;
    }
}
