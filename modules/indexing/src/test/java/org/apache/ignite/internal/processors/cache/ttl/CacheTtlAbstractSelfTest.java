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

package org.apache.ignite.internal.processors.cache.ttl;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.eviction.lru.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.configuration.*;
import javax.cache.expiry.*;
import java.util.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * TTL test.
 */
public abstract class CacheTtlAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int MAX_CACHE_SIZE = 5;

    /** */
    private static final int SIZE = 11;

    /** */
    private static final long DEFAULT_TIME_TO_LIVE = 2000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cache = new CacheConfiguration();

        cache.setCacheMode(cacheMode());
        cache.setAtomicityMode(atomicityMode());
        cache.setMemoryMode(memoryMode());
        cache.setOffHeapMaxMemory(0);
        cache.setEvictionPolicy(new LruEvictionPolicy(MAX_CACHE_SIZE));
        cache.setIndexedTypes(Integer.class, Integer.class);

        cache.setExpiryPolicyFactory(
            FactoryBuilder.factoryOf(new TouchedExpiryPolicy(new Duration(MILLISECONDS, DEFAULT_TIME_TO_LIVE))));

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @return Atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @return Memory mode.
     */
    protected abstract CacheMemoryMode memoryMode();

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return GridCount
     */
    protected abstract int gridCount();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultTimeToLivePut() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache(0);

        List<Integer> keys = primaryKeys(cache, 1);

        cache.put(keys.get(0), 1);

        checkSizeBeforeLive(cache, 1);

        Thread.sleep(DEFAULT_TIME_TO_LIVE + 500);

        checkSizeAfterLive();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultTimeToLivePutAll() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache(0);

        Map<Integer, Integer> entries = new HashMap<>();

        List<Integer> keys = primaryKeys(cache, SIZE);

        for (int i = 0; i < SIZE; ++i)
            entries.put(keys.get(i), i);

        cache.putAll(entries);

        checkSizeBeforeLive(cache, SIZE);

        Thread.sleep(DEFAULT_TIME_TO_LIVE + 500);

        checkSizeAfterLive();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimeToLiveTtl() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache(0);

        long time = DEFAULT_TIME_TO_LIVE + 2000;

        List<Integer> keys = primaryKeys(cache, SIZE);

        for (int i = 0; i < SIZE; i++)
            cache.withExpiryPolicy(new TouchedExpiryPolicy(new Duration(MILLISECONDS, time))).
                put(keys.get(i), i);

        checkSizeBeforeLive(cache, SIZE);

        Thread.sleep(DEFAULT_TIME_TO_LIVE + 500);

        checkSizeBeforeLive(cache, SIZE);

        Thread.sleep(time - DEFAULT_TIME_TO_LIVE + 500);

        checkSizeAfterLive();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkSizeBeforeLive(IgniteCache<Integer, Integer> cache, int size) throws Exception {
        if (memoryMode() == CacheMemoryMode.OFFHEAP_TIERED) {
            assertEquals(0, cache.localSize(CachePeekMode.ONHEAP));
            assertEquals(size, cache.localSize(CachePeekMode.OFFHEAP));
        }
        else {
            assertEquals(size > MAX_CACHE_SIZE ? MAX_CACHE_SIZE : size, cache.localSize(CachePeekMode.ONHEAP));
            assertEquals(size > MAX_CACHE_SIZE ? size - MAX_CACHE_SIZE : 0, cache.localSize(CachePeekMode.OFFHEAP));
        }

        assertFalse(cache.query(new SqlQuery<>(Integer.class, "_val >= 0")).getAll().isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    private void checkSizeAfterLive() throws Exception {
        for (int i = 0; i < gridCount(); ++i) {
            IgniteCache<Integer, Integer> cache = jcache(i);

            assertEquals(0, cache.localSize());
            assertEquals(0, cache.localSize(CachePeekMode.OFFHEAP));
            assertEquals(0, cache.localSize(CachePeekMode.SWAP));
            assertEquals(0, cache.query(new SqlQuery<>(Integer.class, "_val >= 0")).getAll().size());
        }
    }
}
