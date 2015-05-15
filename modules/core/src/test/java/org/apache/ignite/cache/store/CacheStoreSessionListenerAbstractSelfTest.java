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

package org.apache.ignite.cache.store;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import javax.cache.configuration.*;
import java.io.*;
import java.util.concurrent.atomic.*;

/**
 * Tests for store session listeners.
 */
public abstract class CacheStoreSessionListenerAbstractSelfTest extends GridCommonAbstractTest implements Serializable {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected static final AtomicInteger loadCacheCnt = new AtomicInteger();

    /** */
    protected static final AtomicInteger loadCnt = new AtomicInteger();

    /** */
    protected static final AtomicInteger writeCnt = new AtomicInteger();

    /** */
    protected static final AtomicInteger deleteCnt = new AtomicInteger();

    /** */
    protected static final AtomicInteger reuseCnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        loadCacheCnt.set(0);
        loadCnt.set(0);
        writeCnt.set(0);
        deleteCnt.set(0);
        reuseCnt.set(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicCache() throws Exception {
        CacheConfiguration<Integer, Integer> cfg = cacheConfiguration(null, CacheAtomicityMode.ATOMIC);

        try (IgniteCache<Integer, Integer> cache = ignite(0).createCache(cfg)) {
            cache.loadCache(null);
            cache.get(1);
            cache.put(1, 1);
            cache.remove(1);
        }

        assertEquals(3, loadCacheCnt.get());
        assertEquals(1, loadCnt.get());
        assertEquals(1, writeCnt.get());
        assertEquals(1, deleteCnt.get());
        assertEquals(0, reuseCnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionalCache() throws Exception {
        CacheConfiguration<Integer, Integer> cfg = cacheConfiguration(null, CacheAtomicityMode.TRANSACTIONAL);

        try (IgniteCache<Integer, Integer> cache = ignite(0).createCache(cfg)) {
            cache.loadCache(null);
            cache.get(1);
            cache.put(1, 1);
            cache.remove(1);
        }

        assertEquals(3, loadCacheCnt.get());
        assertEquals(1, loadCnt.get());
        assertEquals(1, writeCnt.get());
        assertEquals(1, deleteCnt.get());
        assertEquals(0, reuseCnt.get());

    }

    /**
     * @throws Exception If failed.
     */
    public void testExplicitTransaction() throws Exception {
        CacheConfiguration<Integer, Integer> cfg = cacheConfiguration(null, CacheAtomicityMode.TRANSACTIONAL);

        try (IgniteCache<Integer, Integer> cache = ignite(0).createCache(cfg)) {
            try (Transaction tx = ignite(0).transactions().txStart()) {
                cache.put(1, 1);
                cache.put(2, 2);
                cache.remove(3);
                cache.remove(4);

                tx.commit();
            }
        }

        assertEquals(2, writeCnt.get());
        assertEquals(2, deleteCnt.get());
        assertEquals(3, reuseCnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheTransaction() throws Exception {
        CacheConfiguration<Integer, Integer> cfg1 = cacheConfiguration("cache1", CacheAtomicityMode.TRANSACTIONAL);
        CacheConfiguration<Integer, Integer> cfg2 = cacheConfiguration("cache2", CacheAtomicityMode.TRANSACTIONAL);

        try (
            IgniteCache<Integer, Integer> cache1 = ignite(0).createCache(cfg1);
            IgniteCache<Integer, Integer> cache2 = ignite(0).createCache(cfg2)
        ) {
            try (Transaction tx = ignite(0).transactions().txStart()) {
                cache1.put(1, 1);
                cache2.put(2, 2);
                cache1.remove(3);
                cache2.remove(4);

                tx.commit();
            }
        }

        assertEquals(2, writeCnt.get());
        assertEquals(2, deleteCnt.get());
        assertEquals(3, reuseCnt.get());
    }

    /**
     * @param name Cache name.
     * @param atomicity Atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(String name, CacheAtomicityMode atomicity) {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>();

        cfg.setName(name);
        cfg.setAtomicityMode(atomicity);
        cfg.setCacheStoreFactory(storeFactory());
        cfg.setCacheStoreSessionListenerFactories(sessionListenerFactory());
        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);
        cfg.setLoadPreviousValue(true);

        return cfg;
    }

    /**
     * @return Store factory.
     */
    protected abstract Factory<? extends CacheStore<Integer, Integer>> storeFactory();

    /**
     * @return Session listener factory.
     */
    protected abstract Factory<CacheStoreSessionListener> sessionListenerFactory();
}
