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
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.IgniteReflectionFactory;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;

/**
 * Tests for cache client with and without store.
 */
public class CacheClientStoreSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "test-cache";

    /** */
    private boolean client;

    /** */
    private boolean nearEnabled;

    /** */
    private Factory<CacheStore> factory;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(client);

        CacheConfiguration cc = new CacheConfiguration();

        cc.setName(CACHE_NAME);
        cc.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cc.setCacheStoreFactory(factory);

        if (client && nearEnabled)
            cc.setNearConfiguration(new NearCacheConfiguration());

        cfg.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        client = false;
        factory = new Factory1();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCorrectStore() throws Exception {
        client = true;
        nearEnabled = false;
        factory = new Factory1();

        Ignite ignite = startGrid();

        IgniteCache cache = ignite.cache(CACHE_NAME);

        cache.get(0);
        cache.getAll(F.asSet(0, 1));
        cache.getAndPut(0, 0);
        cache.getAndPutIfAbsent(0, 0);
        cache.getAndRemove(0);
        cache.getAndReplace(0, 0);
        cache.put(0, 0);
        cache.putAll(F.asMap(0, 0, 1, 1));
        cache.putIfAbsent(0, 0);
        cache.remove(0);
        cache.remove(0, 0);
        cache.removeAll(F.asSet(0, 1));
        cache.removeAll();
        cache.invoke(0, new EP());
        cache.invokeAll(F.asSet(0, 1), new EP());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvalidStore() throws Exception {
        client = true;
        nearEnabled = false;
        factory = new Factory2();

        startGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisabledConsistencyCheck() throws Exception {
        client = false;
        nearEnabled = false;
        factory = new Factory2();

        System.setProperty(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK, "true");

        startGrid("client-1");

        factory = new Factory1();

        System.clearProperty(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK);

        startGrid("client-2");
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoStoreNearDisabled() throws Exception {
        nearEnabled = false;

        doTestNoStore();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoStoreNearEnabled() throws Exception {
        nearEnabled = true;

        doTestNoStore();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestNoStore() throws Exception {
        client = true;
        factory = null;

        Ignite ignite = startGrid();

        IgniteCache cache = ignite.cache(CACHE_NAME);

        cache.get(0);
        cache.getAll(F.asSet(0, 1));
        cache.getAndPut(0, 0);
        cache.getAndPutIfAbsent(0, 0);
        cache.getAndRemove(0);
        cache.getAndReplace(0, 0);
        cache.put(0, 0);
        cache.putAll(F.asMap(0, 0, 1, 1));
        cache.putIfAbsent(0, 0);
        cache.remove(0);
        cache.remove(0, 0);
        cache.removeAll(F.asSet(0, 1));
        cache.removeAll();
        cache.invoke(0, new EP());
        cache.invokeAll(F.asSet(0, 1), new EP());
    }

    /**
     * Returns a cache configuration with TestStore as cache store and
     * read through is enabled based on given params
     *
     * @param cacheName Name of the cache that configuration is for
     * @param cacheMode Cache mode for the configuration
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private  CacheConfiguration cacheConfiguration(String cacheName, CacheMode cacheMode) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(cacheName);
        cfg.setCacheMode(cacheMode);
        cfg.setCacheStoreFactory(new Factory3());
        cfg.setReadThrough(true);

        return cfg;
    }

    /**
     * Checks for if a grid with {clientName} is already exist and
     * creates a grid as client if not
     *
     * @param clientName Name for the client grid
     * @throws Exception
     */
    private void startClientGridIfNot(String clientName) throws Exception {
        // Try to get grid with name clientName. Otherwise start one.
        try {
            grid(clientName);
        } catch (IgniteIllegalStateException e) {
            client = true;

            startGrid(clientName);
        }
     }

    /**
     * Load cache created on client as LOCAL and see if it only loaded on client
     *
     * @throws Exception
     */
    public void testLocalLoadClient() throws Exception {
        startClientGridIfNot("client-3");

        CacheConfiguration localCache = cacheConfiguration("localCache1", CacheMode.LOCAL);
        IgniteCache<Object, Object> cache = grid("client-3").createCache(localCache);
        cache.loadCache(null);

        assertEquals(cache.localSize(), 10) ;
        assertEquals(grid(0).cache("localCache1").localSize(), 0) ;
        assertEquals(grid(1).cache("localCache1").localSize(), 0) ;
    }

    /**
     * Load cache from server that created on client as LOCAL and see if it only loaded on server
     *
     * @throws Exception
     */
    public void testLocalLoadServer() throws Exception {
        startClientGridIfNot("client-3");

        CacheConfiguration localCache = cacheConfiguration("localCache2", CacheMode.LOCAL);
        grid("client-3").createCache(localCache);

        IgniteCache<Object, Object> cache = grid(0).cache("localCache2");
        cache.loadCache(null);

        assertEquals(grid("client-3").cache("localCache2").localSize(), 0) ;
        assertEquals(cache.localSize(), 10) ;
    }

    /**
     * Load cache from client that created on server as LOCAL and see if it only loaded on client
     *
     * @throws Exception
     */
    public void testLocalLoadCreateServer() throws Exception {
        startClientGridIfNot("client-3");

        CacheConfiguration localCache = cacheConfiguration("localCache3", CacheMode.LOCAL);
        grid(0).createCache(localCache);

        IgniteCache<Object, Object> cache = grid("client-3").cache("localCache3");
        cache.loadCache(null);

        assertEquals(grid(0).cache("localCache3").localSize(), 0) ;
        assertEquals(grid(1).cache("localCache3").localSize(), 0) ;
        assertEquals(cache.localSize(), 10) ;
    }

    /**
     * Load cache created on client as REPLICATED and see if it only loaded on servers
     */
    public void testReplicatedLoadFromClient() throws Exception {
        startClientGridIfNot("client-3");

        CacheConfiguration replicatedCache = cacheConfiguration("replicatedCache1", CacheMode.REPLICATED);
        IgniteCache<Object, Object> cache = grid("client-3").createCache(replicatedCache);
        cache.loadCache(null);

        assertEquals(cache.localSize(), 0) ;
        assertEquals(grid(0).cache("replicatedCache1").localSize()
                + grid(1).cache("replicatedCache1").localSize(), 10) ;
    }

    /**
     * Load cache created on server as REPLICATED and see if it only loaded on servers
     *
     * @throws Exception
     */
    public void testReplicatedLoadFromServer() throws Exception {
        startClientGridIfNot("client-3");

        CacheConfiguration replicatedCache = cacheConfiguration("replicatedCache2", CacheMode.REPLICATED);
        grid(0).createCache(replicatedCache);

        IgniteCache<Object, Object> cache = grid("client-3").cache("replicatedCache2");
        cache.loadCache(null);

        assertEquals(cache.localSize(), 0) ;
        assertEquals(grid(0).cache("replicatedCache2").localSize()
                + grid(1).cache("replicatedCache2").localSize(), 10) ;
    }

    /**
     */
    private static class Factory1 implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return null;
        }
    }

    /**
     */
    private static class Factory2 implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return null;
        }
    }

    /**
     */
    private static class Factory3 implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new TestStore();
        }
    }

    /**
     */
    private static class EP implements CacheEntryProcessor {
        @Override public Object process(MutableEntry entry, Object... arguments) {
            return null;
        }
    }

    /**
     * Test store that loads 10 item
     */
    public static class TestStore extends CacheStoreAdapter<Object, Object> {
        @Override
        public Integer load(Object key) throws CacheLoaderException {
            return null;
        }

        @Override
        public void write(Cache.Entry<? extends Object, ? extends Object> entry) throws CacheWriterException {
        }

        @Override
        public void delete(Object key) throws CacheWriterException {
        }

        @Override
        public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            U.dumpStack();

            for (int i = 0; i < 10; i++)
                clo.apply(i, i);
        }
    }
}