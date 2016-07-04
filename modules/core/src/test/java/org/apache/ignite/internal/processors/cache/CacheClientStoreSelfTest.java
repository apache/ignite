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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
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
    private volatile boolean nearEnabled;

    /** */
    private volatile Factory<CacheStore> factory;

    /** */
    private volatile CacheMode cacheMode;

    /** */
    private static volatile boolean loadedFromClient;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        boolean client = gridName != null && gridName.startsWith("client");

        cfg.setClientMode(client);

        CacheConfiguration cc = new CacheConfiguration();

        cc.setName(CACHE_NAME);
        cc.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cc.setCacheMode(cacheMode);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setBackups(1);

        cc.setCacheStoreFactory(factory);

        if (factory instanceof Factory3)
            cc.setReadThrough(true);

        if (client && nearEnabled)
            cc.setNearConfiguration(new NearCacheConfiguration());

        cfg.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        loadedFromClient = false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCorrectStore() throws Exception {
        nearEnabled = false;
        cacheMode = CacheMode.PARTITIONED;
        factory = new Factory1();

        startGrids(2);

        Ignite ignite = startGrid("client-1");

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
        nearEnabled = false;
        cacheMode = CacheMode.PARTITIONED;
        factory = new Factory1();

        startGrids(2);

        factory = new Factory2();

        startGrid("client-1");
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisabledConsistencyCheck() throws Exception {
        nearEnabled = false;
        cacheMode = CacheMode.PARTITIONED;
        factory = new Factory1();

        startGrids(2);

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
        cacheMode = CacheMode.PARTITIONED;
        factory = new Factory1();

        startGrids(2);

        doTestNoStore();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoStoreNearEnabled() throws Exception {
        nearEnabled = true;
        cacheMode = CacheMode.PARTITIONED;
        factory = new Factory1();

        startGrids(2);

        doTestNoStore();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestNoStore() throws Exception {
        factory = null;

        Ignite ignite = startGrid("client-1");

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
     * Load cache created on client as LOCAL and see if it only loaded on client
     *
     * @throws Exception
     */
    public void testLocalLoadClient() throws Exception {
        cacheMode = CacheMode.LOCAL;
        factory = new Factory3();

        startGrids(2);

        Ignite client = startGrid("client-1");

        IgniteCache cache = client.cache(CACHE_NAME);

        cache.loadCache(null);

        assertEquals(10, cache.localSize(CachePeekMode.ALL));

        assertEquals(0, grid(0).cache(CACHE_NAME).localSize(CachePeekMode.ALL));
        assertEquals(0, grid(1).cache(CACHE_NAME).localSize(CachePeekMode.ALL));

        assert loadedFromClient;
    }

    /**
     * Load cache from server that created on client as LOCAL and see if it only loaded on server
     *
     * @throws Exception
     */
    public void testLocalLoadServer() throws Exception {
        cacheMode = CacheMode.LOCAL;
        factory = new Factory3();

        startGrids(2);

        Ignite client = startGrid("client-1");

        IgniteCache cache = grid(0).cache(CACHE_NAME);

        cache.loadCache(null);

        assertEquals(10, cache.localSize(CachePeekMode.ALL));
        assertEquals(0, grid(1).cache(CACHE_NAME).localSize(CachePeekMode.ALL));
        assertEquals(0, client.cache(CACHE_NAME).localSize(CachePeekMode.ALL));

        assert !loadedFromClient : "Loaded data from client!";
    }

    /**
     * Load cache created on client as REPLICATED and see if it only loaded on servers
     */
    public void testReplicatedLoadFromClient() throws Exception {
        cacheMode = CacheMode.REPLICATED;
        factory = new Factory3();

        startGrids(2);

        Ignite client = startGrid("client-1");

        IgniteCache cache = client.cache(CACHE_NAME);

        cache.loadCache(null);

        assertEquals(0, cache.localSize(CachePeekMode.ALL));

        assertEquals(10, grid(0).cache(CACHE_NAME).localSize(CachePeekMode.ALL));
        assertEquals(10, grid(1).cache(CACHE_NAME).localSize(CachePeekMode.ALL));

        assert !loadedFromClient : "Loaded data from client!";
    }

    /**
     * Load cache created on client as REPLICATED and see if it only loaded on servers
     */
    public void testPartitionedLoadFromClient() throws Exception {
        cacheMode = CacheMode.PARTITIONED;
        factory = new Factory3();

        startGrids(2);

        Ignite client = startGrid("client-1");

        IgniteCache cache = client.cache(CACHE_NAME);

        cache.loadCache(null);

        assertEquals(0, cache.localSize(CachePeekMode.ALL));

        assertEquals(10, grid(0).cache(CACHE_NAME).localSize(CachePeekMode.ALL));
        assertEquals(10, grid(1).cache(CACHE_NAME).localSize(CachePeekMode.ALL));

        assert !loadedFromClient : "Loaded data from client!";
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
        @IgniteInstanceResource
        private Ignite ignite;

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
            if (ignite.cluster().localNode().isClient())
                loadedFromClient = true;

            for (int i = 0; i < 10; i++)
                clo.apply(i, i);
        }
    }
}