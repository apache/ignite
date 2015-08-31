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

import javax.cache.configuration.Factory;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;

/**
 * Tests for cache client without store.
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
    private static class EP implements CacheEntryProcessor {
        @Override public Object process(MutableEntry entry, Object... arguments) {
            return null;
        }
    }
}