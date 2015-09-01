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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public class IgniteCacheConfigurationTemplateTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String TEMPLATE1 = "org.apache.ignite*";

    /** */
    private static final String TEMPLATE2 = "org.apache.ignite.test.*";

    /** */
    private static final String TEMPLATE3 = "org.apache.ignite.test2.*";

    /** */
    private boolean clientMode;

    /** */
    private boolean addTemplate;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder).setForceServerMode(true);

        if (addTemplate) {
            CacheConfiguration dfltCfg = new CacheConfiguration();

            dfltCfg.setAtomicityMode(TRANSACTIONAL);
            dfltCfg.setBackups(2);

            CacheConfiguration templateCfg1 = new CacheConfiguration();

            templateCfg1.setName(TEMPLATE1);
            templateCfg1.setBackups(3);

            CacheConfiguration templateCfg2 = new CacheConfiguration();

            templateCfg2.setName(TEMPLATE2);
            templateCfg2.setBackups(4);

            cfg.setCacheConfiguration(dfltCfg, templateCfg1, templateCfg2);
        }

        cfg.setClientMode(clientMode);

        cfg.setIncludeEventTypes(EventType.EVT_CACHE_REBALANCE_STARTED, EventType.EVT_CACHE_REBALANCE_STOPPED);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateFromTemplate() throws Exception {
        addTemplate = true;

        Ignite ignite0 = startGrid(0);

        checkCreate(ignite0, "org.apache.ignite.test.cache1", 4);
        checkCreated(ignite0, "org.apache.ignite.test.cache1");

        Ignite ignite1 = startGrid(1);

        checkCreated(ignite1, "org.apache.ignite.test.cache1");

        checkCreate(ignite1, "org.apache.ignite1", 3);
        checkCreated(ignite1, "org.apache.ignite1");

        checkCreated(ignite0, "org.apache.ignite1");

        checkCreate(ignite0, "org.apache1", 2);
        checkCreated(ignite0, "org.apache1");

        checkCreated(ignite1, "org.apache1");

        addTemplate = false;
        clientMode = true;

        Ignite ignite2 = startGrid(2);

        assertNotNull(ignite2.cache("org.apache.ignite.test.cache1"));
        assertNotNull(ignite2.cache("org.apache.ignite1"));
        assertNotNull(ignite2.cache("org.apache1"));

        CacheConfiguration template1 = new CacheConfiguration();

        template1.setName(TEMPLATE3);
        template1.setBackups(5);

        ignite2.addCacheConfiguration(template1);

        checkCreate(ignite0, "org.apache.ignite.test2.cache1", 5);

        checkCreated(ignite0, "org.apache.ignite.test2.cache1");
        checkCreated(ignite1, "org.apache.ignite.test2.cache1");
        checkCreated(ignite2, "org.apache.ignite.test2.cache1");

        Ignite ignite3 = startGrid(3);

        checkCreate(ignite3, "org.apache.ignite.test2.cache2", 5);

        checkNoTemplateCaches(4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetOrCreateFromTemplate() throws Exception {
        addTemplate = true;

        Ignite ignite0 = startGrid(0);

        checkNoTemplateCaches(1);

        checkGetOrCreate(ignite0, "org.apache.ignite.test.cache1", 4);
        checkGetOrCreate(ignite0, "org.apache.ignite.test.cache1", 4);

        Ignite ignite1 = startGrid(1);

        checkGetOrCreate(ignite1, "org.apache.ignite.test.cache1", 4);
        checkGetOrCreate(ignite1, "org.apache.ignite.test.cache1", 4);

        checkGetOrCreate(ignite1, "org.apache.ignite1", 3);
        checkGetOrCreate(ignite1, "org.apache.ignite1", 3);

        checkGetOrCreate(ignite0, "org.apache.ignite1", 3);
        checkGetOrCreate(ignite0, "org.apache.ignite1", 3);

        checkGetOrCreate(ignite0, "org.apache1", 2);
        checkGetOrCreate(ignite1, "org.apache1", 2);

        checkNoTemplateCaches(2);

        addTemplate = false;
        clientMode = true;

        Ignite ignite2 = startGrid(2);

        assertNotNull(ignite2.cache("org.apache.ignite.test.cache1"));
        assertNotNull(ignite2.cache("org.apache.ignite1"));
        assertNotNull(ignite2.cache("org.apache1"));

        checkGetOrCreate(ignite2, "org.apache.ignite.test.cache1", 4);
        checkGetOrCreate(ignite2, "org.apache.ignite1", 3);
        checkGetOrCreate(ignite2, "org.apache1", 2);

        checkGetOrCreate(ignite2, "org.apache.ignite.test.cache2", 4);
        checkGetOrCreate(ignite2, "org.apache.ignite.cache2", 3);
        checkGetOrCreate(ignite2, "org.apache2", 2);

        CacheConfiguration template1 = new CacheConfiguration();

        template1.setName(TEMPLATE3);
        template1.setBackups(5);

        ignite2.addCacheConfiguration(template1);

        checkGetOrCreate(ignite0, "org.apache.ignite.test2.cache1", 5);
        checkGetOrCreate(ignite1, "org.apache.ignite.test2.cache1", 5);
        checkGetOrCreate(ignite2, "org.apache.ignite.test2.cache1", 5);

        Ignite ignite3 = startGrid(3);

        checkGetOrCreate(ignite3, "org.apache.ignite.test2.cache1", 5);

        checkNoTemplateCaches(4);

        // Template with non-wildcard name.
        CacheConfiguration template2 = new CacheConfiguration();

        template2.setName("org.apache.ignite");
        template2.setBackups(6);

        ignite0.addCacheConfiguration(template2);

        checkGetOrCreate(ignite0, "org.apache.ignite", 6);
        checkGetOrCreate(ignite1, "org.apache.ignite", 6);
        checkGetOrCreate(ignite2, "org.apache.ignite", 6);
        checkGetOrCreate(ignite3, "org.apache.ignite", 6);

        // Test name '*'.
        CacheConfiguration template3 = new CacheConfiguration();

        template3.setName("*");
        template3.setBackups(7);

        ignite1.addCacheConfiguration(template3);

        checkGetOrCreate(ignite0, "x", 7);
        checkGetOrCreate(ignite1, "x", 7);
        checkGetOrCreate(ignite2, "x", 7);
        checkGetOrCreate(ignite3, "x", 7);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartClientNodeFirst() throws Exception {
        addTemplate = true;
        clientMode = true;

        Ignite ignite0 = startGrid(0);

        checkNoTemplateCaches(0);

        addTemplate = false;
        clientMode = false;

        Ignite ignite1 = startGrid(1);

        checkGetOrCreate(ignite1, "org.apache.ignite.test.cache1", 4);
        checkGetOrCreate(ignite1, "org.apache.ignite.test.cache1", 4);

        checkGetOrCreate(ignite1, "org.apache.ignite1", 3);
        checkGetOrCreate(ignite1, "org.apache.ignite1", 3);

        checkGetOrCreate(ignite0, "org.apache.ignite1", 3);
        checkGetOrCreate(ignite0, "org.apache.ignite1", 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddCacheConfigurationMultinode() throws Exception {
        addTemplate = true;

        final int GRID_CNT = 3;

        startGridsMultiThreaded(GRID_CNT);

        for (int i = 0; i < 10; i++) {
            log.info("Iteration: " + i);

            final AtomicInteger idx = new AtomicInteger();

            final int iter = i;

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int node = idx.getAndIncrement() % GRID_CNT;

                    Ignite ignite = grid(node);

                    log.info("Add configuration using node: " + ignite.name());

                    CacheConfiguration cfg = new CacheConfiguration();

                    cfg.setName("org.apache.ignite" + iter + "*");

                    cfg.setBackups(iter);

                    for (int i = 0; i < 100; i++)
                        ignite.addCacheConfiguration(cfg);

                    return null;
                }
            }, 15, "add-configuration");

            for (int grid = 0; grid < GRID_CNT; grid++)
                checkGetOrCreate(grid(grid), "org.apache.ignite" + iter, iter);
        }

        Ignite ignite = startGrid(GRID_CNT);

        checkGetOrCreate(ignite, "org.apache.ignite3", 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoPartitionExchangeForTemplate() throws Exception{
        final int GRID_CNT = 3;

        startGridsMultiThreaded(GRID_CNT);

        final CountDownLatch evtLatch = new CountDownLatch(1);

        log.info("Add templates.");

        for (int i = 0; i < GRID_CNT; i++) {
            Ignite ignite = ignite(i);

            ignite.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    log.info("Event: " + evt);

                    evtLatch.countDown();

                    return true;
                }
            }, EventType.EVT_CACHE_REBALANCE_STARTED, EventType.EVT_CACHE_REBALANCE_STOPPED);
        }

        for (int i = 0; i < GRID_CNT; i++) {
            Ignite ignite = ignite(i);

            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setName("cfg-" + i);

            ignite.addCacheConfiguration(ccfg);
        }

        boolean evt = evtLatch.await(3000, TimeUnit.MILLISECONDS);

        assertFalse(evt);

        log.info("Start cache.");

        checkGetOrCreate(ignite(0), "cfg-0", 0);

        evt = evtLatch.await(3000, TimeUnit.MILLISECONDS);

        assertTrue(evt);
    }

    /**
     * @param ignite Ignite.
     * @param name Cache name.
     * @param expBackups Expected number of backups.
     */
    private void checkGetOrCreate(Ignite ignite, String name, int expBackups) {
        IgniteCache cache = ignite.getOrCreateCache(name);

        assertNotNull(cache);

        CacheConfiguration cfg = (CacheConfiguration)cache.getConfiguration(CacheConfiguration.class);

        assertEquals(name, cfg.getName());
        assertEquals(expBackups, cfg.getBackups());
    }

    /**
     * @param ignite Ignite.
     * @param name Cache name.
     * @param expBackups Expected number of backups.
     */
    private void checkCreate(final Ignite ignite, final String name, int expBackups) {
        IgniteCache cache = ignite.createCache(name);

        assertNotNull(cache);

        CacheConfiguration cfg = (CacheConfiguration)cache.getConfiguration(CacheConfiguration.class);

        assertEquals(name, cfg.getName());
        assertEquals(expBackups, cfg.getBackups());
    }

    /**
     * @param cacheName Cache name.
     */
    private void checkCreated(final Ignite ignite, final String cacheName) {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.createCache(cacheName);

                return null;
            }
        }, CacheExistsException.class, null);
    }

    /**
     * @param nodes Nodes number.
     */
    private void checkNoTemplateCaches(int nodes) {
        for (int i = 0; i < nodes; i++) {
            final Ignite ignite = grid(i);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ignite.cache(GridCacheUtils.UTILITY_CACHE_NAME);

                    return null;
                }
            }, IllegalStateException.class, null);

            assertNull(ignite.cache(TEMPLATE1));
            assertNull(ignite.cache(TEMPLATE2));
            assertNull(ignite.cache(TEMPLATE3));
        }
    }
}