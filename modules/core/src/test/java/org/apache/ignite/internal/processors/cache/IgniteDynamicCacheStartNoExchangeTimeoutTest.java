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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleRequest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public class IgniteDynamicCacheStartNoExchangeTimeoutTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi) cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        if (gridName.equals(getTestGridName(NODES - 1)))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(NODES);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultinodeCacheStart() throws Exception {
        for (int i = 0; i < 10; i++) {
            log.info("Iteration: " + i);

            final String name = "cache-" + i;

            final AtomicInteger idx = new AtomicInteger();

            GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Ignite ignite = ignite(idx.getAndIncrement());

                    CacheConfiguration ccfg = new CacheConfiguration();

                    ccfg.setName(name);

                    assertNotNull(ignite.getOrCreateCache(ccfg));

                    return null;
                }
            }, 2, "create-cache").get(15_000);

            awaitPartitionMapExchange();

            checkCache(name);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testOldestNotAffinityNode1() throws Exception {
        for (CacheConfiguration ccfg : cacheConfigurations())
            oldestNotAffinityNode1(ccfg);
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void oldestNotAffinityNode1(final CacheConfiguration ccfg) throws Exception {
        log.info("Test with cache: " + ccfg.getName());

        IgniteEx ignite = grid(0);

        assertEquals(1L, ignite.localNode().order());

        ccfg.setNodeFilter(new TestFilterExcludeOldest());

        assertNotNull(ignite.getOrCreateCache(ccfg));

        awaitPartitionMapExchange();

        checkCache(ccfg.getName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testOldestNotAffinityNode2() throws Exception {
        for (CacheConfiguration ccfg : cacheConfigurations())
            oldestNotAffinityNode2(ccfg);
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void oldestNotAffinityNode2(final CacheConfiguration ccfg) throws Exception {
        log.info("Test with cache: " + ccfg.getName());

        IgniteEx ignite0 = grid(0);
        IgniteEx ignite1 = grid(1);

        assertEquals(1L, ignite0.localNode().order());

        ccfg.setNodeFilter(new TestFilterExcludeOldest());

        assertNotNull(ignite1.getOrCreateCache(ccfg));

        assertNotNull(ignite0.cache(ccfg.getName()));

        awaitPartitionMapExchange();

        checkCache(ccfg.getName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNotAffinityNode1() throws Exception {
        for (CacheConfiguration ccfg : cacheConfigurations())
            notAffinityNode1(ccfg);
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void notAffinityNode1(final CacheConfiguration ccfg) throws Exception {
        log.info("Test with cache: " + ccfg.getName());

        IgniteEx ignite = grid(1);

        assertEquals(2, ignite.localNode().order());

        ccfg.setNodeFilter(new TestFilterExcludeNode(2));

        assertNotNull(ignite.getOrCreateCache(ccfg));

        awaitPartitionMapExchange();

        checkCache(ccfg.getName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNotAffinityNode2() throws Exception {
        for (CacheConfiguration ccfg : cacheConfigurations())
            notAffinityNode2(ccfg);
    }

    /**
     * @param ccfg Cache configuration.
     * @throws Exception If failed.
     */
    private void notAffinityNode2(final CacheConfiguration ccfg) throws Exception {
        log.info("Test with cache: " + ccfg.getName());

        IgniteEx ignite0 = grid(0);
        IgniteEx ignite1 = grid(1);

        assertEquals(2L, ignite1.localNode().order());

        ccfg.setNodeFilter(new TestFilterExcludeNode(2));

        assertNotNull(ignite0.getOrCreateCache(ccfg));

        assertNotNull(ignite1.cache(ccfg.getName()));

        awaitPartitionMapExchange();

        checkCache(ccfg.getName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testOldestChanged1() throws Exception {
        IgniteEx ignite0 = grid(0);

        assertEquals(1L, ignite0.localNode().order());

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setNodeFilter(new TestFilterExcludeOldest());

        assertNotNull(ignite0.getOrCreateCache(ccfg));

        stopGrid(0);

        IgniteEx client = grid(NODES - 1);

        assertTrue(client.configuration().isClientMode());

        assertNotNull(client.getOrCreateCache((String)null));

        awaitPartitionMapExchange();

        checkCache(null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOldestChanged2() throws Exception {
        IgniteEx ignite0 = grid(0);

        assertEquals(1L, ignite0.localNode().order());

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setNodeFilter(new TestFilterIncludeNode(3));

        assertNotNull(ignite0.getOrCreateCache(ccfg));

        stopGrid(0);

        IgniteEx ingite1 = grid(1);

        assertNotNull(ingite1.getOrCreateCache((String)null));

        awaitPartitionMapExchange();

        checkCache(null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOldestChanged3() throws Exception {
        IgniteEx ignite0 = grid(0);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setNodeFilter(new TestFilterIncludeNode(3));

        assertNotNull(ignite0.getOrCreateCache(ccfg));

        stopGrid(0);

        IgniteEx client = grid(NODES - 1);

        assertTrue(client.configuration().isClientMode());

        assertNotNull(client.getOrCreateCache((String)null));

        awaitPartitionMapExchange();

        checkCache(null);
    }

    /**
     * @param name Cache name.
     */
    private void checkCache(@Nullable String name) {
        int key = 0;

        for (Ignite ignite : G.allGrids()) {
            IgniteCache<Object, Object> cache = ignite.cache(name);

            assertNotNull(cache);

            for (int i = 0; i < 100; i++) {
                cache.put(key, key);

                assertEquals(key, cache.get(key));

                key++;
            }
        }
    }

    /**
     * @return Cache configurations.
     */
    private List<CacheConfiguration> cacheConfigurations() {
        List<CacheConfiguration> res = new ArrayList<>();

        {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setName("cache-1");
            ccfg.setAtomicityMode(ATOMIC);
            ccfg.setBackups(0);

            res.add(ccfg);
        }

        {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setName("cache-2");
            ccfg.setAtomicityMode(ATOMIC);
            ccfg.setBackups(1);

            res.add(ccfg);
        }

        {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setName("cache-3");
            ccfg.setAtomicityMode(ATOMIC);
            ccfg.setBackups(1);
            ccfg.setAffinity(new FairAffinityFunction());

            res.add(ccfg);
        }

        {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setName("cache-4");
            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(0);

            res.add(ccfg);
        }

        {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setName("cache-5");
            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(1);

            res.add(ccfg);
        }

        {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setName("cache-4");
            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(1);
            ccfg.setAffinity(new FairAffinityFunction());

            res.add(ccfg);
        }

        return res;
    }

    /**
     *
     */
    private static class TestFilterExcludeOldest implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.order() > 1;
        }
    }

    /**
     *
     */
    private static class TestFilterExcludeNode implements IgnitePredicate<ClusterNode> {
        /** */
        private final long excludeOrder;

        /**
         * @param excludeOrder Node order to exclude.
         */
        public TestFilterExcludeNode(long excludeOrder) {
            this.excludeOrder = excludeOrder;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.order() != excludeOrder;
        }
    }

    /**
     *
     */
    private static class TestFilterIncludeNode implements IgnitePredicate<ClusterNode> {
        /** */
        private final long includeOrder;

        /**
         * @param includeOrder Node order to exclude.
         */
        public TestFilterIncludeNode(long includeOrder) {
            this.includeOrder = includeOrder;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return node.order() == includeOrder;
        }
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            Object msg0 = ((GridIoMessage)msg).message();

            if (msg0 instanceof GridDhtPartitionsSingleRequest) // Sent in case of exchange timeout.
                fail("Unexpected message: " + msg0);

            super.sendMessage(node, msg, ackClosure);
        }
    }
}