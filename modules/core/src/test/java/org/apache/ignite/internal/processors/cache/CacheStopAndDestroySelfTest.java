/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Checks stop and destroy methods behavior.
 */
public class CacheStopAndDestroySelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** key-value used at test. */
    protected static String KEY_VAL = "1";

    /** cache name 1. */
    protected static String CACHE_NAME_DHT = "cache";

    /** cache name 2. */
    protected static String CACHE_NAME_CLIENT = "cache_client";

    /** near cache name. */
    protected static String CACHE_NAME_NEAR = "cache_near";

    /** local cache name. */
    protected static String CACHE_NAME_LOC = "cache_local";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @return Grids count to start.
     */
    protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(gridName);

        if (getTestGridName(2).equals(gridName))
            iCfg.setClientMode(true);

        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setForceServerMode(true);

        iCfg.setCacheConfiguration();

        TcpCommunicationSpi commSpi = new CountingTxRequestsToClientNodeTcpCommunicationSpi();

        commSpi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        commSpi.setTcpNoDelay(true);

        iCfg.setCommunicationSpi(commSpi);

        return iCfg;
    }

    /**
     * Helps to count messages.
     */
    public static class CountingTxRequestsToClientNodeTcpCommunicationSpi extends TcpCommunicationSpi {
        /** Counter. */
        public static AtomicInteger cnt = new AtomicInteger();

        /** Node filter. */
        public static UUID nodeFilter;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            super.sendMessage(node, msg, ackClosure);

            if (nodeFilter != null &&
                node.id().equals(nodeFilter) &&
                msg instanceof GridIoMessage &&
                ((GridIoMessage)msg).message() instanceof GridDhtTxPrepareRequest)
                cnt.incrementAndGet();
        }
    }

    /**
     * @return dht config
     */
    private CacheConfiguration getDhtConfig() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(CACHE_NAME_DHT);
        cfg.setCacheMode(PARTITIONED);
        cfg.setNearConfiguration(null);

        return cfg;
    }

    /**
     * @return client config
     */
    private CacheConfiguration getClientConfig() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(CACHE_NAME_CLIENT);
        cfg.setCacheMode(PARTITIONED);
        cfg.setNearConfiguration(null);

        return cfg;
    }

    /**
     * @return near config
     */
    private CacheConfiguration getNearConfig() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(CACHE_NAME_NEAR);
        cfg.setCacheMode(PARTITIONED);
        cfg.setNearConfiguration(new NearCacheConfiguration());

        return cfg;
    }

    /**
     * @return local config
     */
    private CacheConfiguration getLocalConfig() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(CACHE_NAME_LOC);
        cfg.setCacheMode(LOCAL);
        cfg.setNearConfiguration(null);

        return cfg;
    }

    /**
     * Test Double Destroy.
     *
     * @throws Exception If failed.
     */
    public void testDhtDoubleDestroy() throws Exception {
        dhtDestroy();

        dhtDestroy();
    }

    /**
     * Test DHT Destroy.
     *
     * @throws Exception If failed.
     */
    private void dhtDestroy() throws Exception {
        grid(0).getOrCreateCache(getDhtConfig());

        assertNull(grid(0).cache(CACHE_NAME_DHT).get(KEY_VAL));

        grid(0).cache(CACHE_NAME_DHT).put(KEY_VAL, KEY_VAL);

        assertEquals(KEY_VAL, grid(0).cache(CACHE_NAME_DHT).get(KEY_VAL));
        assertEquals(KEY_VAL, grid(1).cache(CACHE_NAME_DHT).get(KEY_VAL));
        assertEquals(KEY_VAL, grid(2).cache(CACHE_NAME_DHT).get(KEY_VAL));

        assertFalse(grid(0).configuration().isClientMode());

        // DHT Destroy. Cache should be removed from each node.

        IgniteCache<Object, Object> cache = grid(0).cache(CACHE_NAME_DHT);

        cache.destroy();

        checkDestroyed(cache);
    }

    /**
     * Test Double Destroy.
     *
     * @throws Exception If failed.
     */
    public void testClientDoubleDestroy() throws Exception {
        clientDestroy();

        clientDestroy();
    }

    /**
     * Test Client Destroy.
     *
     * @throws Exception If failed.
     */
    private void clientDestroy() throws Exception {
        grid(0).getOrCreateCache(getClientConfig());

        assertNull(grid(0).cache(CACHE_NAME_CLIENT).get(KEY_VAL));

        grid(0).cache(CACHE_NAME_CLIENT).put(KEY_VAL, KEY_VAL);

        assertEquals(KEY_VAL, grid(0).cache(CACHE_NAME_CLIENT).get(KEY_VAL));
        assertEquals(KEY_VAL, grid(1).cache(CACHE_NAME_CLIENT).get(KEY_VAL));
        assertEquals(KEY_VAL, grid(2).cache(CACHE_NAME_CLIENT).get(KEY_VAL));

        // DHT Destroy from client node. Cache should be removed from each node.

        assertTrue(grid(2).configuration().isClientMode());

        IgniteCache<Object, Object> cache = grid(2).cache(CACHE_NAME_CLIENT);

        cache.destroy(); // Client node.

        checkDestroyed(cache);
    }

    /**
     * Test Double Destroy.
     *
     * @throws Exception If failed.
     */
    public void testNearDoubleDestroy() throws Exception {
        nearDestroy();

        nearDestroy();
    }

    /**
     * Test Near Destroy.
     *
     * @throws Exception If failed.
     */
    private void nearDestroy() throws Exception {
        grid(0).getOrCreateCache(getNearConfig());

        grid(2).getOrCreateNearCache(CACHE_NAME_NEAR, new NearCacheConfiguration());

        assertNull(grid(0).cache(CACHE_NAME_NEAR).get(KEY_VAL));
        assertNull(grid(2).cache(CACHE_NAME_NEAR).get(KEY_VAL));

        grid(2).cache(CACHE_NAME_NEAR).put(KEY_VAL, KEY_VAL);
        grid(0).cache(CACHE_NAME_NEAR).put(KEY_VAL, "near-test");

        assertEquals("near-test", grid(2).cache(CACHE_NAME_NEAR).localPeek(KEY_VAL));

        // Near cache destroy. Cache should be removed from each node.

        IgniteCache<Object, Object> cache = grid(2).cache(CACHE_NAME_NEAR);

        cache.destroy();

        checkDestroyed(cache);
    }

    /**
     * Test Double Destroy.
     *
     * @throws Exception If failed.
     */
    public void testLocalDoubleDestroy() throws Exception {
        localDestroy();

        localDestroy();
    }

    /**
     * Test Local Destroy.
     *
     * @throws Exception If failed.
     */
    private void localDestroy() throws Exception {
        grid(0).getOrCreateCache(getLocalConfig());

        assert grid(0).cache(CACHE_NAME_LOC).get(KEY_VAL) == null;
        assert grid(1).cache(CACHE_NAME_LOC).get(KEY_VAL) == null;

        grid(0).cache(CACHE_NAME_LOC).put(KEY_VAL, KEY_VAL + 0);
        grid(1).cache(CACHE_NAME_LOC).put(KEY_VAL, KEY_VAL + 1);

        assert grid(0).cache(CACHE_NAME_LOC).get(KEY_VAL).equals(KEY_VAL + 0);
        assert grid(1).cache(CACHE_NAME_LOC).get(KEY_VAL).equals(KEY_VAL + 1);

        grid(0).cache(CACHE_NAME_LOC).destroy();

        assertNull(grid(0).cache(CACHE_NAME_LOC));
    }

    /**
     * Test Dht close.
     *
     * @throws Exception If failed.
     */
    public void testDhtClose() throws Exception {
        IgniteCache<Integer, Integer> dhtCache0 = grid(0).getOrCreateCache(getDhtConfig());

        final Integer key = primaryKey(dhtCache0);

        assertNull(dhtCache0.get(key));

        dhtCache0.put(key, key);

        assertEquals(key, dhtCache0.get(key));

        // DHT Close. No-op.

        IgniteCache<Integer, Integer> dhtCache1 = grid(1).cache(CACHE_NAME_DHT);
        IgniteCache<Integer, Integer> dhtCache2 = grid(2).cache(CACHE_NAME_DHT);

        dhtCache0.close();

        try {
            dhtCache0.get(key);// Not affected, but can not be taken.

            fail();
        }
        catch (IllegalStateException ignored) {
            // No-op
        }

        assertEquals(key, dhtCache1.get(key)); // Not affected.
        assertEquals(key, dhtCache2.get(key));// Not affected.

        // DHT Creation after closed.

        IgniteCache<Integer, Integer> dhtCache0New = grid(0).cache(CACHE_NAME_DHT);

        assertNotSame(dhtCache0, dhtCache0New);

        assertEquals(key, dhtCache0New.get(key)); // Not affected, can be taken since cache reopened.

        dhtCache2.put(key, key + 1);

        assertEquals((Object)(key + 1), dhtCache0New.get(key));

        // Check close at last node.

        stopAllGrids(true);

        startGrid(0);

        dhtCache0 = grid(0).getOrCreateCache(getDhtConfig());

        assertNull(dhtCache0.get(key));

        dhtCache0.put(key, key);

        assertEquals(key, dhtCache0.get(key));

        // Closing last node.
        dhtCache0.close();

        try {
            dhtCache0.get(key);// Can not be taken.

            fail();
        }
        catch (IllegalStateException ignored) {
            // No-op
        }

        // Reopening cache.
        dhtCache0 = grid(0).cache(CACHE_NAME_DHT);

        assertEquals(key, dhtCache0.get(key)); // Entry not loosed.
    }

    /**
     * Test Dht close.
     *
     * @throws Exception If failed.
     */
    public void testDhtCloseWithTry() throws Exception {
        String curVal = null;

        for (int i = 0; i < 3; i++) {
            try (IgniteCache<String, String> cache0 = grid(0).getOrCreateCache(getDhtConfig())) {
                IgniteCache<String, String> cache1 = grid(1).cache(CACHE_NAME_DHT);
                IgniteCache<String, String> cache2 = grid(2).cache(CACHE_NAME_DHT);

                if (i == 0) {
                    assert cache0.get(KEY_VAL) == null;
                    assert cache1.get(KEY_VAL) == null;
                    assert cache2.get(KEY_VAL) == null;
                }
                else {
                    assert cache0.get(KEY_VAL).equals(curVal);
                    assert cache1.get(KEY_VAL).equals(curVal);
                    assert cache2.get(KEY_VAL).equals(curVal);
                }

                curVal = KEY_VAL + curVal;

                cache0.put(KEY_VAL, curVal);

                assert cache0.get(KEY_VAL).equals(curVal);
                assert cache1.get(KEY_VAL).equals(curVal);
                assert cache2.get(KEY_VAL).equals(curVal);
            }
        }
    }

    /**
     * Test Client close.
     *
     * @throws Exception If failed.
     */
    public void testClientClose() throws Exception {
        IgniteCache<String, String> cache0 = grid(0).getOrCreateCache(getClientConfig());

        assert cache0.get(KEY_VAL) == null;

        cache0.put(KEY_VAL, KEY_VAL);

        assert cache0.get(KEY_VAL).equals(KEY_VAL);

        // DHT Close from client node. Should affect only client node.

        IgniteCache<String, String> cache1 = grid(1).cache(CACHE_NAME_CLIENT);
        IgniteCache<String, String> cache2 = grid(2).cache(CACHE_NAME_CLIENT);

        assert cache2.get(KEY_VAL).equals(KEY_VAL);

        cache2.close();// Client node.

        assert cache0.get(KEY_VAL).equals(KEY_VAL);// Not affected.
        assert cache1.get(KEY_VAL).equals(KEY_VAL);// Not affected.

        try {
            cache2.get(KEY_VAL);// Affected.

            assert false;
        }
        catch (IllegalStateException ignored) {
            // No-op
        }

        // DHT Creation from client node after closed.
        IgniteCache<String, String> cache2New = grid(2).cache(CACHE_NAME_CLIENT);

        assertNotSame(cache2, cache2New);

        assert cache2New.get(KEY_VAL).equals(KEY_VAL);

        cache0.put(KEY_VAL, KEY_VAL + "recreated");

        assert cache0.get(KEY_VAL).equals(KEY_VAL + "recreated");
        assert cache1.get(KEY_VAL).equals(KEY_VAL + "recreated");
        assert cache2New.get(KEY_VAL).equals(KEY_VAL + "recreated");
    }

    /**
     * Test Client close.
     *
     * @throws Exception If failed.
     */
    public void testClientCloseWithTry() throws Exception {
        String curVal = null;

        for (int i = 0; i < 3; i++) {
            try (IgniteCache<String, String> cache2 = grid(2).getOrCreateCache(getClientConfig())) {
                IgniteCache<String, String> cache0 = grid(0).cache(CACHE_NAME_CLIENT);
                IgniteCache<String, String> cache1 = grid(1).cache(CACHE_NAME_CLIENT);

                if (i == 0) {
                    assert cache0.get(KEY_VAL) == null;
                    assert cache1.get(KEY_VAL) == null;
                    assert cache2.get(KEY_VAL) == null;
                }
                else {
                    assert cache0.get(KEY_VAL).equals(curVal);
                    assert cache1.get(KEY_VAL).equals(curVal);
                    assert cache2.get(KEY_VAL).equals(curVal);
                }

                curVal = KEY_VAL + curVal;

                cache2.put(KEY_VAL, curVal);

                assert cache0.get(KEY_VAL).equals(curVal);
                assert cache1.get(KEY_VAL).equals(curVal);
                assert cache2.get(KEY_VAL).equals(curVal);
            }

            awaitPartitionMapExchange();
        }
    }

    /**
     * Test Near close.
     *
     * @throws Exception If failed.
     */
    public void testNearClose() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-2189");

        IgniteCache<String, String> cache0 = grid(0).getOrCreateCache(getNearConfig());

        // GridDhtTxPrepareRequest requests to Client node will be counted.
        CountingTxRequestsToClientNodeTcpCommunicationSpi.nodeFilter = grid(2).context().localNodeId();

        // Near Close from client node.

        IgniteCache<String, String> cache1 = grid(1).cache(CACHE_NAME_NEAR);
        IgniteCache<String, String> cache2 = grid(2).createNearCache(CACHE_NAME_NEAR, new NearCacheConfiguration());

        assert cache2.get(KEY_VAL) == null;

        // Subscribing to events.
        cache2.put(KEY_VAL, KEY_VAL);

        CountingTxRequestsToClientNodeTcpCommunicationSpi.cnt.set(0);

        cache0.put(KEY_VAL, "near-test");

        U.sleep(1000);

        //Ensure near cache was automatically updated.
        assert CountingTxRequestsToClientNodeTcpCommunicationSpi.cnt.get() != 0;

        assert cache2.localPeek(KEY_VAL).equals("near-test");

        cache2.close();

        CountingTxRequestsToClientNodeTcpCommunicationSpi.cnt.set(0);

        // Should not produce messages to client node.
        cache0.put(KEY_VAL, KEY_VAL + 0);

        U.sleep(1000);

        // Ensure near cache was NOT automatically updated.
        assert CountingTxRequestsToClientNodeTcpCommunicationSpi.cnt.get() == 0;

        assert cache0.get(KEY_VAL).equals(KEY_VAL + 0);// Not affected.
        assert cache1.get(KEY_VAL).equals(KEY_VAL + 0);// Not affected.

        try {
            cache2.get(KEY_VAL);// Affected.

            assert false;
        }
        catch (IllegalArgumentException | IllegalStateException ignored) {
            // No-op
        }

        // Near Creation from client node after closed.

        IgniteCache<String, String> cache2New = grid(2).createNearCache(CACHE_NAME_NEAR, new NearCacheConfiguration());

        assertNotSame(cache2, cache2New);

        // Subscribing to events.
        cache2New.put(KEY_VAL, KEY_VAL);

        assert cache2New.localPeek(KEY_VAL).equals(KEY_VAL);

        cache0.put(KEY_VAL, KEY_VAL + "recreated");

        assert cache0.get(KEY_VAL).equals(KEY_VAL + "recreated");
        assert cache1.get(KEY_VAL).equals(KEY_VAL + "recreated");
        assert cache2New.localPeek(KEY_VAL).equals(KEY_VAL + "recreated");
    }

    /**
     * Test Near close.
     *
     * @throws Exception If failed.
     */
    public void testNearCloseWithTry() throws Exception {
        String curVal = null;

        grid(0).getOrCreateCache(getNearConfig());

        NearCacheConfiguration nearCfg = new NearCacheConfiguration();

        for (int i = 0; i < 3; i++) {
            try (IgniteCache<String, String> cache2 = grid(2).getOrCreateNearCache(CACHE_NAME_NEAR, nearCfg)) {
                IgniteCache<String, String> cache0 = grid(0).cache(CACHE_NAME_NEAR);
                IgniteCache<String, String> cache1 = grid(1).cache(CACHE_NAME_NEAR);

                assert cache2.localPeek(KEY_VAL) == null;

                assert cache0.get(KEY_VAL) == null || cache0.get(KEY_VAL).equals(curVal);
                assert cache1.get(KEY_VAL) == null || cache1.get(KEY_VAL).equals(curVal);
                assert cache2.get(KEY_VAL) == null || cache2.get(KEY_VAL).equals(curVal);

                curVal = KEY_VAL + curVal;

                cache2.put(KEY_VAL, curVal);

                assert cache2.localPeek(KEY_VAL).equals(curVal);

                assert cache0.get(KEY_VAL).equals(curVal);
                assert cache1.get(KEY_VAL).equals(curVal);
                assert cache2.get(KEY_VAL).equals(curVal);
            }
        }
    }

    /**
     * Test Local close.
     *
     * @throws Exception If failed.
     */
    public void testLocalClose() throws Exception {
        grid(0).getOrCreateCache(getLocalConfig());

        assert grid(0).cache(CACHE_NAME_LOC).get(KEY_VAL) == null;
        assert grid(1).cache(CACHE_NAME_LOC).get(KEY_VAL) == null;

        grid(0).cache(CACHE_NAME_LOC).put(KEY_VAL, KEY_VAL + 0);
        grid(1).cache(CACHE_NAME_LOC).put(KEY_VAL, KEY_VAL + 1);

        assert grid(0).cache(CACHE_NAME_LOC).get(KEY_VAL).equals(KEY_VAL + 0);
        assert grid(1).cache(CACHE_NAME_LOC).get(KEY_VAL).equals(KEY_VAL + 1);

        // Local close. Same as Local destroy.

        IgniteCache<Object, Object> cache = grid(1).cache(CACHE_NAME_LOC);

        cache.close();

        checkUsageFails(cache);

        assertNull(grid(1).cache(CACHE_NAME_LOC));

        // Local creation after closed.

        AffinityTopologyVersion topVer = grid(1).context().cache().context().exchange().lastTopologyFuture().get();

        grid(0).context().cache().context().exchange().affinityReadyFuture(topVer).get();

        grid(0).getOrCreateCache(getLocalConfig());

        grid(0).cache(CACHE_NAME_LOC).put(KEY_VAL, KEY_VAL + "recreated0");
        grid(1).cache(CACHE_NAME_LOC).put(KEY_VAL, KEY_VAL + "recreated1");
        grid(2).cache(CACHE_NAME_LOC).put(KEY_VAL, KEY_VAL + "recreated2");

        assert grid(0).cache(CACHE_NAME_LOC).get(KEY_VAL).equals(KEY_VAL + "recreated0");
        assert grid(1).cache(CACHE_NAME_LOC).get(KEY_VAL).equals(KEY_VAL + "recreated1");
        assert grid(2).cache(CACHE_NAME_LOC).get(KEY_VAL).equals(KEY_VAL + "recreated2");
    }

    /**
     * Test Local close.
     *
     * @throws Exception If failed.
     */
    public void testLocalCloseWithTry() throws Exception {
        String curVal = null;

        for (int i = 0; i < 3; i++) {
            try (IgniteCache<String, String> cache2 = grid(2).getOrCreateCache(getLocalConfig())) {
                IgniteCache<String, String> cache0 = grid(0).cache(CACHE_NAME_LOC);
                IgniteCache<String, String> cache1 = grid(1).cache(CACHE_NAME_LOC);

                assert cache0.get(KEY_VAL) == null;
                assert cache1.get(KEY_VAL) == null;
                assert cache2.get(KEY_VAL) == null;

                curVal = KEY_VAL + curVal;

                cache0.put(KEY_VAL, curVal + 1);
                cache1.put(KEY_VAL, curVal + 2);
                cache2.put(KEY_VAL, curVal + 3);

                assert cache0.get(KEY_VAL).equals(curVal + 1);
                assert cache1.get(KEY_VAL).equals(curVal + 2);
                assert cache2.get(KEY_VAL).equals(curVal + 3);
            }
        }
    }

    /**
     * Tests start -> destroy -> start -> close using CacheManager.
     */
    public void testTckStyleCreateDestroyClose() {
        CacheManager mgr = Caching.getCachingProvider().getCacheManager();

        String cacheName = "cache";

        mgr.createCache(cacheName, new MutableConfiguration<Integer, String>().setTypes(Integer.class, String.class));

        mgr.destroyCache(cacheName);

        Cache<Integer, String> cache = mgr.createCache(cacheName,
            new MutableConfiguration<Integer, String>().setTypes(Integer.class, String.class));

        cache.close();

        cache.close();

        try {
            cache.get(1);

            fail();
        }
        catch (IllegalStateException ignored) {
            // No-op;
        }
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void checkDestroyed(IgniteCache<Object, Object> cache) throws Exception {
        checkUsageFails(cache);

        awaitPartitionMapExchange();

        String cacheName = cache.getName();

        for (int i = 0; i < 3; i++)
            assertNull("Unexpected cache for node: " + i, grid(i).cache(cacheName));
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void checkUsageFails(IgniteCache<Object, Object> cache) throws Exception {
        try {
            cache.get(0);

            fail();
        }
        catch (IllegalStateException ignored) {
            // No-op.
        }
    }
}
