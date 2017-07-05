/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.standbycluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

/**
 *
 */
public class IgniteStandByClusterTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder vmIpFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(vmIpFinder));
        cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());
        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /**
     * @throws Exception if fail.
     */
    public void testStartDynamicCachesAfterActivation() throws Exception {
        final String cacheName0 = "cache0";
        final String cacheName = "cache";

        IgniteConfiguration cfg1 = getConfiguration("serv1");
        IgniteConfiguration cfg2 = getConfiguration("serv2");

        IgniteConfiguration cfg3 = getConfiguration("client");
        cfg3.setCacheConfiguration(new CacheConfiguration(cacheName0));

        cfg3.setClientMode(true);

        IgniteEx ig1 = startGrid(cfg1);
        IgniteEx ig2 = startGrid(cfg2);
        IgniteEx ig3 = startGrid(cfg3);

        assertTrue(!ig1.active());
        assertTrue(!ig2.active());
        assertTrue(!ig3.active());

        ig3.active(true);

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        ig3.createCache(new CacheConfiguration<>(cacheName));

        assertNotNull(ig3.cache(cacheName));
        assertNotNull(ig1.cache(cacheName));
        assertNotNull(ig2.cache(cacheName));

        assertNotNull(ig1.cache(cacheName0));
        assertNotNull(ig3.cache(cacheName0));
        assertNotNull(ig2.cache(cacheName0));

        ig3.active(false);

        assertTrue(!ig1.active());
        assertTrue(!ig2.active());
        assertTrue(!ig3.active());

        ig3.active(true);

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        assertNotNull(ig1.cache(cacheName));
        assertNotNull(ig2.cache(cacheName));

        Map<String, GridCacheAdapter<?, ?>> caches = U.field(ig3.context().cache(), "caches");

        // Only system cache and cache0
        assertEquals("Unexpected caches: " + caches.keySet(), 3, caches.size());
        assertTrue(caches.containsKey(CU.UTILITY_CACHE_NAME));
        assertTrue(caches.containsKey(cacheName0));
        assertTrue(caches.containsKey(cacheName));

        assertNotNull(ig3.cache(cacheName));
    }

    /**
     * @throws Exception if fail.
     */
    public void testStaticCacheStartAfterActivationWithCacheFilter() throws Exception {
        String cache1 = "cache1";
        String cache2 = "cache2";
        String cache3 = "cache3";

        IgniteConfiguration cfg1 = getConfiguration("node1");

        cfg1.setCacheConfiguration(
            new CacheConfiguration(cache1).setNodeFilter(new NodeFilterIgnoreByName("node2")));

        IgniteConfiguration cfg2 = getConfiguration("node2");

        cfg2.setCacheConfiguration(
            new CacheConfiguration(cache2).setNodeFilter(new NodeFilterIgnoreByName("node3")));

        IgniteConfiguration cfg3 = getConfiguration("node3");

        cfg3.setCacheConfiguration(
            new CacheConfiguration(cache3).setNodeFilter(new NodeFilterIgnoreByName("node1")));

        IgniteEx ig1 = startGrid(cfg1);
        IgniteEx ig2 = startGrid(cfg2);
        IgniteEx ig3 = startGrid(cfg3);

        assertTrue(!ig1.active());
        assertTrue(!ig2.active());
        assertTrue(!ig3.active());

        ig3.active(true);

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        for (IgniteEx ig : Arrays.asList(ig1, ig2, ig3)) {
            Map<String, DynamicCacheDescriptor> desc = U.field(
                U.field(ig.context().cache(), "cachesInfo"), "registeredCaches");

            assertEquals(4, desc.size());

            Map<String, GridCacheAdapter<?, ?>> caches = U.field(ig.context().cache(), "caches");

            assertEquals(3, caches.keySet().size());
        }

        Map<String, GridCacheAdapter<?, ?>> caches1 = U.field(ig1.context().cache(), "caches");

        Assert.assertNotNull(caches1.get(cache1));
        Assert.assertNotNull(caches1.get(cache2));
        Assert.assertNull(caches1.get(cache3));

        Map<String, GridCacheAdapter<?, ?>> caches2 = U.field(ig2.context().cache(), "caches");

        Assert.assertNull(caches2.get(cache1));
        Assert.assertNotNull(caches2.get(cache2));
        Assert.assertNotNull(caches2.get(cache3));

        Map<String, GridCacheAdapter<?, ?>> caches3 = U.field(ig3.context().cache(), "caches");

        Assert.assertNotNull(caches3.get(cache1));
        Assert.assertNull(caches3.get(cache2));
        Assert.assertNotNull(caches3.get(cache3));
    }

    /**
     * @throws Exception if fail.
     */
    public void testSimple() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.active(true);

        IgniteCache<Integer, String> cache0 = ig.getOrCreateCache("cache");

        cache0.put(1, "1");

        assertEquals("1", cache0.get(1));

        ig.active(false);

        assertTrue(!ig.active());

        ig.active(true);

        IgniteCache<Integer, String> cache = ig.cache("cache");

        assertEquals("1", cache.get(1));
    }

    /**
     * @throws Exception if fail.
     */
    public void testJoinDaemonAndDaemonStop() throws Exception {
        IgniteEx ig = startGrid(0);

        IgniteEx daemon = startGrid(
            getConfiguration("daemon")
                .setDaemon(true)
                .setClientMode(true)
        );

        Collection<ClusterNode> daemons = ig.cluster().forDaemons().nodes();

        Assert.assertEquals(1, daemons.size());
        assertEquals(daemon.localNode().id(), daemons.iterator().next().id());

        daemon.close();
    }

    /**
     * @throws Exception if fail.
     */
    public void testRestartCluster() throws Exception {
        IgniteEx ig1 = startGrid(getConfiguration("node1"));
        IgniteEx ig2 = startGrid(getConfiguration("node2"));
        IgniteEx ig3 = startGrid(getConfiguration("node3"));

        assertTrue(!ig1.active());
        assertTrue(!ig2.active());
        assertTrue(!ig3.active());

        ig2.active(true);

        IgniteCache<Integer, String> cache = ig2.getOrCreateCache("cache");

        for (int i = 0; i < 2048; i++)
            cache.put(i, String.valueOf(i));

        ig3.active(false);

        stopAllGrids();

        ig1 = startGrid(getConfiguration("node1"));
        ig2 = startGrid(getConfiguration("node2"));
        ig3 = startGrid(getConfiguration("node3"));

        ig1.active(true);

        IgniteCache<Integer, String> cache2 = ig1.cache("cache");

        for (int i = 0; i < 2048; i++)
            assertEquals(String.valueOf(i), cache2.get(i));
    }

    /**
     *
     */
    private static class NodeFilterIgnoreByName implements IgnitePredicate<ClusterNode> {
        /** */
        private final String name;

        /**
         * @param name Node name.
         */
        private NodeFilterIgnoreByName(String name) {
            this.name = name;
        }

        /** */
        @Override public boolean apply(ClusterNode node) {
            return !name.equals(node.consistentId());
        }
    }

    /**
     *
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true));
    }

    /**
     *
     */
    @Override protected void afterTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true));
    }
}
