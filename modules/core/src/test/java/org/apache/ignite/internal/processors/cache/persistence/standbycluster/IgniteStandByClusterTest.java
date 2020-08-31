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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class IgniteStandByClusterTest extends GridCommonAbstractTest {
    /**
     *
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100L * 1024 * 1024)
                .setPersistenceEnabled(true)));

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /**
     * @throws Exception if fail.
     */
    @Test
    public void testStartDynamicCachesAfterActivation() throws Exception {
        final String cacheName0 = "cache0";
        final String cacheName = "cache";

        IgniteConfiguration cfg1 = getConfiguration("serv1");
        IgniteConfiguration cfg2 = getConfiguration("serv2");

        IgniteConfiguration cfg3 = getConfiguration("client");
        cfg3.setCacheConfiguration(new CacheConfiguration(cacheName0));

        IgniteEx ig1 = startGrid(cfg1);
        IgniteEx ig2 = startGrid(cfg2);
        IgniteEx ig3 = startClientGrid(cfg3);

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
    @Test
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
                (Object)U.field(ig.context().cache(), "cachesInfo"), "registeredCaches");

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
    @Test
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
    @Test
    public void testJoinDaemonAndDaemonStop() throws Exception {
        IgniteEx ig = startGrid(0);

        IgniteEx daemon = startClientGrid(
            getConfiguration("daemon")
                .setDaemon(true)
        );

        Collection<ClusterNode> daemons = ig.cluster().forDaemons().nodes();

        Assert.assertEquals(1, daemons.size());
        assertEquals(daemon.localNode().id(), daemons.iterator().next().id());

        daemon.close();
    }

    /**
     * Check that daemon node does not move cluster to compatibility mode.
     */
    @Test
    public void testJoinDaemonToBaseline() throws Exception {
        Ignite ignite0 = startGrid(0);

        startGrid(1);

        ignite0.cluster().active(true);

        startClientGrid(
            getConfiguration("daemon")
                .setDaemon(true)
        );

        stopGrid(1);

        startGrid(1);
    }

    /**
     * @throws Exception if fail.
     */
    @Test
    public void testCheckStatusFromDaemon() throws Exception {
        IgniteEx ig = startGrid(0);

        assertFalse(ig.active());

        ig.active(true);

        IgniteEx daemon = startClientGrid(
            getConfiguration("daemon")
                .setDaemon(true)
        );

        assertTrue(ig.active());
        assertTrue(daemon.active());

        daemon.active(false);

        assertFalse(ig.active());
        assertFalse(daemon.active());

        daemon.active(true);

        assertTrue(ig.active());
        assertTrue(daemon.active());
    }

    /**
     * @throws Exception if fail.
     */
    @Test
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
     * @throws Exception if fail.
     */
    @Test
    public void testActivateDeActivateCallbackForPluginProviders() throws Exception {
        IgniteEx ig1 = startGrid(getConfiguration("node1"));
        IgniteEx ig2 = startGrid(getConfiguration("node2"));
        IgniteEx ig3 = startGrid(getConfiguration("node3"));

        assertTrue(!ig1.active());
        assertTrue(!ig2.active());
        assertTrue(!ig3.active());

        ig1.active(true);

        checkPlugin(ig1, 1, 0);
        checkPlugin(ig2, 1, 0);
        checkPlugin(ig3, 1, 0);

        ig2.active(false);

        ig3.active(true);

        checkPlugin(ig1, 2, 1);
        checkPlugin(ig2, 2, 1);
        checkPlugin(ig3, 2, 1);

        ig1.active(false);

        ig2.active(true);

        checkPlugin(ig1, 3, 2);
        checkPlugin(ig2, 3, 2);
        checkPlugin(ig3, 3, 2);

    }

    /**
     * @param ig ignite.
     * @param act Expected activation counter.
     * @param deAct Expected deActivation counter.
     */
    private void checkPlugin(Ignite ig, int act, int deAct) {
        IgnitePlugin pl = ig.plugin(StanByClusterTestProvider.NAME);

        assertNotNull(pl);

        StanByClusterTestProvider plugin = (StanByClusterTestProvider)pl;

        assertEquals(act, plugin.actCnt.get());
        assertEquals(deAct, plugin.deActCnt.get());
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
    public static class StanByClusterTestProvider extends AbstractTestPluginProvider implements IgnitePlugin,
        IgniteChangeGlobalStateSupport {
        /** */
        static final String NAME = "StanByClusterTestProvider";

        /** */
        final AtomicInteger actCnt = new AtomicInteger();

        /** */
        final AtomicInteger deActCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public String name() {
            return NAME;
        }

        /** {@inheritDoc} */
        @Override public IgnitePlugin plugin() {
            return this;
        }

        /** {@inheritDoc} */
        @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
            actCnt.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void onDeActivate(GridKernalContext kctx) {
            deActCnt.incrementAndGet();
        }
    }

    /**
     *
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Override protected void afterTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }
}
