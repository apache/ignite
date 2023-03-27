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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheAtomicityMode.values;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/**
 * Tests that cache specified in configuration start on client nodes.
 */
public class IgniteDynamicClientCacheStartSelfTest extends GridCommonAbstractTest {
    /** */
    private CacheConfiguration ccfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConfiguredCacheOnClientNode() throws Exception {
        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        final String cacheName = DEFAULT_CACHE_NAME;

        Ignite ignite0 = startGrid(0);

        checkCache(ignite0, cacheName, true, false);

        Ignite ignite1 = startClientGrid(1);

        checkCache(ignite1, cacheName, false, false);

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setNearConfiguration(new NearCacheConfiguration());

        Ignite ignite2 = startClientGrid(2);

        checkCache(ignite2, cacheName, false, true);

        ccfg = null;

        Ignite ignite3 = startClientGrid(3);

        checkNoCache(ignite3, cacheName);

        assertNotNull(ignite3.cache(cacheName));

        checkCache(ignite3, cacheName, false, false);

        Ignite ignite4 = startClientGrid(4);

        checkNoCache(ignite4, cacheName);

        assertNotNull(ignite4.createNearCache(cacheName, new NearCacheConfiguration<>()));

        checkCache(ignite4, cacheName, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearCacheStartError() throws Exception {
        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        final String cacheName = DEFAULT_CACHE_NAME;

        Ignite ignite0 = startGrid(0);

        checkCache(ignite0, cacheName, true, false);

        final Ignite ignite1 = startClientGrid(1);

        checkCache(ignite1, cacheName, false, false);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite1.getOrCreateNearCache(cacheName, new NearCacheConfiguration<>());

                return null;
            }
        }, CacheException.class, null);

        checkCache(ignite1, cacheName, false, false);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite1.createNearCache(cacheName, new NearCacheConfiguration<>());

                return null;
            }
        }, CacheException.class, null);

        checkCache(ignite1, cacheName, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedCacheClient() throws Exception {
        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(REPLICATED);

        final String cacheName = DEFAULT_CACHE_NAME;

        Ignite ignite0 = startGrid(0);

        checkCache(ignite0, cacheName, true, false);

        final Ignite ignite1 = startClientGrid(1);

        checkCache(ignite1, cacheName, false, false);

        ccfg.setNearConfiguration(new NearCacheConfiguration());

        Ignite ignite2 = startClientGrid(2);

        checkCache(ignite2, cacheName, false, true);

        ccfg = null;

        Ignite ignite3 = startClientGrid(3);

        checkNoCache(ignite3, cacheName);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedWithNearCacheClient() throws Exception {
        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setNearConfiguration(new NearCacheConfiguration());

        ccfg.setCacheMode(REPLICATED);

        final String cacheName = DEFAULT_CACHE_NAME;

        Ignite ignite0 = startGrid(0);

        checkCache(ignite0, cacheName, true, false);

        final Ignite ignite1 = startClientGrid(1);

        checkCache(ignite1, cacheName, false, true);

        ccfg.setNearConfiguration(null);

        Ignite ignite2 = startClientGrid(2);

        checkCache(ignite2, cacheName, false, false);

        ccfg = null;

        Ignite ignite3 = startClientGrid(3);

        checkNoCache(ignite3, cacheName);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateCloseClientCache1() throws Exception {
        Ignite ignite0 = startGrid(0);

        Ignite clientNode = startClientGrid(1);

        ignite0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        clientNode.cache(DEFAULT_CACHE_NAME);

        clientNode.cache(DEFAULT_CACHE_NAME).close();

        clientNode.cache(DEFAULT_CACHE_NAME);

        startGrid(2);

        checkCache(clientNode, DEFAULT_CACHE_NAME, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateCloseClientCache2_1() throws Exception {
        createCloseClientCache2(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateCloseClientCache2_2() throws Exception {
        createCloseClientCache2(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartMultipleClientCaches() throws Exception {
        startMultipleClientCaches(null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartMultipleClientCachesForGroup() throws Exception {
        startMultipleClientCaches("testGrp");
    }

    /**
     * @param grp Caches group name.
     * @throws Exception If failed.
     */
    private void startMultipleClientCaches(@Nullable String grp) throws Exception {
        final int SRVS = 1;

        Ignite srv = startGrids(SRVS);
        Ignite client = startClientGrid(SRVS);

        for (CacheAtomicityMode atomicityMode : values()) {
            for (boolean batch : new boolean[]{false, true})
                startCachesForGroup(srv, client, grp, atomicityMode, batch);
        }
    }

    /**
     * @param srv Server node.
     * @param client Client node.
     * @param grp Cache group.
     * @param atomicityMode Cache atomicity mode.
     * @param batch {@code True} if use {@link Ignite#getOrCreateCaches(Collection)} for cache creation.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void startCachesForGroup(Ignite srv,
        Ignite client,
        @Nullable String grp,
        CacheAtomicityMode atomicityMode,
        boolean batch) throws Exception {
        log.info("Start caches [grp=" + grp + ", atomicity=" + atomicityMode + ", batch=" + batch + ']');

        try {
            srv.createCaches(cacheConfigurations(grp, atomicityMode));

            Collection<IgniteCache> caches;

            if (batch)
                caches = client.getOrCreateCaches(cacheConfigurations(grp, atomicityMode));
            else {
                caches = new ArrayList<>();

                for (CacheConfiguration ccfg : cacheConfigurations(grp, atomicityMode))
                    caches.add(client.getOrCreateCache(ccfg));
            }

            for (IgniteCache cache : caches)
                checkCache(client, cache.getName(), false, false);

            Map<Integer, Integer> map1 = new HashMap<>();
            Map<Integer, Integer> map2 = new HashMap<>();

            for (int i = 0; i < 100; i++) {
                map1.put(i, i);
                map2.put(i, i + 1);
            }

            for (IgniteCache<Integer, Integer> cache : caches) {
                for (Map.Entry<Integer, Integer> e : map1.entrySet())
                    cache.put(e.getKey(), e.getValue());

                checkCacheData(map1, cache.getName());

                cache.putAll(map2);

                checkCacheData(map2, cache.getName());
            }

            for (IgniteCache<Integer, Integer> cache : caches) {
                cache.close();

                checkNoCache(client, cache.getName());
            }
        }
        finally {
            for (CacheConfiguration ccfg : cacheConfigurations(grp, atomicityMode))
                srv.destroyCache(ccfg.getName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testStartNewAndClientCaches() throws Exception {
        final int SRVS = 4;

        Ignite srv = startGrids(SRVS);

        srv.createCaches(cacheConfigurations(null, ATOMIC));

        ccfg = null;

        Ignite client = startClientGrid(SRVS);

        List<CacheConfiguration> cfgs = new ArrayList<>();

        cfgs.addAll(cacheConfigurations(null, ATOMIC));
        cfgs.addAll(cacheConfigurations(null, TRANSACTIONAL));
        cfgs.addAll(cacheConfigurations(null, TRANSACTIONAL_SNAPSHOT));

        assertEquals(9, cfgs.size());

        Collection<IgniteCache> caches = client.getOrCreateCaches(cfgs);

        assertEquals(cfgs.size(), caches.size());

        for (CacheConfiguration cfg : cfgs)
            checkCache(client, cfg.getName(), false, false);

        Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        for (IgniteCache<Object, Object> cache : caches) {
            cache.putAll(map);

            checkCacheData(map, cache.getName());
        }

        for (IgniteCache cache : caches) {
            cache.close();

            checkNoCache(client, cache.getName());
        }
    }

    /**
     * @param grp Group name.
     * @param atomicityMode Atomicity mode.
     * @return Cache configurations.
     */
    private List<CacheConfiguration> cacheConfigurations(@Nullable String grp, CacheAtomicityMode atomicityMode) {
        List<CacheConfiguration> ccfgs = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setGroupName(grp);
            ccfg.setName("cache-" + atomicityMode + "-" + i);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);

            ccfgs.add(ccfg);
        }

        return ccfgs;
    }

    /**
     * @param createFromCacheClient If {@code true} creates cache from cache client node.
     * @throws Exception If failed.
     */
    private void createCloseClientCache2(boolean createFromCacheClient) throws Exception {
        Ignite ignite0 = startGrid(0);

        Ignite ignite1 = startGrid(1);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setNodeFilter(new CachePredicate(F.asList(ignite0.name())));

        if (createFromCacheClient)
            ignite0.createCache(ccfg);
        else {
            ignite1.createCache(ccfg);

            assertNull(((IgniteKernal)ignite0).context().cache().internalCache(DEFAULT_CACHE_NAME));
        }

        assertNotNull(ignite0.cache(DEFAULT_CACHE_NAME));

        ignite0.cache(DEFAULT_CACHE_NAME).close();

        checkNoCache(ignite0, DEFAULT_CACHE_NAME);

        assertNotNull(ignite0.cache(DEFAULT_CACHE_NAME));

        startGrid(2);

        checkCache(ignite0, DEFAULT_CACHE_NAME, false, false);
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name
     * @param srv {@code True} if server cache is expected.
     * @param near {@code True} if near cache is expected.
     * @throws Exception If failed.
     */
    private void checkCache(Ignite ignite, final String cacheName, boolean srv, boolean near) throws Exception {
        GridCacheAdapter<Object, Object> cache = ((IgniteKernal)ignite).context().cache().internalCache(cacheName);

        assertNotNull("No cache on node " + ignite.name(), cache);

        assertEquals(near, cache.context().isNear());

        final ClusterNode node = ((IgniteKernal)ignite).localNode();

        for (Ignite ignite0 : Ignition.allGrids()) {
            final GridDiscoveryManager disco = ((IgniteKernal)ignite0).context().discovery();

            if (srv || ignite == ignite0)
                assertTrue(disco.cacheNode(node, cacheName));
            else {
                assertTrue(ignite0.name(), GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return disco.cacheNode(node, cacheName);
                    }
                }, 5000));
            }

            assertEquals(srv, disco.cacheAffinityNode(node, cacheName));
            assertEquals(near, disco.cacheNearNode(node, cacheName));

            if (srv)
                assertTrue(ignite0.affinity(cacheName).primaryPartitions(node).length > 0);
            else
                assertEquals(0, ignite0.affinity(cacheName).primaryPartitions(node).length);
        }

        assertNotNull(ignite.cache(cacheName));
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void checkNoCache(Ignite ignite, final String cacheName) throws Exception {
        GridCacheAdapter<Object, Object> cache = ((IgniteKernal)ignite).context().cache().internalCache(cacheName);

        assertNull("Unexpected cache on node " + ignite.name(), cache);

        final ClusterNode node = ((IgniteKernal)ignite).localNode();

        for (Ignite ignite0 : Ignition.allGrids()) {
            final GridDiscoveryManager disco = ((IgniteKernal)ignite0).context().discovery();

            if (ignite0 == ignite)
                assertFalse(ignite0.name(), disco.cacheNode(node, cacheName));
            else {
                assertTrue(ignite0.name(), GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return !disco.cacheNode(node, cacheName);
                    }
                }, 5000));
            }

            assertFalse(disco.cacheAffinityNode(node, cacheName));
            assertFalse(disco.cacheNearNode(node, cacheName));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartClientCachesOnCoordinatorWithGroup() throws Exception {
        startGrids(3);

        List<CacheConfiguration> ccfgs = cacheConfigurations("testGrp", ATOMIC);

        for (CacheConfiguration ccfg : ccfgs)
            ccfg.setNodeFilter(new CachePredicate(F.asList(getTestIgniteInstanceName(0))));

        ignite(1).createCaches(ccfgs);

        ccfgs = cacheConfigurations("testGrp", ATOMIC);

        for (CacheConfiguration ccfg : ccfgs)
            ccfg.setNodeFilter(new CachePredicate(F.asList(getTestIgniteInstanceName(0))));

        for (IgniteCache<Object, Object> cache : ignite(0).getOrCreateCaches(ccfgs)) {
            cache.put(1, 1);

            assertEquals(1, cache.get(1));

            cache.close();
        }

        startGrid(4);
    }

    /**
     *
     */
    static class CachePredicate implements IgnitePredicate<ClusterNode> {
        /** */
        private List<String> excludeNodes;

        /**
         * @param excludeNodes Nodes names.
         */
        public CachePredicate(List<String> excludeNodes) {
            this.excludeNodes = excludeNodes;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            String name = clusterNode.attribute(ATTR_IGNITE_INSTANCE_NAME).toString();

            return !excludeNodes.contains(name);
        }
    }
}
