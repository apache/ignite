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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test correct clean up cache configuration data after destroying cache.
 */
public class IgnitePdsDeleteCacheConfigurationDataAfterDestroyCacheTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int CACHES = 3;

    /** */
    private static final int NODES = 3;

    /** */
    private static final int NUM_OF_KEYS = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        return cfg.setDiscoverySpi(new TcpDiscoverySpi()
                        .setIpFinder(IP_FINDER))
                        .setDataStorageConfiguration(new DataStorageConfiguration()
                            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                                .setMaxSize(200 * 1024 * 1024)
                                .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * {@inheritDoc}
     * @returns always {@code true} in order to be able to kill nodes when checkpointer thread hangs.
     */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     *  Test destroy non grouped caches.
     *
     *  @throws Exception If failed.
     */
    public void testDestroyCaches() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        checkDestroyCaches(ignite);
    }

    /**
     *  Test destroy grouped caches.
     *
     *  @throws Exception If failed.
     */
    public void testDestroyGroupCaches() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite);

        checkDestroyCaches(ignite);
    }

    /**
     * Test destroy caches with disabled checkpoints.
     *
     * @throws Exception If failed.
     */
    public void testDestroyCachesAbruptlyWithoutCheckpoints() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        enableCheckpoints(false);

        checkDestroyCachesAbruptly(ignite);
    }

    /**
     * Test destroy group caches with disabled checkpoints.
     *
     * @throws Exception If failed.
     */
    public void testDestroyGroupCachesAbruptlyWithoutCheckpoints() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite);

        enableCheckpoints(false);

        checkDestroyCachesAbruptly(ignite);
    }

    /**
     * Test destroy caches abruptly with checkpoints.
     *
     * @throws Exception If failed.
     */
    public void testDestroyCachesAbruptly() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        checkDestroyCachesAbruptly(ignite);
    }


    /**
     * Test destroy group caches abruptly with checkpoints.
     *
     * @throws Exception If failed.
     */
    public void testDestroyGroupCachesAbruptly() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite);

        checkDestroyCachesAbruptly(ignite);
    }

    /**
     * @param ignite Ignite.
     */
    private void loadCaches(Ignite ignite) {
        for (int i = 0; i < CACHES; i++) {
            try (IgniteDataStreamer<Object, Object> s = ignite.dataStreamer(cacheName(i))) {
                s.allowOverwrite(true);

                for (int j = 0; j < NUM_OF_KEYS; j++)
                    s.addData(j, "cache: " + i + " data: " + j);

                s.flush();
            }
        }
    }

    /**
     * @param ignite Ignite.
     */
    private void checkDestroyCaches(Ignite ignite) throws Exception {
        loadCaches(ignite);

        log.warning("destroying caches....");

        ignite.cache(cacheName(0)).destroy();
        ignite.cache(cacheName(1)).destroy();

        assertEquals(CACHES - 2, ignite.cacheNames().size());

        log.warning("Stopping grid");

        stopAllGrids();

        log.warning("Grid stopped");

        log.warning("Starting grid");

        ignite = startGrids(NODES);

        log.warning("Grid started");

        assertEquals("Check that caches don't survived", CACHES - 2, ignite.cacheNames().size());

        for(Ignite ig: G.allGrids()) {
            IgniteCache cache = ig.cache(cacheName(2));

            for (int j = 0; j < NUM_OF_KEYS; j++)
                assertNotNull("Check that cache2 contains key: " + j + " node: " + ignite.name(), cache.get(j));
        }
    }


    /**
     * @param ignite Ignite instance.
     */
    private void checkDestroyCachesAbruptly(Ignite ignite) throws Exception {
        loadCaches(ignite);

        log.warning("Destroying caches");

        ((GatewayProtectedCacheProxy)ignite.cache(cacheName(0))).destroyAsync();
        ((GatewayProtectedCacheProxy)ignite.cache(cacheName(1))).destroyAsync();

        log.warning("Stopping grid");

        stopAllGrids();

        log.warning("Grid stopped");

        log.warning("Starting grid");

        startGrids(NODES);

        log.warning("Grid started");

        for(Ignite ig: G.allGrids()) {
            assertTrue(ig.cacheNames().contains(cacheName(2)));

            IgniteCache cache = ig.cache(cacheName(2));

            for (int j = 0; j < NUM_OF_KEYS; j++)
                assertNotNull("Check that survived cache cache2 contains key: " + j + " node: " + ig.name(), cache.get(j));
        }
    }

    /**
     * @param ignite Ignite.
     */
    private void startCachesDynamically(Ignite ignite) {
        List<CacheConfiguration> ccfg = new ArrayList<>(CACHES);

        for (int i = 0; i < CACHES; i++)
            ccfg.add(new CacheConfiguration<>(cacheName(i))
                    .setBackups(1)
                    .setAffinity(new RendezvousAffinityFunction(false, 32)));

        ignite.createCaches(ccfg);
    }

    /**
     * @param ignite Ignite instance.
     */
    private void startGroupCachesDynamically(Ignite ignite) {
        List<CacheConfiguration> ccfg = new ArrayList<>(CACHES);

        for (int i = 0; i < CACHES; i++)
            ccfg.add(new CacheConfiguration<>(cacheName(i))
                    .setGroupName(i % 2 == 0 ? "grp-even" : "grp-odd")
                    .setBackups(1)
                    .setAffinity(new RendezvousAffinityFunction(false, 32)));

        ignite.createCaches(ccfg);
    }


    /**
     * Generate cache name from idx.
     *
     * @param idx Index.
     */
    private String cacheName(int idx) {
        return "cache" + idx;
    }

    /**
     * Enable/disable checkpoints on multi JVM nodes only.
     *
     * @param enabled Enabled flag.
     * @throws IgniteCheckedException If failed.
     */
    private void enableCheckpoints(boolean enabled) throws IgniteCheckedException {
        for (Ignite ignite : G.allGrids()) {
            assert !ignite.cluster().localNode().isClient();

            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context()
                    .cache().context().database();

            dbMgr.enableCheckpoints(enabled).get();
        }
    }
}
