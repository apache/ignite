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
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 * Test correct clean up cache configuration data after destroying cache.
 */
public class IgnitePdsDeleteCacheConfigurationDataAfterDestroyCacheTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int CACHES = 3;

    /** */
    private static final int NODES = 3;

    /** */
    private volatile boolean fork = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        return cfg.setDiscoverySpi(new TcpDiscoverySpi()
                        .setIpFinder(ipFinder))
                        .setDataStorageConfiguration(new DataStorageConfiguration()
                                .setCheckpointFrequency(10000)
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

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return fork;
    }

    /**
     *  Test destroy non grouped caches
     */
    public void testDestroyCaches() throws Exception {
        IgniteEx ignite = (IgniteEx)startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        checkDestroyCaches(ignite);

    }

    /**
     *  Test destroy grouped caches
     */
    public void testDestroyGroupCaches() throws Exception {
        IgniteEx ignite = (IgniteEx)startGrids(NODES);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite);

        checkDestroyCaches(ignite);
    }

    /**
     *
     */
    public void testDestroyCachesAbruptlyWithoutCheckpoints() throws Exception {
        fork = true;

        try {
            IgniteEx ignite = (IgniteEx) startGrids(NODES);

            ignite.cluster().active(true);

            startCachesDynamically(ignite);

            enableCheckpoints(false);

            checkDestroyCachesAbruptly(ignite, true);
        }
        finally {
            fork = false;
        }
    }

    /**
     *
     */
    public void testDestroyCachesAbruptly() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        checkDestroyCachesAbruptly(ignite, false);
    }

    /**
     *
     */
    public void testDestroyGroupCachesAbruptlyWithoutCheckpoints() throws Exception {
        fork = true;

        try {
            IgniteEx ignite = (IgniteEx) startGrids(NODES);

            ignite.cluster().active(true);

            startGroupCachesDynamically(ignite);

            enableCheckpoints(false);

            checkDestroyCachesAbruptly(ignite, true);
        }
        finally {
            fork = false;
        }
    }

    /**
     *
     */
    public void testDestroyGroupCachesAbruptly() throws Exception {
        Ignite ignite = startGrids(NODES);

        ignite.cluster().active(true);

        startGroupCachesDynamically(ignite);

        checkDestroyCachesAbruptly(ignite, false);
    }

    /**
     * @param ignite Ignite.
     */
    private void loadCaches(Ignite ignite) throws IgniteCheckedException {

        for (int i = 0; i < CACHES; i++) {
            IgniteCache cache = ignite.cache("cache" + i);

            for (int j = 0; j < 100; j++)
                cache.put(j, "cache: " + i + " data: " + j);
        }
    }

    /**
     * @param ignite Ignite.
     */
    private void checkDestroyCaches(Ignite ignite) throws Exception {
        loadCaches(ignite);

        log.warning("destroying caches....");

        ignite.cache("cache0").destroy();
        ignite.cache("cache1").destroy();

        assertEquals(CACHES - 2, ignite.cacheNames().size());

        log.warning("Stopping grid");

        stopAllGrids();

        log.warning("Grid stopped");

        log.warning("Starting grid");

        ignite = startGrids(NODES);

        log.warning("Grid started");

        assertEquals("Check that caches don't survived", CACHES - 2, ignite.cacheNames().size());

        for(Ignite ig: G.allGrids()) {
            IgniteCache cache = ig.cache("cache2");

            for (int j = 0; j < 100; j++)
                assertNotNull("Check that cache2 contains key: " + j + " node: " + ignite.name(), cache.get(j));
        }
    }


    /**
     * @param woCheckpoints If woCheckpoints equals {@code true} it means that test Grid runs in MultiJVM.
     */
    private void checkDestroyCachesAbruptly(Ignite ignite, boolean woCheckpoints) throws Exception {
        loadCaches(ignite);

        log.warning("Destroying caches");

        ((GatewayProtectedCacheProxy)ignite.cache("cache0")).destroyAsync();
        ((GatewayProtectedCacheProxy)ignite.cache("cache1")).destroyAsync();

        log.warning("Stopping grid");

        if (woCheckpoints) {
           IgniteProcessProxy.killAll();

           stopGrid(0);
        }
        else
            stopAllGrids();

        log.warning("Grid stopped");

        log.warning("Starting grid");

        startGrids(NODES);

        log.warning("Grid started");

        for(Ignite ig: G.allGrids()) {
            if (woCheckpoints) {
                assertEquals("Check that all caches survives when checkpoint is disabled", 3, ig.cacheNames().size());

                //check that data is not corrupted.
                for (String name : ig.cacheNames()) {
                    IgniteCache cache = ig.cache(name);

                    for (int j = 0; j < 100; j++) {
                        assertNotNull("Check that survived cache " + name + " contains key: " + j + " node: " + ig.name(),
                                cache.get(j));
                    }
                }
            }
            else {
                assertTrue(ig.cacheNames().contains("cache2"));

                IgniteCache cache = ig.cache("cache2");

                for (int j = 0; j < 100; j++)
                    assertNotNull("Check that cache2 contains key: " + j + " node: " + ig.name(), cache.get(j));
            }
        }
    }

    /**
     * @param ignite Ignite.
     */
    private void startCachesDynamically(IgniteEx ignite) {
        List<CacheConfiguration> ccfg = new ArrayList<>(CACHES);

        for (int i = 0; i < CACHES; i++)
            ccfg.add(new CacheConfiguration<>("cache" + i)
                    .setBackups(1)
                    .setAffinity(new RendezvousAffinityFunction(false, 32)));

        ignite.createCaches(ccfg);
    }

    /**
     * @param ignite Ignite.
     */
    private void startGroupCachesDynamically(Ignite ignite) {
        List<CacheConfiguration> ccfg = new ArrayList<>(CACHES);

        for (int i = 0; i < CACHES; i++)
            ccfg.add(new CacheConfiguration<>("cache" + i)
                    .setGroupName(i % 2 == 0 ? "grp-even" : "grp-odd")
                    .setBackups(1)
                    .setAffinity(new RendezvousAffinityFunction(false, 32)));

        ignite.createCaches(ccfg);
    }

    private void enableCheckpoints(boolean enabled) throws IgniteCheckedException {
        for (Ignite ignite : G.allGrids()) {
            if (ignite.cluster().localNode().isClient())
                continue;

            if(getTestIgniteInstanceIndex(ignite.name()) == 0)
                continue;

            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context()
                    .cache().context().database();

            dbMgr.enableCheckpoints(enabled).get();
        }
    }
}
