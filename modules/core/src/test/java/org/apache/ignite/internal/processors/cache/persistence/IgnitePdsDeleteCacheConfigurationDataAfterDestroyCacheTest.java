/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

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


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        return cfg.setDiscoverySpi(new TcpDiscoverySpi()
                        .setIpFinder(ipFinder))
                        .setDataStorageConfiguration(new DataStorageConfiguration()
                            .setCheckpointFrequency(1_000)
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
     *  Test destroy non grouped caches
     */
    public void testDestroyCaches() throws Exception {
        IgniteEx ignite = (IgniteEx)startGrids(NODES);

        ignite.cluster().active(true);

        startCachesDynamically(ignite);

        loadCaches(ignite);

        checkDestroyCaches(ignite);

    }

    /**
     *  Test destroy grouped caches
     */
    public void testDestroyGroupCaches() throws Exception {
        IgniteEx ignite = (IgniteEx)startGrids(NODES);

        ignite.cluster().active(true);

        statGroupCachesDynamically(ignite);

        checkDestroyCaches(ignite);
    }


    /**
     * @param ignite Ignite.
     */
    private void checkDestroyCaches(IgniteEx ignite) throws Exception {
        loadCaches(ignite);

        ignite.cache("cache0").destroy();
        ignite.cache("cache1").destroy();

        assertEquals(ignite.cacheNames().size(), CACHES - 2);

        stopAllGrids();

        info("grid stopped");

        ignite = (IgniteEx)startGrids(NODES);

        assertEquals("Check that caches don't be restored", ignite.cacheNames().size(), CACHES - 2);
    }


    /**
     * @param ignite Ignite.
     */
    private void startCachesDynamically(IgniteEx ignite) {
        List<CacheConfiguration> ccfg = new ArrayList<>(CACHES);

        for (int i = 0; i < CACHES; i++)
            ccfg.add(new CacheConfiguration<>("cache" + i)
                    .setBackups(1)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setAffinity(new RendezvousAffinityFunction(false, 32))
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));

        ignite.createCaches(ccfg);
    }

    /**
     * @param ignite Ignite.
     */
    private void statGroupCachesDynamically(IgniteEx ignite) {
        List<CacheConfiguration> ccfg = new ArrayList<>(CACHES);

        for (int i = 0; i < CACHES; i++)
            ccfg.add(new CacheConfiguration<>("cache" + i)
                    .setGroupName(i % 2 == 0 ? "grp-even" : "grp-odd")
                    .setBackups(1)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setAffinity(new RendezvousAffinityFunction(false, 32))
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));

        ignite.createCaches(ccfg);
    }

    /**
     * @param ignite Ignite.
     */
    private void loadCaches(IgniteEx ignite) {
        for (int i = 0; i < CACHES; i++) {
            IgniteCache cache = ignite.cache("cache" + i);

            if (i % 2 == 1)
                continue;

            for (int j = 0; j < 100; j++)
                cache.put(j, (i % 2 == 0 ? "1 str " : "2 str ") + j);
        }
    }
}
