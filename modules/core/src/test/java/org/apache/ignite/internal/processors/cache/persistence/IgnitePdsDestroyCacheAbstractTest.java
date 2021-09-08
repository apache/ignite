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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Base class for  {@link IgnitePdsDestroyCacheTest} and {@link IgnitePdsDestroyCacheWithoutCheckpointsTest}
 */
public abstract class IgnitePdsDestroyCacheAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static final int CACHES = 3;

    /** */
    protected static final int NODES = 3;

    /** */
    private static final int NUM_OF_KEYS = 2000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        return cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalSegmentSize(4 * 1024 * 1024)
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
     * @param ignite Ignite.
     * @param singlePart {@code True} to load data to single partition.
     */
    protected void loadCaches(Ignite ignite, boolean singlePart) {
        final List<Integer> keys;

        keys = singlePart ? partitionKeys(ignite.cache(cacheName(0)), 1, NUM_OF_KEYS, 0) :
            IntStream.range(0, NUM_OF_KEYS).boxed().collect(Collectors.toList());

        for (int i = 0; i < CACHES; i++) {
            try (IgniteDataStreamer<Object, Object> s = ignite.dataStreamer(cacheName(i))) {
                s.allowOverwrite(true);

                for (Integer j : keys)
                    s.addData(j, "cache: " + i + " data: " + j);

                s.flush();
            }
        }
    }

    /**
     * @param ignite Ignite.
     */
    protected void checkDestroyCaches(Ignite ignite) throws Exception {
        loadCaches(ignite, false);

        log.warning("destroying caches....");

        ignite.cache(cacheName(0)).destroy();
        ignite.cache(cacheName(1)).destroy();

        assertEquals(CACHES - 2, ignite.cacheNames().size());

        log.warning("Stopping grid");

        stopAllGrids(false);

        log.warning("Grid stopped");

        log.warning("Starting grid");

        ignite = startGrids(NODES);

        ignite.cluster().active(true);

        log.warning("Grid started");

        assertEquals("Check that caches don't survived", CACHES - 2, ignite.cacheNames().size());

        for (Ignite ig: G.allGrids()) {
            IgniteCache cache = ig.cache(cacheName(2));

            for (int j = 0; j < NUM_OF_KEYS; j++)
                assertNotNull("Check that cache2 contains key: " + j + " node: " + ignite.name(), cache.get(j));
        }
    }

    /**
     * @param ignite Ignite instance.
     */
    protected void checkDestroyCachesAbruptly(Ignite ignite) throws Exception {
        loadCaches(ignite, false);

        log.warning("Destroying caches");

        ((GatewayProtectedCacheProxy)ignite.cache(cacheName(0))).destroyAsync();
        ((GatewayProtectedCacheProxy)ignite.cache(cacheName(1))).destroyAsync();

        log.warning("Stopping grid");

        stopAllGrids();

        log.warning("Grid stopped");

        log.warning("Starting grid");

        ignite = startGrids(NODES);

        ignite.cluster().active(true);

        log.warning("Grid started");

        for (Ignite ig: G.allGrids()) {
            assertTrue(ig.cacheNames().contains(cacheName(2)));

            IgniteCache cache = ig.cache(cacheName(2));

            for (int j = 0; j < NUM_OF_KEYS; j++)
                assertNotNull("Check that survived cache cache2 contains key: " + j + " node: " + ig.name(), cache.get(j));
        }
    }

    /**
     * @param ignite Ignite.
     */
    protected void startCachesDynamically(Ignite ignite) {
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
    protected void startGroupCachesDynamically(Ignite ignite) {
        startGroupCachesDynamically(ignite, false);
    }

    /**
     * @param ignite Ignite instance.
     * @param loc {@code True} for local caches.
     */
    protected void startGroupCachesDynamically(Ignite ignite, boolean loc) {
        List<CacheConfiguration> ccfg = new ArrayList<>(CACHES);

        for (int i = 0; i < CACHES; i++)
            ccfg.add(new CacheConfiguration<>(cacheName(i)).setCacheMode(loc ? LOCAL : CacheMode.PARTITIONED)
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
    protected String cacheName(int idx) {
        return "cache" + idx;
    }
}
