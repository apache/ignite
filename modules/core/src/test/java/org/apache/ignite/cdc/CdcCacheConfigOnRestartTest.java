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

package org.apache.ignite.cdc;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Test cache stored data for in-memory CDC caches cleared on node restart.
 */
public class CdcCacheConfigOnRestartTest extends AbstractCdcTest {
    /** Ignite node. */
    private IgniteEx node;

    /** CDC future. */
    private IgniteInternalFuture<?> cdcFut;

    /** Consumer. */
    private TrackCacheEventsConsumer cnsmr;

    /** */
    public static final String PERSISTENCE = "persistence";

    /** */
    public static final String IN_MEMORY_CDC = "in-memory-cdc";

    /** */
    public static final String PURE_IN_MEMORY = "pure-in-memory";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration persistenceDr = new DataRegionConfiguration()
            .setName(PERSISTENCE)
            .setPersistenceEnabled(true);

        DataRegionConfiguration inMemoryCdc = new DataRegionConfiguration()
            .setName(IN_MEMORY_CDC)
            .setPersistenceEnabled(false)
            .setCdcEnabled(true);

        DataRegionConfiguration pureInMemory = new DataRegionConfiguration()
            .setName(PURE_IN_MEMORY)
            .setPersistenceEnabled(false)
            .setCdcEnabled(false);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDataRegionConfigurations(persistenceDr, inMemoryCdc, pureInMemory));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        node = startGrid();

        node.cluster().state(ClusterState.ACTIVE);

        cnsmr = new TrackCacheEventsConsumer();

        cdcFut = runAsync(createCdc(cnsmr, node.configuration()));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (cdcFut != null) {
            assertFalse(cdcFut.isDone());

            cdcFut.cancel();
        }

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testInMemoryCdcClearedOnRestart() throws Exception {
        String pureInMemoryCache = "grouped-pure-in-memory-cache";
        String inMemoryCdcCache = "grouped-in-memory-cdc-cache";

        node.createCache(pureInMemory(PURE_IN_MEMORY));
        node.createCache(pureInMemory(pureInMemoryCache).setGroupName("grp"));
        node.createCache(inMemoryCdc(IN_MEMORY_CDC));
        node.createCache(inMemoryCdc(inMemoryCdcCache).setGroupName("grp2"));

        assertTrue(waitForCondition(
            () -> cnsmr.evts.containsKey(CU.cacheId(IN_MEMORY_CDC))
                && cnsmr.evts.containsKey(CU.cacheId(inMemoryCdcCache)),
            getTestTimeout()
        ));

        // Pure in-memory caches must node store data.
        assertFalse(cnsmr.evts.containsKey(CU.cacheId(PURE_IN_MEMORY)));
        assertFalse(cnsmr.evts.containsKey(CU.cacheId(pureInMemoryCache)));

        stopAllGrids();
        node = startGrid();

        // Cache config must be removed on node restart.
        assertTrue(waitForCondition(() -> cnsmr.evts.isEmpty(), getTestTimeout()));
    }

    /** */
    @Test
    public void testPersistenceNotClearedOnRestart() throws Exception {
        String persistenceCache = "grouped-persistence-cache";
        String persistenceCache2 = "persistence-2";
        String inMemoryCdcCache = "grouped-in-memory-cdc-cache";

        node.createCache(persistence(PERSISTENCE));
        node.createCache(persistence(persistenceCache).setGroupName("grp"));
        node.createCache(inMemoryCdc(IN_MEMORY_CDC));
        node.createCache(inMemoryCdc(inMemoryCdcCache).setGroupName("grp2"));

        assertTrue(waitForCondition(
            () -> cnsmr.evts.containsKey(CU.cacheId(IN_MEMORY_CDC))
                && cnsmr.evts.containsKey(CU.cacheId(inMemoryCdcCache))
                && cnsmr.evts.containsKey(CU.cacheId(PERSISTENCE))
                && cnsmr.evts.containsKey(CU.cacheId(persistenceCache)),
            getTestTimeout()
        ));

        stopAllGrids();
        node = startGrid();

        // Create one more cache to know for sure CDC updated cache data.
        node.createCache(persistence(persistenceCache2));

        assertTrue(waitForCondition(() -> cnsmr.evts.containsKey(CU.cacheId(persistenceCache2)), getTestTimeout()));

        // Cache config for in-memory CDC caches must be removed on node restart.
        assertFalse(cnsmr.evts.containsKey(CU.cacheId(IN_MEMORY_CDC)));
        assertFalse(cnsmr.evts.containsKey(CU.cacheId(inMemoryCdcCache)));

        assertTrue(cnsmr.evts.containsKey(CU.cacheId(PERSISTENCE)));
        assertTrue(cnsmr.evts.containsKey(CU.cacheId(persistenceCache)));

        assertEquals(3, cnsmr.evts.size());
    }

    /** */
    private CacheConfiguration<?, ?> persistence(String cacheName) {
        return new CacheConfiguration<Integer, Integer>(cacheName).setDataRegionName(PERSISTENCE);
    }

    /** */
    private CacheConfiguration<?, ?> inMemoryCdc(String cacheName) {
        return new CacheConfiguration<Integer, Integer>(cacheName).setDataRegionName(IN_MEMORY_CDC);
    }

    /** */
    private CacheConfiguration<?, ?> pureInMemory(String cacheName) {
        return new CacheConfiguration<Integer, Integer>(cacheName).setDataRegionName(PURE_IN_MEMORY);
    }
}
