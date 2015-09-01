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

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Attribute validation self test.
 */
public class GridCacheConfigurationValidationSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String NON_DFLT_CACHE_NAME = "non-dflt-cache";

    /** */
    private static final String WRONG_PRELOAD_MODE_GRID_NAME = "preloadModeCheckFails";

    /** */
    private static final String WRONG_CACHE_MODE_GRID_NAME = "cacheModeCheckFails";

    /** */
    private static final String WRONG_AFFINITY_GRID_NAME = "cacheAffinityCheckFails";

    /** */
    private static final String WRONG_AFFINITY_MAPPER_GRID_NAME = "cacheAffinityMapperCheckFails";

    /** */
    private static final String WRONG_OFF_HEAP_GRID_NAME = "cacheOhhHeapCheckFails";

    /** */
    private static final String DUP_CACHES_GRID_NAME = "duplicateCachesCheckFails";

    /** */
    private static final String DUP_DFLT_CACHES_GRID_NAME = "duplicateDefaultCachesCheckFails";

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * Constructs test.
     */
    public GridCacheConfigurationValidationSelfTest() {
        super(/* don't start grid */ false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        // Default cache config.
        CacheConfiguration dfltCacheCfg = defaultCacheConfiguration();

        dfltCacheCfg.setCacheMode(PARTITIONED);
        dfltCacheCfg.setRebalanceMode(ASYNC);
        dfltCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        dfltCacheCfg.setAffinity(new RendezvousAffinityFunction());
        dfltCacheCfg.setIndexedTypes(
            Integer.class, String.class
        );

        // Non-default cache configuration.
        CacheConfiguration namedCacheCfg = defaultCacheConfiguration();

        namedCacheCfg.setCacheMode(PARTITIONED);
        namedCacheCfg.setRebalanceMode(ASYNC);
        namedCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        namedCacheCfg.setName(NON_DFLT_CACHE_NAME);
        namedCacheCfg.setAffinity(new RendezvousAffinityFunction());

        // Modify cache config according to test parameters.
        if (gridName.contains(WRONG_PRELOAD_MODE_GRID_NAME))
            dfltCacheCfg.setRebalanceMode(SYNC);
        else if (gridName.contains(WRONG_CACHE_MODE_GRID_NAME))
            dfltCacheCfg.setCacheMode(REPLICATED);
        else if (gridName.contains(WRONG_AFFINITY_GRID_NAME))
            dfltCacheCfg.setAffinity(new TestRendezvousAffinityFunction());
        else if (gridName.contains(WRONG_AFFINITY_MAPPER_GRID_NAME))
            dfltCacheCfg.setAffinityMapper(new TestCacheDefaultAffinityKeyMapper());
        else if (gridName.contains(WRONG_OFF_HEAP_GRID_NAME))
            dfltCacheCfg.setMemoryMode(OFFHEAP_VALUES);

        if (gridName.contains(DUP_CACHES_GRID_NAME))
            cfg.setCacheConfiguration(namedCacheCfg, namedCacheCfg);
        else if (gridName.contains(DUP_DFLT_CACHES_GRID_NAME))
            cfg.setCacheConfiguration(dfltCacheCfg, dfltCacheCfg);
        else
            // Normal configuration.
            cfg.setCacheConfiguration(dfltCacheCfg, namedCacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * This test method does not require remote nodes.
     *
     * @throws Exception If failed.
     */
    public void testDuplicateCacheConfigurations() throws Exception {
        // This grid should not start.
        startInvalidGrid(DUP_CACHES_GRID_NAME);

        // This grid should not start.
        startInvalidGrid(DUP_DFLT_CACHES_GRID_NAME);
    }

    /**
     * @throws Exception If fails.
     */
    public void testCacheAttributesValidation() throws Exception {
        try {
            startGrid(0);

            // This grid should not start.
            startInvalidGrid(WRONG_PRELOAD_MODE_GRID_NAME);

            // This grid should not start.
            startInvalidGrid(WRONG_CACHE_MODE_GRID_NAME);

            // This grid should not start.
            startInvalidGrid(WRONG_AFFINITY_GRID_NAME);

            // This grid should not start.
            startInvalidGrid(WRONG_AFFINITY_MAPPER_GRID_NAME);

            // This grid will start normally.
            startGrid(1);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvalidOffHeapConfiguration() throws Exception {
        startInvalidGrid(WRONG_OFF_HEAP_GRID_NAME);
    }

    /**
     * Starts grid that will fail to start due to invalid configuration.
     *
     * @param name Name of the grid which will have invalid configuration.
     */
    private void startInvalidGrid(String name) {
        try {
            startGrid(name);

            assert false : "Exception should have been thrown.";
        }
        catch (Exception e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     *
     */
    private static class TestRendezvousAffinityFunction extends RendezvousAffinityFunction {
        // No-op. Just to have another class name.

        /**
         * Empty constructor required by Externalizable.
         */
        public TestRendezvousAffinityFunction() {
            // No-op.
        }
    }

    /**
     *
     */
    private static class TestCacheDefaultAffinityKeyMapper extends GridCacheDefaultAffinityKeyMapper {
        // No-op. Just to have another class name.
    }
}