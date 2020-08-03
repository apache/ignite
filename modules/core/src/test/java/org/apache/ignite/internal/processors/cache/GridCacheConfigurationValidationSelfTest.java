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

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.LOCAL;
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
    private static final String WRONG_PRELOAD_MODE_IGNITE_INSTANCE_NAME = "preloadModeCheckFails";

    /** */
    private static final String WRONG_CACHE_MODE_IGNITE_INSTANCE_NAME = "cacheModeCheckFails";

    /** */
    private static final String WRONG_AFFINITY_IGNITE_INSTANCE_NAME = "cacheAffinityCheckFails";

    /** */
    private static final String WRONG_AFFINITY_MAPPER_IGNITE_INSTANCE_NAME = "cacheAffinityMapperCheckFails";

    /** */
    private static final String DUP_CACHES_IGNITE_INSTANCE_NAME = "duplicateCachesCheckFails";

    /** */
    private static final String DUP_DFLT_CACHES_IGNITE_INSTANCE_NAME = "duplicateDefaultCachesCheckFails";

    /** */
    private static final String RESERVED_FOR_DATASTRUCTURES_CACHE_NAME_IGNITE_INSTANCE_NAME =
        "reservedForDsCacheNameCheckFails";

    /** */
    private static final String RESERVED_FOR_DATASTRUCTURES_CACHE_GROUP_NAME_IGNITE_INSTANCE_NAME =
            "reservedForDsCacheGroupNameCheckFails";

    /** */
    private static final String CACHE_NAME_WITH_SPECIAL_CHARACTERS_REPLICATED = "--№=+:(replicated)";

    /** */
    private static final String CACHE_NAME_WITH_SPECIAL_CHARACTERS_PARTITIONED = ":_&:: (partitioned)";

    /**
     * Constructs test.
     */
    public GridCacheConfigurationValidationSelfTest() {
        super(/* don't start grid */ false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Default cache config.
        CacheConfiguration dfltCacheCfg = defaultCacheConfiguration();

        dfltCacheCfg.setCacheMode(PARTITIONED);
        dfltCacheCfg.setRebalanceMode(ASYNC);
        dfltCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        dfltCacheCfg.setAffinity(new RendezvousAffinityFunction());
        dfltCacheCfg.setIndexedTypes(
            Integer.class, String.class
        );
        dfltCacheCfg.setName(CACHE_NAME_WITH_SPECIAL_CHARACTERS_PARTITIONED);

        // Non-default cache configuration.
        CacheConfiguration namedCacheCfg = defaultCacheConfiguration();

        namedCacheCfg.setCacheMode(PARTITIONED);
        namedCacheCfg.setRebalanceMode(ASYNC);
        namedCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        namedCacheCfg.setAffinity(new RendezvousAffinityFunction());

        // Local cache configuration.
        CacheConfiguration localCacheCfg = defaultCacheConfiguration();

        localCacheCfg.setCacheMode(LOCAL);

        // Modify cache config according to test parameters.
        if (igniteInstanceName.contains(WRONG_PRELOAD_MODE_IGNITE_INSTANCE_NAME))
            dfltCacheCfg.setRebalanceMode(SYNC);
        else if (igniteInstanceName.contains(WRONG_CACHE_MODE_IGNITE_INSTANCE_NAME))
            dfltCacheCfg.setCacheMode(REPLICATED);
        else if (igniteInstanceName.contains(WRONG_AFFINITY_IGNITE_INSTANCE_NAME))
            dfltCacheCfg.setAffinity(new TestRendezvousAffinityFunction());
        else if (igniteInstanceName.contains(WRONG_AFFINITY_MAPPER_IGNITE_INSTANCE_NAME))
            dfltCacheCfg.setAffinityMapper(new TestCacheDefaultAffinityKeyMapper());

        if (igniteInstanceName.contains(DUP_CACHES_IGNITE_INSTANCE_NAME))
            cfg.setCacheConfiguration(namedCacheCfg, namedCacheCfg);
        else if (igniteInstanceName.contains(DUP_DFLT_CACHES_IGNITE_INSTANCE_NAME))
            cfg.setCacheConfiguration(dfltCacheCfg, dfltCacheCfg);
        else {
            // Normal configuration.
            if (!cfg.isClientMode())
                cfg.setCacheConfiguration(dfltCacheCfg, namedCacheCfg, localCacheCfg);
        }

        if (igniteInstanceName.contains(RESERVED_FOR_DATASTRUCTURES_CACHE_NAME_IGNITE_INSTANCE_NAME))
            namedCacheCfg.setName(DataStructuresProcessor.ATOMICS_CACHE_NAME + "@abc");
        else {
            namedCacheCfg.setCacheMode(REPLICATED);
            namedCacheCfg.setName(CACHE_NAME_WITH_SPECIAL_CHARACTERS_REPLICATED);
        }

        if (igniteInstanceName.contains(RESERVED_FOR_DATASTRUCTURES_CACHE_GROUP_NAME_IGNITE_INSTANCE_NAME))
            namedCacheCfg.setGroupName("default-ds-group");

        return cfg;
    }

    /**
     * This test method does not require remote nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDuplicateCacheConfigurations() throws Exception {
        // This grid should not start.
        startInvalidGrid(DUP_CACHES_IGNITE_INSTANCE_NAME);

        // This grid should not start.
        startInvalidGrid(DUP_DFLT_CACHES_IGNITE_INSTANCE_NAME);
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testCacheAttributesValidation() throws Exception {
        try {
            startGrid(0);

            // This grid should not start.
            startInvalidGrid(WRONG_PRELOAD_MODE_IGNITE_INSTANCE_NAME);

            // This grid should not start.
            startInvalidGrid(WRONG_CACHE_MODE_IGNITE_INSTANCE_NAME);

            // This grid should not start.
            startInvalidGrid(WRONG_AFFINITY_IGNITE_INSTANCE_NAME);

            // This grid should not start.
            startInvalidGrid(WRONG_AFFINITY_MAPPER_IGNITE_INSTANCE_NAME);

            // This grid should not start.
            startInvalidGrid(RESERVED_FOR_DATASTRUCTURES_CACHE_NAME_IGNITE_INSTANCE_NAME);

            // This grid should not start.
            startInvalidGrid(RESERVED_FOR_DATASTRUCTURES_CACHE_GROUP_NAME_IGNITE_INSTANCE_NAME);

            // This grid will start normally.
            startGrid(1);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testCacheNames() throws Exception {
        try {
            startGridsMultiThreaded(2);

            Collection<String> names = grid(0).cacheNames();

            assertEquals(3, names.size());

            for (String name : names)
                assertTrue(name.equals(CACHE_NAME_WITH_SPECIAL_CHARACTERS_PARTITIONED)
                        || name.equals(CACHE_NAME_WITH_SPECIAL_CHARACTERS_REPLICATED)
                        || DEFAULT_CACHE_NAME.equals(name));

            Ignite client = startClientGrid(2);

            names = client.cacheNames();

            assertEquals(3, names.size());
        }
        finally {
            stopAllGrids();
        }
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
