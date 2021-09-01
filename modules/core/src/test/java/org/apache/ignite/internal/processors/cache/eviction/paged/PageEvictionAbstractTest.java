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
package org.apache.ignite.internal.processors.cache.eviction.paged;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class PageEvictionAbstractTest extends GridCommonAbstractTest {
    /** Offheap size for memory policy. */
    private static final int SIZE = 20 * 1024 * 1024;

    /** Page size. */
    static final int PAGE_SIZE = 2048;

    /** Number of entries. */
    static final int ENTRIES = 12_000;

    /** Empty pages pool size. */
    private static final int EMPTY_PAGES_POOL_SIZE = 100;

    /** Eviction threshold. */
    private static final double EVICTION_THRESHOLD = 0.9;

    /** Default policy name. */
    private static final String DEFAULT_POLICY_NAME = "dfltPlc";

    /**
     * @param mode Eviction mode.
     * @param configuration Configuration.
     * @return Configuration with given eviction mode set.
     */
    static IgniteConfiguration setEvictionMode(DataPageEvictionMode mode, IgniteConfiguration configuration) {
        DataRegionConfiguration[] policies = configuration.getDataStorageConfiguration().getDataRegionConfigurations();

        if (policies != null) {
            for (DataRegionConfiguration plcCfg : policies)
                plcCfg.setPageEvictionMode(mode);
        }

        configuration.getDataStorageConfiguration().getDefaultDataRegionConfiguration().setPageEvictionMode(mode);

        return configuration;
    }

    /**
     * @return Near enabled flag.
     */
    protected boolean nearEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        DataRegionConfiguration plc = new DataRegionConfiguration();

        // This will test additional segment allocation.
        plc.setInitialSize(SIZE / 2);
        plc.setMaxSize(SIZE);
        plc.setEmptyPagesPoolSize(EMPTY_PAGES_POOL_SIZE);
        plc.setEvictionThreshold(EVICTION_THRESHOLD);
        plc.setName(DEFAULT_POLICY_NAME);

        dbCfg.setDefaultDataRegionConfiguration(plc);
        dbCfg.setPageSize(PAGE_SIZE);

        cfg.setDataStorageConfiguration(dbCfg);

        return cfg;
    }

    /**
     * @param name Name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param writeSynchronizationMode Write synchronization mode.
     * @param memoryPlcName Memory policy name.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Object, Object> cacheConfig(
        @NotNull String name,
        String memoryPlcName,
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        CacheWriteSynchronizationMode writeSynchronizationMode
    ) {
        CacheConfiguration<Object, Object> cacheConfiguration = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setName(name)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setCacheMode(cacheMode)
            .setAtomicityMode(atomicityMode)
            .setDataRegionName(memoryPlcName)
            .setWriteSynchronizationMode(writeSynchronizationMode);

        if (cacheMode == CacheMode.PARTITIONED)
            cacheConfiguration.setBackups(1);

        if (nearEnabled())
            cacheConfiguration.setNearConfiguration(new NearCacheConfiguration<>());

        return cacheConfiguration;
    }
}
