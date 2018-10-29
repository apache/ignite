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
package org.apache.ignite.internal.processors.database;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Data region metrics test with enabled persistence.
 */
public class DataRegionMetricsPdsSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(30 * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setMetricsEnabled(true)
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override public void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    public void testTotalAllocatedPagesMetric() throws Exception {
        Ignite node = startGrid(0);

        node.cluster().active(true);

        IgniteCache<String, String> cache = node.createCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1_000; i++)
            cache.put("key " + i, "value");

        forceCheckpoint(node);

        checkAllocationTracker(GridCacheDatabaseSharedManager.METASTORE_DATA_REGION_NAME);
        checkAllocationTracker(DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME);
        checkAllocationTracker(IgniteCacheDatabaseSharedManager.SYSTEM_DATA_REGION_NAME);
    }

    /**
     * @param regionName Memory region name.
     * @throws Exception If failed.
     */
    private void checkAllocationTracker(String regionName) throws Exception {
        GridCacheSharedContext cctx = grid(0).context().cache().context();

        long allocatedSize = cctx.database().dataRegion(regionName).memoryMetrics().getTotalAllocatedSize();

        assertTrue("\"" + regionName + "\" region size=" + allocatedSize, allocatedSize > 0);
    }
}
