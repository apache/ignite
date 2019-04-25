/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.database.baseline;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunAtomicCacheOpTest;

/**
 *
 */
public class IgniteBaselineLockPartitionOnAffinityRunAtomicCacheTest extends IgniteCacheLockPartitionOnAffinityRunAtomicCacheOpTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setInitialSize(200L * 1024 * 1024)
                        .setMaxSize(200L * 1024 * 1024)
                        .setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setBackups(2);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        int gridCnt = gridCount();

        startGrids(gridCnt + 1);

        grid(0).active(true);

        stopGrid(gridCnt);

        startGrid(gridCnt + 1);

        fillCaches();

        createCaches();

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        destroyCaches();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();
    }
}
