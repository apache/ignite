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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Test covers parallel start and stop of caches.
 */
public class CacheParallelStartTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHES_COUNT = 5000;

    /** */
    private static final int GROUPS_COUNT = 50;

    /** */
    private static final String STATIC_CACHE_PREFIX = "static-cache-";

    /** */
    private static final String STATIC_CACHE_CACHE_GROUP_NAME = "static-cache-group";

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSystemThreadPoolSize(Runtime.getRuntime().availableProcessors() * 3);

        long sz = 512 * 1024 * 1024;

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setPageSize(1024)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(false)
                        .setInitialSize(sz)
                        .setMaxSize(sz)
                )
                .setWalMode(WALMode.LOG_ONLY).setCheckpointFrequency(24L * 60 * 60 * 1000);

        cfg.setDataStorageConfiguration(memCfg);

        ArrayList<Object> staticCaches = new ArrayList<>(CACHES_COUNT);

        for (int i = 0; i < CACHES_COUNT; i++)
            staticCaches.add(cacheConfiguration(STATIC_CACHE_PREFIX, i));

        cfg.setCacheConfiguration(staticCaches.toArray(new CacheConfiguration[CACHES_COUNT]));

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName, int i) {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(cacheName + i);
        cfg.setBackups(1);
        cfg.setGroupName(STATIC_CACHE_CACHE_GROUP_NAME + i % GROUPS_COUNT);
        cfg.setIndexedTypes(Long.class, Long.class);

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanupTestData();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanupTestData();
    }

    /** */
    private void cleanupTestData() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IgniteSystemProperties.IGNITE_ALLOW_START_CACHES_IN_PARALLEL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testParallelStartAndStop() throws Exception {
        testParallelStartAndStop(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testSequentialStartAndStop() throws Exception {
        testParallelStartAndStop(false);
    }

    /**
     *
     */
    private void testParallelStartAndStop(boolean parallel) throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_ALLOW_START_CACHES_IN_PARALLEL, String.valueOf(parallel));

        IgniteEx igniteEx = startGrid(0);

        IgniteEx igniteEx2 = startGrid(1);

        igniteEx.cluster().active(true);

        assertCaches(igniteEx);

        assertCaches(igniteEx2);

        igniteEx.cluster().active(false);

        assertCachesAfterStop(igniteEx);

        assertCachesAfterStop(igniteEx2);
    }

    /**
     *
     */
    private void assertCachesAfterStop(IgniteEx igniteEx) {
        assertNull(igniteEx
                .context()
                .cache()
                .cacheGroup(CU.cacheId(STATIC_CACHE_CACHE_GROUP_NAME)));

        assertTrue(igniteEx.context().cache().cacheGroups().isEmpty());

        for (int i = 0; i < CACHES_COUNT; i++) {
            assertNull(igniteEx.context().cache().cache(STATIC_CACHE_PREFIX + i));
            assertNull(igniteEx.context().cache().internalCache(STATIC_CACHE_PREFIX + i));
        }
    }

    /**
     *
     */
    private void assertCaches(IgniteEx igniteEx) {
        for (int i = 0; i < GROUPS_COUNT; i++) {
            Collection<GridCacheContext> caches = igniteEx
                    .context()
                    .cache()
                    .cacheGroup(CU.cacheId(STATIC_CACHE_CACHE_GROUP_NAME + i))
                    .caches();

            assertEquals(CACHES_COUNT / GROUPS_COUNT, caches.size());

            @Nullable CacheGroupContext cacheGrp = igniteEx
                    .context()
                    .cache()
                    .cacheGroup(CU.cacheId(STATIC_CACHE_CACHE_GROUP_NAME + i));

            for (GridCacheContext cacheContext : caches)
                assertEquals(cacheContext.group(), cacheGrp);
        }
    }
}
