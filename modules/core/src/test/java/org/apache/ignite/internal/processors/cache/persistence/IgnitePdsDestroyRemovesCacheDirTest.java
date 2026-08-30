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

import java.io.File;
import java.util.Arrays;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that {@link IgniteCache#destroy()} removes the (empty) cache storage directories from the persistent storage,
 * see IGNITE-13989.
 */
public class IgnitePdsDestroyRemovesCacheDirTest extends GridCommonAbstractTest {
    /** Cache name for the shared cache group tests. */
    private static final String CACHE_1 = "cache-1";

    /** Cache name for the shared cache group tests. */
    private static final String CACHE_2 = "cache-2";

    /** Cache group name. */
    private static final String GROUP_NAME = "grp";

    /** Additional storage path for multi-storage test. */
    private File extraStorage;

    /** Separate index path for index storage test. */
    private File indexPath;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            );

        if (extraStorage != null)
            dsCfg.setExtraStoragePaths(extraStorage.getAbsolutePath());

        if (indexPath != null) {
            // The index storage must be one of the DataStorageConfiguration storage paths, otherwise cache
            // start validation fails.
            if (extraStorage != null)
                dsCfg.setExtraStoragePaths(extraStorage.getAbsolutePath(), indexPath.getAbsolutePath());
            else
                dsCfg.setExtraStoragePaths(indexPath.getAbsolutePath());
        }

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        extraStorage = null;
        indexPath = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        if (extraStorage != null)
            U.delete(extraStorage);

        if (indexPath != null)
            U.delete(indexPath);

        super.afterTest();
    }

    /**
     * @param ignite Node.
     * @param ccfg Cache configuration.
     * @return Storage directories of the cache group.
     */
    private File[] cacheStorageDirs(IgniteEx ignite, CacheConfiguration<?, ?> ccfg) {
        return ignite.context().pdsFolderResolver().fileTree().cacheStorages(ccfg);
    }

    /**
     * @param ignite Node.
     * @param ccfg Cache configuration.
     */
    private void assertStorageDirsExist(IgniteEx ignite, CacheConfiguration<?, ?> ccfg) {
        for (File dir : cacheStorageDirs(ignite, ccfg))
            assertTrue("Cache storage directory must exist: " + dir, dir.exists());
    }

    /**
     * @param dirs Storage directories to await removal of.
     */
    private void awaitStorageDirsRemoved(final File... dirs) throws Exception {
        boolean res = GridTestUtils.waitForCondition(() -> Arrays.stream(dirs).noneMatch(File::exists), 30_000);

        assertTrue("Cache storage directories must be removed after destroy: " + Arrays.toString(dirs), res);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyStandaloneCacheRemovesDirectory() throws Exception {
        try (IgniteEx ignite = startGrid(0)) {
            ignite.cluster().state(ClusterState.ACTIVE);

            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

            IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

            File[] dirs = cacheStorageDirs(ignite, ccfg);

            assertStorageDirsExist(ignite, ccfg);

            cache.destroy();

            awaitStorageDirsRemoved(dirs);
        }
    }

    /**
     * Checks that the shared group directory is removed only when the last cache of the group is destroyed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDestroySharedCacheGroupRemovesDirectoryOnlyWhenEmpty() throws Exception {
        try (IgniteEx ignite = startGrid(0)) {
            ignite.cluster().state(ClusterState.ACTIVE);

            CacheConfiguration<Object, Object> ccfg1 = new CacheConfiguration<>(CACHE_1).setGroupName(GROUP_NAME);
            CacheConfiguration<Object, Object> ccfg2 = new CacheConfiguration<>(CACHE_2).setGroupName(GROUP_NAME);

            ignite.createCache(ccfg1);
            ignite.createCache(ccfg2);

            File[] dirs = cacheStorageDirs(ignite, ccfg1);

            assertStorageDirsExist(ignite, ccfg1);

            ignite.cache(CACHE_1).destroy();

            awaitPartitionMapExchange();

            // The group still has another cache, so its storage directory must be kept.
            assertTrue("Shared group storage directory must stay while other caches remain: " + Arrays.toString(dirs),
                Arrays.stream(dirs).allMatch(File::exists));

            ignite.cache(CACHE_2).destroy();

            awaitStorageDirsRemoved(dirs);
        }
    }

    /**
     * Checks that all storage directories (including an extra storage and a dedicated index storage) are removed after
     * destroy.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyRemovesAllStorageDirectories() throws Exception {
        extraStorage = new File(U.defaultWorkDirectory(), "extra_storage");
        indexPath = new File(U.defaultWorkDirectory(), "index_storage");

        try (IgniteEx ignite = startGrid(0)) {
            ignite.cluster().state(ClusterState.ACTIVE);

            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setStoragePaths(extraStorage.getAbsolutePath())
                .setIndexPath(indexPath.getAbsolutePath());

            IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

            File[] dirs = cacheStorageDirs(ignite, ccfg);

            assertStorageDirsExist(ignite, ccfg);

            cache.destroy();

            awaitStorageDirsRemoved(dirs);
        }
    }
}
