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
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
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
 * Tests that {@link IgniteCache#destroy()} removes the (now empty) cache storage directories from the persistent
 * storage, see IGNITE-13989.
 */
public class IgnitePdsDestroyRemovesCacheDirTest extends GridCommonAbstractTest {
    /** Cache name used for most of the tests. */
    private static final String CACHE_NAME = "cache";

    /** Cache name for the shared cache group tests. */
    private static final String CACHE_1 = "cache-1";

    /** Cache name for the shared cache group tests. */
    private static final String CACHE_2 = "cache-2";

    /** Cache group name. */
    private static final String GROUP_NAME = "grp";

    /** Number of keys written before destroy to make the cache persistent-backed. */
    private static final int KEYS_CNT = 1000;

    /**
     * Unique per-test roots of the extra storage paths ({@code DataStorageConfiguration#setExtraStoragePaths}).
     * {@code Null} when a test uses only the default storage.
     */
    private List<File> extraStorages;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            );

        if (extraStorages != null) {
            String[] extra = extraStorages.stream().map(File::getAbsolutePath).toArray(String[]::new);

            dsCfg.setExtraStoragePaths(extra);
        }

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        extraStorages = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        if (extraStorages != null)
            extraStorages.forEach(U::delete);

        super.afterTest();
    }

    /**
     * @param ignite Node.
     * @param ccfg Cache configuration.
     * @return Storage directories of the cache group on the given node.
     */
    private File[] cacheStorageDirs(IgniteEx ignite, CacheConfiguration<?, ?> ccfg) {
        return ignite.context().pdsFolderResolver().fileTree().cacheStorages(ccfg);
    }

    /**
     * Asserts that every cache storage directory exists on the given node.
     *
     * @param ignite Node.
     * @param ccfg Cache configuration.
     */
    private void assertStorageDirsExist(IgniteEx ignite, CacheConfiguration<?, ?> ccfg) {
        File[] dirs = cacheStorageDirs(ignite, ccfg);

        for (File dir : dirs)
            assertTrue("Cache storage directory must exist [node=" + ignite.name() + ", dir=" + dir + ']',
                dir.exists());
    }

    /**
     * Asserts that the given cache storage directories have been removed. {@code cache.destroy()} is synchronous, so
     * the files are expected to be gone right after it returns; a short bounded poll is used only to absorb any
     * remaining asynchronous fs activity.
     *
     * @param dirs Storage directories to assert removal of.
     */
    private void assertStorageDirsRemoved(final File... dirs) throws Exception {
        boolean res = GridTestUtils.waitForCondition(() -> Arrays.stream(dirs).noneMatch(File::exists), 5_000);

        if (!res) {
            String remaining = Arrays.stream(dirs)
                .map(File::getAbsolutePath)
                .collect(Collectors.joining(", "));

            fail("Cache storage directories must be removed after destroy, remaining: " + remaining);
        }
    }

    /**
     * Puts some data into the given cache and forces a checkpoint so that partition/index files are actually written
     * to the persistent storage before {@code destroy()}.
     *
     * @param ignite Node.
     * @param cache Cache.
     */
    private void loadDataAndCheckpoint(IgniteEx ignite, IgniteCache<Object, Object> cache) throws Exception {
        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(i, "value-" + i);

        forceCheckpoint(ignite);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyStandaloneCacheRemovesDirectory() throws Exception {
        try (IgniteEx ignite = startGrid(0)) {
            ignite.cluster().state(ClusterState.ACTIVE);

            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME);

            IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

            loadDataAndCheckpoint(ignite, cache);

            File[] dirs = cacheStorageDirs(ignite, ccfg);

            assertStorageDirsExist(ignite, ccfg);

            cache.destroy();

            assertStorageDirsRemoved(dirs);
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
            for (File dir : dirs)
                assertTrue("Shared group storage directory must stay while other caches remain [dir=" + dir + ']',
                    dir.exists());

            ignite.cache(CACHE_2).destroy();

            assertStorageDirsRemoved(dirs);
        }
    }

    /**
     * Checks that all storage directories (two data storage paths plus a distinct index path) are removed after
     * destroy.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyRemovesAllStorageDirectories() throws Exception {
        extraStorages = Arrays.asList(newUniqueStorageDir(), newUniqueStorageDir(), newUniqueStorageDir());

        try (IgniteEx ignite = startGrid(0)) {
            ignite.cluster().state(ClusterState.ACTIVE);

            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME)
                .setStoragePaths(extraStorages.get(0).getAbsolutePath(), extraStorages.get(1).getAbsolutePath())
                .setIndexPath(extraStorages.get(2).getAbsolutePath());

            IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

            loadDataAndCheckpoint(ignite, cache);

            File[] dirs = cacheStorageDirs(ignite, ccfg);

            // Two data storages plus one index storage.
            assertEquals("Unexpected number of storage directories", 3, dirs.length);

            assertStorageDirsExist(ignite, ccfg);

            cache.destroy();

            assertStorageDirsRemoved(dirs);
        }
    }

    /**
     * Checks that the storage directories are removed on both server nodes of a two-node cluster after destroy.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyOnTwoServersRemovesDirectoriesOnBothNodes() throws Exception {
        try (IgniteEx ignite0 = startGrid(0); IgniteEx ignite1 = startGrid(1)) {
            ignite0.cluster().state(ClusterState.ACTIVE);

            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME)
                .setBackups(1);

            IgniteCache<Object, Object> cache = ignite0.createCache(ccfg);

            loadDataAndCheckpoint(ignite0, cache);

            File[] dirs0 = cacheStorageDirs(ignite0, ccfg);
            File[] dirs1 = cacheStorageDirs(ignite1, ccfg);

            assertStorageDirsExist(ignite0, ccfg);
            assertStorageDirsExist(ignite1, ccfg);

            cache.destroy();

            assertStorageDirsRemoved(dirs0);
            assertStorageDirsRemoved(dirs1);
        }
    }

    /**
     * Checks that a cache of the same name can be immediately recreated after destroy and remains functional.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyAndRecreateCache() throws Exception {
        try (IgniteEx ignite = startGrid(0)) {
            ignite.cluster().state(ClusterState.ACTIVE);

            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME);

            IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

            loadDataAndCheckpoint(ignite, cache);

            File[] dirs = cacheStorageDirs(ignite, ccfg);

            assertStorageDirsExist(ignite, ccfg);

            cache.destroy();

            assertStorageDirsRemoved(dirs);

            // Immediately recreate a cache with the same name.
            IgniteCache<Object, Object> newCache = ignite.createCache(ccfg);

            assertStorageDirsExist(ignite, ccfg);

            newCache.put(1, 1);

            assertEquals(1, newCache.get(1));

            // The recreated cache must be able to persist and read back its own data.
            forceCheckpoint(ignite);

            newCache.put(2, 2);

            assertEquals(2, newCache.get(2));
        }
    }

    /**
     * @return A unique, per-test directory under the default work directory to be used as an external storage root.
     */
    private File newUniqueStorageDir() throws Exception {
        return new File(U.defaultWorkDirectory(), getClass().getSimpleName() + "-" + UUID.randomUUID());
    }
}
