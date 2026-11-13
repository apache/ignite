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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
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
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyStandaloneCacheRemovesDirectory() throws Exception {
        IgniteEx ignite = startActiveGrid(0);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME);

        IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

        persistEntry(ignite, cache);

        File[] dirs = cacheStorageDirs(ignite, ccfg);

        assertStorageDirsExist(dirs);

        cache.destroy();

        assertStorageDirsRemoved(dirs);
    }

    /**
     * Checks that the shared group directory is removed only when the last cache of the group is destroyed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDestroySharedCacheGroupRemovesDirectoryOnlyWhenEmpty() throws Exception {
        IgniteEx ignite = startActiveGrid(0);

        CacheConfiguration<Object, Object> ccfg1 = new CacheConfiguration<>(CACHE_1).setGroupName(GROUP_NAME);
        CacheConfiguration<Object, Object> ccfg2 = new CacheConfiguration<>(CACHE_2).setGroupName(GROUP_NAME);

        ignite.createCache(ccfg1);
        ignite.createCache(ccfg2);

        File[] dirs = cacheStorageDirs(ignite, ccfg1);

        assertStorageDirsExist(dirs);

        ignite.cache(CACHE_1).destroy();

        // The group still has another cache, so its storage directory must be kept.
        assertStorageDirsExist(dirs);

        ignite.cache(CACHE_2).destroy();

        assertStorageDirsRemoved(dirs);
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

        IgniteEx ignite = startActiveGrid(0);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME)
            .setStoragePaths(extraStorages.get(0).getAbsolutePath(), extraStorages.get(1).getAbsolutePath())
            .setIndexPath(extraStorages.get(2).getAbsolutePath());

        IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

        // Storage selection for a data partition is storages[partition % storages.length], so a key of an even
        // partition lands in the first storage path and a key of an odd one in the second. Pick one key per path so
        // that both data storages are deterministically populated.
        List<Integer> evenPartKeys = partitionKeys(cache, 0, 1, 0);
        List<Integer> oddPartKeys = partitionKeys(cache, 1, 1, 0);

        cache.put(evenPartKeys.get(0), 1);
        cache.put(oddPartKeys.get(0), 2);

        forceCheckpoint(ignite);

        File[] dirs = cacheStorageDirs(ignite, ccfg);

        // Two data storages plus one index storage.
        assertEquals("Unexpected number of storage directories", 3, dirs.length);

        assertStorageDirsExist(dirs);

        // Each of the configured storage directories must actually have been used as a page-store location, not just
        // created and thereafter deleted as an empty directory.
        assertTrue("Every configured storage directory must contain a page-store file",
            Arrays.stream(dirs).allMatch(IgnitePdsDestroyRemovesCacheDirTest::containsPageStoreFile));

        cache.destroy();

        assertStorageDirsRemoved(dirs);
    }

    /**
     * Checks that the storage directories are removed on both server nodes of a two-node cluster after destroy.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyOnTwoServersRemovesDirectoriesOnBothNodes() throws Exception {
        // Start both nodes first and only then activate the cluster so that both nodes enter the baseline and both
        // actually store data. A node that is started after activation and thus not in the baseline does not store
        // data and cleans its storage directory asynchronously, which would make the synchronous assertion below
        // flaky.
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME)
            .setBackups(1);

        IgniteCache<Object, Object> cache = ignite0.createCache(ccfg);

        persistEntry(ignite0, cache);

        File[] dirs0 = cacheStorageDirs(ignite0, ccfg);
        File[] dirs1 = cacheStorageDirs(ignite1, ccfg);

        assertStorageDirsExist(dirs0);
        assertStorageDirsExist(dirs1);

        cache.destroy();

        // On the initiating node the directory is removed synchronously as part of the destroy exchange.
        assertStorageDirsRemoved(dirs0);

        // On a remote server the local directory cleanup runs in its own exchange thread and may not be finished by
        // the time destroy() returns on the initiating node, so a bounded wait is required here.
        assertStorageDirsRemovedEventually(dirs1);
    }

    /**
     * Checks that a cache of the same name can be immediately recreated after destroy and remains functional.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheCanBeRecreatedAfterDirectoryRemoval() throws Exception {
        IgniteEx ignite = startActiveGrid(0);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME);

        IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

        persistEntry(ignite, cache);

        File[] dirs = cacheStorageDirs(ignite, ccfg);

        assertStorageDirsExist(dirs);

        cache.destroy();

        assertStorageDirsRemoved(dirs);

        // Immediately recreate a cache with the same name.
        IgniteCache<Object, Object> newCache = ignite.createCache(ccfg);

        File[] newDirs = cacheStorageDirs(ignite, ccfg);

        assertStorageDirsExist(newDirs);

        newCache.put(1, 1);

        assertEquals(1, newCache.get(1));
    }

    /**
     * Checks that a non-empty storage directory is preserved on destroy: the fix removes only empty leftover
     * directories and never deletes unrelated files.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyKeepsNonEmptyStorageDirectory() throws Exception {
        IgniteEx ignite = startActiveGrid(0);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME);

        IgniteCache<Object, Object> cache = ignite.createCache(ccfg);

        persistEntry(ignite, cache);

        File dir = cacheStorageDirs(ignite, ccfg)[0];

        Path marker = dir.toPath().resolve("do-not-delete");

        Files.write(marker, new byte[] {1});

        cache.destroy();

        assertTrue("Non-empty storage directory must be preserved", dir.exists());
        assertTrue("Foreign file must not be removed", Files.exists(marker));
    }

    /**
     * Starts a grid node and activates the cluster.
     *
     * @param idx Node index.
     * @return Started node.
     */
    private IgniteEx startActiveGrid(int idx) throws Exception {
        IgniteEx ignite = startGrid(idx);

        ignite.cluster().state(ClusterState.ACTIVE);

        return ignite;
    }

    /**
     * Puts a single entry into the given cache and forces a checkpoint so that partition/index files are actually
     * written to the persistent storage before {@code destroy()}.
     *
     * @param ignite Node.
     * @param cache Cache.
     */
    private void persistEntry(IgniteEx ignite, IgniteCache<Object, Object> cache) throws Exception {
        cache.put(0, 0);

        forceCheckpoint(ignite);
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
     * Asserts that every cache storage directory exists.
     *
     * @param dirs Storage directories.
     */
    private void assertStorageDirsExist(File... dirs) {
        for (File dir : dirs)
            assertTrue("Cache storage directory must exist [dir=" + dir + ']', dir.exists());
    }

    /**
     * Asserts that the given cache storage directories have been removed. {@code cache.destroy()} is synchronous and
     * shuts the page stores down before deleting the directories, so they are expected to be gone immediately after
     * it returns.
     *
     * @param dirs Storage directories to assert removal of.
     */
    private void assertStorageDirsRemoved(File... dirs) {
        for (File dir : dirs)
            assertFalse("Storage directory was not removed [dir=" + dir + ']', dir.exists());
    }

    /**
     * Asserts that the given cache storage directories are eventually removed. Unlike the initiating node, a remote
     * server performs its local directory cleanup in its own exchange thread, which may lag slightly behind the
     * {@code destroy()} call, so a short bounded wait is used.
     *
     * @param dirs Storage directories to assert removal of.
     */
    private void assertStorageDirsRemovedEventually(File... dirs) throws Exception {
        boolean res = GridTestUtils.waitForCondition(() -> Arrays.stream(dirs).noneMatch(File::exists), 10_000);

        assertTrue("Storage directories were not removed [dirs=" + Arrays.toString(dirs) + ']', res);
    }

    /**
     * @param dir Directory.
     * @return {@code True} if the directory is non-empty, i.e. it actually stored page-store files.
     */
    private static boolean containsPageStoreFile(File dir) {
        File[] files = dir.listFiles();

        return files != null && files.length > 0;
    }

    /**
     * @return A unique, per-test directory under the default work directory to be used as an external storage root.
     */
    private File newUniqueStorageDir() throws Exception {
        return new File(U.defaultWorkDirectory(), getClass().getSimpleName() + "-" + UUID.randomUUID());
    }
}
