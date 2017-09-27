/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.filename;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdGeneratingFoldersResolver;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 * Test for new and old stype persistent storage folders generation
 */
public class IgniteUidAsConsistentIdMigrationTest extends GridCommonAbstractTest {

    /** Cache name for test. */
    public static final String CACHE_NAME = "dummy";

    private boolean deleteAfter = false;
    private boolean deleteBefore = true;

    /** Configured consistent id. */
    private String configuredConsistentId;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        if (deleteBefore)
            deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        if (deleteAfter)
            deleteWorkFiles();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        assertTrue(deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false)));
        assertTrue(deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", false)));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);
        if (configuredConsistentId != null)
            cfg.setConsistentId(configuredConsistentId);
        final PersistentStoreConfiguration psCfg = new PersistentStoreConfiguration();
        cfg.setPersistentStoreConfiguration(psCfg);

        final MemoryConfiguration memCfg = new MemoryConfiguration();
        final MemoryPolicyConfiguration memPolCfg = new MemoryPolicyConfiguration();
        memPolCfg.setMaxSize(32 * 1024 * 1024); // we don't need much memory for this test
        memCfg.setMemoryPolicies(memPolCfg);
        cfg.setMemoryConfiguration(memCfg);
        return cfg;
    }

    /**
     * Checks start on empty PDS folder, in that case node 0 should start with random UUID.
     *
     * @throws Exception if failed.
     */
    public void testNewStyleIdIsGenerated() throws Exception {
        IgniteEx igniteEx = startGrid(0);
        igniteEx.active(true);
        igniteEx.getOrCreateCache(CACHE_NAME).put("hi", "there!");

        final String consistentId = igniteEx.cluster().localNode().consistentId().toString();
        assertPdsDirsDefaultExist(newStyleSubfolder(igniteEx, 0));
        stopGrid(0);
        UUID.fromString(consistentId); //test UUID is parsaable from consistent ID test
    }

    /**
     * Checks start on empty PDS folder using configured ConsistentId. We should start using this ID in compatible mode.
     *
     * @throws Exception if failed.
     */
    public void testPreconfiguredConsitentIdIsApplied() throws Exception {
        this.configuredConsistentId = "someConfiguredConsistentId";
        IgniteEx igniteEx = startGrid(0);
        igniteEx.active(true);
        igniteEx.getOrCreateCache(CACHE_NAME).put("hi", "there!");

        assertPdsDirsDefaultExist(configuredConsistentId);
        stopGrid(0);
    }

    /**
     * Checks start on configured ConsistentId with same value as default, this emulate old style folder is already
     * available. We should restart using this folder.
     *
     * @throws Exception if failed
     */
    public void testRestartOnExistingOldStyleId() throws Exception {
        final String expDfltConsistentId = "127.0.0.1:47500";
        this.configuredConsistentId = expDfltConsistentId; //this is for create old node folder

        final Ignite igniteEx = startGrid(0);
        igniteEx.active(true);

        final String expVal = "there is compatible mode with old style folders!";

        igniteEx.getOrCreateCache(CACHE_NAME).put("hi", expVal);

        assertPdsDirsDefaultExist(U.maskForFileName(configuredConsistentId));
        stopGrid(0);

        this.configuredConsistentId = null; //now set up grid on existing folder

        final Ignite igniteRestart = startGrid(0);
        igniteRestart.active(true);

        assertEquals(expDfltConsistentId, igniteRestart.cluster().localNode().consistentId());
        final IgniteCache<Object, Object> cache = igniteRestart.cache(CACHE_NAME);

        assertNotNull("Expected to have cache [" + CACHE_NAME + "] using [" + expDfltConsistentId + "] as PDS folder", cache);
        final Object valFromCache = cache.get("hi");

        assertNotNull("Expected to load data from cache using [" + expDfltConsistentId + "] as PDS folder", valFromCache);
        assertTrue(expVal.equals(valFromCache));
        stopGrid(0);
    }

    /**
     * Start stop grid without activation should cause lock to be released and restarted node should have index 0
     *
     * @throws Exception if failed
     */
    public void testStartWithoutActivate() throws Exception {
        //start stop grid without activate
        startGrid(0);
        stopGrid(0);

        IgniteEx igniteRestart = startGrid(0);
        igniteRestart.active(true);
        igniteRestart.getOrCreateCache(CACHE_NAME).put("hi", "there!");
        assertPdsDirsDefaultExist("Node0-" + igniteRestart.cluster().localNode().consistentId().toString());
        stopGrid(0);
    }

    /**
     * Checks start on empty PDS folder, in that case node 0 should start with random UUID
     *
     * @throws Exception if failed
     */
    public void testRestartOnSameFolderWillCauseSameUuidGeneration() throws Exception {
        final UUID uuid;
        {
            final Ignite ignite = startGrid(0);
            ignite.active(true);
            ignite.getOrCreateCache(CACHE_NAME).put("hi", "there!");

            assertPdsDirsDefaultExist(newStyleSubfolder(ignite, 0));

            uuid = (UUID)ignite.cluster().localNode().consistentId();
            stopGrid(0);
        }

        {
            final Ignite igniteRestart = startGrid(0);
            igniteRestart.active(true);
            assertTrue("there!".equals(igniteRestart.cache(CACHE_NAME).get("hi")));

            final Object consIdRestart = igniteRestart.cluster().localNode().consistentId();
            assertPdsDirsDefaultExist(newStyleSubfolder(igniteRestart, 0));
            stopGrid(0);

            assertEquals(uuid, consIdRestart);
        }
    }

    /**
     * Generates folder name in new style using constant prefix and UUID
     *
     * @param ignite ignite instance
     * @param nodeIdx expected node index to check
     * @return name of storage related subfolders
     */
    @NotNull private String newStyleSubfolder(final Ignite ignite, final int nodeIdx) {
        final String consistentId = ignite.cluster().localNode().consistentId().toString();
        return PdsConsistentIdGeneratingFoldersResolver.DB_FOLDER_PREFIX + Integer.toString(nodeIdx) + "-" + consistentId;
    }

    /**
     * test two nodes started at the same db root folder, second node should get index 1
     *
     * @throws Exception if failed
     */
    public void  testNodeIndexIncremented() throws Exception {
        final Ignite ignite0 = startGrid(0);
        final Ignite ignite1 = startGrid(1);

        ignite0.active(true);

        ignite0.getOrCreateCache(CACHE_NAME).put("hi", "there!");
        ignite1.getOrCreateCache(CACHE_NAME).put("hi1", "there!");

        assertPdsDirsDefaultExist(newStyleSubfolder(ignite0, 0));
        assertPdsDirsDefaultExist(newStyleSubfolder(ignite1, 1));

        stopGrid(0);
        stopGrid(1);
    }

    /**
     * Test verified that new style folder is taken always with lowest index
     *
     * @throws Exception if failed
     */
    public void testNewStyleAlwaysSmallestNodeIndexIsCreated() throws Exception {
        final Ignite ignite0 = startGrid(0);
        final Ignite ignite1 = startGrid(1);
        final Ignite ignite2 = startGrid(2);
        final Ignite ignite3 = startGrid(3);
        final Ignite ignite4 = startGrid(4);

        ignite0.active(true);

        ignite0.getOrCreateCache(CACHE_NAME).put("hi", "there!");
        ignite3.getOrCreateCache(CACHE_NAME).put("hi1", "there!");

        assertPdsDirsDefaultExist(newStyleSubfolder(ignite0, 0));
        assertPdsDirsDefaultExist(newStyleSubfolder(ignite1, 1));
        assertPdsDirsDefaultExist(newStyleSubfolder(ignite2, 2));
        assertPdsDirsDefaultExist(newStyleSubfolder(ignite3, 3));
        assertPdsDirsDefaultExist(newStyleSubfolder(ignite4, 4));

        stopAllGrids();

        //this grid should take folder with index 0 as unlocked
        final Ignite ignite4Restart = startGrid(4);
        ignite4Restart.active(true);
        assertPdsDirsDefaultExist(newStyleSubfolder(ignite4Restart, 0));
        stopAllGrids();
    }


    /**
     * Test verified that new style folder is taken always with lowest index
     *
     * @throws Exception if failed
     */
    public void testNewStyleAlwaysSmallestNodeIndexIsCreated2() throws Exception {
        final Ignite ignite0 = startGridsMultiThreaded(11);

        ignite0.active(true);

        ignite0.getOrCreateCache(CACHE_NAME).put("hi", "there!");
        ignite0.getOrCreateCache(CACHE_NAME).put("hi1", "there!");

        assertPdsDirsDefaultExist(newStyleSubfolder(ignite0, 0));

        stopAllGrids();

        //this grid should take folder with index 0 as unlocked
        final Ignite ignite4Restart = startGrid(4);
        ignite4Restart.active(true);
        assertPdsDirsDefaultExist(newStyleSubfolder(ignite4Restart, 0));
        stopAllGrids();
    }


    /**
     * Checks existence of all storage-related directories
     *
     * @param subDirName sub directories name expected
     * @throws IgniteCheckedException if IO error occur
     */
    private void assertPdsDirsDefaultExist(String subDirName) throws IgniteCheckedException {
        assertDirectoryExist("binary_meta", subDirName);
        assertDirectoryExist(PersistentStoreConfiguration.DFLT_WAL_STORE_PATH, subDirName);
        assertDirectoryExist(PersistentStoreConfiguration.DFLT_WAL_ARCHIVE_PATH, subDirName);
        assertDirectoryExist(PdsConsistentIdGeneratingFoldersResolver.DB_DEFAULT_FOLDER, subDirName);
    }

    /**
     * Checks one folder existence
     *
     * @param subFolderNames subfolders array to touch
     * @throws IgniteCheckedException if IO error occur
     */
    private void assertDirectoryExist(String... subFolderNames) throws IgniteCheckedException {
        File curFolder = new File(U.defaultWorkDirectory());
        for (String name : subFolderNames) {
            curFolder = new File(curFolder, name);
        }
        final String path;
        try {
            path = curFolder.getCanonicalPath();
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to convert path: [" + curFolder.getAbsolutePath() + "]", e);
        }
        assertTrue("Directory " + Arrays.asList(subFolderNames).toString()
            + " is expected to exist [" + path + "]", curFolder.exists() && curFolder.isDirectory());
    }

}
