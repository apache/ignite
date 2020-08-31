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
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CONSISTENT_ID_BY_HOST_WITHOUT_PORT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DATA_STORAGE_FOLDER_BY_CONSISTENT_ID;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.parseSubFolderName;

/**
 * Test for new and old style persistent storage folders generation
 */
public class IgniteUidAsConsistentIdMigrationTest extends GridCommonAbstractTest {
    /** Cache name for test. */
    public static final String CACHE_NAME = "dummy";

    /** Clear DB folder after each test. May be set to false for local debug */
    private static final boolean deleteAfter = true;

    /** Clear DB folder before each test. */
    private static final boolean deleteBefore = true;

    /** Fail test if delete of DB folder was not completed. */
    private static final boolean failIfDeleteNotCompleted = true;

    /** Configured consistent id. */
    private String configuredConsistentId;

    /** Logger to accumulate messages, null will cause logger won't be customized */
    private GridStringLogger strLog;

    /** Clear properties after this test run. Flag protects from failed test */
    private boolean clearPropsAfterTest = false;

    /** Place storage in temp folder for current test run. */
    private boolean placeStorageInTemp;

    /** A path to persistent store custom path for current test run. */
    private File pstStoreCustomPath;

    /** A path to persistent store WAL work custom path. */
    private File pstWalStoreCustomPath;

    /** A path to persistent store WAL archive custom path. */
    private File pstWalArchCustomPath;

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

        if (clearPropsAfterTest) {
            System.clearProperty(IGNITE_DATA_STORAGE_FOLDER_BY_CONSISTENT_ID);
            System.clearProperty(IGNITE_CONSISTENT_ID_BY_HOST_WITHOUT_PORT);
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        boolean ok = true;

        if (pstStoreCustomPath != null)
            ok &= U.delete(pstStoreCustomPath);
        else
            ok &= U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), FilePageStoreManager.DFLT_STORE_DIR, false));

        if (pstWalArchCustomPath != null)
            ok &= U.delete(pstWalArchCustomPath);

        if (pstWalStoreCustomPath != null)
            ok &= U.delete(pstWalStoreCustomPath);

        ok &= U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DataStorageConfiguration.DFLT_BINARY_METADATA_PATH, false));

        if (failIfDeleteNotCompleted)
            assertTrue(ok);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (configuredConsistentId != null)
            cfg.setConsistentId(configuredConsistentId);

        final DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        if (placeStorageInTemp) {
            final File tempDir = new File(System.getProperty("java.io.tmpdir"));

            pstStoreCustomPath = new File(tempDir, "Store");
            pstWalStoreCustomPath = new File(tempDir, "WalStore");
            pstWalArchCustomPath = new File(tempDir, "WalArchive");

            dsCfg.setStoragePath(pstStoreCustomPath.getAbsolutePath());
            dsCfg.setWalPath(pstWalStoreCustomPath.getAbsolutePath());
            dsCfg.setWalArchivePath(pstWalArchCustomPath.getAbsolutePath());
        }

        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(32L * 1024 * 1024)
            .setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(dsCfg);

        if (strLog != null)
            cfg.setGridLogger(strLog);

        return cfg;
    }

    /**
     * Checks start on empty PDS folder, in that case node 0 should start with random UUID.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testNewStyleIdIsGenerated() throws Exception {
        final Ignite ignite = startActivateFillDataGrid(0);

        //test UUID is parsable from consistent ID test
        UUID.fromString(ignite.cluster().localNode().consistentId().toString());
        assertPdsDirsDefaultExist(genNewStyleSubfolderName(0, ignite));
        stopGrid(0);
    }

    /**
     * Checks start on empty PDS folder, in that case node 0 should start with random UUID.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testNewStyleIdIsGeneratedInCustomStorePath() throws Exception {
        placeStorageInTemp = true;
        final Ignite ignite = startActivateFillDataGrid(0);

        //test UUID is parsable from consistent ID test
        UUID.fromString(ignite.cluster().localNode().consistentId().toString());
        final String subfolderName = genNewStyleSubfolderName(0, ignite);

        assertDirectoryExist(DataStorageConfiguration.DFLT_BINARY_METADATA_PATH, subfolderName);

        assertDirectoryExist(pstWalArchCustomPath, subfolderName);
        assertDirectoryExist(pstWalArchCustomPath, subfolderName);
        assertDirectoryExist(pstStoreCustomPath, subfolderName);

        stopGrid(0);
    }

    /**
     * Checks start on empty PDS folder using configured ConsistentId. We should start using this ID in compatible mode.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testPreconfiguredConsitentIdIsApplied() throws Exception {
        this.configuredConsistentId = "someConfiguredConsistentId";
        Ignite ignite = startActivateFillDataGrid(0);

        assertPdsDirsDefaultExist(configuredConsistentId);
        stopGrid(0);
    }

    /**
     * Checks start on configured ConsistentId with same value as default, this emulate old style folder is already
     * available. We should restart using this folder.
     *
     * @throws Exception if failed
     */
    @Test
    public void testRestartOnExistingOldStyleId() throws Exception {
        final String expDfltConsistentId = "127.0.0.1:47500";

        this.configuredConsistentId = expDfltConsistentId; //this is for create old node folder

        final Ignite igniteEx = startActivateGrid(0);

        final String expVal = "there is compatible mode with old style folders!";

        igniteEx.getOrCreateCache(CACHE_NAME).put("hi", expVal);

        assertPdsDirsDefaultExist(U.maskForFileName(configuredConsistentId));
        stopGrid(0);

        this.configuredConsistentId = null; //now set up grid on existing folder

        final Ignite igniteRestart = startActivateGrid(0);

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
    @Test
    public void testStartWithoutActivate() throws Exception {
        //start stop grid without activate
        startGrid(0);
        stopGrid(0);

        Ignite igniteRestart = startActivateFillDataGrid(0);
        assertPdsDirsDefaultExist(genNewStyleSubfolderName(0, igniteRestart));
        stopGrid(0);
    }

    /**
     * Checks start on empty PDS folder, in that case node 0 should start with random UUID
     *
     * @throws Exception if failed
     */
    @Test
    public void testRestartOnSameFolderWillCauseSameUuidGeneration() throws Exception {
        final UUID uuid;
        {
            final Ignite ignite = startActivateFillDataGrid(0);

            assertPdsDirsDefaultExist(genNewStyleSubfolderName(0, ignite));

            uuid = (UUID)ignite.cluster().localNode().consistentId();
            stopGrid(0);
        }

        {
            final Ignite igniteRestart = startActivateGrid(0);

            assertTrue("there!".equals(igniteRestart.cache(CACHE_NAME).get("hi")));

            final Object consIdRestart = igniteRestart.cluster().localNode().consistentId();

            assertPdsDirsDefaultExist(genNewStyleSubfolderName(0, igniteRestart));
            stopGrid(0);

            assertEquals(uuid, consIdRestart);
        }
    }

    /**
     * This test starts node, activates, deactivates node, and then start second node.
     * Expected behaviour is following: second node will join topology with separate node folder
     *
     * @throws Exception if failed
     */
    @Test
    public void testStartNodeAfterDeactivate() throws Exception {
        final UUID uuid;
        {
            final Ignite ignite = startActivateFillDataGrid(0);

            assertPdsDirsDefaultExist(genNewStyleSubfolderName(0, ignite));

            uuid = (UUID)ignite.cluster().localNode().consistentId();
            ignite.active(false);
        }
        {
            final Ignite igniteRestart = startActivateGrid(1);

            grid(0).active(true);
            final Object consIdRestart = igniteRestart.cluster().localNode().consistentId();

            assertPdsDirsDefaultExist(genNewStyleSubfolderName(1, igniteRestart));

            stopGrid(1);
            assertFalse(consIdRestart.equals(uuid));
        }
        stopGrid(0);
        assertNodeIndexesInFolder(0, 1);
    }

    /**
     * @param idx Index of the grid to start.
     * @return Started and activated grid.
     * @throws Exception If failed.
     */
    @NotNull private Ignite startActivateFillDataGrid(int idx) throws Exception {
        final Ignite ignite = startActivateGrid(idx);

        ignite.getOrCreateCache(CACHE_NAME).put("hi", "there!");

        return ignite;
    }

    /**
     * Starts and activates new grid with given index.
     *
     * @param idx Index of the grid to start.
     * @return Started and activated grid.
     * @throws Exception If anything failed.
     */
    @NotNull private Ignite startActivateGrid(int idx) throws Exception {
        final Ignite ignite = startGrid(idx);

        ignite.active(true);

        return ignite;
    }

    /**
     * Generates folder name in new style using constant prefix and UUID
     *
     * @param nodeIdx expected node index to check
     * @param ignite ignite instance
     * @return name of storage related subfolders
     */
    @NotNull private String genNewStyleSubfolderName(final int nodeIdx, final Ignite ignite) {
        final Object consistentId = ignite.cluster().localNode().consistentId();

        assertTrue("For new style folders consistent ID should be UUID," +
                " but actual class is " + (consistentId == null ? null : consistentId.getClass()),
            consistentId instanceof UUID);

        return PdsConsistentIdProcessor.genNewStyleSubfolderName(nodeIdx, (UUID)consistentId);
    }

    /**
     * test two nodes started at the same db root folder, second node should get index 1
     *
     * @throws Exception if failed
     */
    @Test
    public void testNodeIndexIncremented() throws Exception {
        final Ignite ignite0 = startGrid(0);
        final Ignite ignite1 = startGrid(1);

        ignite0.active(true);

        ignite0.getOrCreateCache(CACHE_NAME).put("hi", "there!");
        ignite1.getOrCreateCache(CACHE_NAME).put("hi1", "there!");

        assertPdsDirsDefaultExist(genNewStyleSubfolderName(0, ignite0));
        assertPdsDirsDefaultExist(genNewStyleSubfolderName(1, ignite1));

        stopGrid(0);
        stopGrid(1);
        assertNodeIndexesInFolder(0, 1);
    }

    /**
     * Test verified that new style folder is taken always with lowest index
     *
     * @throws Exception if failed
     */
    @Test
    public void testNewStyleAlwaysSmallestNodeIndexIsCreated() throws Exception {
        final Ignite ignite0 = startGrid(0);
        final Ignite ignite1 = startGrid(1);
        final Ignite ignite2 = startGrid(2);
        final Ignite ignite3 = startGrid(3);

        ignite0.active(true);

        ignite0.getOrCreateCache(CACHE_NAME).put("hi", "there!");
        ignite3.getOrCreateCache(CACHE_NAME).put("hi1", "there!");

        assertPdsDirsDefaultExist(genNewStyleSubfolderName(0, ignite0));
        assertPdsDirsDefaultExist(genNewStyleSubfolderName(1, ignite1));
        assertPdsDirsDefaultExist(genNewStyleSubfolderName(2, ignite2));
        assertPdsDirsDefaultExist(genNewStyleSubfolderName(3, ignite3));

        assertNodeIndexesInFolder(0, 1, 2, 3);
        stopAllGrids();

        //this grid should take folder with index 0 as unlocked
        final Ignite ignite4Restart = startActivateGrid(3);
        assertPdsDirsDefaultExist(genNewStyleSubfolderName(0, ignite4Restart));

        assertNodeIndexesInFolder(0, 1, 2, 3);
        stopAllGrids();
    }

    /**
     * Test verified that new style folder is taken always with lowest index
     *
     * @throws Exception if failed
     */
    @Test
    public void testNewStyleAlwaysSmallestNodeIndexIsCreatedMultithreaded() throws Exception {
        final Ignite ignite0 = startGridsMultiThreaded(11);

        ignite0.active(true);

        ignite0.getOrCreateCache(CACHE_NAME).put("hi", "there!");
        ignite0.getOrCreateCache(CACHE_NAME).put("hi1", "there!");

        assertPdsDirsDefaultExist(genNewStyleSubfolderName(0, ignite0));

        assertNodeIndexesInFolder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        stopAllGrids();

        //this grid should take folder with index 0 as unlocked
        final Ignite ignite4Restart = startActivateGrid(4);
        assertPdsDirsDefaultExist(genNewStyleSubfolderName(0, ignite4Restart));
        stopAllGrids();

        assertNodeIndexesInFolder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    /**
     * Test start two nodes with predefined conistent ID (emulate old fashion node). Then restart two nodes. Expected
     * both nodes will get its own old folders
     *
     * @throws Exception if failed.
     */
    @Test
    public void testStartTwoOldStyleNodes() throws Exception {
        final String expDfltConsistentId1 = "127.0.0.1:47500";

        this.configuredConsistentId = expDfltConsistentId1; //this is for create old node folder
        final Ignite ignite = startGrid(0);

        final String expDfltConsistentId2 = "127.0.0.1:47501";

        this.configuredConsistentId = expDfltConsistentId2; //this is for create old node folder
        final Ignite ignite2 = startGrid(1);

        ignite.active(true);

        final String expVal = "there is compatible mode with old style folders!";

        ignite2.getOrCreateCache(CACHE_NAME).put("hi", expVal);

        assertPdsDirsDefaultExist(U.maskForFileName(expDfltConsistentId1));
        assertPdsDirsDefaultExist(U.maskForFileName(expDfltConsistentId2));
        stopAllGrids();

        this.configuredConsistentId = null; //now set up grid on existing folder

        final Ignite igniteRestart = startGrid(0);
        final Ignite igniteRestart2 = startGrid(1);

        igniteRestart2.active(true);

        assertEquals(expDfltConsistentId1, igniteRestart.cluster().localNode().consistentId());
        assertEquals(expDfltConsistentId2, igniteRestart2.cluster().localNode().consistentId());

        final IgniteCache<Object, Object> cache = igniteRestart.cache(CACHE_NAME);

        assertNotNull("Expected to have cache [" + CACHE_NAME + "] using [" + expDfltConsistentId1 + "] as PDS folder", cache);
        final Object valFromCache = cache.get("hi");

        assertNotNull("Expected to load data from cache using [" + expDfltConsistentId1 + "] as PDS folder", valFromCache);
        assertTrue(expVal.equals(valFromCache));

        assertNodeIndexesInFolder(); //no new style nodes should be found
        stopGrid(0);
    }

    /**
     * Tests compatible mode enabled by this test to start.
     * Expected to be 2 folders and no new style folders in this case.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testStartOldStyleNodesByCompatibleProperty() throws Exception {
        clearPropsAfterTest = true;
        System.setProperty(IGNITE_DATA_STORAGE_FOLDER_BY_CONSISTENT_ID, "true");

        final Ignite ignite1 = startGrid(0);
        final Ignite ignite2 = startGrid(1);

        ignite1.active(true);

        final String expVal = "there is compatible mode with old style folders!";

        ignite2.getOrCreateCache(CACHE_NAME).put("hi", expVal);

        assertNodeIndexesInFolder(); // expected to have no new style folders

        final Object consistentId1 = ignite1.cluster().localNode().consistentId();

        assertPdsDirsDefaultExist(U.maskForFileName(consistentId1.toString()));
        final Object consistentId2 = ignite2.cluster().localNode().consistentId();

        assertPdsDirsDefaultExist(U.maskForFileName(consistentId2.toString()));
        stopAllGrids();

        System.clearProperty(IGNITE_DATA_STORAGE_FOLDER_BY_CONSISTENT_ID);
        final Ignite igniteRestart = startGrid(0);
        final Ignite igniteRestart2 = startGrid(1);

        igniteRestart2.active(true);

        assertEquals(consistentId1, igniteRestart.cluster().localNode().consistentId());
        assertEquals(consistentId2, igniteRestart2.cluster().localNode().consistentId());

        assertNodeIndexesInFolder(); //new style nodes should not be found
        stopGrid(0);
    }

    /**
     * Tests compatible mode enabled by this test to start, also no port is enabled.
     * Expected to be 1 folder and no new style folders in this case.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testStartOldStyleNoPortsNodesByCompatibleProperty() throws Exception {
        clearPropsAfterTest = true;
        System.setProperty(IGNITE_DATA_STORAGE_FOLDER_BY_CONSISTENT_ID, "true");
        System.setProperty(IGNITE_CONSISTENT_ID_BY_HOST_WITHOUT_PORT, "true");

        final Ignite ignite1 = startGrid(0);

        ignite1.active(true);

        final String expVal = "there is compatible mode with old style folders!";

        ignite1.getOrCreateCache(CACHE_NAME).put("hi", expVal);

        assertNodeIndexesInFolder(); // expected to have no new style folders

        final Object consistentId1 = ignite1.cluster().localNode().consistentId();

        assertPdsDirsDefaultExist(U.maskForFileName(consistentId1.toString()));
        stopAllGrids();

        System.clearProperty(IGNITE_DATA_STORAGE_FOLDER_BY_CONSISTENT_ID);

        final Ignite igniteRestart = startGrid(0);

        igniteRestart.active(true);

        assertEquals(consistentId1, igniteRestart.cluster().localNode().consistentId());

        assertNodeIndexesInFolder(); //new style nodes should not be found
        stopGrid(0);
        System.clearProperty(IGNITE_CONSISTENT_ID_BY_HOST_WITHOUT_PORT);
    }

    /**
     * Test case If there are no matching folders,
     * but the directory contains old-style consistent IDs.
     * Ignite should print out a warning.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testOldStyleNodeWithUnexpectedPort() throws Exception {
        this.configuredConsistentId = "127.0.0.1:49999"; //emulated old-style node with not appropriate consistent ID
        final Ignite ignite = startActivateFillDataGrid(0);
        final IgniteCache<Object, Object> second = ignite.getOrCreateCache("second");

        final int entries = 100;

        for (int i = 0; i < entries; i++)
            second.put((int)(Math.random() * entries), getClass().getName());

        final String prevVerFolder = U.maskForFileName(ignite.cluster().localNode().consistentId().toString());
        final String path = new File(new File(U.defaultWorkDirectory(), "db"), prevVerFolder).getCanonicalPath();

        assertPdsDirsDefaultExist(prevVerFolder);
        stopAllGrids();

        this.configuredConsistentId = null;
        this.strLog = new GridStringLogger();
        startActivateGrid(0);
        assertNodeIndexesInFolder(0); //one 0 index folder is created

        final String wholeNodeLog = strLog.toString();
        stopAllGrids();

        String foundWarning = null;
        for (String line : wholeNodeLog.split("\n")) {
            if (line.contains("There is other non-empty storage folder under storage base directory")) {
                foundWarning = line;
                break;
            }
        }

        if (foundWarning != null)
            log.info("\nWARNING generated successfully [\n" + foundWarning + "\n]");

        assertTrue("Expected to warn user on existence of old style path",
            foundWarning != null);

        assertTrue("Expected to warn user on existence of old style path [" + path + "]",
            foundWarning.contains(path));

        assertTrue("Expected to print some size for [" + path + "]",
            Pattern.compile(" [0-9]* bytes").matcher(foundWarning).find());

        strLog = null;
        startActivateGrid(0);
        assertNodeIndexesInFolder(0); //one 0 index folder is created
        stopAllGrids();
    }

    /**
     * @param indexes expected new style node indexes in folders
     * @throws IgniteCheckedException if failed
     */
    private void assertNodeIndexesInFolder(Integer... indexes) throws IgniteCheckedException {
        assertEquals(new TreeSet<>(Arrays.asList(indexes)), getAllNodeIndexesInFolder());
    }

    /**
     * @return set of all indexes of nodes found in work folder
     * @throws IgniteCheckedException if failed.
     */
    @NotNull private Set<Integer> getAllNodeIndexesInFolder() throws IgniteCheckedException {
        final File curFolder = new File(U.defaultWorkDirectory(), PdsConsistentIdProcessor.DB_DEFAULT_FOLDER);
        final Set<Integer> indexes = new TreeSet<>();
        final File[] files = curFolder.listFiles(PdsConsistentIdProcessor.DB_SUBFOLDERS_NEW_STYLE_FILTER);

        for (File file : files) {
            final PdsConsistentIdProcessor.FolderCandidate uid = parseSubFolderName(file, log);

            if (uid != null)
                indexes.add(uid.nodeIndex());
        }

        return indexes;
    }

    /**
     * Checks existence of all storage-related directories
     *
     * @param subDirName sub directories name expected
     * @throws IgniteCheckedException if IO error occur
     */
    private void assertPdsDirsDefaultExist(String subDirName) throws IgniteCheckedException {
        assertDirectoryExist(DataStorageConfiguration.DFLT_BINARY_METADATA_PATH, subDirName);
        assertDirectoryExist(DataStorageConfiguration.DFLT_WAL_PATH, subDirName);
        assertDirectoryExist(DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH, subDirName);
        assertDirectoryExist(PdsConsistentIdProcessor.DB_DEFAULT_FOLDER, subDirName);
    }

    /**
     * Checks one folder existence.
     *
     * @param subFolderNames sub folders chain array to touch.
     * @throws IgniteCheckedException if IO error occur.
     */
    private void assertDirectoryExist(String... subFolderNames) throws IgniteCheckedException {
        final File curFolder = new File(U.defaultWorkDirectory());

        assertDirectoryExist(curFolder, subFolderNames);
    }

    /**
     * Checks one folder existence.
     *
     * @param workFolder current work folder.
     * @param subFolderNames sub folders chain array to touch.
     * @throws IgniteCheckedException if IO error occur.
     */
    private void assertDirectoryExist(final File workFolder, String... subFolderNames) throws IgniteCheckedException {
        File curFolder = workFolder;

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
