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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for new and old stype persistent storage folders generation
 */
public class IgniteUidAsConsistentIdMigrationTest extends GridCommonAbstractTest {

    private boolean deleteAfter = false;
    private boolean deleteBefore = true;

    /** Configured consistent id. */
    private String configuredConsistentId;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
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
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", false));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);
        if (configuredConsistentId != null)
            cfg.setConsistentId(configuredConsistentId);
        final PersistentStoreConfiguration psCfg = new PersistentStoreConfiguration();
        cfg.setPersistentStoreConfiguration(psCfg);
        return cfg;
    }

    /**
     * Checks start on empty PDS folder, in that case node 0 should start with random UUID
     *
     * @throws Exception if failed
     */
    public void testNewStyleIdIsGenerated() throws Exception {
        IgniteEx igniteEx = startGrid(0);
        igniteEx.active(true);
        igniteEx.getOrCreateCache("dummy").put("hi", "there!");

        final String consistentId = igniteEx.cluster().localNode().consistentId().toString();
        String consistentIdMasked = "Node0-" + consistentId;
        assertPdsDirsDefaultExist(consistentIdMasked);
        stopGrid(0);
        final UUID uuid = UUID.fromString(consistentId);
    }

    /**
     * Checks start on empty PDS folder, in that case node 0 should start with random UUID
     *
     * @throws Exception if failed
     */
    public void testPreconfiguredConsitentIdIsApplied() throws Exception {
        this.configuredConsistentId = "someConfiguredConsistentId";
        IgniteEx igniteEx = startGrid(0);
        igniteEx.active(true);
        igniteEx.getOrCreateCache("dummy").put("hi", "there!");

        assertPdsDirsDefaultExist(configuredConsistentId);
        stopGrid(0);
    }


    /**
     * Checks start on empty PDS folder, in that case node 0 should start with random UUID
     *
     * @throws Exception if failed
     */
    public void testRestartOnExistingOldStyleId() throws Exception {
        this.configuredConsistentId = "127.0.0.1:47500"; //this is for create old node folder
        IgniteEx igniteEx = startGrid(0);
        igniteEx.active(true);
        final String expectedVal = "there is compatible mode with old style folders!";
        igniteEx.getOrCreateCache("dummy").put("hi", expectedVal);

        assertPdsDirsDefaultExist(U.maskForFileName(configuredConsistentId));
        stopGrid(0);

        this.configuredConsistentId = null; //now set up grid on folder

        IgniteEx igniteRestart = startGrid(0);
        igniteRestart.active(true);
        assertTrue(expectedVal.equals(igniteRestart.cache("dummy").get("hi")));
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
            IgniteEx igniteEx = startGrid(0);
            igniteEx.active(true);
            igniteEx.getOrCreateCache("dummy").put("hi", "there!");

            final String consistentId = igniteEx.cluster().localNode().consistentId().toString();
            String consistentIdMasked = "Node0-" + consistentId;
            assertPdsDirsDefaultExist(consistentIdMasked);
            stopGrid(0);

            uuid = UUID.fromString(consistentId);
        }

        {
            IgniteEx igniteRestart = startGrid(0);
            igniteRestart.active(true);
            assertTrue("there!".equals(igniteRestart.cache("dummy").get("hi")));

            final String consistentId = igniteRestart.cluster().localNode().consistentId().toString();
            String consistentIdMasked = "Node0-" + consistentId;
            assertPdsDirsDefaultExist(consistentIdMasked);
            stopGrid(0);

            assertEquals(uuid, UUID.fromString(consistentId));
        }
    }

    public void testNodeIndexIncremented() throws Exception {
        IgniteEx igniteEx = startGrid(0);
        IgniteEx igniteEx1 = startGrid(1);
        igniteEx.active(true);
        igniteEx.getOrCreateCache("dummy").put("hi", "there!");
        igniteEx1.getOrCreateCache("dummy").put("hi1", "there!");
        String consistentIdMasked = "Node0-" + (igniteEx.cluster().localNode().consistentId().toString());
        assertPdsDirsDefaultExist(consistentIdMasked);

        String consistentIdMasked1 = "Node1-" + (igniteEx1.cluster().localNode().consistentId().toString());
        assertPdsDirsDefaultExist(consistentIdMasked1);

        stopGrid(0);
        stopGrid(1);
    }

    private void assertPdsDirsDefaultExist(String consistentIdMasked) throws IgniteCheckedException, IOException {
        assertDirectoryExist("binary_meta", consistentIdMasked);
        assertDirectoryExist(PersistentStoreConfiguration.DFLT_WAL_STORE_PATH, consistentIdMasked);
        assertDirectoryExist(PersistentStoreConfiguration.DFLT_WAL_ARCHIVE_PATH, consistentIdMasked);
        assertDirectoryExist("db", consistentIdMasked);
    }

    private void assertDirectoryExist(String... subFolderNames) throws IgniteCheckedException, IOException {
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
