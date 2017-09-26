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
        final PersistentStoreConfiguration psCfg = new PersistentStoreConfiguration();
        cfg.setPersistentStoreConfiguration(psCfg);
        return cfg;
    }

    public void testNewStyleIdIsGenerated() throws Exception {
        IgniteEx igniteEx = startGrid(0);
        igniteEx.active(true);
        igniteEx.getOrCreateCache("dummy").put("hi", "there!");
        //   File db = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);
        // List<File> files = Arrays.asList(db.listFiles());

        String consistentIdMasked = "Node0-" + (igniteEx.cluster().localNode().consistentId().toString());
        assertDirectoryExist("binary_meta", consistentIdMasked);
        assertDirectoryExist(PersistentStoreConfiguration.DFLT_WAL_STORE_PATH, consistentIdMasked);
        assertDirectoryExist("db", consistentIdMasked);
        stopGrid(0);
    }


    public void testNodeIndexIncremented() throws Exception {
        IgniteEx igniteEx = startGrid(0);
        IgniteEx igniteEx1 = startGrid(1);
        igniteEx.active(true);
        igniteEx.getOrCreateCache("dummy").put("hi", "there!");
        igniteEx1.getOrCreateCache("dummy").put("hi1", "there!");
        String consistentIdMasked1 = "Node1-" + (igniteEx1.cluster().localNode().consistentId().toString());
        assertDirectoryExist("binary_meta", consistentIdMasked1);
        assertDirectoryExist(PersistentStoreConfiguration.DFLT_WAL_STORE_PATH, consistentIdMasked1);
        assertDirectoryExist("db", consistentIdMasked1);


        String consistentIdMasked = "Node0-" + (igniteEx.cluster().localNode().consistentId().toString());
        assertDirectoryExist("binary_meta", consistentIdMasked);
        assertDirectoryExist(PersistentStoreConfiguration.DFLT_WAL_STORE_PATH, consistentIdMasked);
        assertDirectoryExist("db", consistentIdMasked);
        stopGrid(0);
        stopGrid(1);
    }


    private void assertDirectoryExist(String ...subFolderNames) throws IgniteCheckedException, IOException {
        File curFolder = new File(U.defaultWorkDirectory());
        for (String name : subFolderNames) {
            curFolder = new File(curFolder, name);
        }
        assertTrue("Directory "+ Arrays.asList(subFolderNames).toString()
            +" is expected to exist [" + curFolder.getCanonicalPath() + "]", curFolder.exists() && curFolder.isDirectory());
    }

}
