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
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that warning is logged when persistence store directory equals {@code System.getProperty("java.io.tmpdir")}.
 */
public class PersistenceDirectoryWarningLoggingTest extends GridCommonAbstractTest {
    /** Warning message to test. */
    private static final String WARN_MSG_PREFIX = "Persistence store directory is in the temp " +
        "directory and may be cleaned.";

    /** String logger to check. */
    private GridStringLogger log0 = new GridStringLogger();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(log0);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPdsDirWarningSuppressed() throws Exception {
        startGrid();

        assertFalse(log0.toString().contains(WARN_MSG_PREFIX));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPdsDirWarningIsLogged() throws Exception {
        IgniteConfiguration cfg = getConfiguration("0");

        String tempDir = System.getProperty("java.io.tmpdir");

        assertNotNull(tempDir);

        // Emulates that Ignite work directory has not been calculated,
        // and IgniteUtils#workDirectory resolved directory into "java.io.tmpdir"
        cfg.setWorkDirectory(tempDir);

        startGrid(cfg);

        assertTrue(log0.toString().contains(WARN_MSG_PREFIX));
    }

    /**
     * Test that temporary work directory warning is not printed, if PDS is not actually in temporary directory
     * @throws Exception If failed.
     */
    @Test
    public void testPdsDirWarningIsNotLogged() throws Exception {
        IgniteConfiguration cfg = getConfiguration("0");

        String tempDir = System.getProperty("java.io.tmpdir");

        assertNotNull(tempDir);

        File workDir = new File(U.defaultWorkDirectory(), tempDir);

        // set working directory to file not in tmp directory, but with temp directory folder name in path
        cfg.setWorkDirectory(workDir.getAbsolutePath());

        startGrid(cfg);

        assertFalse(log0.toString().contains(WARN_MSG_PREFIX));
    }

}
