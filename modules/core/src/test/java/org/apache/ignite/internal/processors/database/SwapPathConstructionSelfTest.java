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

package org.apache.ignite.internal.processors.database;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test verifies correct construction of swap file path {@link DataRegionConfiguration#setSwapPath(String)}
 * when absolute or relative paths are provided via configuration.
 */
public class SwapPathConstructionSelfTest extends GridCommonAbstractTest {
    /** */
    private DataStorageConfiguration memCfg;

    /** */
    private static final String RELATIVE_SWAP_PATH = "relSwapPath";

    /** */
    private static final String ABSOLUTE_SWAP_PATH = "absoluteSwapPath";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanUpSwapDir();
    }

    /**
     * Cleans up swap files and directories after test.
     */
    private void cleanUpSwapDir() {
        Path relDir = Paths.get(U.getIgniteHome(), RELATIVE_SWAP_PATH);

        U.delete(relDir.toFile());

        Path absDir = Paths.get(getTmpDir(), ABSOLUTE_SWAP_PATH);

        U.delete(absDir.toFile());
    }

    /**
     * Verifies relative swap file path construction. Directory with swap files is cleaned up during after-test phase.
     */
    @Test
    public void testRelativeSwapFilePath() throws Exception {
        memCfg = createMemoryConfiguration(true);

        IgniteEx ignite = startGrid(0);

        String allocPath = extractDefaultPageMemoryAllocPath(ignite.context());

        assertNotNull(allocPath);

        assertTrue(allocPath.contains(Paths.get(U.getIgniteHome(), RELATIVE_SWAP_PATH).toString()));
    }

    /**
     * Verifies absolute swap file path construction. System tmp directory is used to allocate swap files,
     * so no clean up is needed.
     */
    @Test
    public void testAbsoluteSwapFilePath() throws Exception {
        memCfg = createMemoryConfiguration(false);

        IgniteEx ignite = startGrid(0);

        String allocPath = extractDefaultPageMemoryAllocPath(ignite.context());

        assertNotNull(allocPath);

        String expectedPath = Paths.get(getTmpDir(), ABSOLUTE_SWAP_PATH).toString();

        assertTrue("Expected path: "
                        + expectedPath
                        + "; actual path: "
                        + allocPath,
                allocPath.startsWith(expectedPath));
    }

    /**
     * @param context Context.
     */
    private String extractDefaultPageMemoryAllocPath(GridKernalContext context) {
        IgniteCacheDatabaseSharedManager dbMgr = context.cache().context().database();

        Map<String, DataRegion> memPlcMap = U.field(dbMgr, "dataRegionMap");

        PageMemory pageMem = memPlcMap.get("default").pageMemory();

        Object memProvider = U.field(pageMem, "directMemoryProvider");

        Object memProvider0 = U.field(memProvider, "memProvider");

        return ((File) U.field(memProvider0, "allocationPath")).getAbsolutePath();
    }

    /**
     * @param isRelativePath flag is set to {@code true} if relative path should be used for data region configuration.
     */
    private DataStorageConfiguration createMemoryConfiguration(boolean isRelativePath) {
        DataStorageConfiguration memCfg = new DataStorageConfiguration();

        DataRegionConfiguration memPlcCfg = new DataRegionConfiguration();

        memPlcCfg.setName("default");
        memPlcCfg.setMaxSize(20L * 1024 * 1024);

        if (isRelativePath)
            memPlcCfg.setSwapPath(RELATIVE_SWAP_PATH);
        else
            memPlcCfg.setSwapPath(Paths.get(getTmpDir(), ABSOLUTE_SWAP_PATH).toString());

        memCfg.setDefaultDataRegionConfiguration(memPlcCfg);

        return memCfg;
    }

    /**
     *
     */
    private String getTmpDir() {
        return System.getProperty("java.io.tmpdir");
    }
}
