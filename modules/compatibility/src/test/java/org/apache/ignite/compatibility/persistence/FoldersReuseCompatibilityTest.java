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

package org.apache.ignite.compatibility.persistence;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdGeneratingFoldersResolver;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdGeneratingFoldersResolver.parseSubFolderName;

/**
 * Test for new and old style persistent storage folders generation
 */
public class FoldersReuseCompatibilityTest extends IgnitePersistenceCompatibilityAbstractTest {

    private Serializable configuredConsistentId = null;

    /** Cache name for test. */
    public static final String CACHE_NAME = "dummy";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // No-op. super.afterTest();

        stopAllGrids();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        configPersistence(cfg);
        return cfg;
    }

    public void testFoldersReuseCompatibility_2_2() throws Exception {
        runFoldersReuse("2.2.0");
    }

    public void testFoldersReuseCompatibility_2_1() throws Exception {
        runFoldersReuse("2.1.0");

    }

    private void runFoldersReuse(String ver) throws Exception {
        final IgniteEx grid = startGrid(1, ver, new ConfigurationClosure(), new PostStartupClosure());

        // assertPdsDirsDefaultExist(U.maskForFileName(grid.cluster().node().consistentId().toString()));

        grid.close();
        stopAllGrids();

        IgniteEx ignite = startGrid(0);
        ignite.active(true);
        ignite.getOrCreateCache("cache2createdForNewGrid").put("Object", "Value");
        assertEquals(1, ignite.context().discovery().topologyVersion());

        assertPdsDirsDefaultExist(U.maskForFileName(ignite.cluster().node().consistentId().toString()));

        ignite.active(true);
        assertNodeIndexesInFolder();// should not create any new style directories

        stopAllGrids();
    }

    /** */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);
            ignite.getOrCreateCache(CACHE_NAME).put("ObjectFromPast", "ValueFromPast");
        }
    }

    /** */
    private class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLocalHost("127.0.0.1");
            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);

            if (configuredConsistentId != null)
                cfg.setConsistentId(configuredConsistentId);
            configPersistence(cfg);
        }
    }

    private static void configPersistence(IgniteConfiguration cfg) {
        final PersistentStoreConfiguration psCfg = new PersistentStoreConfiguration();
        cfg.setPersistentStoreConfiguration(psCfg);

        final MemoryConfiguration memCfg = new MemoryConfiguration();
        final MemoryPolicyConfiguration memPolCfg = new MemoryPolicyConfiguration();

        memPolCfg.setMaxSize(32 * 1024 * 1024); // we don't need much memory for this test
        memCfg.setMemoryPolicies(memPolCfg);
        cfg.setMemoryConfiguration(memCfg);
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
        final File curFolder = new File(U.defaultWorkDirectory(), PdsConsistentIdGeneratingFoldersResolver.DB_DEFAULT_FOLDER);
        final Set<Integer> indexes = new TreeSet<>();
        final File[] files = curFolder.listFiles(PdsConsistentIdGeneratingFoldersResolver.DB_SUBFOLDERS_NEW_STYLE_FILTER);
        for (File file : files) {
            final PdsConsistentIdGeneratingFoldersResolver.FolderCandidate uid
                = parseSubFolderName(file, log);
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
