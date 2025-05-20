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

package org.apache.ignite.util;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Command line tests of --persitence commands.
 */
@RunWith(Parameterized.class)
public class GridPersistenceCommandsTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** Use explicit cache storage for each group. */
    @Parameterized.Parameter(1)
    public boolean separateStorage;

    /** */
    @Parameterized.Parameters(name = "cmdHnd={0},separateStorage={1}")
    public static List<Object[]> params() {
        List<Object[]> res = new ArrayList<>();

        for (String cmdHnd : CMD_HNDS.keySet()) {
            for (boolean separateStorage: new boolean[] {true, false})
                res.add(new Object[] {cmdHnd, separateStorage});
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        if (separateStorage) {
            U.delete(new File(U.defaultWorkDirectory(), storagePath(DEFAULT_CACHE_NAME + "0")));
            U.delete(new File(U.defaultWorkDirectory(), storagePath(DEFAULT_CACHE_NAME + "1")));
            U.delete(new File(U.defaultWorkDirectory(), storagePath(DEFAULT_CACHE_NAME + "2")));
            U.delete(new File(U.defaultWorkDirectory(), storagePath(DEFAULT_CACHE_NAME + "3")));
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (separateStorage) {
            cfg.getDataStorageConfiguration().setExtraStoragePathes(
                storagePath(DEFAULT_CACHE_NAME + "0"),
                storagePath(DEFAULT_CACHE_NAME + "1"),
                storagePath(DEFAULT_CACHE_NAME + "2"),
                storagePath(DEFAULT_CACHE_NAME + "3")
            );
        }

        return cfg;
    }

    /**
     * Test verifies persistence clean command with explicit list of caches to be cleaned.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistenceCleanSpecifiedCachesCommand() throws Exception {
        String cacheName0 = DEFAULT_CACHE_NAME + "0";
        String cacheName1 = DEFAULT_CACHE_NAME + "1";
        String cacheName2 = DEFAULT_CACHE_NAME + "2";
        String cacheName3 = DEFAULT_CACHE_NAME + "3";

        String nonExistingCacheName = DEFAULT_CACHE_NAME + "4";

        NodeFileTree ft = startGridAndPutNodeToMaintenance(
            new CacheConfiguration[]{
                cacheConfiguration(cacheName0),
                cacheConfiguration(cacheName1),
                cacheConfiguration(cacheName2),
                cacheConfiguration(cacheName3)
            },
            s -> !s.equals(cacheName3));

        IgniteEx ig1 = startGrid(1);

        String port = connectorPort(ig1);

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute(
            "--host", "localhost", "--port", port,
            "--persistence", "clean", "caches",
            nonExistingCacheName));

        assertEquals(EXIT_CODE_OK, execute(
            "--host", "localhost", "--port", port,
            "--persistence", "clean", "caches",
            cacheName0 + "," + cacheName1));

        boolean cleanedEmpty = ft.existingCacheDirs().stream()
            .filter(f -> f.getName().contains(cacheName0) || f.getName().contains(cacheName1))
            .map(f -> f.listFiles().length == 1)
            .reduce(true, (t, u) -> t && u);

        assertTrue(cleanedEmpty);

        boolean nonCleanedNonEmpty = ft.existingCacheDirs().stream()
            .filter(f -> f.getName().contains(cacheName2) || f.getName().contains(cacheName3))
            .map(f -> f.listFiles().length > 1)
            .reduce(true, (t, u) -> t && u);

        assertTrue(nonCleanedNonEmpty);

        stopGrid(1);

        ig1 = startGrid(1);

        assertTrue(ig1.context().maintenanceRegistry().isMaintenanceMode());

        assertEquals(EXIT_CODE_OK, execute(
            "--host", "localhost", "--port", port,
            "--persistence", "clean", "caches", cacheName2));

        stopGrid(1);

        ig1 = startGrid(1);

        assertFalse(ig1.context().maintenanceRegistry().isMaintenanceMode());
    }

    /**
     * Test verifies persistence clean command cleaning only corrupted caches and not touching others.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistenceCleanCorruptedCachesCommand() throws Exception {
        String cacheName0 = DEFAULT_CACHE_NAME + "0";
        String cacheName1 = DEFAULT_CACHE_NAME + "1";
        String cacheName2 = DEFAULT_CACHE_NAME + "2";
        String cacheName3 = DEFAULT_CACHE_NAME + "3";

        NodeFileTree ft = startGridAndPutNodeToMaintenance(
            new CacheConfiguration[]{
                cacheConfiguration(cacheName0),
                cacheConfiguration(cacheName1),
                cacheConfiguration(cacheName2),
                cacheConfiguration(cacheName3)
            },
            s -> !s.equals(cacheName3));

        IgniteEx ig1 = startGrid(1);

        String port = connectorPort(ig1);

        assertEquals(EXIT_CODE_OK, execute("--host", "localhost", "--port", port, "--persistence"));
        assertEquals(EXIT_CODE_OK, execute("--host", "localhost", "--port", port, "--persistence", "info"));

        assertEquals(EXIT_CODE_OK, execute("--host", "localhost", "--port", port, "--persistence", "clean", "corrupted"));

        boolean cleanedEmpty = ft.existingCacheDirs().stream()
            .filter(f ->
                f.getName().contains(cacheName0)
                || f.getName().contains(cacheName1)
                || f.getName().contains(cacheName2)
            )
            .map(f -> f.listFiles().length == 1)
            .reduce(true, (t, u) -> t && u);

        assertTrue(cleanedEmpty);

        stopGrid(1);

        ig1 = startGrid(1);

        assertFalse(ig1.context().maintenanceRegistry().isMaintenanceMode());
    }

    /**
     * Test verifies persistence clean all command that cleans all cache directories.
     *
     * @throws Exception
     */
    @Test
    public void testPersistenceCleanAllCachesCommand() throws Exception {
        String cacheName0 = DEFAULT_CACHE_NAME + "0";
        String cacheName1 = DEFAULT_CACHE_NAME + "1";

        NodeFileTree ft = startGridAndPutNodeToMaintenance(
            new CacheConfiguration[]{
                cacheConfiguration(cacheName0),
                cacheConfiguration(cacheName1)
            },
            s -> s.equals(cacheName0));

        IgniteEx ig1 = startGrid(1);

        assertEquals(EXIT_CODE_OK, execute("--host", "localhost", "--port", connectorPort(ig1),
            "--persistence", "clean", "all"));

        boolean allEmpty = ft.existingCacheDirs().stream()
            .filter(File::isDirectory)
            .filter(NodeFileTree::cacheDir)
            .map(f -> f.listFiles().length == 1)
            .reduce(true, (t, u) -> t && u);

        assertTrue(allEmpty);

        stopGrid(1);

        ig1 = startGrid(1);

        assertFalse(ig1.context().maintenanceRegistry().isMaintenanceMode());
    }

    /**
     * Test verifies that persistence backup command to backup all caches backs up all cache directories.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistenceBackupAllCachesCommand() throws Exception {
        String cacheName0 = DEFAULT_CACHE_NAME + "0";
        String cacheName1 = DEFAULT_CACHE_NAME + "1";

        NodeFileTree ft = startGridAndPutNodeToMaintenance(
            new CacheConfiguration[]{
                cacheConfiguration(cacheName0),
                cacheConfiguration(cacheName1)
            },
            s -> s.equals(cacheName0));

        IgniteEx ig1 = startGrid(1);

        assertEquals(EXIT_CODE_OK, execute("--host", "localhost", "--port", connectorPort(ig1),
            "--persistence", "backup", "all"));

        Set<String> backedUpCacheDirs = ft.allStorages()
            .flatMap(storageRoot -> Arrays.stream(storageRoot.listFiles()))
            .filter(File::isDirectory)
            .filter(f -> f.getName().startsWith("backup_"))
            .map(f -> f.getName().substring("backup_".length()))
            .collect(Collectors.toCollection(TreeSet::new));

        Set<String> allCacheDirs = ft.existingCacheDirs().stream()
            .filter(File::isDirectory)
            .filter(NodeFileTree::cacheDir)
            .map(File::getName)
            .collect(Collectors.toCollection(TreeSet::new));

        assertEqualsCollections(allCacheDirs, backedUpCacheDirs);

        checkCacheAndBackupDirsContent(ft);
    }

    /**
     * Test verifies that persistence backup command copies all corrupted caches content to backup directory
     * but does not touch other directories.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistenceBackupCorruptedCachesCommand() throws Exception {
        String cacheName0 = DEFAULT_CACHE_NAME + "0";
        String cacheName1 = DEFAULT_CACHE_NAME + "1";

        NodeFileTree ft = startGridAndPutNodeToMaintenance(
            new CacheConfiguration[]{
                cacheConfiguration(cacheName0),
                cacheConfiguration(cacheName1)
            },
            s -> s.equals(cacheName0));

        IgniteEx ig1 = startGrid(1);

        assertEquals(EXIT_CODE_OK, execute("--host", "localhost", "--port", connectorPort(ig1),
            "--persistence", "backup", "corrupted"));

        long backedUpCachesCnt = ft.allStorages()
            .flatMap(storagRoot -> Arrays.stream(storagRoot.listFiles()))
            .filter(File::isDirectory)
            .filter(f -> f.getName().startsWith("backup_"))
            .filter(f -> f.getName().contains(cacheName0))
            .count();

        assertEquals(1, backedUpCachesCnt);

        checkCacheAndBackupDirsContent(ft);
    }

    /**
     * Test verifies that persistence backup command with specified caches copied only content of that caches and
     * doesn't touch other directories.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistenceBackupSpecifiedCachesCommand() throws Exception {
        String cacheName0 = DEFAULT_CACHE_NAME + "0";
        String cacheName1 = DEFAULT_CACHE_NAME + "1";
        String cacheName2 = DEFAULT_CACHE_NAME + "2";

        String nonExistingCacheName = "nonExistingCache";

        NodeFileTree ft = startGridAndPutNodeToMaintenance(
            new CacheConfiguration[]{
                cacheConfiguration(cacheName0),
                cacheConfiguration(cacheName1),
                cacheConfiguration(cacheName2)
            },
            s -> s.equals(cacheName0) || s.equals(cacheName2));

        IgniteEx ig1 = startGrid(1);

        String port = connectorPort(ig1);;

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--host", "localhost", "--port", port,
            "--persistence", "backup", "caches",
            nonExistingCacheName));

        assertEquals(EXIT_CODE_OK, execute("--host", "localhost", "--port", port,
            "--persistence", "backup", "caches",
            cacheName0 + "," + cacheName2));

        long backedUpCachesCnt = ft.allStorages()
            .flatMap(root -> Arrays.stream(root.listFiles()))
            .filter(File::isDirectory)
            .filter(f -> f.getName().startsWith("backup_"))
            .count();

        assertEquals(2, backedUpCachesCnt);

        checkCacheAndBackupDirsContent(ft);
    }

    /** */
    private CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration(cacheName)
            .setStoragePath(separateStorage ? storagePath(cacheName) : null)
            .setAtomicityMode(TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1);

        return ccfg;
    }

    /** */
    private String storagePath(String cacheName) {
        assertTrue(cacheName.startsWith(DEFAULT_CACHE_NAME));
        return "ex_storage" + cacheName.replace(DEFAULT_CACHE_NAME, "");
    }

    /**
     * Starts cluster of two nodes and prepares situation of corrupted PDS on node2
     * so it enters maintenance mode on restart.
     *
     * @param cachesToStart Configurations of caches that should be started in cluster.
     * @param cacheToCorrupt Function determining should cache with given name be corrupted or not.
     */
    private NodeFileTree startGridAndPutNodeToMaintenance(CacheConfiguration[] cachesToStart,
                                                  @Nullable Function<String, Boolean> cacheToCorrupt) throws Exception {
        assert cachesToStart != null && cachesToStart.length > 0;

        IgniteEx ig0 = startGrid(0);
        IgniteEx ig1 = startGrid(1);

        NodeFileTree ft1 = ig1.context().pdsFolderResolver().fileTree();

        ig0.cluster().baselineAutoAdjustEnabled(false);
        ig0.cluster().state(ACTIVE);

        IgniteCache dfltCache = ig0.getOrCreateCache(cachesToStart[0]);

        if (cachesToStart.length > 1) {
            for (int i = 1; i < cachesToStart.length; i++)
                ig0.getOrCreateCache(cachesToStart[i]);
        }

        for (int k = 0; k < 1000; k++)
            dfltCache.put(k, k);

        GridCacheDatabaseSharedManager dbMrg0 = (GridCacheDatabaseSharedManager)ig0.context().cache().context().database();
        GridCacheDatabaseSharedManager dbMrg1 = (GridCacheDatabaseSharedManager)ig1.context().cache().context().database();

        dbMrg0.forceCheckpoint("cp").futureFor(CheckpointState.FINISHED).get();
        dbMrg1.forceCheckpoint("cp").futureFor(CheckpointState.FINISHED).get();

        Arrays.stream(cachesToStart)
            .map(CacheConfiguration::getName)
            .filter(name -> cacheToCorrupt.apply(name))
            .forEach(name -> ig0.cluster().disableWal(name));

        for (int k = 1000; k < 2000; k++)
            dfltCache.put(k, k);

        stopGrid(1);

        File[] cpMarkers = ft1.checkpoint().listFiles();

        for (File cpMark : cpMarkers) {
            if (cpMark.getName().contains("-END"))
                cpMark.delete();
        }

        assertThrows(log, () -> startGrid(1), Exception.class, null);

        return ft1;
    }

    /** */
    private void checkCacheAndBackupDirsContent(NodeFileTree ft) {
        List<File> backupDirs = Arrays.stream(ft.nodeStorage().listFiles())
            .filter(File::isDirectory)
            .filter(f -> f.getName().startsWith("backup_"))
            .collect(Collectors.toList());

        Path mntcNodeWorkDirPath = ft.nodeStorage().toPath();

        for (File bDir : backupDirs) {
            File origCacheDir = mntcNodeWorkDirPath.resolve(bDir.getName().substring("backup_".length())).toFile();

            assertTrue(origCacheDir.isDirectory());

            assertEquals(origCacheDir.listFiles().length, bDir.listFiles().length);
        }
    }
}
