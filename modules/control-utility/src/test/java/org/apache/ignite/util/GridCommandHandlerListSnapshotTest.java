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
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.snapshot.SnapshotListCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/** Test for the command '--snapshot list'. */
public class GridCommandHandlerListSnapshotTest extends GridCommandHandlerAbstractTest {
    /** */
    private static final int GRIDS_CNT = 3;

    /** External storage path. */
    private static final String EXT_STORAGE_PATH = "extStorage";

    /** Flag to use {@link DataStorageConfiguration#setExtraSnapshotPaths(String...)}. */
    private boolean extStorage;

    /** Resolved external storages paths. {@code null} if {@code extStorage} is {@code false}. */
    private @Nullable String[] extStoragePaths;

    /** Node consistent id postfix if required. */
    private @Nullable String cstIdPostfix = "";

    /** Flag to use the custom paths for the test snapshots. */
    @Parameter(1)
    public boolean customPath;

    /** Flag to use separated node working directories. */
    @Parameter(2)
    public boolean separatedWorkDir;

    /** Number of incremental snapshots to add to the main test snapshots. */
    @Parameter(3)
    public int incrementalsCnt;

    /** */
    @Parameters(name = "cmdHnd={0}, customPath={1}, separatedWorkDir={2}, incrementalsCnt={3}")
    public static Collection<?> parameters() {
        return GridTestUtils.cartesianProduct(
            commandHandlers(),
            F.asList(false, true), // Custom test snapshots paths.
            F.asList(false, true), // Separated nodes' work directories.
            F.asList(0, 2) // Number of incremental snapshots to add to the main test snapshots.
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        /** Handy if test running is interrupted and {@link #afterTest()} isn't invoked. */
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        // Cleans everything: separated working directories and snapshots with custom paths.
        try (DirectoryStream<Path> files = newDirectoryStream(Paths.get(U.defaultWorkDirectory()))) {
            for (Path path : files)
                U.delete(path);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        String workDir = separatedWorkDir
            ? new File(U.defaultWorkDirectory(), igniteInstanceName).getAbsolutePath()
            : U.defaultWorkDirectory();

        cfg.setWorkDirectory(workDir);

        cfg.getDataStorageConfiguration().setWalCompactionEnabled(incrementalsCnt > 0);

        if (extStorage) {
            cfg.getDataStorageConfiguration().setExtraStoragePaths(
                workDir + File.separator,
                workDir + File.separator + EXT_STORAGE_PATH
            );

            extStoragePaths = cfg.getDataStorageConfiguration().getExtraStoragePaths();

            cfg.getDataStorageConfiguration().setExtraSnapshotPaths("", EXT_STORAGE_PATH);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return super.getTestIgniteInstanceName() + cstIdPostfix;
    }

    /** Tests the main scenario when there are no snapshots at all. */
    @Test
    public void testNoSnapshots() throws Exception {
        // Incremental snapshots do not matter here. Speeds up the tests.
        assumeTrue(incrementalsCnt == 0);

        doTestSnapshotsLists(0);
    }

    /** Tests the main scenario for a single snapshot. */
    @Test
    public void testSingleSnapshot() throws Exception {
        doTestSnapshotsLists(1);
    }

    /** Tests the main scenario for several snapshots. */
    @Test
    public void testSeveralSnapshots() throws Exception {
        doTestSnapshotsLists(3);
    }

    /** Tests the main scenario with an external storage added to the test snapshots. */
    @Test
    public void testExternalStorage() throws Exception {
        // External storage paths must match those configured in the node PDS storages, so the cases with different work
        // directories are skipped. Also, extra snapshot storages aren't used if snapshot is created with a custom path.
        assumeFalse(separatedWorkDir || customPath);

        extStorage = true;

        doTestSnapshotsLists(2);
    }

    /** Tests listing the snapshots created before the cluster restart changed the node consistent IDs. */
    @Test
    public void testChangedConsistentId() throws Exception {
        // Newly created work directories obviously contain no snapshots.
        assumeFalse(separatedWorkDir);

        Runnable snapshotsCreatedLsnr = () -> {
            stopAllGrids();

            cstIdPostfix = "_changed";

            try {
                startGridsMultiThreaded(GRIDS_CNT);

                startGrid(CLIENT_NODE_NAME_PREFIX);
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to restart the cluster", e);
            }
        };

        doTestSnapshotsLists(1, snapshotsCreatedLsnr);
    }

    /**
     * The main test scenario:
     * <ul>
     *     <li>Step 1: Starts cluster</li>
     *     <li>
     *         Step 2: If {@code snapshotsCnt} &gt 0, creates a cache, fills it and creates this number of the test snapshots.
     *         <p>If {@link #incrementalsCnt} &gt; 0, adds this number of incrementals to each snapshot.</p>
     *         <p>If {@link #customPath} is set, uses it as the snapshot path to create each test snapshot.</p>
     *         <p>If {@code snapshotsCnt} == 0, doesn't create any test snapshot.</p>
     *     </li>
     *     <li>Step 3: If {@code snapshotsCreatedLsnr} is not null, calls it.</li>
     *     <li>Step 4: Requests the snapshots list and parses the command's output.</li>
     *     <li>Step 5: Ensures that all the nodes are present in the output.</li>
     *     <li>Step 6: If {@code snapshotsCnt} == 0, ensures that the output says there are no snapshots on any node and quits.</li>
     *     <li>
     *         Step 7: If {@code snapshotsCnt} &gt; 0, checks the snapshots list of each node in the output.
     *         <p>If {@link #extStorage} is set, also checks that the output's snapshots list of every node has proper
     *         a descriptions of the current snapshot's external storage.</p>
     *         <p>If {@link #incrementalsCnt} &gt; 0, also verifies the total number of incremental snapshots found in the output.</p>
     *     </li>
     * </ul>
     *
     * @param snapshotsCnt Number of the test snapshots to create before calling the snapshots list command.
     * @param snapshotsCreatedLsnr Optional listener. Is called after the test snapshots creation.
     */
    private void doTestSnapshotsLists(int snapshotsCnt, @Nullable Runnable snapshotsCreatedLsnr) throws Exception {
        // A custom snapshot path actually puts snapshots in a shared directory. This skews the results when dedicated
        // work directories are set.
        assumeFalse(customPath && separatedWorkDir);

        // Step 1: Starting the cluster.
        IgniteEx ig = (IgniteEx)startGridsMultiThreaded(GRIDS_CNT);

        startGrid(CLIENT_NODE_NAME_PREFIX);

        ig.cluster().state(ACTIVE);

        File snpsRootFile = customPath
            ? new File(ig.context().pdsFolderResolver().fileTree().snapshotsRoot(), "ex_snapshots")
            : null;

        // Step 2: Cache and snapshot creation, if any.
        if (snapshotsCnt > 0) {
            int entriesCnt = 10;
            int partitions = 4;

            // Create cache.
            createCacheAndPreload(ig, DEFAULT_CACHE_NAME, entriesCnt, partitions, null);

            String absPathStr;

            // Create the test snapshots.
            for (int snpIdx = 0; snpIdx < snapshotsCnt; snpIdx++) {
                absPathStr = customPath ? snpsRootFile.getAbsolutePath() : null;

                snp(ig).createSnapshot("testSnapshot" + snpIdx, absPathStr, false, false).get(getTestTimeout());

                // Create the optional incremental snapshots.
                for (int incIdx = 0; incIdx < incrementalsCnt; incIdx++) {
                    try (IgniteDataStreamer<Integer, Integer> ds = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
                        for (int val = (snpIdx + incIdx + 1) * entriesCnt; val < (snpIdx + incIdx + 2) * entriesCnt; val++)
                            ds.addData(val, val);
                    }

                    snp(ig).createSnapshot("testSnapshot" + snpIdx, absPathStr, true, false).get(getTestTimeout());
                }
            }
        }

        // Step 3: After the tests snapshots are created, invoke optional test listener.
        if (snapshotsCreatedLsnr != null)
            snapshotsCreatedLsnr.run();

        // Step 4: Prepare and invoke the snapshots list command.
        injectTestSystemOut();

        if (customPath) {
            assertEquals(EXIT_CODE_OK, execute(newCommandHandler(createTestLogger()), "--snapshot", "list", "--src",
                snpsRootFile.getAbsolutePath()));
        }
        else
            assertEquals(EXIT_CODE_OK, execute(newCommandHandler(createTestLogger()), "--snapshot", "list"));

        String out = testOut.toString();

        // Step 5: Find the nodes in the output.
        for (Ignite g : G.allGrids()) {
            ClusterNode n = g.cluster().localNode();

            assertEquals(n.isClient() ? 0 : 1, countEntries(out, "Node '%s'".formatted(n.consistentId().toString())));
        }

        // Step 6: Ensure that there are no snapshots if none are expected and exit.
        if (snapshotsCnt == 0) {
            assertFalse(out.contains("Snapshot '"));

            assertEquals(GRIDS_CNT, countEntries(out, SnapshotListCommand.NO_SNAPSHOTS));

            return;
        }

        // Step 7: Find the test snapshots in the output.
        assertEquals(0, countEntries(out, SnapshotListCommand.NO_SNAPSHOTS));

        for (int snpIdx = 0; snpIdx < snapshotsCnt; snpIdx++) {
            assertEquals(GRIDS_CNT, countEntries(out, "Snapshot 'testSnapshot" + snpIdx + "'"));

            // If the external storage is configured, find the info about it.
            if (extStorage)
                assertEquals(GRIDS_CNT * snapshotsCnt, countEntries(out, "external storages: cnt=1, size="));
        }

        // If the incremental snapshots are configured, find the info about them.
        if (incrementalsCnt > 0)
            assertEquals(GRIDS_CNT * snapshotsCnt, countEntries(out, "incremental snapshots: cnt=" + incrementalsCnt));
    }

    /** {@link #doTestSnapshotsLists(int, Runnable)} with not set {@code snapshotsCreatedLsnr} by default. */
    private void doTestSnapshotsLists(int snapshotsCnt) throws Exception {
        doTestSnapshotsLists(snapshotsCnt, null);
    }

    /** @return Number of the test entries in original text. */
    private static int countEntries(String txt, String entry) {
        String prev = txt;

        txt = txt.replace(entry, "");

        return (prev.length() - txt.length()) / entry.length();
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration<?, ?> testCacheConfiguration(
        String cacheName,
        int partitions,
        @Nullable IgnitePredicate<ClusterNode> filter
    ) {
        CacheConfiguration<?, ?> ccfg = super.testCacheConfiguration(cacheName, partitions, filter);

        if (extStorage)
            ccfg.setStoragePaths(extStoragePaths);

        return ccfg;
    }
}
