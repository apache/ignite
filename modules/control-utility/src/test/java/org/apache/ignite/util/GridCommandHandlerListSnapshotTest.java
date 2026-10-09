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
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
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
    /** Extra storage path. */
    private static final String EXT_STORAGE_PATH = "extStorage";

    /** Flag to use {@link DataStorageConfiguration#setExtraSnapshotPaths(String...)}. */
    private boolean extraStorages;

    /** Resolved extra storages paths. {@code null} if {@code extraStorages} is {@code null}. */
    private @Nullable String[] extStoragePaths;

    /** Node consistent id postfix. */
    private @Nullable String cstId_postfix = "";

    /** Flag setting the usage of a custom snapshot path. */
    @Parameter(1)
    public boolean customPath;

    /** Flag setting the usage of dedicated, own node working directories. */
    @Parameter(2)
    public boolean separatedWorkDir;

    /** Flag to add extra server node after the snapshot creation. */
    @Parameter(3)
    public boolean addExtraSrvr;

    /** Number of incremental snapshots to add to the main test snapshots. */
    @Parameter(4)
    public int incCnt;

    /** */
    @Parameters(name = "cmdHnd={0},customPath={1},ownWorkDir={2},addExtraSrvr={3},incCnt={4}")
    public static Collection<?> parameters() {
        return GridTestUtils.cartesianProduct(
            commandHandlers(),
            F.asList(false, true), // Custom snapshot path
            F.asList(false, true), // Separated (own) work directories
            F.asList(false, true), // Add a server node after the snapshot creation
            F.asList(0, 2) // Number of incremental snapshots
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
        super.cleanPersistenceDir();

        // Also cleans separated snapshot working directories and custom snapshot paths.
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

        cfg.getDataStorageConfiguration().setWalCompactionEnabled(incCnt > 0);

        if (extraStorages) {
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
        return super.getTestIgniteInstanceName() + cstId_postfix;
    }

    /** */
    @Test
    public void testNoSnapshots() throws Exception {
        assumeFalse(addExtraSrvr || incCnt > 0);

        doTestSnapshotsLists(0, false, false);
    }

    /** */
    @Test
    public void testSingleSnapshot() throws Exception {
        doTestSnapshotsLists(1, false, false);
    }

    /** */
    @Test
    public void testSeveralSnapshots() throws Exception {
        doTestSnapshotsLists(3, false, false);
    }

    /** */
    @Test
    public void testOneNodeMisses() throws Exception {
        // Doesn't matter here.
        assumeFalse(incCnt > 0);
        // Let's keep just one node not seeing the snapshot.
        assumeTrue(separatedWorkDir);
        // Almost the same tests.
        assumeFalse(addExtraSrvr);

        doTestSnapshotsLists(2, true, false);
    }

    /** */
    @Test
    public void testExtraStorages() throws Exception {
        // Extra storeages are required to be the same as configured in the node's PDS storages. Thus, we skip different work folders.
        // Also, extra snapshot storages aren't used if snapshot is created with a custom path.
        assumeFalse(separatedWorkDir || customPath);

        extraStorages = true;

        doTestSnapshotsLists(2, false, false);
    }

    /** */
    @Test
    public void testChangedConsistentId() throws Exception {
        // In new created working directories there will be obviously no snapshots.
        assumeFalse(separatedWorkDir);
        // Fastens the tests
        assumeFalse(incCnt > 0);

        doTestSnapshotsLists(1, false, true);
    }

    /** */
    private void doTestSnapshotsLists(int snpCnt, boolean deleteOnOneNode, boolean restartWithChangedCstIds) throws Exception {
        // A custom snapshot path actually puts snapshots in a shared directory. This skews the results when dedicated
        // work directories are set.
        assumeFalse(customPath && separatedWorkDir);

        int srvrsCnt = 3;
        int entriesCnt = 10;
        int partitions = 4;

        IgniteEx ig = (IgniteEx)startGridsMultiThreaded(srvrsCnt);

        startGrid(CLIENT_NODE_NAME_PREFIX);

        ig.cluster().state(ACTIVE);

        File snpsRootFile = customPath
            ? new File(ig.context().pdsFolderResolver().fileTree().snapshotsRoot(), "ex_snapshots")
            : null;

        // Flag if 'testSnapshot0' deleted on node0.
        boolean grid0HasNoSnapshot0 = false;

        // Create snapshots.
        if (snpCnt > 0) {
            createCacheAndPreload(ig, DEFAULT_CACHE_NAME, entriesCnt, partitions, null);

            String absPathStr = null;

            for (int snpIdx = 0; snpIdx < snpCnt; snpIdx++) {
                absPathStr = customPath ? snpsRootFile.getAbsolutePath() : null;

                snp(ig).createSnapshot("testSnapshot" + snpIdx, absPathStr, false, false).get(getTestTimeout());

                for (int incIdx = 0; incIdx < incCnt; incIdx++) {
                    try (IgniteDataStreamer<Integer, Integer> ds = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
                        for (int val = (snpIdx + incIdx + 1) * entriesCnt; val < (snpIdx + incIdx + 2) * entriesCnt; val++)
                            ds.addData(val, val);
                    }

                    snp(ig).createSnapshot("testSnapshot" + snpIdx, absPathStr, true, false).get(getTestTimeout());
                }
            }

            if (deleteOnOneNode) {
                SnapshotFileTree sft = new SnapshotFileTree(ig.context(), "testSnapshot0", absPathStr);

                assertTrue(sft.root().exists());
                assertTrue(U.delete(sft.root()));
                assertFalse(sft.root().exists());

                grid0HasNoSnapshot0 = true;
            }
        }

        if (restartWithChangedCstIds) {
            stopAllGrids();

            cstId_postfix = "_changed";

            startGridsMultiThreaded(srvrsCnt);

            startGrid(CLIENT_NODE_NAME_PREFIX);
        }

        // Add a server.
        if (addExtraSrvr)
            startGrid(G.allGrids().size());

        injectTestSystemOut();

        // Requests snapshots.
        if (customPath) {
            assertEquals(EXIT_CODE_OK, execute(newCommandHandler(createTestLogger()), "--snapshot", "list", "--src",
                snpsRootFile.getAbsolutePath()));
        }
        else
            assertEquals(EXIT_CODE_OK, execute(newCommandHandler(createTestLogger()), "--snapshot", "list"));

        String out = testOut.toString();

        // Find the nodes in the output.
        for (Ignite g : G.allGrids()) {
            ClusterNode n = g.cluster().localNode();

            assertEquals(n.isClient() ? 0 : 1, countEntries(out, "Node '%s'".formatted(n.consistentId().toString())));
        }

        // Ensure that there are no snapshots.
        if (snpCnt == 0) {
            assertFalse(out.contains("Snapshot '"));

            assertEquals(srvrsCnt + (addExtraSrvr ? 1 : 0), countEntries(out, SnapshotListCommand.NO_SNAPSHOTS));

            return;
        }

        assertEquals((addExtraSrvr && separatedWorkDir ? 1 : 0), countEntries(out, SnapshotListCommand.NO_SNAPSHOTS));

        // The additional server node doesn't have snapshots. But it can see them if shared the snapshot directory.
        int snpsRecordsCnt = srvrsCnt + (addExtraSrvr
            ? (separatedWorkDir ? 0 : 1)
            : 0
        );

        // Find the snapshots in the output.
        for (int snpIdx = 0; snpIdx < snpCnt; snpIdx++) {
            // 'testSnapshot0' has fewer records if was deleted on one node.
            int certainSnpRecordsCnt = snpIdx == 0 && grid0HasNoSnapshot0
                ? snpsRecordsCnt - 1
                : snpsRecordsCnt;

            assertEquals(certainSnpRecordsCnt, countEntries(out, "Snapshot 'testSnapshot" + snpIdx + "'"));

            for (int i = 0; i < incCnt; i++)
                assertEquals(certainSnpRecordsCnt * snpCnt, countEntries(out, "incremental snapshots: cnt=" + incCnt));

            if (extraStorages)
                assertEquals(certainSnpRecordsCnt * snpCnt, countEntries(out, "external storages: cnt=1, size="));
        }
    }

    /** */
    private static int countEntries(String txt, String entry) {
        String prev = txt;

        txt = txt.replaceAll(entry, "");

        return (prev.length() - txt.length()) / entry.length();
    }

    /** */
    @Override protected CacheConfiguration<?, ?> testCacheConfiguration(
        String cacheName,
        int partitions,
        @Nullable IgnitePredicate<ClusterNode> filter
    ) {
        CacheConfiguration<?, ?> ccfg = super.testCacheConfiguration(cacheName, partitions, filter);

        if (extraStorages)
            ccfg.setStoragePaths(extStoragePaths);

        return ccfg;
    }
}
