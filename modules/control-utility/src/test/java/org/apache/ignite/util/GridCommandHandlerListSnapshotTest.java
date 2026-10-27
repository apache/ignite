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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.management.snapshot.SnapshotListCommand;
import org.apache.ignite.internal.management.snapshot.SnapshotListTask;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.systemview.view.ComputeJobView;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.JOBS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
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

    /** Resolved external storage paths. {@code null} if {@code extStorage} is {@code false}. */
    private @Nullable String[] extStoragePaths;

    /** Node consistent id postfix if required. */
    private String cstIdPostfix = "";

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

        /** Handy if the test run is interrupted and {@link #afterTest()} isn't invoked. */
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

        cfg.getDataStorageConfiguration().setWalCompactionEnabled(true);

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

    /** Test scenario for snapshots list order. */
    @Test
    public void testSnapshotsOrder() throws Exception {
        // Is not so important here, speeds up the tests.
        assumeFalse(customPath);
        // We process incremental snapshots manually here.
        assumeTrue(incrementalsCnt == 0);

        IgniteEx ig = (IgniteEx)startGridsMultiThreaded(GRIDS_CNT);
        ig.cluster().state(ACTIVE);

        int partitions = 4;
        int entriesCnt = partitions * 3;

        createCacheAndPreload(ig, DEFAULT_CACHE_NAME, entriesCnt, partitions, null);

        // Create the snapshots in an order by their names. The sorting order should differ from the alphabetical order.
        // 'testSnapshot0' is created first (earliest date).
        snp(ig).createSnapshot("testSnapshot0", null, false, false).get(getTestTimeout());

        try (IgniteDataStreamer<Integer, Integer> ds = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int val = entriesCnt; val < entriesCnt * 2; val++)
                ds.addData(val, val);
        }

        // Ensures a noticeable difference in the creation times.
        U.sleep(50L);

        // Next snapshot has the latest date.
        snp(ig).createSnapshot("testSnapshot1", null, false, false).get(getTestTimeout());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute(newCommandHandler(createTestLogger()), "--snapshot", "list"));

        String out = testOut.toString();

        int fromIdx = 0;

        // In descending date order testSnapshot1 (later) must appear before testSnapshot0 (earlier).
        // The checking is for every node, so n times.
        for (int n = 0; n < GRIDS_CNT; ++n) {
            int idx1 = out.indexOf("testSnapshot1", fromIdx);
            int idx0 = out.indexOf("testSnapshot0", fromIdx);

            assertTrue(idx1 < idx0);

            fromIdx = idx0 + "testSnapshot0".length();
        }

        // Ensures a noticeable difference in the times.
        U.sleep(50L);

        // Let's add an incremental part to 'testSnapshot0'. That will change its modification date.
        try (IgniteDataStreamer<Integer, Integer> ds = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int val = entriesCnt * 2; val < entriesCnt * 3; val++)
                ds.addData(val, val);
        }

        snp(ig).createSnapshot("testSnapshot0", null, true, false).get(getTestTimeout());

        testOut.reset();
        fromIdx = 0;

        assertEquals(EXIT_CODE_OK, execute(newCommandHandler(createTestLogger()), "--snapshot", "list"));

        out = testOut.toString();

        // Now, 'testSnapshot0' is expected to be first.
        for (int n = 0; n < GRIDS_CNT; ++n) {
            int idx0 = out.indexOf("testSnapshot0", fromIdx);
            int idx1 = out.indexOf("testSnapshot1", fromIdx);

            assertTrue(idx0 < idx1);

            fromIdx = idx1 + "testSnapshot1".length();
        }
    }

    /** Tests that a node stopping during the snapshots list is reported as a per-node error only. */
    @Test
    public void testNodeStoppedDuringSnapshotsList() throws Exception {
        // Is not important here, speeds up the tests.
        assumeTrue(incrementalsCnt == 0 && !customPath);

        IgniteEx ig = (IgniteEx)startGridsMultiThreaded(GRIDS_CNT);

        ig.cluster().state(ACTIVE);

        createCacheAndPreload(ig, DEFAULT_CACHE_NAME, 410, 4, null);

        snp(ig).createSnapshot("testSnapshot0", null, false, false).get(getTestTimeout());

        // The node to stop concurrently with the list command.
        int stoppedNodeIdx = GRIDS_CNT - 1;

        IgniteEx stoppedIg = grid(stoppedNodeIdx);

        String stoppedConsId = stoppedIg.cluster().localNode().consistentId().toString();

        // Blocks the snapshot executor of the node to be stopped, so that its list job cannot complete
        // before the node stops.
        CountDownLatch blockedLatch = new CountDownLatch(stoppedIg.configuration().getSnapshotThreadPoolSize());
        CountDownLatch unblockLatch = new CountDownLatch(1);

        ExecutorService snpExec = stoppedIg.context().pools().getSnapshotExecutorService();

        for (int i = 0; i < stoppedIg.configuration().getSnapshotThreadPoolSize(); i++) {
            snpExec.execute(() -> {
                blockedLatch.countDown();

                try {
                    unblockLatch.await();
                }
                catch (InterruptedException ignored) {
                    // No-op: the node is stopping.
                }
            });
        }

        assertTrue(blockedLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        injectTestSystemOut();

        IgniteInternalFuture<Integer> listFut = GridTestUtils.runAsync(() ->
            execute(newCommandHandler(createTestLogger()), "--snapshot", "list"));

        // Waits until the list job hangs on the blocked node. From this moment on, the node is guaranteed
        // to report an error instead of a result, whatever happens to the job.
        assertTrue(waitForCondition(() -> {
            for (ComputeJobView job : stoppedIg.context().systemView().<ComputeJobView>view(JOBS_VIEW)) {
                if (SnapshotListTask.class.getName().equals(job.taskClassName()))
                    return true;
            }

            return false;
        }, getTestTimeout()));

        // The kernal stopping waits for all the jobs to finish. We've blocked one so that we have to use a future and
        // wait for the flag.
        IgniteInternalFuture<?> stopFut = runAsync(() -> stopGrid(stoppedNodeIdx));

        assertTrue(waitForCondition(() -> grid(stoppedNodeIdx).context().isStopping(), getTestTimeout()));

        unblockLatch.countDown();

        // The command succeeds despite the failed node.
        assertEquals(EXIT_CODE_OK, (int)listFut.get());

        stopFut.get(getTestTimeout());

        // Ensure the test node is stopped.
        assertTrue(waitForCondition(() -> grid(0).cluster().nodes().size() == GRIDS_CNT - 1, getTestTimeout()));

        String out = testOut.toString();

        // The stopped node is reported with an error in the standard per-node format.
        assertEquals(GRIDS_CNT, countEntries(out, "Node '"));
        assertEquals(1, countEntries(out, "Node '" + stoppedConsId + "'"));

        // The other nodes still list their snapshots.
        assertEquals(0, countEntries(out, SnapshotListCommand.NO_SNAPSHOTS));

        assertEquals(GRIDS_CNT - 1, countEntries(out, "testSnapshot0"));
        assertEquals(GRIDS_CNT - 1, countEntries(out, "Total size"));

        assertEquals(1, countEntries(out, SnapshotListCommand.EXCEPTION_PREF +
            "Local node is stopping. [errorType=NodeStoppingException.class]"));
    }

    /**
     * The main test scenario:
     * <ul>
     *     <li>Step 1: Starts cluster</li>
     *     <li>
     *         Step 2: If {@code snapshotsCnt} &gt; 0, creates a cache, fills it and creates this number of the test snapshots.
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
     *         <p>If {@link #extStorage} is set, also verifies that external storages are noted in the output.</p>
     *         <p>If {@link #incrementalsCnt} &gt; 0, also verifies that the incremental snapshots are noted in the output.</p>
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
            int partitions = 4;
            int entriesCnt = partitions * 3;

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

        // Step 3: After the test snapshots are created, invoke optional test listener.
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
            assertFalse(out.contains("Total size"));
            assertFalse(out.contains("Creation date"));

            assertEquals(GRIDS_CNT, countEntries(out, SnapshotListCommand.NO_SNAPSHOTS));

            return;
        }

        // Step 7: Find the test snapshots in the output.
        assertEquals(0, countEntries(out, SnapshotListCommand.NO_SNAPSHOTS));

        for (int snpIdx = 0; snpIdx < snapshotsCnt; snpIdx++) {
            assertEquals(GRIDS_CNT, countEntries(out, "testSnapshot" + snpIdx));

            // If the external storage is configured, find their titles.
            if (extStorage)
                assertEquals(GRIDS_CNT, countEntries(out, "External storages size"));
        }

        // If the incremental snapshots are configured, find their titles.
        if (incrementalsCnt > 0) {
            assertEquals(GRIDS_CNT, countEntries(out, "Last incremental date"));
            assertEquals(GRIDS_CNT, countEntries(out, "Incremental parts size"));
        }
    }

    /** {@link #doTestSnapshotsLists(int, Runnable)} with {@code snapshotsCreatedLsnr}. */
    private void doTestSnapshotsLists(int snapshotsCnt) throws Exception {
        doTestSnapshotsLists(snapshotsCnt, null);
    }

    /** @return Number of the test entries in the original text. */
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
