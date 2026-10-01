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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.snapshot.SnapshotListCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;
import static org.junit.Assume.assumeTrue;

/** Test for the command '--snapshot list'. */
public class GridCommandHandlerListSnapshotTest extends GridCommandHandlerAbstractTest {
    /** */
    @Parameter(1)
    public boolean customPath;

    /** */
    @Parameter(2)
    public boolean separatedWorkDir;

    /** */
    @Parameter(3)
    public boolean addExtraSrvr;

    /** */
    @Parameter(4)
    public int incCnt;

    /** */
    @Parameters(name = "cmdHnd={0},customPath={1},ownWorkDir={2},addExtraSrvr={3},incCnt={4}")
    public static Collection<?> parameters() {
        return GridTestUtils.cartesianProduct(
            commandHandlers(),
            F.asList(false, true), // Use custom snapshot path
            F.asList(false, true), // Separated (own) work directory
            F.asList(false, true), // Add server node
            F.asList(2) // TODO : Number of incremental snapshots
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

        if (separatedWorkDir)
            cfg.setWorkDirectory(new File(U.defaultWorkDirectory(), igniteInstanceName).getAbsolutePath());

        cfg.getDataStorageConfiguration().setWalCompactionEnabled(incCnt > 0);

        return cfg;
    }

    /** */
    @Test
    public void testNoSnapshots() throws Exception {
        doTestSnapshotsLists(0);
    }

    /** */
    @Test
    public void testSingleSnapshot() throws Exception {
        doTestSnapshotsLists(1);
    }

    /** */
    @Test
    public void testSeveralSnapshots() throws Exception {
        doTestSnapshotsLists(4);
    }

    /** */
    private void doTestSnapshotsLists(int snpCnt) throws Exception {
        // A custom snapshot path actually puts snapshots in a shared directory. This skews the results when dedicated
        // work directories are set.
        assumeTrue(!customPath || !separatedWorkDir);

        assumeTrue(incCnt < 1 || snpCnt > 0);

        int srvrsCnt = 3;
        int entriesCnt = 20;

        IgniteEx ig = (IgniteEx)startGridsMultiThreaded(srvrsCnt);

        startGrid(CLIENT_NODE_NAME_PREFIX);

        ig.cluster().state(ACTIVE);

        File cstSnpsRoot = customPath
            ? new File(grid(0).context().pdsFolderResolver().fileTree().snapshotsRoot(), "ex_snapshots")
            : null;

        // Create shapshots.
        if (snpCnt > 0) {
            createCacheAndPreload(ig, 10);

            for (int s = 0; s < snpCnt; ++s) {
                snp(ig).createSnapshot("testSnapshot" + s, customPath ? cstSnpsRoot.getAbsolutePath() : null, false, false)
                    .get(getTestTimeout());

                for (int i = 0; i < incCnt; ++i) {
                    try (IgniteDataStreamer<Integer, Integer> ds = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
                        for (int k = (i + 1) * entriesCnt; k < (i + 2) * entriesCnt; ++k)
                            ds.addData(k, k);
                    }

                    snp(ig).createSnapshot("testSnapshot" + s, customPath ? cstSnpsRoot.getAbsolutePath() : null, true, false)
                        .get(getTestTimeout());
                }
            }
        }

        // Add a server.
        if (addExtraSrvr)
            startGrid(G.allGrids().size());

        injectTestSystemOut();

        // Requests snapshots.
        if (customPath) {
            assertEquals(EXIT_CODE_OK, execute(newCommandHandler(createTestLogger()), "--snapshot", "list", "--src",
                cstSnpsRoot.getAbsolutePath()));
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
        for (int i = 0; i < snpCnt; ++i)
            assertEquals(snpsRecordsCnt, countEntries(out, "Snapshot 'testSnapshot" + i + "'"));
    }

    /**
     * Counts occurrences of the node prefix in the output.
     */
    private int countNodeOccurrences(String output) {
        int cnt = 0;
        int idx = 0;

        while ((idx = output.indexOf(SnapshotListCommand.NODE_PREF, idx)) != -1) {
            cnt++;
            idx += SnapshotListCommand.NODE_PREF.length();
        }

        return cnt;
    }

    /** */
    private static int countEntries(String txt, String entry) {
        String prev = txt;

        txt = txt.replaceAll(entry, "");

        return  (prev.length() - txt.length()) / entry.length();
    }
}
