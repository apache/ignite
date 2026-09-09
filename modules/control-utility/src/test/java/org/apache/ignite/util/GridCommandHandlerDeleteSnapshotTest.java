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
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Test for the "`--snapshot delete` command". */
public class GridCommandHandlerDeleteSnapshotTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    protected @Nullable ListeningTestLogger listeningLog;

    /** */
    protected boolean separateWorkDir;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Handy if other test runs interrupted.
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        try (DirectoryStream<Path> files = newDirectoryStream(Paths.get(U.defaultWorkDirectory()))) {
            for (Path path : files)
                U.delete(path);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (listeningLog != null)
            cfg.setGridLogger(listeningLog);

        if (separateWorkDir)
            cfg.setWorkDirectory(new File(U.defaultWorkDirectory(), igniteInstanceName).getAbsolutePath());

        return cfg;
    }

    /** */
    @Test
    public void testSnapshotDelete() throws Exception {
        doTestSnapshotDelete(null, false, false, false, false);
    }

    /** */
    @Test
    public void testSnapshotDeleteAddServer() throws Exception {
        doTestSnapshotDelete(true, false, false, false, false);
    }

    /** */
    @Test
    public void testSnapshotDeleteAddClient() throws Exception {
        doTestSnapshotDelete(false, false, false, false, false);
    }

    /** */
    @Test
    public void testSnapshotDeleteAddServerAddIncrementals() throws Exception {
        doTestSnapshotDelete(true, false, true, false, false);
    }

    /** */
    @Test
    public void testSnapshotDeleteChangeBaseline() throws Exception {
        doTestSnapshotDelete(null, false, false, true, false);
    }

    /** */
    @Test
    public void testSnapshotDeleteAddServerChangeBaseline() throws Exception {
        doTestSnapshotDelete(true, false, false, true, false);
    }

    /** */
    @Test
    public void testSnapshotDeleteAddServerIncrementalsChangeBaseline() throws Exception {
        doTestSnapshotDelete(true, false, false, true, false);
    }

    /** */
    @Test
    public void testSnapshotDeleteAddClientChangeBaseline() throws Exception {
        doTestSnapshotDelete(false, false, false, true, false);
    }

    /** */
    @Test
    public void testSnapshotDeleteAddClientChangeBaselineIncrementals() throws Exception {
        doTestSnapshotDelete(false, false, true, true, false);
    }

    /** */
    @Test
    public void testSnapshotDeleteSetCustomPath() throws Exception {
        doTestSnapshotDelete(null, true, false, false, false);
    }

    /** */
    @Test
    public void testSnapshotDeleteSetCustomPathAddClient() throws Exception {
        doTestSnapshotDelete(false, true, false, false, false);
    }

    /** */
    @Test
    public void testSnapshotDeleteIncrementsSetCustomPathAddClient() throws Exception {
        doTestSnapshotDelete(false, true, true, false, false);
    }

    /** */
    @Test
    public void testSnapshotDeleteSetCustomPathAddClientChangeBaseline() throws Exception {
        doTestSnapshotDelete(false, true, false, true, false);
    }

    /** */
    @Test
    public void testSnapshotDeleteSameWorkDirectory() throws Exception {
        doTestSnapshotDelete(null, false, false, false, true);
    }

    /** */
    @Test
    public void testSnapshotDeleteSameWorkDirectoryAddServer() throws Exception {
        doTestSnapshotDelete(true, false, false, false, true);
    }

    /** */
    @Test
    public void testSnapshotDeleteSameWorkDirectoryAddClient() throws Exception {
        doTestSnapshotDelete(false, false, false, false, true);
    }

    /** */
    @Test
    public void testSnapshotDeleteSameWorkDirectoryAddServerChangeBaseline() throws Exception {
        doTestSnapshotDelete(true, false, false, true, true);
    }

    /** */
    @Test
    public void testSnapshotDeleteSameWorkDirectoryAddServerChangeBaselineSetCustomPath() throws Exception {
        doTestSnapshotDelete(true, true, false, true, true);
    }

    /** */
    private void doTestSnapshotDelete(
        @Nullable Boolean extraNodeIsServer,
        boolean customPath,
        boolean addIncrements,
        boolean changeBaseline,
        boolean sameWorkDir
    ) throws Exception {
        int entriesCnt = 4000;
        int initNodes = 3;

        walCompactionEnabled(addIncrements);

        separateWorkDir = !sameWorkDir;

        IgniteEx ig = (IgniteEx)startGridsMultiThreaded(initNodes);

        if (changeBaseline) {
            ig.cluster().baselineAutoAdjustEnabled(false);

            ig.cluster().setBaselineTopology(ig.cluster().topologyVersion());
        }

        ig.cluster().state(ACTIVE);

        createCacheAndPreload(ig, entriesCnt);

        File cstSnpsRoot = customPath ? new File(U.defaultWorkDirectory(), "ex_snapshots") : null;
        File snpDir = new File(customPath ? cstSnpsRoot : ig.context().pdsFolderResolver().fileTree().snapshotsRoot(), "testSnapshot");

        snp(ig).createSnapshot("testSnapshot", customPath ? cstSnpsRoot.getAbsolutePath() : null, false, false)
            .get(getTestTimeout());

        if (addIncrements) {
            for (int i = 0; i < 3; ++i) {
                int dataIdx = entriesCnt + entriesCnt / 4 * i;

                try (IgniteDataStreamer<Object, Object> streamer = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
                    for (int d = dataIdx; d < dataIdx + entriesCnt / 4; ++d)
                        streamer.addData(i, i);
                }

                snp(ig).createSnapshot("testSnapshot", customPath ? cstSnpsRoot.getAbsolutePath() : null, true, false)
                    .get(getTestTimeout());
            }
        }

        // Optionally restarts with the same servers number, but changed baseline. The snapshot is kept on the same
        // previous nodes independenlty of the baseline.
        if (changeBaseline) {
            ig.destroyCache(DEFAULT_CACHE_NAME);
            awaitPartitionMapExchange();

            stopAllGrids();

            ig = (IgniteEx)startGridsMultiThreaded(initNodes - 1);

            ig.cluster().setBaselineTopology(ig.cluster().topologyVersion());

            startGrid(initNodes - 1);

            assertEquals(initNodes - 1, ig.cluster().currentBaselineTopology().size());
            assertEquals(initNodes, ig.cluster().nodes().size());
        }

        // Optionally adds extra server or client node.
        if (Boolean.TRUE.equals(extraNodeIsServer))
            startGrid(initNodes);
        else if (Boolean.FALSE.equals(extraNodeIsServer))
            startGrid(CLIENT_NODE_NAME_PREFIX);

        injectTestSystemOut();

        // Tests missing snapshot deletion.
        if (customPath) {
            assertEquals(EXIT_CODE_OK, execute(newCommandHandler(), "--snapshot", "delete", "--src",
                cstSnpsRoot.getAbsolutePath(), "wrongSnapshot"));
        }
        else
            assertEquals(EXIT_CODE_OK, execute(newCommandHandler(), "--snapshot", "delete", "wrongSnapshot"));

        String out = testOut.toString();

        assertFalse(out.contains("Snapshot removed on the following nodes"));
        assertFalse(out.contains("the following nodes didn't find any snapshot data, nothing to delete"));
        assertTrue(out.contains("Snapshot not found on current server nodes"));

        testOut.reset();
        assertTrue(testOut.toString().isEmpty());

        if (customPath) {
            assertEquals(EXIT_CODE_OK, execute(newCommandHandler(), "--snapshot", "delete", "--src",
                cstSnpsRoot.getAbsolutePath(), "testSnapshot"));
        }
        else
            assertEquals(EXIT_CODE_OK, execute(newCommandHandler(), "--snapshot", "delete", "testSnapshot"));

        out = testOut.toString();

        if (sameWorkDir) {
            // When nodes use a shared work dirictory, there is a race for the delete operation. One node can get faster
            // than anothers and remove snasphot completely quickly. The others might not find snapshot files. We can be
            // only sure that at least one node removes snapshot.
            assertTrue(out.contains("Snapshot removed on the following nodes [cnt="));
        }
        else {
            // When the nodes use own separated work dirictory, we expect a strict result.
            assertTrue(out.contains("Snapshot removed on the following nodes [cnt=%d]:".formatted(initNodes)));

            if (Boolean.FALSE.equals(extraNodeIsServer))
                assertFalse(out.contains("the following nodes didn't find any snapshot data, nothing to delete"));
            else if (Boolean.TRUE.equals(extraNodeIsServer))
                assertTrue(out.contains("the following nodes didn't find any snapshot data, nothing to delete [cnt=1]:"));
        }

        assertFalse(out.contains("Snapshot not found on current server nodes"));

        assertTrue(waitForCondition(() -> !snpDir.exists(), getTestTimeout()));
    }
}
