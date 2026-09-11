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
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assume.assumeTrue;

/** Test for the command '--snapshot delete'. */
@RunWith(Parameterized.class)
public class GridCommandHandlerDeleteSnapshotTest extends GridCommandHandlerAbstractTest {
    /** Value: -1 - do not use, 1 - server node, 0 - client node. */
    @Parameter(1)
    public int extraNodeIsServer = -1;

    /** */
    @Parameter(2)
    public boolean addIncrements;

    /** */
    @Parameter(3)
    public boolean changeBaseline;

    /** */
    @Parameter(4)
    public boolean customPath;

    /** */
    @Parameter(5)
    public boolean separatedWorkDir;

    /** */
    @Parameters(name = "client={0},useExtraNode={1},inc={2},chBaseln={3},cstSnpPath={4},ownWorkDir={5}")
    public static Collection<?> parameters() {
        return GridTestUtils.cartesianProduct(
            commandHandlers(),
            F.asList(-1, 1, 0), // Use extra node (do not use at all, server node, client node);
            F.asList(false, true), // Add increments to the test snapshot;
            F.asList(false, true), // Change baseline;
            F.asList(false, true), // Use custom snapshot path;
            F.asList(true, false) // Separated (own) work directory.
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

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

        if (separatedWorkDir)
            cfg.setWorkDirectory(new File(U.defaultWorkDirectory(), igniteInstanceName).getAbsolutePath());

        return cfg;
    }

    /** */
    @Test
    public void testSnapshotDelete() throws Exception {
        // A custom snapshot path actually puts snapshots in a shared directory. This skews the results when dedicated
        // work directories are set.
        assumeTrue(!customPath || !separatedWorkDir);

        int entriesCnt = 4000;
        int initNodes = 3;

        walCompactionEnabled(addIncrements);

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
        // previous nodes independently of the baseline.
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
        if (extraNodeIsServer == 1)
            startGrid(initNodes);
        else if (extraNodeIsServer == 0)
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

        if (separatedWorkDir) {
            // When the nodes use own separated work directory, we expect a strict result.
            assertTrue(out.contains("Snapshot removed on the following nodes [cnt=%d]:".formatted(initNodes)));

            if (extraNodeIsServer == 1)
                assertTrue(out.contains("the following nodes didn't find any snapshot data, nothing to delete [cnt=1]:"));
            else if (extraNodeIsServer == 0)
                assertFalse(out.contains("the following nodes didn't find any snapshot data, nothing to delete"));
        }
        else {
            // When nodes use a shared work directory, there is a race for the delete operation. One node can get faster
            // than others and remove snapshot completely quickly. The others might not find snapshot files. We can be
            // only sure that at least one node removes snapshot.
            assertTrue(out.contains("Snapshot removed on the following nodes [cnt="));
        }

        assertFalse(out.contains("Snapshot not found on current server nodes"));

        assertTrue(waitForCondition(() -> !snpDir.exists(), getTestTimeout()));
    }
}
