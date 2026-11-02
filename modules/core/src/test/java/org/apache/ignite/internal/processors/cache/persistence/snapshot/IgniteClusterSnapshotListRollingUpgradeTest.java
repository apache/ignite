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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.management.snapshot.SnapshotListCommandArg;
import org.apache.ignite.internal.management.snapshot.SnapshotListTask;
import org.apache.ignite.internal.processors.rollingupgrade.AbstractRollingUpgradeTest;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RU_PREPARE_VERSION_FINALIZATION;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class IgniteClusterSnapshotListRollingUpgradeTest extends AbstractRollingUpgradeTest {
    /** */
    private static final int ALL_GRIDS = 4;

    /** */
    private static final int CLIENTS = 1;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        // Clean everything, including the separated working directories.
        try (DirectoryStream<Path> files = newDirectoryStream(Paths.get(U.defaultWorkDirectory()))) {
            for (Path path : files)
                U.delete(path);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, String ver) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName, ver);

        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(dataRegionCfg));

        cfg.setWorkDirectory(new File(U.defaultWorkDirectory(), igniteInstanceName).getAbsolutePath());

        return cfg;
    }

    /** */
    @Test
    public void testParallelRollingUpgradeInProgress() throws Exception {
        for (int i = 0; i < ALL_GRIDS; i++)
            startGrid(i, "2.18.0", i >= ALL_GRIDS - CLIENTS);

        grid(0).cluster().active(true);

        int testNodeIx = ALL_GRIDS - CLIENTS - 1;

        createCacheAndSnapshot(testNodeIx);

        ru(grid(testNodeIx)).enableVersionUpgrade();

        for (int i = 0; i < ALL_GRIDS; i++) {
            assertTrue(ru(grid(i)).isVersionUpgradeEnabled());

            upgradeNodeVersion(i, "2.18.0", MASTER_VER_REF);
        }

        spi(grid(testNodeIx)).blockMessages((node, msg) -> msg instanceof SingleNodeMessage<?> snm &&
            snm.type() == RU_PREPARE_VERSION_FINALIZATION.ordinal());

        IgniteInternalFuture<?> finalizeFut = GridTestUtils.runAsync(() -> ru(testNodeIx).finalizeClusterVersion());

        assertTrue(spi(grid(testNodeIx)).waitForBlocked(1, getTestTimeout()));

        ensureSnapshotListCommandFails();

        spi(grid(testNodeIx)).stopBlock();

        assertFalse(spi(grid(testNodeIx)).hasBlockedMessages());

        finalizeFut.get(getTestTimeout());

        for (int i = 0; i < ALL_GRIDS; i++) {
            int i0 = i;

            assertTrue(waitForCondition(() -> !ru(grid(i0)).isVersionUpgradeEnabled(), getTestTimeout()));
        }

        ensureSnapshotListSucceeds();
    }

    /** */
    @Test
    public void testSnapshotListFeature() throws Exception {
        for (int i = 0; i < ALL_GRIDS; i++)
            startGrid(i, "2.18.0", i >= ALL_GRIDS - CLIENTS);

        grid(0).cluster().active(true);

        createCacheAndSnapshot(1);

        ensureSnapshotListCommandFails();

        ru(grid(0)).enableVersionUpgrade();

        for (int i = 0; i < ALL_GRIDS; i++) {
            assertTrue(ru(grid(i)).isVersionUpgradeEnabled());

            upgradeNodeVersion(i, "2.18.0", MASTER_VER_REF);

            ensureSnapshotListCommandFails();
        }

        ru(grid(1)).finalizeClusterVersion();

        for (int i = 0; i < ALL_GRIDS; i++) {
            int i0 = i;

            assertTrue(waitForCondition(() -> !ru(grid(i0)).isVersionUpgradeEnabled(), getTestTimeout()));
        }

        ensureSnapshotListSucceeds();
    }

    /** */
    private void ensureSnapshotListSucceeds() throws Exception {
        for (int i = 0; i < ALL_GRIDS; i++) {
            for (int k = 0; k < ALL_GRIDS; k++) {
                SnapshotListTaskResult listOpRes = launchSnapshotsListCommand(i, grid(k).localNode().id());

                assertEquals(3, listOpRes.nodesSnapshots().length);

                // Tests client exclusion.
                if (CLIENTS > 0) {
                    UUID cliId = grid(ALL_GRIDS - CLIENTS).cluster().localNode().id();

                    assertFalse(new HashSet<>(Arrays.asList(listOpRes.nodesIds())).contains(cliId));
                }

                for (SnapshotListJobResult nodeRes : listOpRes.nodesSnapshots())
                    assertEquals(1, nodeRes.snapshots().size());
            }
        }
    }

    /** */
    private void createCacheAndSnapshot(int gridIdx) {
        int partsCnt = 4;
        int keysCnt = partsCnt * 5;

        grid(gridIdx).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.REPLICATED)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(partsCnt))
            .setAtomicityMode(CacheAtomicityMode.ATOMIC));

        try (IgniteDataStreamer<Integer, Integer> ds = grid(gridIdx).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < keysCnt; i++)
                ds.addData(i, i);
        }

        grid(gridIdx).context().cache().context().snapshotMgr().createSnapshot("testSnapshot").get(getTestTimeout());
    }

    /** */
    private void ensureSnapshotListCommandFails() {
        for (int i = 0; i < ALL_GRIDS; i++) {
            for (int k = 0; k < ALL_GRIDS; k++) {
                int i0 = i;
                int k0 = k;

                assertThrowsAnyCause(
                    null,
                    () -> launchSnapshotsListCommand(i0, grid(k0).localNode().id()),
                    IgniteException.class,
                    SnapshotListTask.FEATURE_NOT_ACTIVE_ERR_MSG
                );
            }
        }
    }

    /** */
    private SnapshotListTaskResult launchSnapshotsListCommand(int gridFromIdx, UUID targetGrid) throws Exception {
        return grid(gridFromIdx).compute().execute(
            SnapshotListTask.class,
            new VisorTaskArgument<>(Collections.singleton(targetGrid), new SnapshotListCommandArg(), false)
        ).result();
    }
}
