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
import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
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

    /** */
    private static final String SNP_NAME = "testSnapshot";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();

        // Clean all: also separated snapshot working directories.
        try (DirectoryStream<Path> files = newDirectoryStream(Paths.get(U.defaultWorkDirectory()))) {
            for (Path path : files)
                U.delete(path);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, String ver) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName, ver);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                )
        );

        cfg.setWorkDirectory(new File(U.defaultWorkDirectory(), igniteInstanceName).getAbsolutePath());

        return cfg;
    }

    /** */
    @Test
    public void testParallelRollingUpgradeInProgress() throws Exception {
        for (int i = 0; i < ALL_GRIDS; i++)
            startGrid(i, "2.19.4", i >= ALL_GRIDS - CLIENTS);

        grid(0).cluster().active(true);

        int testNodeIx = ALL_GRIDS - CLIENTS - 1;

        createCacheAndSnapshot(testNodeIx);

        ru(grid(testNodeIx)).enableVersionUpgrade();

        for (int i = 0; i < ALL_GRIDS; i++) {
            assertTrue(ru(grid(i)).isVersionUpgradeEnabled());

            upgradeNodeVersion(i, "2.19.4", "2.19.5");
        }

        spi(grid(testNodeIx)).blockMessages((node, msg) -> msg instanceof SingleNodeMessage<?> snm &&
            snm.type() == RU_PREPARE_VERSION_FINALIZATION.ordinal());

        IgniteInternalFuture<?> finalizeFut = GridTestUtils.runAsync(() -> ru(testNodeIx).finalizeClusterVersion());

        assertTrue(spi(grid(testNodeIx)).waitForBlocked(1, getTestTimeout()));

        ensureSnapshotListFailed();

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
            startGrid(i, "2.19.4", i >= ALL_GRIDS - CLIENTS);

        grid(0).cluster().active(true);

        createCacheAndSnapshot(1);

        ensureSnapshotListFailed();

        ru(grid(0)).enableVersionUpgrade();

        for (int i = 0; i < ALL_GRIDS; i++) {
            assertTrue(ru(grid(i)).isVersionUpgradeEnabled());

            upgradeNodeVersion(i, "2.19.4", "2.19.5");

            ensureSnapshotListFailed();
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
        for (int g = 0; g < ALL_GRIDS; g++) {
            SnapshotListTaskResult listOpRes = listSnapshots(grid(g));

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

        createSnapshot(gridIdx);
    }

    /** */
    private void createSnapshot(int gridIdx) {
        snp(gridIdx).createSnapshot(SNP_NAME).get(getTestTimeout());
    }

    /** */
    private void ensureSnapshotListFailed() {
        String err = "The snapshot list feature isn't activated yet";

        for (int i = 0; i < ALL_GRIDS; i++) {
            int i0 = i;

            assertThrowsAnyCause(
                null,
                () -> listSnapshots(grid(i0)),
                IgniteException.class,
                err
            );
        }
    }

    /** */
    private IgniteSnapshotManager snp(int gridIdx) {
        return grid(gridIdx).context().cache().context().snapshotMgr();
    }

    /** */
    private static SnapshotListTaskResult listSnapshots(IgniteEx grid) throws Exception {
        SnapshotListCommandArg arg = new SnapshotListCommandArg();

        Collection<UUID> nodes = grid.cluster().forServers().nodes().stream().map(ClusterNode::id).toList();

        return grid.compute().execute(SnapshotListTask.class, new VisorTaskArgument<>(nodes, arg, false)).result();
    }
}
