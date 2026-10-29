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
import java.io.Serializable;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.snapshot.SnapshotStatusTask;
import org.apache.ignite.internal.management.snapshot.SnapshotStatusTask.SnapshotOperation;
import org.apache.ignite.internal.processors.rollingupgrade.AbstractRollingUpgradeTest;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.VisorTaskResult;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.rollingupgrade.feature.ManagementApiVersionValidationTest.withCoreVersion;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CHECK_SNAPSHOT_METAS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.END_SNAPSHOT;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RU_PREPARE_VERSION_FINALIZATION;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class IgniteClusterSnapshotCheckStatusRollingUpgradeTest extends AbstractRollingUpgradeTest {
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
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

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
    public void testClientOfThePreviousVersion() throws Exception {
        doTestClientOfVersion(MASTER_VER_REF, "2.18.0");
    }

    /** */
    @Test
    public void testClientOfTheNextVersion() throws Exception {
        doTestClientOfVersion("2.18.0", MASTER_VER_REF);
    }

    /** */
    @Test
    public void testClientOfTheNextVersionWithCreateInProgress() throws Exception {
        for (int i = 0; i < ALL_GRIDS - CLIENTS; i++)
            startGrid(i, "2.18.0", false);

        grid(0).cluster().state(ClusterState.ACTIVE);

        createCache(0);

        // The snapshot to check. It must be created before the messages are blocked.
        createSnapshot(0, "testSnapshotToCheck");

        // Blocks the finalization of the create operation and the check operation to keep them in progress while
        // the status is requested.
        for (int i = 0; i < ALL_GRIDS - CLIENTS; i++) {
            spi(grid(i)).blockMessages((node, msg) ->
                msg instanceof SingleNodeMessage<?> snm &&
                    (snm.type() == END_SNAPSHOT.ordinal() || snm.type() == CHECK_SNAPSHOT_METAS.ordinal()));
        }

        int chkNodeIdx = 1;

        IgniteInternalFuture<SnapshotPartitionsVerifyResult> chkFut = snp(chkNodeIdx).checkSnapshot("testSnapshotToCheck", null);
        IgniteFuture<Void> createFut = snp(0).createSnapshot(SNP_NAME);

        try {
            assertTrue(waitForCondition(
                () -> snp(chkNodeIdx).isSnapshotCreating() && snp(chkNodeIdx).isSnapshotChecking("testSnapshotToCheck"), 10_000L));

            Collection<UUID> nodes = Collections.singleton(nodeId(0));

            Collection<SnapshotStatusTask.SnapshotOperationStatus> statuses =
                snapshotStatusTaskWithClient(new VisorTaskArgument<>(nodes, new NoArg(), false));

            // The check operation is not reported by the cluster of the previous version.
            assertEquals(1, statuses.size());

            SnapshotStatusTask.SnapshotOperationStatus status = F.first(statuses);

            assertEquals(SnapshotOperation.CREATE, status.operation());
            assertEquals(SNP_NAME, status.name());
            assertEquals(ALL_GRIDS - CLIENTS, status.progress().size());
        }
        finally {
            for (int i = 0; i < ALL_GRIDS - CLIENTS; i++)
                spi(grid(i)).stopBlock();
        }

        createFut.get(10_000L);

        // Ensure that the snapshot check process correctly finished too.
        SnapshotPartitionsVerifyResult chkRes = chkFut.get(getTestTimeout());

        assertTrue(F.isEmpty(chkRes.exceptions()));
        assertTrue(F.isEmpty(chkRes.idleVerifyResult().exceptions()));
        assertFalse(chkRes.idleVerifyResult().hasConflicts());
    }

    /** */
    private void doTestClientOfVersion(String clusterVer, String clientVer) throws Exception {
        for (int i = 0; i < ALL_GRIDS - CLIENTS; i++)
            startGrid(i, clusterVer, false);

        grid(0).cluster().state(ClusterState.ACTIVE);

        createCacheAndSnapshot(0);

        Collection<UUID> nodes = Collections.singleton(nodeId(0));

        VisorTaskArgument<NoArg> vtArg = withCoreVersion(clientVer, () -> new VisorTaskArgument<>(nodes, new NoArg(), false));

        validateSnapshotCheckStatusFeature(false, vtArg);
    }

    /** */
    @Test
    public void testParallelRollingUpgradeInProgress() throws Exception {
        for (int i = 0; i < ALL_GRIDS; i++)
            startGrid(i, "2.18.0", i >= ALL_GRIDS - CLIENTS);

        grid(0).cluster().active(true);

        int testNodeIx = ALL_GRIDS - CLIENTS - 1;

        createCacheAndSnapshot(testNodeIx);

        validateSnapshotCheckStatusFeature(false);

        ru(grid(testNodeIx)).enableVersionUpgrade();

        for (int i = 0; i < ALL_GRIDS; i++) {
            assertTrue(ru(grid(i)).isVersionUpgradeEnabled());

            upgradeNodeVersion(i, "2.18.0", MASTER_VER_REF);
        }

        spi(grid(testNodeIx)).blockMessages((node, msg) -> msg instanceof SingleNodeMessage<?> snm &&
            snm.type() == RU_PREPARE_VERSION_FINALIZATION.ordinal());

        IgniteInternalFuture<?> finalizeFut = runAsync(() -> ru(testNodeIx).finalizeClusterVersion());

        assertTrue(spi(grid(testNodeIx)).waitForBlocked(1, getTestTimeout()));

        validateSnapshotCheckStatusFeature(false);

        spi(grid(testNodeIx)).stopBlock();

        assertFalse(spi(grid(testNodeIx)).hasBlockedMessages());

        finalizeFut.get(getTestTimeout());

        for (int i = 0; i < ALL_GRIDS; i++) {
            int i0 = i;

            assertTrue(waitForCondition(() -> !ru(grid(i0)).isVersionUpgradeEnabled(), getTestTimeout()));
        }

        validateSnapshotCheckStatusFeature(true);
    }

    /** */
    @Test
    public void testSnapshotStatusFeature() throws Exception {
        for (int i = 0; i < ALL_GRIDS; i++)
            startGrid(i, "2.18.0", i >= ALL_GRIDS - CLIENTS);

        grid(0).cluster().active(true);

        createCacheAndSnapshot(1);

        validateSnapshotCheckStatusFeature(false);

        ru(grid(0)).enableVersionUpgrade();

        for (int i = 0; i < ALL_GRIDS; i++) {
            assertTrue(ru(grid(i)).isVersionUpgradeEnabled());

            upgradeNodeVersion(i, "2.18.0", MASTER_VER_REF);
        }

        validateSnapshotCheckStatusFeature(false);

        ru(grid(1)).finalizeClusterVersion();

        for (int i = 0; i < ALL_GRIDS; i++) {
            int i0 = i;

            assertTrue(waitForCondition(() -> !ru(grid(i0)).isVersionUpgradeEnabled(), getTestTimeout()));
        }

        validateSnapshotCheckStatusFeature(true);
    }

    /** */
    private void createCacheAndSnapshot(int gridIdx) {
        createCache(gridIdx);

        createSnapshot(gridIdx);
    }

    /** Creates a replicated cache and fills it with data. */
    private void createCache(int gridIdx) {
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
    }

    /** */
    private void createSnapshot(int gridIdx) {
        createSnapshot(gridIdx, SNP_NAME);
    }

    /** */
    private void createSnapshot(int gridIdx, String snpName) {
        snp(gridIdx).createSnapshot(snpName).get(getTestTimeout());
    }

    /** @return {@code True} if the create operation of the snapshot {@code snpName} is in progress on all the server nodes. */
    private boolean createInProgress(String snpName) {
        for (int i = 0; i < ALL_GRIDS - CLIENTS; i++) {
            SnapshotOperationRequest req = snp(i).currentCreateRequest();

            if (req == null || !snpName.equals(req.snapshotName()))
                return false;
        }

        return true;
    }

    /** */
    private void validateSnapshotCheckStatusFeature(boolean supports, @Nullable VisorTaskArgument<NoArg> clientArg) throws Exception {
        int blockNodeIdx = 1;

        spi(grid(blockNodeIdx)).blockMessages((node, msg) -> msg instanceof SingleNodeMessage<?> snm &&
            snm.type() == CHECK_SNAPSHOT_METAS.ordinal());

        IgniteInternalFuture<SnapshotPartitionsVerifyResult> chkFut = snp(1).checkSnapshot(SNP_NAME, null);

        assertTrue(waitForCondition(() -> spi(grid(blockNodeIdx)).hasBlockedMessages(), getTestTimeout()));

        if (clientArg != null) {
            Serializable rawRes = snapshotStatusTaskWithClient(clientArg);

            if (supports) {
                assertTrue(rawRes instanceof Collection<?> cll && !cll.isEmpty() &&
                    F.first(cll) instanceof SnapshotStatusTask.SnapshotOperationStatus);
            }
            else
                assertTrue(rawRes == null);
        }
        else {
            for (Ignite ig : G.allGrids()) {
                Serializable rawRes = snapshotStatusTaskWithGrid(ig);

                if (supports) {
                    assertTrue(rawRes instanceof Collection<?> cll && !cll.isEmpty() &&
                        F.first(cll) instanceof SnapshotStatusTask.SnapshotOperationStatus);
                }
                else
                    assertTrue(rawRes == null);
            }
        }

        spi(grid(blockNodeIdx)).stopBlock();

        assertTrue(waitForCondition(() -> !spi(grid(blockNodeIdx)).hasBlockedMessages(), getTestTimeout()));

        // Ensure that the snapshot check process correctly finished too.
        SnapshotPartitionsVerifyResult chkRes = chkFut.get(getTestTimeout());

        assertTrue(F.isEmpty(chkRes.exceptions()));
        assertFalse(F.isEmpty(chkRes.metas()));
        assertTrue(F.isEmpty(chkRes.idleVerifyResult().exceptions()));
        assertFalse(chkRes.idleVerifyResult().hasConflicts());
    }

    /** */
    private void validateSnapshotCheckStatusFeature(boolean supports) throws Exception {
        validateSnapshotCheckStatusFeature(supports, null);
    }

    /** */
    private IgniteSnapshotManager snp(int gridIdx) {
        return grid(gridIdx).context().cache().context().snapshotMgr();
    }

    /** */
    private static <T extends Serializable> T snapshotStatusTaskWithGrid(Ignite grid) throws Exception {
        Collection<UUID> nodes = grid.cluster().nodes().stream().map(ClusterNode::id).toList();

        VisorTaskArgument<NoArg> vtArg = new VisorTaskArgument<>(nodes, new NoArg(), false);

        return (T)grid.compute().execute(SnapshotStatusTask.class, vtArg).result();
    }

    /** */
    private <T> T snapshotStatusTaskWithClient(VisorTaskArgument<NoArg> vtArg) throws Exception {
        String taskName = SnapshotStatusTask.class.getName();

        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
            VisorTaskResult<T> vtRes = cli.compute().execute(taskName, vtArg);

            return vtRes.result();
        }
    }
}
