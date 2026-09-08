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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.FullMessage;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CHECK_SNAPSHOT_METAS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.DELETE_SNAPSHOT;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.START_SNAPSHOT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Cluster-wide snapshot delete procedure tests. In addition to the concurrent snapshot delete tests, where the
 * operations are paired with the snapshot create, check and restore procedures to verify that concurrent snapshot
 * operations are correctly rejected (or allowed) when a delete operation is in progress, this class also contains the
 * basic snapshot delete functionality tests.
 */
public class IgniteClusterSnapshotDeleteTest extends AbstractSnapshotSelfTest {
    /** Cache partitions count. */
    private static final int CACHE_PARTS_CNT = 32;

    /** Tests the basic cluster-wide snapshot delete functionality. */
    @Test
    public void testClusterSnapshotDelete() throws Exception {
        IgniteEx ignite = prepareGridsAndSnapshot(3, 2, 2, false);

        // Sanity check: the snapshot exists on the cluster.
        assertNotNull(
            "Snapshot must be available on the cluster",
            snp(ignite).checkSnapshot(SNAPSHOT_NAME, null).get().idleVerifyResult()
        );

        snp(ignite).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

        // The snapshot must not be present on any of the cluster nodes.
        for (Ignite node : G.allGrids()) {
            assertTrue(
                "Snapshot must be deleted on node " + node.name(),
                snp((IgniteEx)node).localSnapshotNames(null).isEmpty()
            );
        }

        // The check procedure must report that the snapshot does not exist anymore.
        assertThrowsAnyCause(
            log,
            () -> {
                snp(ignite).checkSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

                return null;
            },
            IllegalArgumentException.class,
            "Snapshot does not exists"
        );
    }

    /** Tests that a snapshot delete is declined when a snapshot create operation is in progress. */
    @Test
    public void testSnapshotDeleteWhenCreateInProgress() throws Exception {
        prepareGridsAndSnapshot(3, 2, 2, false);

        doTestConcurrentSnpDeleteOperation(
            () -> snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary),
            START_SNAPSHOT,
            () -> {
                assertThrowsAnyCause(
                    log,
                    () -> {
                        snp(grid(1)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

                        return null;
                    },
                    ClusterTopologyCheckedException.class,
                    "Snapshot deletion was rejected. a snapshot operation is in progress"
                );

                return null;
            }
        );
    }

    /** Tests that a snapshot create is declined when a snapshot delete operation is in progress. */
    @Test
    public void testSnapshotCreateWhenDeleteInProgress() throws Exception {
        prepareGridsAndSnapshot(3, 2, 2, false);

        doTestConcurrentSnpDeleteOperation(
            () -> snp(grid(0)).deleteSnapshot(SNAPSHOT_NAME, null),
            DELETE_SNAPSHOT,
            () -> {
                assertThrowsAnyCause(
                    log,
                    () -> {
                        snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary);

                        return null;
                    },
                    IgniteException.class,
                    "Snapshot delete operation is currently in progress"
                );

                return null;
            }
        );
    }

    /** Tests that a snapshot delete is declined when a snapshot check operation is in progress. */
    @Test
    public void testSnapshotDeleteWhenCheckInProgress() throws Exception {
        prepareGridsAndSnapshot(3, 2, 2, false);

        doTestConcurrentSnpDeleteOperation(
            () -> new IgniteFutureImpl<>(snp(grid(0)).checkSnapshot(SNAPSHOT_NAME, null)),
            CHECK_SNAPSHOT_METAS,
            () -> {
                assertThrowsAnyCause(
                    log,
                    () -> {
                        snp(grid(1)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

                        return null;
                    },
                    ClusterTopologyCheckedException.class,
                    "Snapshot deletion was rejected. the snapshot is being checked"
                );

                return null;
            }
        );
    }

    /** Tests that a snapshot check is not declined when a snapshot delete operation is in progress. */
    @Test
    public void testSnapshotCheckWhenDeleteInProgress() throws Exception {
        prepareGridsAndSnapshot(3, 2, 2, false);

        doTestConcurrentSnpDeleteOperation(
            () -> snp(grid(0)).deleteSnapshot(SNAPSHOT_NAME, null),
            DELETE_SNAPSHOT,
            () -> {
                new IgniteFutureImpl<>(snp(grid(1)).checkSnapshot(SNAPSHOT_NAME, null)).get(getTestTimeout());

                return null;
            }
        );
    }

    /** Tests that a snapshot delete is declined when a snapshot restore operation is in progress. */
    @Test
    public void testSnapshotDeleteWhenRestoreInProgress() throws Exception {
        prepareGridsAndSnapshot(3, 2, 2, true);

        doTestConcurrentSnpDeleteOperation(
            () -> snp(grid(0)).restoreSnapshot(SNAPSHOT_NAME, null, null, 0, true),
            RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE,
            () -> {
                assertThrowsAnyCause(
                    log,
                    () -> {
                        snp(grid(1)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

                        return null;
                    },
                    ClusterTopologyCheckedException.class,
                    "Snapshot deletion was rejected. the snapshot is being restored"
                );

                return null;
            }
        );
    }

    /** Tests that a snapshot restore is declined when a snapshot delete operation is in progress. */
    @Test
    public void testSnapshotRestoreWhenDeleteInProgress() throws Exception {
        prepareGridsAndSnapshot(3, 2, 2, true);

        doTestConcurrentSnpDeleteOperation(
            () -> snp(grid(0)).deleteSnapshot(SNAPSHOT_NAME, null),
            DELETE_SNAPSHOT,
            () -> {
                assertThrowsAnyCause(
                    log,
                    () -> {
                        snp(grid(0)).restoreSnapshot(SNAPSHOT_NAME, null, null, 0, true).get(getTestTimeout());

                        return null;
                    },
                    IgniteException.class,
                    "A snapshot delete operation is in progress"
                );

                return null;
            }
        );
    }

    /**
     * Tests the concurrent snapshot delete procedure against another snapshot operation.
     *
     * <p>The {@code originatorOp} is blocked on the coordinator discovery, so it is kept in-progress while the
     * {@code trierStep} is executed and its conflict behavior is asserted.
     *
     * @param originatorOp First snapshot operation on the coordinator node to keep in-progress.
     * @param firstDelay First distributed process full message of {@code originatorOp} to delay on the coordinator
     *                   to launch {@code trierStep}.
     * @param trierStep Second concurrent snapshot operation with asserted result.
     */
    private void doTestConcurrentSnpDeleteOperation(
        Supplier<IgniteFuture<?>> originatorOp,
        DistributedProcess.DistributedProcessType firstDelay,
        Callable<?> trierStep
    ) throws Exception {
        try {
            AtomicBoolean firstDelayed = new AtomicBoolean();

            // Block only the first matching message so the originator operation stays in-progress.
            discoSpi(grid(0)).block(
                msg -> msg instanceof FullMessage
                    && ((FullMessage<?>)msg).type() == firstDelay.ordinal()
                    && firstDelayed.compareAndSet(false, true));

            IgniteFuture<?> fut = originatorOp.get();

            discoSpi(grid(0)).waitBlocked(getTestTimeout());

            if (trierStep != null)
                trierStep.call();

            discoSpi(grid(0)).unblock();

            fut.get(getTestTimeout());
        }
        finally {
            discoSpi(grid(0)).unblock();

            awaitPartitionMapExchange();
        }
    }

    /**
     * @param servers Number of server nodes.
     * @param baseLineCnt Number of baseline nodes.
     * @param clients Number of client nodes.
     * @param removeTheCache If {@code true}, the cache is destroyed after the snapshot is created (restore scenario).
     * @return The last started (server) grid, which the snapshot was created on.
     */
    private IgniteEx prepareGridsAndSnapshot(int servers, int baseLineCnt, int clients, boolean removeTheCache) throws Exception {
        assert baseLineCnt > 0 && baseLineCnt <= servers;

        IgniteEx ignite = null;

        for (int i = 0; i < servers + clients; ++i) {
            IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(i));

            if (i >= servers)
                cfg.setClientMode(true);

            ignite = startGrid(cfg);

            if (i == baseLineCnt - 1) {
                ignite.cluster().state(ACTIVE);

                ignite.cluster().setBaselineTopology(ignite.cluster().topologyVersion());
            }
        }

        try (IgniteDataStreamer<Integer, Integer> ds = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            // Ensure all the partitions are created: several records per partition.
            for (int i = 0; i < CACHE_PARTS_CNT * 4; ++i)
                ds.addData(i, i);
        }

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        if (removeTheCache)
            ignite.destroyCache(DEFAULT_CACHE_NAME);

        return ignite;
    }
}
