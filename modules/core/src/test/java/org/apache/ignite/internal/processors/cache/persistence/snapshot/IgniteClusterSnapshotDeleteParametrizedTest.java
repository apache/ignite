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

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CHECK_SNAPSHOT_METAS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CHECK_SNAPSHOT_PARTS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.END_SNAPSHOT;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_START;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_INCREMENTAL_SNAPSHOT_START;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.START_SNAPSHOT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.junit.Assume.assumeTrue;

/** */
@RunWith(Parameterized.class)
public class IgniteClusterSnapshotDeleteParametrizedTest extends AbstractSnapshotSelfTest {
    /** */
    @Parameter(2)
    public boolean incremental = true;

    /** Parameters. */
    @Parameterized.Parameters(name = "encryption={0}, onlyPrimay={1}, incremental={2}")
    public static Collection<?> runParams() {
        Collection<Object[]> res = new ArrayList<>();

        for (boolean incremental : F.asList(false, true)) {
            for (Object[] src0 : params()) {
                Object[] res0 = new Object[src0.length + 1];
                System.arraycopy(src0, 0, res0, 0, src0.length);

                res0[src0.length] = incremental;

                res.add(res0);
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void afterTestSnapshot() throws Exception {
        super.afterTestSnapshot();

        G.allGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override public void beforeTestSnapshot() throws Exception {
        super.beforeTestSnapshot();

        // Handy
        cleanPersistenceDir();
    }

    /** Tests that a snapshot deletion is declined when a snapshot check operation is in progress. */
    @Test
    public void testSnapshotDeleteWhenCheckInProgress() throws Exception {
        // Incremental snapshots don't support encription.
        assumeTrue(!incremental || !encryption);

        doTestConcurrentSnapshotDelete(
            () -> new IgniteFutureImpl<>(snp(grid(2)).checkSnapshot(SNAPSHOT_NAME, null, incremental ? 1 : 0)),
            F.asList(CHECK_SNAPSHOT_METAS, CHECK_SNAPSHOT_PARTS),
            true,
            null,
            "Snapshot with this name is being checked"
        );
    }

    /** Tests that a snapshot deletion is declined when a snapshot create operation is in progress. */
    @Test
    public void testSnapshotDeleteWhenCreateInProgress() throws Exception {
        // Incremental snapshots don't support encription and only-primary mode.
        assumeTrue(!incremental || !(encryption || onlyPrimary));

        doTestConcurrentSnapshotDelete(
            () -> snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, incremental, onlyPrimary),
            F.asList(START_SNAPSHOT, END_SNAPSHOT),
            false,
            () -> {
                snp(grid(0)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

                if (incremental)
                    snp(grid(0)).createSnapshot(SNAPSHOT_NAME).get(getTestTimeout());
            },
            "Snapshot with this name is being created"
        );
    }

    /** Tests that a snapshot deletion is declined when a snapshot restore begins. */
    @Test
    public void testSnapshotDeleteWhenRestoreBegins() throws Exception {
        // Incremental snapshots don't support encription.
        assumeTrue(!incremental || !encryption);

        doTestConcurrentSnapshotDelete(
            () -> {
                if (incremental)
                    return snp(grid(2)).restoreSnapshot(SNAPSHOT_NAME, null, 1);
                else
                    return snp(grid(2)).restoreSnapshot(SNAPSHOT_NAME, null);
            },
            F.asList(CHECK_SNAPSHOT_METAS, CHECK_SNAPSHOT_PARTS),
            true,
            () -> {
                grid(0).destroyCache(DEFAULT_CACHE_NAME);

                awaitPartitionMapExchange();
            },
            "Snapshot with this name is being checked"
        );
    }

    /** Tests that a snapshot deletion is declined when a snapshot restore is in progress. */
    @Test
    public void testSnapshotDeleteWhenRestoreInProgress() throws Exception {
        // Incremental snapshots don't support encription.
        assumeTrue(!incremental || !encryption);

        var restoreMsgs = F.asList(
            RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE,
            RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD,
            RESTORE_CACHE_GROUP_SNAPSHOT_START
        );

        if (incremental) {
            restoreMsgs = new ArrayList<>(restoreMsgs);
            restoreMsgs.add(RESTORE_INCREMENTAL_SNAPSHOT_START);
        }

        doTestConcurrentSnapshotDelete(
            () -> {
                if (incremental)
                    return snp(grid(2)).restoreSnapshot(SNAPSHOT_NAME, null, 1);
                else
                    return snp(grid(2)).restoreSnapshot(SNAPSHOT_NAME, null);
            },
            restoreMsgs,
            true,
            () -> {
                grid(0).destroyCache(DEFAULT_CACHE_NAME);

                awaitPartitionMapExchange();
            },
            "Snapshot with this name is being restored"
        );
    }

    /**
     * @param firstOp First cluster-wide snapshot operation.
     * @param msgsToWatch {@link SingleNodeMessage#type()} relating to {@code firstOp} bo block on one node.
     * @param precreateSnp If {@code true}, creates snapshot after the cluster start.
     * @param prepareIteration If not {@code null}, is invoked in the beggining of test iteration at each {@code msgsToWatch}.
     * @param concurrentMsgErr Test of failed concurrent to {@code firstOp} delete snapshot operation to watch.
     */
    protected void doTestConcurrentSnapshotDelete(
        Supplier<IgniteFuture<?>> firstOp,
        Collection<DistributedProcess.DistributedProcessType> msgsToWatch,
        boolean precreateSnp,
        @Nullable Runnable prepareIteration,
        String concurrentMsgErr
    ) throws Exception {
        startGridsWithCache(3, CACHE_KEYS_RANGE, i -> i, dfltCacheCfg);

        if (precreateSnp) {
            snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary).get(TIMEOUT);

            if (incremental)
                addIncrementalSnapshot();
        }

        TestRecordingCommunicationSpi commSpi1 = (TestRecordingCommunicationSpi)grid(1).configuration().getCommunicationSpi();

        for (var nodeResMsgType : msgsToWatch) {
            if (prepareIteration != null)
                prepareIteration.run();

            commSpi1.blockMessages((node, msg) ->
                msg instanceof SingleNodeMessage<?> msg0 && msg0.type() == nodeResMsgType.ordinal());

            var firstFut = firstOp.get();

            commSpi1.waitForBlocked(1, getTestTimeout());

            assertThrowsAnyCause(
                null,
                () -> {
                    snp(grid(1)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

                    return null;
                },
                IgniteIllegalStateException.class,
                concurrentMsgErr
            );

            commSpi1.stopBlock();

            firstFut.get(getTestTimeout());
        }
    }

    /** */
    private void addIncrementalSnapshot() {
        try (var streamer = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = CACHE_KEYS_RANGE; i < CACHE_KEYS_RANGE + CACHE_KEYS_RANGE / 4; ++i)
                streamer.addData(i, i);
        }

        snp(grid(0)).createIncrementalSnapshot(SNAPSHOT_NAME).get(getTestTimeout());
    }

    /** {@inheritDoc} */
    @Override protected void awaitPartitionMapExchange() {
        try {
            super.awaitPartitionMapExchange();
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Interrupted.", e);
        }
    }
}
