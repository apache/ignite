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

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CHECK_SNAPSHOT_METAS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CHECK_SNAPSHOT_PARTS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.END_SNAPSHOT;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_START;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.START_SNAPSHOT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/** */
public class IgniteClusterSnapshotDeleteTest extends AbstractSnapshotSelfTest {
    /** {@inheritDoc} */
    @Override public void afterTestSnapshot() throws Exception {
        super.afterTestSnapshot();

        G.allGrids();

        cleanPersistenceDir();
    }

    /** Tests that a snapshot deletion is declined when a snapshot check operation is in progress. */
    @Test
    public void testSnapshotDeleteWhenCheckInProgress() throws Exception {
        doTestConcurrentSnapshotDelete(
            F.asList(CHECK_SNAPSHOT_METAS, CHECK_SNAPSHOT_PARTS),
            null,
            () -> new IgniteFutureImpl<>(snp(grid(2)).checkSnapshot(SNAPSHOT_NAME, null)),
            "Snapshot with this name is being checked",
            true
        );
    }

    /** Tests that a snapshot deletion is declined when a snapshot create operation is in progress. */
    @Test
    public void testSnapshotDeleteWhenCreateInProgress() throws Exception {
        doTestConcurrentSnapshotDelete(
            F.asList(START_SNAPSHOT, END_SNAPSHOT),
            () -> snp(grid(0)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout()),
            () -> snp(grid(2)).createSnapshot(SNAPSHOT_NAME),
            "Snapshot with this name is being created",
            false
        );
    }

    /** Tests that a snapshot deletion is declined when a snapshot restore begins. */
    @Test
    public void testSnapshotDeleteWhenRestoreBegins() throws Exception {
        doTestConcurrentSnapshotDelete(
            F.asList(CHECK_SNAPSHOT_METAS, CHECK_SNAPSHOT_PARTS),
            () -> {
                grid(0).destroyCache(DEFAULT_CACHE_NAME);

                try {
                    awaitPartitionMapExchange();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted.", e);
                }
            },
            () -> snp(grid(2)).restoreSnapshot(SNAPSHOT_NAME, null),
            "Snapshot with this name is being checked",
            true
        );
    }

    /** Tests that a snapshot deletion is declined when a snapshot restore is in progress. */
    @Test
    public void testSnapshotDeleteWhenRestoreInProgress() throws Exception {
        var restoreMsgs = F.asList(
            RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE,
            RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD,
            RESTORE_CACHE_GROUP_SNAPSHOT_START
        );

        doTestConcurrentSnapshotDelete(
            restoreMsgs,
            () -> {
                grid(0).destroyCache(DEFAULT_CACHE_NAME);

                try {
                    awaitPartitionMapExchange();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted.", e);
                }
            },
            () -> snp(grid(2)).restoreSnapshot(SNAPSHOT_NAME, null),
            "Snapshot with this name is being restored",
            true
        );
    }

    /** */
    protected void doTestConcurrentSnapshotDelete(
        Collection<DistributedProcess.DistributedProcessType> msgsToWatch,
        @Nullable Runnable prepareIteration,
        Supplier<IgniteFuture<?>> firstOp,
        String concurrentMsgErr,
        boolean precreateSnp
    ) throws Exception {
        if (precreateSnp)
            startGridsWithSnapshot(3, CACHE_KEYS_RANGE, false, true);
        else
            startGridsWithCache(3, CACHE_KEYS_RANGE, i -> i);

        TestRecordingCommunicationSpi commSpi1 = (TestRecordingCommunicationSpi)grid(1).configuration().getCommunicationSpi();

        for (var nodeResMsgType : msgsToWatch) {
            if (prepareIteration != null)
                prepareIteration.run();

            commSpi1.blockMessages((node, msg) ->
                msg instanceof SingleNodeMessage<?> msg0 && msg0.type() == nodeResMsgType.ordinal());

            var checkFut = firstOp.get();

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

            checkFut.get(getTestTimeout());
        }
    }
}
