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
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CHECK_SNAPSHOT_METAS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CHECK_SNAPSHOT_PARTS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.DELETE_SNAPSHOT;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.END_SNAPSHOT;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_ROLLBACK;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_START;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_INCREMENTAL_SNAPSHOT_START;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.START_SNAPSHOT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/** */
@RunWith(Parameterized.class)
public class IgniteClusterSnapshotDeleteTest extends AbstractSnapshotSelfTest {
    /** */
    private boolean separatedWorkDir;

    /** */
    @Parameter(2)
    public boolean incremental = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        var cfg = super.getConfiguration(igniteInstanceName);

        if (separatedWorkDir)
            cfg.setWorkDirectory(new File(U.defaultWorkDirectory(), igniteInstanceName).getAbsolutePath());

        return cfg;
    }

    /** Parameters. */
    @Parameterized.Parameters(name = "encryption={0}, onlyPrimary={1}, incremental={2}")
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

        /** Handy if test running is interrupted and {@link #afterTestSnapshot()} isn't invoked. */
        cleanPersistenceDir();
    }

    /** Tests snapshot deletion when one node finds snapshot but fails to delete its data. */
    @Test
    public void testUncompletedNodes() throws Exception {
        separatedWorkDir = true;

        // Simulates a deletion error on some node.
        pluginProvider = new AbstractTestPluginProvider() {
            @Override public String name() {
                return "TestSnpMgrProvider";
            }

            @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
                if (IgniteSnapshotManager.class.isAssignableFrom(cls)) {
                    return (T)new IgniteSnapshotManager(((IgniteEx)ctx.grid()).context()) {
                        @Override public boolean deleteLocalSnapshot(SnapshotFileTree sft, @Nullable AtomicBoolean existsFlag) {
                            if (ctx.localNode().id().equals(grid(1).localNode().id())) {
                                existsFlag.set(true);

                                return false;
                            }

                            return super.deleteLocalSnapshot(sft, existsFlag);
                        }
                    };
                }

                return super.createComponent(ctx, cls);
            }
        };

        startGridsWithCache(3, CACHE_KEYS_RANGE, i -> i, dfltCacheCfg);

        snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary).get(getTestTimeout());

        if (incremental)
            addIncrementalSnapshot(null);

        var delSnpRes = snp(grid(1)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

        assertTrue(F.isEmpty(delSnpRes.emptyNodes));
        assertFalse(F.isEmpty(delSnpRes.uncompletedNodes));
        assertTrue(delSnpRes.uncompletedNodes.contains(grid(1).localNode().id()));
    }

    /** Tests snapshot deletion when one node has no snapshot data. */
    @Test
    public void testEmptyNodes() throws Exception {
        separatedWorkDir = true;

        startGridsWithCache(2, CACHE_KEYS_RANGE, i -> i, dfltCacheCfg);

        snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary).get(getTestTimeout());

        if (incremental)
            addIncrementalSnapshot(null);

        startGrid(G.allGrids().size());

        var delSnpRes = snp(grid(1)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

        assertFalse(F.isEmpty(delSnpRes.emptyNodes));
        assertTrue(delSnpRes.emptyNodes.contains(grid(G.allGrids().size() - 1).localNode().id()));
        assertTrue(F.isEmpty(delSnpRes.uncompletedNodes));
    }

    /** Tests snapshot deletion repeat after an offline node restarts. */
    @Test
    public void testDeletionRepeatAfterOfflineNodeStarts() throws Exception {
        separatedWorkDir = true;

        startGridsWithCache(3, CACHE_KEYS_RANGE, i -> i, dfltCacheCfg);

        snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary).get(getTestTimeout());

        if (incremental)
            addIncrementalSnapshot(null);

        int stoppedNodeIdx = G.allGrids().size() - 1;

        UUID stoppedNodeId = grid(stoppedNodeIdx).localNode().id();

        stopGrid(stoppedNodeIdx);

        var delSnpRes = snp(grid(1)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

        assertEquals(2, delSnpRes.completedNodes.size());
        assertFalse(delSnpRes.completedNodes.contains(stoppedNodeId));

        assertTrue(F.isEmpty(delSnpRes.uncompletedNodes));
        assertTrue(F.isEmpty(delSnpRes.emptyNodes));

        startGrid(stoppedNodeIdx);

        stoppedNodeId = grid(stoppedNodeIdx).localNode().id();

        delSnpRes = snp(grid(1)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

        assertEquals(1, delSnpRes.completedNodes.size());
        assertTrue(delSnpRes.completedNodes.contains(stoppedNodeId));

        assertTrue(F.isEmpty(delSnpRes.uncompletedNodes));
        assertEquals(2, delSnpRes.emptyNodes.size());
    }

    /** Test snapshot deletion process when one node leaves. */
    @Test
    public void testNodeStopsInTheMiddle() throws Exception {
        // Incremental snapshots don't support only-primary and encryption mode.
        assumeTrue(!incremental || !(onlyPrimary || encryption));

        separatedWorkDir = true;

        CountDownLatch beginLatch = new CountDownLatch(1);
        CountDownLatch proceedLatch = new CountDownLatch(1);

        // Simulates a deletion error on some node.
        pluginProvider = new AbstractTestPluginProvider() {
            @Override public String name() {
                return "TestSnpMgrProvider";
            }

            @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
                if (IgniteSnapshotManager.class.isAssignableFrom(cls)) {
                    return (T)new IgniteSnapshotManager(((IgniteEx)ctx.grid()).context()) {
                        @Override public boolean deleteLocalSnapshot(SnapshotFileTree sft, @Nullable AtomicBoolean existsFlag) {
                            if (ctx.localNode().id().equals(grid(1).localNode().id())) {
                                beginLatch.countDown();

                                try {
                                    assertTrue(proceedLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));
                                }
                                catch (InterruptedException e) {
                                    throw new RuntimeException("Interrupted.", e);
                                }
                            }

                            return super.deleteLocalSnapshot(sft, existsFlag);
                        }
                    };
                }

                return super.createComponent(ctx, cls);
            }
        };

        startGridsWithCache(3, CACHE_KEYS_RANGE, i -> i, dfltCacheCfg);

        snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary).get(getTestTimeout());

        if (incremental)
            addIncrementalSnapshot(null);

        var delFut = snp(grid(2)).deleteSnapshot(SNAPSHOT_NAME, null);

        assertTrue(beginLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        UUID stoppedGridId = grid(1).localNode().id();

        stopGrid(1);

        proceedLatch.countDown();

        var delRes = delFut.get(getTestTimeout());

        assertEquals(2, delRes.completedNodes.size());
        assertFalse(delRes.completedNodes.contains(stoppedGridId));

        startGrid(1);

        delRes = snp(grid(2)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

        assertEquals(1, delRes.completedNodes.size());
        assertTrue(delRes.completedNodes.contains(grid(1).localNode().id()));
    }

    /** Tests that a concurrent deletion of a snapshot with the same name but different path is allowed. */
    @Test
    public void testConcurrentDeleteOfTheSameSnapshotDifferentPath() throws Exception {
        // Incremental snapshots don't support encryption and only-primary mode.
        assumeTrue(!incremental || !(encryption || onlyPrimary));

        startGridsWithCache(3, CACHE_KEYS_RANGE, i -> i, dfltCacheCfg);

        snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary).get(TIMEOUT);

        if (incremental)
            addIncrementalSnapshot(null);

        String snpPath = new File(U.defaultWorkDirectory(), "ex_snapshots").getAbsolutePath();

        snp(grid(0)).createSnapshot(SNAPSHOT_NAME, snpPath, false, onlyPrimary).get(getTestTimeout());

        if (incremental)
            addIncrementalSnapshot(snpPath);

        TestRecordingCommunicationSpi commSpi1 = (TestRecordingCommunicationSpi)grid(1).configuration().getCommunicationSpi();

        commSpi1.blockMessages((node, msg) ->
            msg instanceof SingleNodeMessage<?> msg0 && msg0.type() == DELETE_SNAPSHOT.ordinal());

        var delFut0 = snp(grid(0)).deleteSnapshot(SNAPSHOT_NAME, null);
        var delFut1 = snp(grid(1)).deleteSnapshot(SNAPSHOT_NAME, snpPath);

        commSpi1.waitForBlocked(2, getTestTimeout());

        commSpi1.stopBlock();

        var delRes0 = delFut0.get(getTestTimeout());
        var delRes1 = delFut1.get(getTestTimeout());

        assertFalse((delRes0.completedNodes().isEmpty()));
        assertFalse(delRes1.completedNodes().isEmpty());
    }

    /** Tests that a concurrent deletion of the same snapshot is declined. */
    @Test
    public void testConcurrentDeleteOfTheSameSnapshot() throws Exception {
        doTestConcurrentSnapshotDeleteOperation(
            () -> startGridsWithSnapshot(3, CACHE_KEYS_RANGE, false),
            () -> snp(grid(1)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout()),
            e -> e.getMessage().contains("Deletion of the snapshot has already started"),
            false
        );
    }

    /** Tests that a snapshot deletion is declined when a snapshot check operation is in progress. */
    @Test
    public void testSnapshotDeleteWhenCheckInProgress() throws Exception {
        // Incremental snapshots don't support encryption.
        assumeTrue(!incremental || !encryption);

        doTestConcurrentSnapshotDelete(
            () -> new IgniteFutureImpl<>(snp(grid(2)).checkSnapshot(SNAPSHOT_NAME, null, incremental ? 1 : 0)),
            F.asList(CHECK_SNAPSHOT_METAS, CHECK_SNAPSHOT_PARTS),
            true,
            null,
            "Snapshot with this name is being checked",
            false
        );
    }

    /** Tests that a snapshot deletion is declined when a snapshot create operation is in progress. */
    @Test
    public void testSnapshotDeleteWhenCreateInProgress() throws Exception {
        // Incremental snapshots don't support encryption and only-primary mode.
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
            "Snapshot with this name is being created",
            false
        );
    }

    /** Tests that a snapshot deletion is declined when a snapshot restore begins. */
    @Test
    public void testSnapshotDeleteWhenRestoreBegins() throws Exception {
        // Incremental snapshots don't support encryption.
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
            "Snapshot with this name is being checked",
            false
        );
    }

    /** Tests that a snapshot deletion is declined when a snapshot restore is in progress. */
    @Test
    public void testSnapshotDeleteWhenRestoreInProgress() throws Exception {
        // Incremental snapshots don't support encryption.
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
            "Snapshot with this name is being restored",
            false
        );
    }

    /** Tests that a snapshot deletion is declined when a snapshot restore is in progress but fails. */
    @Test
    public void testSnapshotDeleteWhenRestoreProgressFails() throws Exception {
        // An in-the-middle failure won't allow to start restoring the incrementals.
        assumeFalse(incremental);

        var restoreMsgs = F.asList(RESTORE_CACHE_GROUP_SNAPSHOT_ROLLBACK);

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

                SnapshotFileTree sft = snapshotFileTree(grid(1), SNAPSHOT_NAME);

                String failingFilePath = sft.partitionFile(dfltCacheCfg, primaries[0]).getAbsolutePath()
                    .replace(sft.nodeStorage().getAbsolutePath(), "");

                grid(1).context().cache().context().snapshotMgr().ioFactory((file, modes) -> {
                    FileIO delegate = new RandomAccessFileIOFactory().create(file, modes);

                    if (file.getPath().endsWith(failingFilePath))
                        throw new RuntimeException("Test exception");

                    return delegate;
                });
            },
            "Snapshot with this name is being restored",
            true
        );
    }

    /**
     * @param firstOp First cluster-wide snapshot operation.
     * @param msgsToWatch {@link SingleNodeMessage#type()} relating to {@code firstOp} to block on one node.
     * @param precreateSnp If {@code true}, creates snapshot after the cluster start.
     * @param prepareIteration If not {@code null}, is invoked in the beginning of test iteration at each {@code msgsToWatch}.
     * @param concurrentMsgErr Test of failed concurrent to {@code firstOp} delete snapshot operation to watch.
     * @param ignoreFirstOpFailure If {@code true}, possible failure of {@code firstOp} is ignored.
     */
    protected void doTestConcurrentSnapshotDelete(
        Supplier<IgniteFuture<?>> firstOp,
        Collection<DistributedProcess.DistributedProcessType> msgsToWatch,
        boolean precreateSnp,
        @Nullable Runnable prepareIteration,
        String concurrentMsgErr,
        boolean ignoreFirstOpFailure
    ) throws Exception {
        startGridsWithCache(3, CACHE_KEYS_RANGE, i -> i, dfltCacheCfg);

        if (precreateSnp) {
            snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary).get(TIMEOUT);

            if (incremental)
                addIncrementalSnapshot(null);
        }

        TestRecordingCommunicationSpi commSpi1 = (TestRecordingCommunicationSpi)grid(1).configuration().getCommunicationSpi();

        for (var nodeResMsgType : msgsToWatch) {
            if (log.isInfoEnabled())
                log.info("Iteration with message-to-wait-for type: " + nodeResMsgType);

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

            if (ignoreFirstOpFailure) {
                try {
                    firstFut.get(getTestTimeout());
                }
                catch (Exception e) {
                    if (log.isDebugEnabled())
                        log.debug("The first operation failed but a failure is expected. Failure: " + e.getMessage());
                }
            }
            else
                firstFut.get(getTestTimeout());
        }
    }

    /** */
    private void addIncrementalSnapshot(@Nullable String path) {
        try (var ds = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = CACHE_KEYS_RANGE; i < CACHE_KEYS_RANGE + CACHE_KEYS_RANGE / 4; i++)
                ds.addData(i, i);
        }

        snp(grid(0)).createSnapshot(SNAPSHOT_NAME, path, true, onlyPrimary).get(getTestTimeout());
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
