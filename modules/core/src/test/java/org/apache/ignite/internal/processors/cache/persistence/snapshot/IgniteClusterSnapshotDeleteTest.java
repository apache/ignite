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
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
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
import org.apache.ignite.internal.util.CommonUtils;
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

    /** */
    private @Nullable String cstIdSuffix;

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
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        var cfg = super.getConfiguration(igniteInstanceName);

        if (separatedWorkDir)
            cfg.setWorkDirectory(new File(U.defaultWorkDirectory(), igniteInstanceName).getAbsolutePath());

        if (cstIdSuffix != null)
            cfg.setConsistentId(cfg.getConsistentId().toString() + '_' + cstIdSuffix);

        return cfg;
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
        // Incremental snapshots don't support only-primary and encryption modes.
        assumeTrue(!incremental || !(onlyPrimary || encryption));

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
        assertTrue(delSnpRes.uncompletedNodes.containsKey(grid(1).localNode().id()));
    }

    /** */
    @Test
    public void testDeleteOtherConsistentId() throws Exception {
        startGridsWithSnapshot(3, CACHE_KEYS_RANGE, false);

        stopAllGrids();

        cstIdSuffix = "_ext";

        startGridsMultiThreaded(3);

        var delRes = snp(grid(1)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

        assertTrue(F.isEmpty(delRes.emptyNodes));

        for (var ig : G.allGrids()) {
            assertTrue(Files.list(((IgniteEx)ig).context().pdsFolderResolver().fileTree().snapshotsRoot().toPath())
                .findFirst().isEmpty());
        }
    }

    /** */
    @Test
    public void testDeleteSnapshotNoMetaSharedDirectory() throws Exception {
        doTestDeleteNotSnapshot(false, false);
    }

    /** */
    @Test
    public void testDeleteSnapshotNoMetaDedicatedDirectories() throws Exception {
        doTestDeleteNotSnapshot(true, false);
    }

    /** */
    @Test
    public void testDeleteSnapshotCorruptedMetaSharedDirectory() throws Exception {
        doTestDeleteNotSnapshot(false, true);
    }

    /** */
    @Test
    public void testDeleteSnapshotCorruptedMetaDedicatedDirectories() throws Exception {
        doTestDeleteNotSnapshot(true, true);
    }

    /** */
    protected void doTestDeleteNotSnapshot(boolean separatedWorkDir, boolean corruptFile) throws Exception {
        this.separatedWorkDir = separatedWorkDir;

        startGridsWithCache(3, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        snp(grid(1)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary).get(getTestTimeout());

        var snpSft = new SnapshotFileTree(grid(1).context(), SNAPSHOT_NAME, null);

        // Ensure that all the snapshot node folders exist.
        assertTrue(snpSft.binaryMeta().exists());
        assertTrue(new SnapshotFileTree(grid(0).context(), SNAPSHOT_NAME, null, folderName(0), consistentId(0))
            .binaryMeta().exists());
        assertTrue(new SnapshotFileTree(grid(2).context(), SNAPSHOT_NAME, null, folderName(2), consistentId(2))
            .binaryMeta().exists());

        assertTrue(snpSft.meta().exists());

        if (corruptFile) {
            try (var rwf = new RandomAccessFile(snpSft.meta(), "rw")) {
                byte[] slop = new byte[128];

                new Random().nextBytes(slop);

                rwf.write(slop);
            }
        }
        else {
            assertTrue(U.delete(snpSft.meta()));
            assertFalse(snpSft.meta().exists());
        }

        var delSnpRes = snp(grid(2)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

        // Check the result.
        if (separatedWorkDir) {
            // One node doesn't find meta, decided not a snapshot.
            assertTrue(F.isEmpty(delSnpRes.uncompletedNodes));
            assertEquals(2, delSnpRes.completedNodes.size());
            assertEquals(1, delSnpRes.emptyNodes.size());
            assertTrue(delSnpRes.emptyNodes.containsKey(grid(1).localNode().id()));
            assertTrue(snpSft.binaryMeta().exists());
        }
        else
            assertEquals(3, delSnpRes.uncompletedNodes.size() + delSnpRes.completedNodes.size() + delSnpRes.emptyNodes.size());

        assertFalse(new SnapshotFileTree(grid(0).context(), SNAPSHOT_NAME, null, folderName(0), consistentId(0))
            .binaryMeta().exists());
        assertFalse(new SnapshotFileTree(grid(2).context(), SNAPSHOT_NAME, null, folderName(2), consistentId(2))
            .binaryMeta().exists());
    }

    /** */
    private String consistentId(int gridIdx) {
        return grid(gridIdx).configuration().getConsistentId().toString();
    }

    /** */
    private String folderName(int gridIdx) {
        return grid(gridIdx).context().pdsFolderResolver().fileTree().folderName();
    }

    /** Test delete snapshot directly in Ignite. */
    @Test
    public void testDeletionInIgnite() throws Exception {
        /** No need to multiply this test. */
        assumeFalse(onlyPrimary || encryption || incremental);

        startGridsMultiThreaded(3);

        List<String> dirsToTest = new ArrayList<>(30);

        var ignFileTree = grid(0).context().pdsFolderResolver().fileTree();

        dirsToTest.add(CommonUtils.getIgniteHome());
        dirsToTest.add(ignFileTree.root().getAbsolutePath());

        ignFileTree.allStorages().forEach(s -> {
            for (var sub : s.listFiles()) {
                if (!sub.isDirectory() || sub.compareTo(ignFileTree.snapshotsRoot()) == 0)
                    continue;

                dirsToTest.add(sub.getAbsolutePath());
            }
        });

        IgniteSnapshotManager snpMgr = snp(grid(0));
        var fileSep = File.separator;
        var belongsToErrMsg = "belongs to a an Ignite's directory";

        for (var dir : dirsToTest) {
            List<String> tests = new ArrayList<>(20);

            tests.add(dir + fileSep);
            // Double path separator.
            tests.add(dir.replaceAll("\\".equals(fileSep) ? "\\\\" : fileSep, "\\".equals(fileSep) ? "\\\\\\\\" : fileSep));
            tests.add(dir + fileSep + "unexisting");

            for (var test : tests) {
                if (log.isInfoEnabled())
                    log.info("Testing path: " + test);

                assertThrowsAnyCause(
                    null,
                    () -> snpMgr.deleteSnapshot(SNAPSHOT_NAME, test).get(getTestTimeout()),
                    IllegalArgumentException.class,
                    belongsToErrMsg
                );
            }
        }

        var testDir = new File(fileSep + "unexisting_" + UUID.randomUUID()).getAbsolutePath();

        if (log.isInfoEnabled())
            log.info("Testing path: " + testDir);

        var delRes = snpMgr.deleteSnapshot(SNAPSHOT_NAME, testDir).get(getTestTimeout());

        assertEquals(3, delRes.emptyNodes.size());

        var snpRoot = ignFileTree.snapshotsRoot();

        delRes = snpMgr.deleteSnapshot(SNAPSHOT_NAME, snpRoot.getAbsolutePath()).get(getTestTimeout());

        assertTrue(F.isEmpty(delRes.completedNodes));
        assertTrue(F.isEmpty(delRes.uncompletedNodes));
        assertEquals(3, delRes.emptyNodes.size());

        delRes = snpMgr.deleteSnapshot(SNAPSHOT_NAME, new File(snpRoot, "unexisting").getAbsolutePath()).get(getTestTimeout());

        assertTrue(F.isEmpty(delRes.completedNodes));
        assertTrue(F.isEmpty(delRes.uncompletedNodes));
        assertEquals(3, delRes.emptyNodes.size());
    }

    /** Tests snapshot deletion when one node has no snapshot data. */
    @Test
    public void testEmptyNodes() throws Exception {
        // Incremental snapshots don't support only-primary and encryption modes.
        assumeTrue(!incremental || !(onlyPrimary || encryption));

        separatedWorkDir = true;

        startGridsWithCache(2, CACHE_KEYS_RANGE, i -> i, dfltCacheCfg);

        snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary).get(getTestTimeout());

        if (incremental)
            addIncrementalSnapshot(null);

        startGrid(G.allGrids().size());

        var delSnpRes = snp(grid(1)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

        assertFalse(F.isEmpty(delSnpRes.emptyNodes));
        assertTrue(delSnpRes.emptyNodes.containsKey(grid(G.allGrids().size() - 1).localNode().id()));
        assertTrue(F.isEmpty(delSnpRes.uncompletedNodes));
    }

    /** Tests snapshot deletion repeat after an offline node restarts. */
    @Test
    public void testDeletionRepeatAfterOfflineNodeStarts() throws Exception {
        // Incremental snapshots don't support only-primary and encryption modes.
        assumeTrue(!incremental || !(onlyPrimary || encryption));

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
        assertFalse(delSnpRes.completedNodes.containsKey(stoppedNodeId));

        assertTrue(F.isEmpty(delSnpRes.uncompletedNodes));
        assertTrue(F.isEmpty(delSnpRes.emptyNodes));

        startGrid(stoppedNodeIdx);

        stoppedNodeId = grid(stoppedNodeIdx).localNode().id();

        delSnpRes = snp(grid(1)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

        assertEquals(1, delSnpRes.completedNodes.size());
        assertTrue(delSnpRes.completedNodes.containsKey(stoppedNodeId));

        assertTrue(F.isEmpty(delSnpRes.uncompletedNodes));
        assertEquals(2, delSnpRes.emptyNodes.size());
    }

    /** Test snapshot deletion process when one node leaves. */
    @Test
    public void testNodeStopsInTheMiddle() throws Exception {
        // Incremental snapshots don't support only-primary and encryption modes.
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
        assertFalse(delRes.completedNodes.containsKey(stoppedGridId));

        startGrid(1);

        delRes = snp(grid(2)).deleteSnapshot(SNAPSHOT_NAME, null).get(getTestTimeout());

        assertEquals(1, delRes.completedNodes.size());
        assertTrue(delRes.completedNodes.containsKey(grid(1).localNode().id()));
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

        String snpPath = new File(grid(0).context().pdsFolderResolver().fileTree().snapshotsRoot(), "ex_snapshots").getAbsolutePath();

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

        assertTrue(!delRes0.completedNodes().isEmpty() || !delRes0.uncompletedNodes().isEmpty());
        assertTrue(!delRes1.completedNodes().isEmpty() || !delRes1.uncompletedNodes().isEmpty());
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
        // Incremental snapshots don't support only-primary and encryption modes.
        assumeTrue(!incremental || !(onlyPrimary || encryption));

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
        // Incremental snapshots don't support only-primary and encryption modes.
        assumeTrue(!incremental || !(onlyPrimary || encryption));

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
        // Incremental snapshots don't support only-primary and encryption modes.
        assumeTrue(!incremental || !(onlyPrimary || encryption));

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
