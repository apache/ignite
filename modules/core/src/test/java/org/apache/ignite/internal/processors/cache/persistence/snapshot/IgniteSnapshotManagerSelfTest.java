/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.internal.MarshallerContextImpl.mappingFileStoreWorkDir;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.CP_SNAPSHOT_REASON;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.databaseRelativePath;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;

/**
 * Default snapshot manager test.
 */
public class IgniteSnapshotManagerSelfTest extends AbstractSnapshotSelfTest {
    /** The size of value array to fit 3 pages. */
    private static final int SIZE_FOR_FIT_3_PAGES = 12008;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Disable implicit checkpoints for this test to avoid a race if an implicit cp has been scheduled before
        // listener registration and calling snpFutTask.start().
        cfg.getDataStorageConfiguration().setCheckpointFrequency(TimeUnit.DAYS.toMillis(365));

        return cfg;
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotLocalPartitions() throws Exception {
        IgniteEx ig = startGridsWithCache(1, 4096, key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        for (int i = 4096; i < 8192; i++) {
            ig.cache(DEFAULT_CACHE_NAME).put(i, new Account(i, i) {
                @Override public String toString() {
                    return "_" + super.toString();
                }
            });
        }

        GridCacheSharedContext<?, ?> cctx = ig.context().cache().context();
        IgniteSnapshotManager mgr = snp(ig);

        // Collection of pairs group and appropriate cache partition to be snapshot.
        IgniteInternalFuture<?> snpFut = startLocalSnapshotTask(cctx,
            SNAPSHOT_NAME,
            F.asMap(CU.cacheId(DEFAULT_CACHE_NAME), null),
            mgr.localSnapshotSenderFactory().apply(SNAPSHOT_NAME));

        snpFut.get();

        File cacheWorkDir = ((FilePageStoreManager)ig.context()
            .cache()
            .context()
            .pageStore())
            .cacheWorkDir(dfltCacheCfg);

        // Checkpoint forces on cluster deactivation (currently only single node in cluster),
        // so we must have the same data in snapshot partitions and those which left
        // after node stop.
        stopGrid(ig.name());

        // Calculate CRCs.
        IgniteConfiguration cfg = ig.context().config();
        PdsFolderSettings settings = ig.context().pdsFolderResolver().resolveFolders();
        String nodePath = databaseRelativePath(settings.folderName());
        File binWorkDir = binaryWorkDir(cfg.getWorkDirectory(), settings.folderName());
        File marshWorkDir = mappingFileStoreWorkDir(U.workDirectory(cfg.getWorkDirectory(), cfg.getIgniteHome()));
        File snpBinWorkDir = binaryWorkDir(mgr.snapshotLocalDir(SNAPSHOT_NAME).getAbsolutePath(), settings.folderName());
        File snpMarshWorkDir = mappingFileStoreWorkDir(mgr.snapshotLocalDir(SNAPSHOT_NAME).getAbsolutePath());

        final Map<String, Integer> origPartCRCs = calculateCRC32Partitions(cacheWorkDir);
        final Map<String, Integer> snpPartCRCs = calculateCRC32Partitions(
            FilePageStoreManager.cacheWorkDir(U.resolveWorkDirectory(mgr.snapshotLocalDir(SNAPSHOT_NAME)
                    .getAbsolutePath(),
                nodePath,
                false),
                cacheDirName(dfltCacheCfg)));

        assertEquals("Partitions must have the same CRC after file copying and merging partition delta files",
            origPartCRCs, snpPartCRCs);
        assertEquals("Binary object mappings must be the same for local node and created snapshot",
            calculateCRC32Partitions(binWorkDir), calculateCRC32Partitions(snpBinWorkDir));
        assertEquals("Marshaller meta mast be the same for local node and created snapshot",
            calculateCRC32Partitions(marshWorkDir), calculateCRC32Partitions(snpMarshWorkDir));

        File snpWorkDir = mgr.snapshotTmpDir();

        assertEquals("Snapshot working directory must be cleaned after usage", 0, snpWorkDir.listFiles().length);
    }

    /**
     * Test that all partitions are copied successfully even after multiple checkpoints occur during
     * the long copy of cache partition files.
     *
     * Data consistency checked through a test node started right from snapshot directory and all values
     * read successes.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testSnapshotLocalPartitionMultiCpWithLoad() throws Exception {
        int valMultiplier = 2;
        CountDownLatch slowCopy = new CountDownLatch(1);

        // Start grid node with data before each test.
        IgniteEx ig = startGridsWithCache(1, CACHE_KEYS_RANGE, key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        GridCacheSharedContext<?, ?> cctx = ig.context().cache().context();
        AtomicInteger cntr = new AtomicInteger();
        CountDownLatch ldrLatch = new CountDownLatch(1);
        IgniteSnapshotManager mgr = snp(ig);
        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)cctx.database();

        IgniteInternalFuture<?> loadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                U.await(ldrLatch);

                while (!Thread.currentThread().isInterrupted())
                    ig.cache(DEFAULT_CACHE_NAME).put(cntr.incrementAndGet(),
                        new Account(cntr.incrementAndGet(), cntr.incrementAndGet()));
            }
            catch (IgniteInterruptedCheckedException e) {
                log.warning("Loader has been interrupted", e);
            }
        }, 5, "cache-loader-");

        // Register task but not schedule it on the checkpoint.
        SnapshotFutureTask snpFutTask = mgr.registerSnapshotTask(SNAPSHOT_NAME,
            cctx.localNodeId(),
            F.asMap(CU.cacheId(DEFAULT_CACHE_NAME), null),
            false,
            new DelegateSnapshotSender(log, mgr.snapshotExecutorService(), mgr.localSnapshotSenderFactory().apply(SNAPSHOT_NAME)) {
                @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                    try {
                        U.await(slowCopy);

                        delegate.sendPart0(part, cacheDirName, pair, length);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            });

        db.addCheckpointListener(new CheckpointListener() {
            /** {@inheritDoc} */
            @Override public void beforeCheckpointBegin(Context ctx) {
                // No-op.
            }

            /** {@inheritDoc} */
            @Override public void onMarkCheckpointBegin(Context ctx) {
                // No-op.
            }

            /** {@inheritDoc} */
            @Override public void onCheckpointBegin(Context ctx) {
                Map<Integer, Set<Integer>> processed = GridTestUtils.getFieldValue(snpFutTask,
                    SnapshotFutureTask.class,
                    "processed");

                if (!processed.isEmpty())
                    ldrLatch.countDown();
            }
        });

        try {
            snpFutTask.start();

            // Change data before snapshot creation which must be included into it with correct value multiplier.
            for (int i = 0; i < CACHE_KEYS_RANGE; i++)
                ig.cache(DEFAULT_CACHE_NAME).put(i, new Account(i, valMultiplier * i));

            // Snapshot is still in the INIT state. beforeCheckpoint has been skipped
            // due to checkpoint already running and we need to schedule the next one
            // right after current will be completed.
            cctx.database().forceCheckpoint(String.format(CP_SNAPSHOT_REASON, SNAPSHOT_NAME));

            snpFutTask.started().get();

            db.forceCheckpoint("snapshot is ready to be created")
                .futureFor(CheckpointState.MARKER_STORED_TO_DISK)
                .get();

            // Change data after snapshot.
            for (int i = 0; i < CACHE_KEYS_RANGE; i++)
                ig.cache(DEFAULT_CACHE_NAME).put(i, new Account(i, 3 * i));

            // Snapshot on the next checkpoint must copy page to delta file before write it to a partition.
            forceCheckpoint(ig);

            slowCopy.countDown();

            snpFutTask.get();
        }
        finally {
            loadFut.cancel();
        }

        // Now can stop the node and check created snapshots.
        stopGrid(0);

        cleanPersistenceDir(ig.name());

        // Start Ignite instance from snapshot directory.
        IgniteEx ig2 = startGridsFromSnapshot(1, SNAPSHOT_NAME);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++) {
            assertEquals("snapshot data consistency violation [key=" + i + ']',
                i * valMultiplier, ((Account)ig2.cache(DEFAULT_CACHE_NAME).get(i)).balance);
        }
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotLocalPartitionNotEnoughSpace() throws Exception {
        String err_msg = "Test exception. Not enough space.";
        AtomicInteger throwCntr = new AtomicInteger();
        RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();

        IgniteEx ig = startGridWithCache(dfltCacheCfg.setAffinity(new ZeroPartitionAffinityFunction()),
            CACHE_KEYS_RANGE);

        // Change data after backup.
        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, 2 * i);

        GridCacheSharedContext<?, ?> cctx0 = ig.context().cache().context();

        IgniteSnapshotManager mgr = snp(ig);

        mgr.ioFactory(new FileIOFactory() {
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                FileIO fileIo = ioFactory.create(file, modes);

                if (file.getName().equals(IgniteSnapshotManager.partDeltaFileName(0)))
                    return new FileIODecorator(fileIo) {
                        @Override public int writeFully(ByteBuffer srcBuf) throws IOException {
                            if (throwCntr.incrementAndGet() == 3)
                                throw new IOException(err_msg);

                            return super.writeFully(srcBuf);
                        }
                    };

                return fileIo;
            }
        });

        IgniteInternalFuture<?> snpFut = startLocalSnapshotTask(cctx0,
            SNAPSHOT_NAME,
            F.asMap(CU.cacheId(DEFAULT_CACHE_NAME), null),
            mgr.localSnapshotSenderFactory().apply(SNAPSHOT_NAME));

        // Check the right exception thrown.
        assertThrowsAnyCause(log,
            snpFut::get,
            IOException.class,
            err_msg);
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotCreateLocalCopyPartitionFail() throws Exception {
        String err_msg = "Test. Fail to copy partition: ";
        IgniteEx ig = startGridWithCache(dfltCacheCfg, CACHE_KEYS_RANGE);

        Map<Integer, Set<Integer>> parts = new HashMap<>();
        parts.put(CU.cacheId(DEFAULT_CACHE_NAME), new HashSet<>(Collections.singletonList(0)));

        IgniteSnapshotManager mgr0 = snp(ig);

        IgniteInternalFuture<?> fut = startLocalSnapshotTask(ig.context().cache().context(),
            SNAPSHOT_NAME,
            parts,
            new DelegateSnapshotSender(log, mgr0.snapshotExecutorService(),
                mgr0.localSnapshotSenderFactory().apply(SNAPSHOT_NAME)) {
                @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                    if (pair.getPartitionId() == 0)
                        throw new IgniteException(err_msg + pair);

                    delegate.sendPart0(part, cacheDirName, pair, length);
                }
            });

        assertThrowsAnyCause(log,
            fut::get,
            IgniteException.class,
            err_msg);
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotRemoteWithNodeFiler() throws Exception {
        int grids = 3;
        CacheConfiguration<Integer, Integer> ccfg = txCacheConfig(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME))
            .setNodeFilter(node -> node.consistentId().toString().endsWith("1"));

        for (int i = 0; i < grids; i++)
            startGrid(optimize(getConfiguration(getTestIgniteInstanceName(i)).setCacheConfiguration()));

        IgniteEx ig0 = grid(0);
        ig0.cluster().baselineAutoAdjustEnabled(false);
        ig0.cluster().state(ACTIVE);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig0.getOrCreateCache(ccfg).put(i, i);

        Map<Integer, Set<Integer>> parts = new HashMap<>();
        parts.put(CU.cacheId(DEFAULT_CACHE_NAME), null);

        IgniteInternalFuture<?> fut = snp(grid(0)).requestRemoteSnapshot(grid(2).localNode().id(),
            parts,
            new BiConsumer<File, GroupPartitionId>() {
                @Override public void accept(File file, GroupPartitionId gprPartId) {
                    fail("We should not receive anything.");
                }
            });

        assertThrowsAnyCause(log,
            fut::get,
            IgniteCheckedException.class,
            "The snapshot operation stopped");
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotRemotePartitionsWithLoad() throws Exception {
        IgniteEx ig0 = startGrids(2);

        ig0.cluster().state(ClusterState.ACTIVE);

        AtomicInteger cntr = new AtomicInteger();

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig0.cache(DEFAULT_CACHE_NAME).put(i, cntr.incrementAndGet());

        GridCacheSharedContext<?, ?> cctx1 = grid(1).context().cache().context();
        GridCacheDatabaseSharedManager db1 = (GridCacheDatabaseSharedManager)cctx1.database();

        forceCheckpoint();

        Map<String, Integer> rmtPartCRCs = new HashMap<>();
        CountDownLatch cancelLatch = new CountDownLatch(1);

        db1.addCheckpointListener(new CheckpointListener() {
            /** {@inheritDoc} */
            @Override public void beforeCheckpointBegin(Context ctx) {
                //No-op.
            }

            /** {@inheritDoc} */
            @Override public void onMarkCheckpointBegin(Context ctx) {
                // No-op.
            }

            /** {@inheritDoc} */
            @Override public void onCheckpointBegin(Context ctx) {
                SnapshotFutureTask task = cctx1.snapshotMgr().lastScheduledRemoteSnapshotTask(grid(0).localNode().id());

                // Skip first remote snapshot creation due to it will be cancelled.
                if (task == null || cancelLatch.getCount() > 0)
                    return;

                Map<Integer, Set<Integer>> processed = GridTestUtils.getFieldValue(task,
                    SnapshotFutureTask.class,
                    "processed");

                if (!processed.isEmpty()) {
                    assert rmtPartCRCs.isEmpty();

                    // Calculate actual partition CRCs when the checkpoint will be finished on this node.
                    ctx.finishedStateFut().listen(f -> {
                        File cacheWorkDir = ((FilePageStoreManager)grid(1).context().cache().context().pageStore())
                            .cacheWorkDir(dfltCacheCfg);

                        rmtPartCRCs.putAll(calculateCRC32Partitions(cacheWorkDir));
                    });
                }
            }
        });

        IgniteSnapshotManager mgr0 = snp(ig0);

        UUID rmtNodeId = grid(1).localNode().id();
        Map<String, Integer> snpPartCRCs = new HashMap<>();

        Map<Integer, Set<Integer>> parts = new HashMap<>();
        parts.put(CU.cacheId(DEFAULT_CACHE_NAME), null);

        IgniteInternalFuture<?> loadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!Thread.currentThread().isInterrupted())
                ig0.cache(DEFAULT_CACHE_NAME).put(cntr.incrementAndGet(), cntr.incrementAndGet());
        }, 5, "cache-loader-");

        try {
            // Snapshot must be taken on node1 and transmitted to node0.
            IgniteInternalFuture<?> fut = mgr0.requestRemoteSnapshot(rmtNodeId,
                parts,
                new BiConsumer<File, GroupPartitionId>() {
                    @Override public void accept(File file, GroupPartitionId gprPartId) {
                        log.info("Snapshot partition received successfully [rmtNodeId=" + rmtNodeId +
                            ", part=" + file.getAbsolutePath() + ", gprPartId=" + gprPartId + ']');

                        cancelLatch.countDown();
                    }
                });

            cancelLatch.await();

            fut.cancel();

            IgniteInternalFuture<?> fut2 = mgr0.requestRemoteSnapshot(rmtNodeId,
                parts,
                (part, pair) -> {
                    try {
                        snpPartCRCs.put(part.getName(), FastCrc.calcCrc(part));
                    }
                    catch (IOException e) {
                        throw new IgniteException(e);
                    }
                });

            fut2.get();
        }
        finally {
            loadFut.cancel();
        }

        assertEquals("Partitions from remote node must have the same CRCs as those which have been received",
            rmtPartCRCs, snpPartCRCs);
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotRemoteOnBothNodes() throws Exception {
        IgniteEx ig0 = startGrids(2);

        ig0.cluster().state(ClusterState.ACTIVE);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig0.cache(DEFAULT_CACHE_NAME).put(i, i);

        forceCheckpoint(ig0);

        IgniteSnapshotManager mgr0 = snp(ig0);
        IgniteSnapshotManager mgr1 = snp(grid(1));

        UUID node0 = grid(0).localNode().id();
        UUID node1 = grid(1).localNode().id();

        Map<Integer, Set<Integer>> fromNode1 = owningParts(ig0,
            new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))),
            node1);

        Map<Integer, Set<Integer>> fromNode0 = owningParts(grid(1),
            new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))),
            node0);

        // Snapshot must be taken on node1 and transmitted to node0.
        IgniteInternalFuture<?> futFrom1To0 = mgr0.requestRemoteSnapshot(node1, fromNode1,
            (part, pair) -> assertTrue("Received partition has not been requested", fromNode1.get(pair.getGroupId())
                    .remove(pair.getPartitionId())));
        IgniteInternalFuture<?> futFrom0To1 = mgr1.requestRemoteSnapshot(node0, fromNode0,
            (part, pair) -> assertTrue("Received partition has not been requested", fromNode0.get(pair.getGroupId())
                .remove(pair.getPartitionId())));

        futFrom0To1.get();
        futFrom1To0.get();

        assertTrue("Not all of partitions have been received: " + fromNode1,
            fromNode1.get(CU.cacheId(DEFAULT_CACHE_NAME)).isEmpty());
        assertTrue("Not all of partitions have been received: " + fromNode0,
            fromNode0.get(CU.cacheId(DEFAULT_CACHE_NAME)).isEmpty());
    }

    /** @throws Exception If fails. */
    @Test(expected = ClusterTopologyCheckedException.class)
    public void testRemoteSnapshotRequestedNodeLeft() throws Exception {
        IgniteEx ig0 = startGridWithCache(dfltCacheCfg, CACHE_KEYS_RANGE);
        IgniteEx ig1 = startGrid(1);

        ig0.cluster().setBaselineTopology(ig0.cluster().forServers().nodes());

        awaitPartitionMapExchange();

        CountDownLatch hold = new CountDownLatch(1);

        ((GridCacheDatabaseSharedManager)ig1.context().cache().context().database())
            .addCheckpointListener(new CheckpointListener() {
                /** {@inheritDoc} */
                @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
                    // Listener will be executed inside the checkpoint thead.
                    U.await(hold);
                }

                /** {@inheritDoc} */
                @Override public void onMarkCheckpointBegin(Context ctx) {
                    // No-op.
                }

                /** {@inheritDoc} */
                @Override public void onCheckpointBegin(Context ctx) {
                    // No-op.
                }
            });

        UUID rmtNodeId = ig1.localNode().id();

        Map<Integer, Set<Integer>> parts = new HashMap<>();
        parts.put(CU.cacheId(DEFAULT_CACHE_NAME), null);

        snp(ig0).requestRemoteSnapshot(rmtNodeId, parts, (part, grp) -> {});

        IgniteInternalFuture<?>[] futs = new IgniteInternalFuture[1];

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                IgniteInternalFuture<?> snpFut = snp(ig1)
                    .lastScheduledRemoteSnapshotTask(ig0.localNode().id());

                if (snpFut == null)
                    return false;
                else
                    futs[0] = snpFut;

                return true;
            }
        }, 5_000L));

        stopGrid(0);

        hold.countDown();

        futs[0].get();
    }

    /**
     * <pre>
     * 1. Start 2 nodes.
     * 2. Request snapshot from 2-nd node
     * 3. Block snapshot-request message.
     * 4. Start 3-rd node and change BLT.
     * 5. Stop 3-rd node and change BLT.
     * 6. 2-nd node now have MOVING partitions to be preloaded.
     * 7. Release snapshot-request message.
     * 8. Should get an error of snapshot creation since MOVING partitions cannot be snapshot.
     * </pre>
     *
     * @throws Exception If fails.
     */
    @Test(expected = IgniteCheckedException.class)
    public void testRemoteOutdatedSnapshot() throws Exception {
        dfltCacheCfg.setBackups(1);

        IgniteEx ig0 = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);

        TestRecordingCommunicationSpi.spi(ig0)
            .blockMessages((node, msg) -> msg instanceof SnapshotRequestMessage);

        UUID rmtNodeId = grid(1).localNode().id();

        IgniteSnapshotManager mgr0 = snp(ig0);

        // Snapshot must be taken on node1 and transmitted to node0.
        IgniteInternalFuture<?> snpFut = mgr0.requestRemoteSnapshot(rmtNodeId,
            owningParts(ig0, new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))), rmtNodeId),
            (part, grp) -> {});

        TestRecordingCommunicationSpi.spi(ig0)
            .waitForBlocked();

        startGrid(2);

        ig0.cluster().setBaselineTopology(ig0.cluster().forServers().nodes());

        awaitPartitionMapExchange();

        stopGrid(2);

        TestRecordingCommunicationSpi.spi(grid(1))
            .blockMessages((node, msg) -> msg instanceof GridDhtPartitionDemandMessage);

        ig0.cluster().setBaselineTopology(ig0.cluster().forServers().nodes());

        TestRecordingCommunicationSpi.spi(ig0)
            .stopBlock(true, obj -> obj.ioMessage().message() instanceof SnapshotRequestMessage);

        snpFut.get();
    }

    /** @throws Exception If fails. */
    @Test(expected = IgniteCheckedException.class)
    public void testLocalSnapshotOnCacheStopped() throws Exception {
        IgniteEx ig = startGridWithCache(dfltCacheCfg, CACHE_KEYS_RANGE);

        startGrid(1);

        ig.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        GridCacheSharedContext<?, ?> cctx0 = ig.context().cache().context();
        IgniteSnapshotManager mgr = snp(ig);

        CountDownLatch cpLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> snpFut = startLocalSnapshotTask(cctx0,
            SNAPSHOT_NAME,
            F.asMap(CU.cacheId(DEFAULT_CACHE_NAME), null),
            new DelegateSnapshotSender(log, mgr.snapshotExecutorService(), mgr.localSnapshotSenderFactory().apply(SNAPSHOT_NAME)) {
                @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                    try {
                        U.await(cpLatch);

                        delegate.sendPart0(part, cacheDirName, pair, length);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            });

        IgniteCache<?, ?> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.destroy();

        cpLatch.countDown();

        snpFut.get(5_000, TimeUnit.MILLISECONDS);
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotIteratorRandomizedLoader() throws Exception {
        Random rnd = new Random();
        int maxKey = 15_000;
        int maxValSize = 32_768;
        int loadingTimeMs = 30_000;

        CacheConfiguration<Integer, Value> ccfg = txCacheConfig(new CacheConfiguration<Integer, Value>("tx1"))
            .setAffinity(new RendezvousAffinityFunction(false, 1));

        IgniteEx ignite = startGridsWithCache(1, CACHE_KEYS_RANGE, k -> new Value(new byte[1024]), ccfg);

        IgniteCache<Integer, Value> cache = ignite.cache(ccfg.getName());

        long startTime = U.currentTimeMillis();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!Thread.currentThread().isInterrupted() && startTime + loadingTimeMs > U.currentTimeMillis()) {
                if (rnd.nextBoolean())
                    cache.put(rnd.nextInt(maxKey), new Value(new byte[rnd.nextInt(maxValSize)]));
                else
                    cache.remove(rnd.nextInt(maxKey));
            }

        }, 10, "change-loader-");

        fut.get();

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        Map<Integer, Value> iterated = new HashMap<>();

        try (GridCloseableIterator<CacheDataRow> iter = snp(ignite).partitionRowIterator(ignite.context(),
            SNAPSHOT_NAME,
            ignite.context().pdsFolderResolver().resolveFolders().folderName(),
            ccfg.getName(),
            0)
        ) {
            CacheObjectContext coctx = ignite.cachex(ccfg.getName()).context().cacheObjectContext();

            while (iter.hasNext()) {
                CacheDataRow row = iter.next();

                iterated.put(row.key().value(coctx, true), row.value().value(coctx, true));
            }
        }

        stopAllGrids();

        IgniteEx snpIgnite = startGridsFromSnapshot(1, SNAPSHOT_NAME);

        IgniteCache<Integer, Value> snpCache = snpIgnite.cache(ccfg.getName());

        assertEquals(snpCache.size(CachePeekMode.PRIMARY), iterated.size());
        snpCache.forEach(e -> {
            Value val = iterated.remove(e.getKey());

            assertNotNull(val);
            assertEquals(val.arr().length, e.getValue().arr().length);
        });

        assertTrue(iterated.isEmpty());
    }

    /** @throws Exception If fails */
    @Test
    public void testSnapshotIterator() throws Exception {
        int keys = 127;

        IgniteEx ignite = startGridsWithCache(2,
            dfltCacheCfg.setAffinity(new RendezvousAffinityFunction(false, 1)), keys);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        int rows = 0;

        try (GridCloseableIterator<CacheDataRow> iter = snp(ignite).partitionRowIterator(ignite.context(),
            SNAPSHOT_NAME,
            ignite.context().pdsFolderResolver().resolveFolders().folderName(),
            dfltCacheCfg.getName(),
            0)
        ) {
            CacheObjectContext coctx = ignite.cachex(dfltCacheCfg.getName()).context().cacheObjectContext();

            while (iter.hasNext()) {
                CacheDataRow row = iter.next();

                // Invariant for cache: cache key always equals to cache value.
                assertEquals("Invalid key/value pair [key=" + row.key() + ", val=" + row.value() + ']',
                    row.key().value(coctx, false, U.resolveClassLoader(ignite.configuration())),
                    (Integer)row.value().value(coctx, false));

                rows++;
            }
        }

        assertEquals("Invalid number of rows: " + rows, keys, rows);
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotIteratorLargeRows() throws Exception {
        int keys = 2;
        CacheConfiguration<Integer, Value> ccfg = txCacheConfig(new CacheConfiguration<Integer, Value>(DEFAULT_CACHE_NAME))
            .setAffinity(new RendezvousAffinityFunction(false, 1));

        IgniteEx ignite = startGridsWithoutCache(2);

        assertEquals(DFLT_PAGE_SIZE, ignite.configuration().getDataStorageConfiguration().getPageSize());

        for (int i = 0; i < keys; i++)
            ignite.getOrCreateCache(ccfg).put(i, new Value(new byte[SIZE_FOR_FIT_3_PAGES]));

        forceCheckpoint();

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        int rows = 0;

        try (GridCloseableIterator<CacheDataRow> iter = snp(ignite).partitionRowIterator(ignite.context(),
            SNAPSHOT_NAME,
            ignite.context().pdsFolderResolver().resolveFolders().folderName(),
            dfltCacheCfg.getName(),
            0)
        ) {
            CacheObjectContext coctx = ignite.cachex(dfltCacheCfg.getName()).context().cacheObjectContext();

            while (iter.hasNext()) {
                CacheDataRow row = iter.next();

                assertEquals(SIZE_FOR_FIT_3_PAGES, ((Value)row.value().value(coctx, false)).arr().length);
                assertTrue((Integer)row.key().value(coctx, false, null) < 2);

                rows++;
            }
        }

        assertEquals("Invalid number of rows: " + rows, keys, rows);
    }

    /**
     * @param ignite Ignite instance to set factory.
     * @param factory New factory to use.
     */
    private static void snapshotStoreFactory(IgniteEx ignite, BiFunction<Integer, Boolean, FileVersionCheckingFactory> factory) {
        setFieldValue(snp(ignite), "storeFactory", factory);
    }

    /**
     * @param snpName Unique snapshot name.
     * @param parts Collection of pairs group and appropriate cache partition to be snapshot.
     * @param snpSndr Sender which used for snapshot sub-task processing.
     * @return Future which will be completed when snapshot is done.
     */
    private static SnapshotFutureTask startLocalSnapshotTask(
        GridCacheSharedContext<?, ?> cctx,
        String snpName,
        Map<Integer, Set<Integer>> parts,
        SnapshotSender snpSndr
    ) throws IgniteCheckedException {
        SnapshotFutureTask snpFutTask = cctx.snapshotMgr().registerSnapshotTask(snpName, cctx.localNodeId(), parts, false, snpSndr);

        snpFutTask.start();

        // Snapshot is still in the INIT state. beforeCheckpoint has been skipped
        // due to checkpoint already running and we need to schedule the next one
        // right after current will be completed.
        cctx.database().forceCheckpoint(String.format(CP_SNAPSHOT_REASON, snpName));

        snpFutTask.started().get();

        return snpFutTask;
    }

    /**
     * @param src Source node to calculate.
     * @param grps Groups to collect owning parts.
     * @param rmtNodeId Remote node id.
     * @return Map of collected parts.
     */
    private static Map<Integer, Set<Integer>> owningParts(IgniteEx src, Set<Integer> grps, UUID rmtNodeId) {
        Map<Integer, Set<Integer>> result = new HashMap<>();

        for (Integer grpId : grps) {
            Set<Integer> parts = src.context()
                .cache()
                .cacheGroup(grpId)
                .topology()
                .partitions(rmtNodeId)
                .entrySet()
                .stream()
                .filter(p -> p.getValue() == GridDhtPartitionState.OWNING)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            result.put(grpId, parts);
        }

        return result;
    }

    /** */
    private static class ZeroPartitionAffinityFunction extends RendezvousAffinityFunction {
        @Override public int partition(Object key) {
            return 0;
        }
    }
}
