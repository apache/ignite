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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_SNAPSHOT_DIRECTORY;
import static org.apache.ignite.internal.MarshallerContextImpl.mappingFileStoreWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.CP_SNAPSHOT_REASON;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DFLT_SNAPSHOT_TMP_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.igniteCacheStoragePath;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.snapshotPath;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 *
 */
public class IgniteSnapshotManagerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String SNAPSHOT_NAME = "testSnapshot";

    /** */
    private static final int CACHE_PARTS_COUNT = 8;

    /** */
    private static final int CACHE_KEYS_RANGE = 1024;

    /** */
    private static final DataStorageConfiguration memCfg = new DataStorageConfiguration()
        .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(100L * 1024 * 1024)
            .setPersistenceEnabled(true))
        .setCheckpointFrequency(3000)
        .setPageSize(1024);

    /** */
    private CacheConfiguration<Integer, Integer> dfltCacheCfg =
        new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false)
                .setPartitions(CACHE_PARTS_COUNT));

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_SNAPSHOT_DIRECTORY, false));
    }

    /**
     * @throws Exception If fails.
     */
    @Before
    public void beforeTestSnapshot() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @After
    public void afterTestSnapshot() {
        try {
            for (Ignite ig : G.allGrids()) {
                File storeWorkDir = ((FilePageStoreManager)((IgniteEx)ig).context()
                    .cache().context().pageStore()).workDir();

                Path snpTempDir = Paths.get(storeWorkDir.getAbsolutePath(), DFLT_SNAPSHOT_TMP_DIR);

                assertTrue("Snapshot working directory must be empty at the moment test execution stopped: " + snpTempDir,
                    F.isEmptyDirectory(snpTempDir));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(true))
                .setCheckpointFrequency(3000)
                .setPageSize(1024))
            .setCacheConfiguration(dfltCacheCfg)
            // Default work directory must be resolved earlier if snapshot used to start grids.
            .setWorkDirectory(U.defaultWorkDirectory());
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testSnapshotLocalPartitions() throws Exception {
        // Start grid node with data before each test.
        IgniteEx ig = startGridWithCache(dfltCacheCfg, 2048);

        // The following data will be included into checkpoint.
        for (int i = 2048; i < 4096; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, new TestOrderItem(i, i));

        for (int i = 4096; i < 8192; i++) {
            ig.cache(DEFAULT_CACHE_NAME).put(i, new TestOrderItem(i, i) {
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
            mgr.localSnapshotSender(SNAPSHOT_NAME));

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
        String nodePath = igniteCacheStoragePath(ig.context().pdsFolderResolver().resolveFolders());
        File binWorkDir = ((CacheObjectBinaryProcessorImpl)ig.context().cacheObjects())
            .binaryFileStoreWorkDir(cfg.getWorkDirectory());
        File marshWorkDir = mappingFileStoreWorkDir(U.workDirectory(cfg.getWorkDirectory(), cfg.getIgniteHome()));
        File snpBinWorkDir = ((CacheObjectBinaryProcessorImpl)ig.context().cacheObjects())
            .binaryFileStoreWorkDir(mgr.snapshotDir(SNAPSHOT_NAME).getAbsolutePath());
        File snpMarshWorkDir = mappingFileStoreWorkDir(mgr.snapshotDir(SNAPSHOT_NAME).getAbsolutePath());

        final Map<String, Integer> origPartCRCs = calculateCRC32Partitions(cacheWorkDir);
        final Map<String, Integer> snpPartCRCs = calculateCRC32Partitions(
            FilePageStoreManager.cacheWorkDir(U.resolveWorkDirectory(mgr.snapshotDir(SNAPSHOT_NAME)
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
        IgniteEx ig = startGrid(0);

        ig.cluster().baselineAutoAdjustEnabled(false);
        ig.cluster().state(ClusterState.ACTIVE);
        GridCacheSharedContext<?, ?> cctx = ig.context().cache().context();

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, new TestOrderItem(i, i));

        forceCheckpoint(ig);

        AtomicInteger cntr = new AtomicInteger();
        CountDownLatch ldrLatch = new CountDownLatch(1);
        IgniteSnapshotManager mgr = snp(ig);
        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)cctx.database();

        IgniteInternalFuture<?> loadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                U.await(ldrLatch);

                while (!Thread.currentThread().isInterrupted())
                    ig.cache(DEFAULT_CACHE_NAME).put(cntr.incrementAndGet(),
                        new TestOrderItem(cntr.incrementAndGet(), cntr.incrementAndGet()));
            }
            catch (IgniteInterruptedCheckedException e) {
                log.warning("Loader has been interrupted", e);
            }
        }, 5, "cache-loader-");

        // Register task but not schedule it on the checkpoint.
        SnapshotFutureTask snpFutTask = mgr.registerSnapshotTask(SNAPSHOT_NAME,
            cctx.localNodeId(),
            F.asMap(CU.cacheId(DEFAULT_CACHE_NAME), null),
            new DelegateSnapshotSender(log, mgr.snapshotExecutorService(), mgr.localSnapshotSender(SNAPSHOT_NAME)) {
                @Override
                public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                    try {
                        U.await(slowCopy);

                        delegate.sendPart0(part, cacheDirName, pair, length);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            });

        db.addCheckpointListener(new DbCheckpointListener() {
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

            // Change data before snapshot creation which must be included into it witch correct value multiplier.
            for (int i = 0; i < CACHE_KEYS_RANGE; i++)
                ig.cache(DEFAULT_CACHE_NAME).put(i, new TestOrderItem(i, valMultiplier * i));

            // Snapshot is still in the INIT state. beforeCheckpoint has been skipped
            // due to checkpoint already running and we need to schedule the next one
            // right after current will be completed.
            cctx.database().forceCheckpoint(String.format(CP_SNAPSHOT_REASON, SNAPSHOT_NAME));

            snpFutTask.awaitStarted();

            db.forceCheckpoint("snapshot is ready to be created")
                .futureFor(CheckpointState.MARKER_STORED_TO_DISK)
                .get();

            // Change data after snapshot.
            for (int i = 0; i < CACHE_KEYS_RANGE; i++)
                ig.cache(DEFAULT_CACHE_NAME).put(i, new TestOrderItem(i, 3 * i));

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
                i * valMultiplier, ((TestOrderItem)ig2.cache(DEFAULT_CACHE_NAME).get(i)).value);
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
            mgr.localSnapshotSender(SNAPSHOT_NAME));

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
                mgr0.localSnapshotSender(SNAPSHOT_NAME)) {
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

        db1.addCheckpointListener(new DbCheckpointListener() {
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
        Map<Integer, Set<Integer>> parts = owningParts(ig0,
            new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))),
            rmtNodeId);

        IgniteInternalFuture<?> loadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!Thread.currentThread().isInterrupted())
                ig0.cache(DEFAULT_CACHE_NAME).put(cntr.incrementAndGet(), cntr.incrementAndGet());
        }, 5, "cache-loader-");

        try {
            // Snapshot must be taken on node1 and transmitted to node0.
            IgniteInternalFuture<?> fut = mgr0.createRemoteSnapshot(rmtNodeId,
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

            IgniteInternalFuture<?> fut2 = mgr0.createRemoteSnapshot(rmtNodeId,
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
        IgniteInternalFuture<?> futFrom1To0 = mgr0.createRemoteSnapshot(node1, fromNode1,
            (part, pair) -> assertTrue("Received partition has not been requested", fromNode1.get(pair.getGroupId())
                    .remove(pair.getPartitionId())));
        IgniteInternalFuture<?> futFrom0To1 = mgr1.createRemoteSnapshot(node0, fromNode0,
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
            .addCheckpointListener(new DbCheckpointListener() {
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

        snp(ig0).createRemoteSnapshot(rmtNodeId,
            owningParts(ig0,
                new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))),
                rmtNodeId),
                (part, grp) -> {});

        IgniteInternalFuture<?>[] futs = new IgniteInternalFuture[1];

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                IgniteInternalFuture<Boolean> snpFut = snp(ig1)
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
        IgniteEx ig0 = startGrids(2);

        ig0.cluster().state(ClusterState.ACTIVE);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig0.cache(DEFAULT_CACHE_NAME).put(i, i);

        awaitPartitionMapExchange();

        forceCheckpoint();

        TestRecordingCommunicationSpi.spi(ig0)
            .blockMessages((node, msg) -> msg instanceof SnapshotRequestMessage);

        UUID rmtNodeId = grid(1).localNode().id();

        IgniteSnapshotManager mgr0 = snp(ig0);

        // Snapshot must be taken on node1 and transmitted to node0.
        IgniteInternalFuture<?> snpFut = mgr0.createRemoteSnapshot(rmtNodeId,
            owningParts(ig0, new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))), rmtNodeId),
            (part, grp) -> {});

        TestRecordingCommunicationSpi.spi(ig0)
            .waitForBlocked();

        startGrid(2);

        ig0.cluster().setBaselineTopology(ig0.cluster().forServers().nodes());

        awaitPartitionMapExchange();

        stopGrid(2);

        TestRecordingCommunicationSpi.spi(grid(1))
            .blockMessages((node, msg) ->  msg instanceof GridDhtPartitionDemandMessage);

        ig0.cluster().setBaselineTopology(ig0.cluster().forServers().nodes());

        TestRecordingCommunicationSpi.spi(ig0)
            .stopBlock(true, obj -> obj.get2().message() instanceof SnapshotRequestMessage);

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
            new DelegateSnapshotSender(log, mgr.snapshotExecutorService(), mgr.localSnapshotSender(SNAPSHOT_NAME)) {
                @Override
                public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                    try {
                        U.await(cpLatch);

                            delegate.sendPart0(part, cacheDirName, pair, length);
                        } catch (IgniteInterruptedCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }
                });

        IgniteCache<?, ?> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.destroy();

        cpLatch.countDown();

        snpFut.get(5_000, TimeUnit.MILLISECONDS);
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
    ) throws IgniteCheckedException{
        SnapshotFutureTask snpFutTask = cctx.snapshotMgr().registerSnapshotTask(snpName, cctx.localNodeId(), parts, snpSndr);

        snpFutTask.start();

        // Snapshot is still in the INIT state. beforeCheckpoint has been skipped
        // due to checkpoint already running and we need to schedule the next one
        // right after current will be completed.
        cctx.database().forceCheckpoint(String.format(CP_SNAPSHOT_REASON, snpName));

        snpFutTask.awaitStarted();

        return snpFutTask;
    }

    /**
     * Calculate CRC for all partition files of specified cache.
     *
     * @param cacheDir Cache directory to iterate over partition files.
     * @return The map of [fileName, checksum].
     */
    private static Map<String, Integer> calculateCRC32Partitions(File cacheDir) {
        assert cacheDir.isDirectory() : cacheDir.getAbsolutePath();

        Map<String, Integer> result = new HashMap<>();

        try {
            try (DirectoryStream<Path> partFiles = newDirectoryStream(cacheDir.toPath(),
                p -> p.toFile().getName().startsWith(PART_FILE_PREFIX) && p.toFile().getName().endsWith(FILE_SUFFIX))
            ) {
                for (Path path : partFiles)
                    result.put(path.toFile().getName(), FastCrc.calcCrc(path.toFile()));
            }

            return result;
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param ccfg Default cache configuration.
     * @return Ignite instance.
     * @throws Exception If fails.
     */
    private IgniteEx startGridWithCache(CacheConfiguration<Integer, Integer> ccfg, int range) throws Exception {
        dfltCacheCfg = ccfg;

        // Start grid node with data before each test.
        IgniteEx ig = startGrid(0);

        ig.cluster().baselineAutoAdjustEnabled(false);
        ig.cluster().state(ClusterState.ACTIVE);

        for (int i = 0; i < range; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, i);

        forceCheckpoint(ig);

        return ig;
    }

    /**
     * @param cnt Number of grids to start.
     * @param snpName Snapshot to start grids from.
     * @return Coordinator ignite instance.
     * @throws Exception If fails.
     */
    protected IgniteEx startGridsFromSnapshot(int cnt, String snpName) throws Exception {
        return startGridsFromSnapshot(cnt, cfg -> snapshotPath(cfg).toString(), snpName);
    }

    /**
     * @param cnt Number of grids to start.
     * @param path Snapshot path resolver.
     * @param snpName Snapshot to start grids from.
     * @return Coordinator ignite instance.
     * @throws Exception If fails.
     */
    protected IgniteEx startGridsFromSnapshot(int cnt,
        Function<IgniteConfiguration, String> path,
        String snpName
    ) throws Exception {
        IgniteEx crd = null;

        for (int i = 0; i < cnt; i++) {
            IgniteConfiguration cfg = optimize(getConfiguration(getTestIgniteInstanceName(i)));

            cfg.setWorkDirectory(Paths.get(path.apply(cfg), snpName).toString());

            if (crd == null)
                crd = startGrid(cfg);
            else
                startGrid(cfg);
        }

        crd.cluster().baselineAutoAdjustEnabled(false);
        crd.cluster().state(ACTIVE);

        return crd;
    }

    /**
     * @param ignite Ignite instance.
     * @return Snapshot manager related to given ignite instance.
     */
    public static IgniteSnapshotManager snp(IgniteEx ignite) {
        return ignite.context().cache().context().snapshotMgr();
    }

    /** */
    private static class ZeroPartitionAffinityFunction extends RendezvousAffinityFunction {
        @Override public int partition(Object key) {
            return 0;
        }
    }

    /** */
    private static class DelegateSnapshotSender extends SnapshotSender {
        /** Delegate call to. */
        protected final SnapshotSender delegate;

        /**
         * @param delegate Delegate call to.
         */
        public DelegateSnapshotSender(IgniteLogger log, Executor exec, SnapshotSender delegate) {
            super(log, exec);

            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override protected void init() throws IgniteCheckedException {
            delegate.init();
        }

        /** {@inheritDoc} */
        @Override public void sendCacheConfig0(File ccfg, String cacheDirName) {
            delegate.sendCacheConfig(ccfg, cacheDirName);
        }

        /** {@inheritDoc} */
        @Override public void sendMarshallerMeta0(List<Map<Integer, MappedName>> mappings) {
            delegate.sendMarshallerMeta(mappings);
        }

        /** {@inheritDoc} */
        @Override public void sendBinaryMeta0(Collection<BinaryType> types) {
            delegate.sendBinaryMeta(types);
        }

        /** {@inheritDoc} */
        @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
            delegate.sendPart(part, cacheDirName, pair, length);
        }

        /** {@inheritDoc} */
        @Override public void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
            delegate.sendDelta(delta, cacheDirName, pair);
        }

        /** {@inheritDoc} */
        @Override public void close0(Throwable th) {
            delegate.close(th);
        }
    }

    /** */
    private static class TestOrderItem implements Serializable {
        /** Serial version. */
        private static final long serialVersionUID = 0L;

        /** Order key. */
        private final int key;

        /** Order value. */
        private final int value;

        public TestOrderItem(int key, int value) {
            this.key = key;
            this.value = value;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestOrderItem item = (TestOrderItem)o;

            return key == item.key &&
                value == item.value;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(key, value);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestOrderItem [key=" + key + ", value=" + value + ']';
        }
    }
}
