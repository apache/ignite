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

package org.apache.ignite.internal.processors.cache.persistence.backup;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.TransmissionPolicy;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointFuture;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PagesAllocationRange;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

import static java.nio.file.StandardOpenOption.READ;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFileEx;

/** */
public class IgniteBackupManager extends GridCacheSharedManagerAdapter {
    /** File with delta pages suffix. */
    public static final String DELTA_SUFFIX = ".delta";

    /** File name template consists of delta pages. */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + DELTA_SUFFIX;

    /** File name template for index delta pages. */
    public static final String INDEX_DELTA_NAME = INDEX_FILE_NAME + DELTA_SUFFIX;

    /** The reason of checkpoint start for needs of bakcup. */
    public static final String BACKUP_CP_REASON = "Wakeup for checkpoint to take backup [name=%s]";

    /** Default working directory for backup temporary files. */
    public static final String DFLT_BACKUP_DIRECTORY = "backup";

    /** Prefix for backup threads. */
    private static final String BACKUP_RUNNER_THREAD_PREFIX = "backup-runner";

    /** Total number of thread to perform local backup. */
    private static final int BACKUP_POOL_SIZE = 4;

    /** Map of registered cache backup processes and their corresponding contexts. */
    private final ConcurrentMap<String, BackupContext> backupCtxs = new ConcurrentHashMap<>();

    /** All registered page writers of all running backup processes. */
    private final ConcurrentMap<GroupPartitionId, List<PageStoreSerialWriter>> partWriters = new ConcurrentHashMap<>();

    /** Lock to protect the resources is used. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** Main backup directory to store files. */
    private File snapshotWorkDir;

    /** Factory to working with delta as file storage. */
    private volatile FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** Backup thread pool. */
    private IgniteThreadPoolExecutor backupRunner;

    /** Checkpoint listener to handle scheduled backup requests. */
    private DbCheckpointListener cpLsnr;

    /** Database manager for enabled persistence. */
    private GridCacheDatabaseSharedManager dbMgr;

    /** Configured data storage page size. */
    private int pageSize;

    /**
     * @param ctx Kernal context.
     */
    public IgniteBackupManager(GridKernalContext ctx) {
        assert CU.isPersistenceEnabled(ctx.config());

    }

    /**
     * @param snapshotCacheDir Snapshot directory to store files.
     * @param partId Cache partition identifier.
     * @return A file representation.
     */
    public static File getPartionDeltaFile(File snapshotCacheDir, int partId) {
        return new File(snapshotCacheDir, getPartitionDeltaFileName(partId));
    }

    /**
     * @param partId Partitoin id.
     * @return File name of delta partition pages.
     */
    public static String getPartitionDeltaFileName(int partId) {
        assert partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION;

        return partId == INDEX_PARTITION ? INDEX_DELTA_NAME : String.format(PART_DELTA_TEMPLATE, partId);
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        pageSize = cctx.kernalContext()
            .config()
            .getDataStorageConfiguration()
            .getPageSize();

        assert pageSize > 0;

        if (!cctx.kernalContext().clientNode()) {
            backupRunner = new IgniteThreadPoolExecutor(
                BACKUP_RUNNER_THREAD_PREFIX,
                cctx.igniteInstanceName(),
                BACKUP_POOL_SIZE,
                BACKUP_POOL_SIZE,
                30_000,
                new LinkedBlockingQueue<>(),
                SYSTEM_POOL,
                (t, e) -> cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e)));
        }

        IgnitePageStoreManager store = cctx.pageStore();

        assert store instanceof FilePageStoreManager : "Invalid page store manager was created: " + store;

        snapshotWorkDir = U.resolveWorkDirectory(((FilePageStoreManager)store).workDir().getAbsolutePath(),
            DFLT_BACKUP_DIRECTORY, false);

        dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

        dbMgr.addCheckpointListener(cpLsnr = new DbCheckpointListener() {
            @Override public void beforeCheckpointBegin(Context ctx) {
                for (BackupContext bctx0 : backupCtxs.values()) {
                    if (bctx0.started)
                        continue;

                    // Gather partitions metainfo for thouse which will be copied.
                    ctx.gatherPartStats(bctx0.parts);
                }
            }

            @Override public void onMarkCheckpointBegin(Context ctx) {
                // No-op.
            }

            @Override public void onMarkCheckpointEnd(Context ctx) {
                // Under the write lock here. It's safe to add new stores
                for (BackupContext bctx0 : backupCtxs.values()) {
                    if (bctx0.started)
                        continue;

                    try {
                        PartitionAllocationMap allocationMap = ctx.partitionStatMap();

                        allocationMap.prepareForSnapshot();

                        assert !allocationMap.isEmpty() : "Partitions statistics has not been gathered: " + bctx0;

                        final FilePageStoreManager storeMgr = (FilePageStoreManager)cctx.pageStore();

                        for (GroupPartitionId pair : bctx0.parts) {
                            PagesAllocationRange allocRange = allocationMap.get(pair);

                            assert allocRange != null : "Pages not allocated [pairId=" + pair + ", ctx=" + bctx0 + ']';

                            PageStore store = storeMgr.getStore(pair.getGroupId(), pair.getPartitionId());

                            bctx0.partFileLengths.put(pair, store.size());
                            bctx0.partDeltaWriters.get(pair)
                                .init(allocRange.getCurrAllocatedPageCnt());
                        }

                        for (Map.Entry<GroupPartitionId, PageStoreSerialWriter> e : bctx0.partDeltaWriters.entrySet()) {
                            partWriters.computeIfAbsent(e.getKey(), p -> new LinkedList<>())
                                .add(e.getValue());
                        }
                    }
                    catch (IgniteCheckedException e) {
                        bctx0.backupFut.onDone(e);
                    }
                }

                // Remove not used delta stores.
                for (List<PageStoreSerialWriter> list0 : partWriters.values())
                    list0.removeIf(PageStoreSerialWriter::stopped);
            }

            @Override public void onCheckpointBegin(Context ctx) {
                for (BackupContext bctx0 : backupCtxs.values()) {
                    if (bctx0.started || bctx0.backupFut.isDone())
                        continue;

                    // Submit all tasks for partitions and deltas processing.
                    submitTasks(bctx0);

                    bctx0.started = true;
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        dbMgr.removeCheckpointListener(cpLsnr);

        for (BackupContext ctx : backupCtxs.values())
            closeBackupResources(ctx);

        partWriters.clear();
        backupRunner.shutdown();
    }

    /**
     * @param snapshotWorkDir Current backup working directory.
     * @param backupName Backup name.
     * @return Backup directory.
     */
    public static File snapshotDir(File snapshotWorkDir, String backupName) {
        return new File(snapshotWorkDir, backupName);
    }

    /**
     * @return Backup directory used by manager.
     */
    public File backupWorkDir() {
        assert snapshotWorkDir != null;

        return snapshotWorkDir;
    }

    /**
     * @param backupName Unique backup name.
     * @return Future which will be completed when backup is done.
     * @throws IgniteCheckedException If initialiation fails.
     */
    public IgniteInternalFuture<String> createLocalSnapshot(
        String backupName,
        List<Integer> grpIds
    ) throws IgniteCheckedException {
        // Collection of pairs group and appropratate cache partition to be backuped.
        // TODO filter in-memory caches
        Map<Integer, Set<Integer>> parts = grpIds.stream()
            .collect(Collectors.toMap(grpId -> grpId,
                grpId -> {
                    int partsCnt = cctx.cache()
                        .cacheGroup(grpId)
                        .affinity()
                        .partitions();

                    return Stream.iterate(0, n -> n + 1)
                        .limit(partsCnt)
                        .collect(Collectors.toSet());
                }));

        File backupDir0 = snapshotDir(snapshotWorkDir, backupName);

        return scheduleSnapshot(backupName,
            parts,
            backupDir0,
            backupRunner,
            () -> localSnapshotReceiver(backupDir0));
    }

    /**
     * @param parts Collection of pairs group and appropratate cache partition to be backuped.
     * @param rmtNodeId The remote node to connect to.
     * @param topic The remote topic to connect to.
     * @throws IgniteCheckedException If initialiation fails.
     */
    public void createRemoteSnapshot(
        Map<Integer, Set<Integer>> parts,
        byte plc,
        UUID rmtNodeId,
        Object topic
    ) throws IgniteCheckedException {
        String snapshotName = UUID.randomUUID().toString();

        File snapshotDir0 = snapshotDir(snapshotWorkDir, snapshotName);

        IgniteInternalFuture<?> fut = scheduleSnapshot(snapshotName,
            parts,
            snapshotDir0,
            new SerialExecutor(cctx.kernalContext()
                .pools()
                .poolForPolicy(plc)),
            () -> remoteSnapshotReceiver(rmtNodeId, topic));

        fut.listen(f -> {
            if (log.isInfoEnabled()) {
                log.info("The requested bakcup has been send [result=" + (f.error() == null) +
                    ", name=" + snapshotName + ']');
            }

            boolean done = snapshotDir0.delete();

            assert done;
        });
    }

    /**
     * @param snapshotName Unique backup name.
     * @param parts Collection of pairs group and appropratate cache partition to be backuped.
     * @param snapshotDir Local directory to save cache partition deltas and snapshots to.
     * @param rcvFactory Factory which produces snapshot receiver instance.
     * @return Future which will be completed when backup is done.
     * @throws IgniteCheckedException If initialiation fails.
     */
    IgniteInternalFuture<String> scheduleSnapshot(
        String snapshotName,
        Map<Integer, Set<Integer>> parts,
        File snapshotDir,
        Executor exec,
        Supplier<PartitionSnapshotReceiver> rcvFactory
    ) throws IgniteCheckedException {
        if (backupCtxs.containsKey(snapshotName))
            throw new IgniteCheckedException("Backup with requested name is already scheduled: " + snapshotName);

        BackupContext bctx = null;

        try {
            // Atomic operation, fails with exception if not.
            Files.createDirectory(snapshotDir.toPath());

            bctx = new BackupContext(snapshotName,
                snapshotDir,
                parts,
                exec,
                rcvFactory);

            final BackupContext bctx0 = bctx;

            bctx.backupFut.listen(f -> {
                backupCtxs.remove(snapshotName);

                closeBackupResources(bctx0);
            });

            for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet()) {
                final CacheGroupContext gctx = cctx.cache().cacheGroup(e.getKey());

                // Create cache backup directory if not.
                File grpDir = U.resolveWorkDirectory(bctx.snapshotDir.getAbsolutePath(),
                    cacheDirName(gctx.config()), false);

                U.ensureDirectory(grpDir,
                    "bakcup directory for cache group: " + gctx.groupId(),
                    null);

                CompletableFuture<Boolean> cpEndFut0 = bctx.cpEndFut;

                for (int partId : e.getValue()) {
                    final GroupPartitionId pair = new GroupPartitionId(e.getKey(), partId);

                    bctx.partDeltaWriters.put(pair,
                        new PageStoreSerialWriter(log,
                            () -> cpEndFut0.isDone() && !cpEndFut0.isCompletedExceptionally(),
                            bctx.backupFut,
                            getPartionDeltaFile(grpDir, partId),
                            ioFactory,
                            pageSize));
                }
            }

            BackupContext ctx0 = backupCtxs.putIfAbsent(snapshotName, bctx);

            assert ctx0 == null : ctx0;

            CheckpointFuture cpFut = dbMgr.forceCheckpoint(String.format(BACKUP_CP_REASON, snapshotName));

            BackupContext finalBctx = bctx;

            cpFut.finishFuture()
                .listen(f -> {
                    if (f.error() == null)
                        finalBctx.cpEndFut.complete(true);
                    else
                        finalBctx.cpEndFut.completeExceptionally(f.error());
                });

            cpFut.beginFuture()
                .get();

            U.log(log, "Backup operation scheduled with the following context: " + bctx);
        }
        catch (IOException e) {
            closeBackupResources(bctx);

            try {
                Files.delete(snapshotDir.toPath());
            }
            catch (IOException ioe) {
                throw new IgniteCheckedException("Error deleting backup directory during context initialization " +
                    "failed: " + snapshotName, e);
            }

            throw new IgniteCheckedException(e);
        }

        return bctx.backupFut;
    }

    /**
     *
     * @param snapshotDir Snapshot directory.
     * @return Snapshot receiver instance.
     */
    PartitionSnapshotReceiver localSnapshotReceiver(File snapshotDir) {
        return new LocalPartitionSnapshotReceiver(log,
            snapshotDir,
            ioFactory,
            ((FilePageStoreManager)cctx.pageStore()).getFilePageStoreFactory(),
            pageSize);
    }

    /**
     * @param rmtNodeId Remote node id to send snapshot to.
     * @param topic Remote topic.
     * @return Snapshot receiver instance.
     */
    PartitionSnapshotReceiver remoteSnapshotReceiver(UUID rmtNodeId, Object topic) {
        return new RemotePartitionSnapshotReceiver(log, cctx.gridIO().openTransmissionSender(rmtNodeId, topic));
    }

    /**
     * @return The executor service used to run backup tasks.
     */
    ExecutorService backupExecutorService() {
        assert backupRunner != null;

        return backupRunner;
    }

    /**
     * @param bctx Context to clouse all resources.
     */
    private void closeBackupResources(BackupContext bctx) {
        if (bctx == null)
            return;

        for (PageStoreSerialWriter writer : bctx.partDeltaWriters.values())
            U.closeQuiet(writer);
    }

    /**
     * @param bctx Context to handle.
     */
    private void submitTasks(BackupContext bctx) {
        List<CompletableFuture<Void>> futs = new ArrayList<>(bctx.parts.size());
        File workDir = ((FilePageStoreManager) cctx.pageStore()).workDir();

        U.log(log, "Partition allocated lengths: " + bctx.partFileLengths);

        for (GroupPartitionId pair : bctx.parts) {
            CacheConfiguration ccfg = cctx.cache().cacheGroup(pair.getGroupId()).config();
            String cacheDirName = cacheDirName(ccfg);

            final PartitionSnapshotReceiver rcv = bctx.rcvFactory.get();

            CompletableFuture<Void> fut0 = CompletableFuture.runAsync(() ->
                    rcv.receivePart(
                        getPartitionFileEx(
                            cacheWorkDir(workDir, cacheDirName),
                            pair.getPartitionId()),
                        cacheDirName,
                        pair,
                        bctx.partFileLengths.get(pair)),
                bctx.exec)
                .thenRun(() -> bctx.partDeltaWriters.get(pair).partProcessed = true)
                // Wait for the completion of both futures - checkpoint end, copy partition
                .runAfterBothAsync(bctx.cpEndFut,
                    () -> rcv.receiveDelta(
                        getPartionDeltaFile(
                            cacheWorkDir(bctx.snapshotDir, cacheDirName),
                            pair.getPartitionId()),
                        pair),
                    bctx.exec)
                .whenComplete((t, v) -> U.closeQuiet(rcv));

            futs.add(fut0);
        }

        CompletableFuture.allOf(futs.toArray(new CompletableFuture[bctx.parts.size()]))
             .whenComplete(new BiConsumer<Void, Throwable>() {
                 @Override public void accept(Void res, Throwable t) {
                     if (t == null)
                         bctx.backupFut.onDone(bctx.snapshotName);
                     else
                         bctx.backupFut.onDone(t);
                 }
             });
    }

    /**
     * @param backupName Unique backup name.
     */
    public void stopCacheBackup(String backupName) {

    }

    /**
     * @param pairId Cache group, partition identifiers pair.
     * @param pageId Tracked page id.
     * @param buf Buffer with page data.
     */
    public void beforeStoreWrite(GroupPartitionId pairId, long pageId, ByteBuffer buf, PageStore store) {
        assert buf.position() == 0 : buf.position();
        assert buf.order() == ByteOrder.nativeOrder() : buf.order();

        List<PageStoreSerialWriter> writers = partWriters.get(pairId);

        if (writers == null || writers.isEmpty())
            return;

        for (PageStoreSerialWriter writer : writers)
            writer.write(pageId, buf, store);
    }

    /**
     * @param ioFactory Factory to create IO interface over a page stores.
     */
    void ioFactory(FileIOFactory ioFactory) {
        this.ioFactory = ioFactory;
    }

    /**
     *
     */
    private static class PageStoreSerialWriter implements Closeable {
        /** Ignite logger to use. */
        @GridToStringExclude
        private final IgniteLogger log;

        /** Buse lock to perform write opertions. */
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        /** Local buffer to perpform copy-on-write operations. */
        private final ThreadLocal<ByteBuffer> localBuff;

        /** {@code true} if need the original page from PageStore instead of given buffer. */
        private final BooleanSupplier checkpointComplete;

        /** If backup has been stopped due to an error. */
        private final GridFutureAdapter<?> backupFut;

        /** IO over the underlying file */
        private volatile FileIO fileIo;

        /** {@code true} if current writer is stopped. */
        private volatile boolean partProcessed;

        /**
         * Array of bits. 1 - means pages written, 0 - the otherwise.
         * Size of array can be estimated only under checkpoint write lock.
         */
        private volatile AtomicIntegerArray pagesWrittenBits;

        /**
         * @param log Ignite logger to use.
         * @param checkpointComplete Checkpoint finish flag.
         * @param pageSize Size of page to use for local buffer.
         * @param cfgFile Configuration file provider.
         * @param factory Factory to produce an IO interface over underlying file.
         */
        public PageStoreSerialWriter(
            IgniteLogger log,
            BooleanSupplier checkpointComplete,
            GridFutureAdapter<?> backupFut,
            File cfgFile,
            FileIOFactory factory,
            int pageSize
        ) throws IOException {
            this.checkpointComplete = checkpointComplete;
            this.backupFut = backupFut;
            this.log = log.getLogger(PageStoreSerialWriter.class);

            localBuff = ThreadLocal.withInitial(() ->
                ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder()));

            fileIo = factory.create(cfgFile);
        }

        /**
         * @param allocPages Total number of tracking pages.
         * @return This for chaining.
         */
        public PageStoreSerialWriter init(int allocPages) {
            pagesWrittenBits = new AtomicIntegerArray(allocPages);

            return this;
        }

        /**
         * @return {@code true} if writer is stopped and cannot write pages.
         */
        public boolean stopped() {
            return (checkpointComplete.getAsBoolean() && partProcessed) || backupFut.isDone();
        }

        /**
         * @param pageId Page id to write.
         * @param buf Page buffer.
         * @param store Storage to write to.
         */
        public void write(long pageId, ByteBuffer buf, PageStore store) {
            assert pagesWrittenBits != null;

            if (stopped())
                return;

            try {
                if (checkpointComplete.getAsBoolean()) {
                    int pageIdx = PageIdUtils.pageIndex(pageId);

                    // Page out of backup scope.
                    if (pageIdx > pagesWrittenBits.length())
                        return;

                    // Page already written.
                    if (!pagesWrittenBits.compareAndSet(pageIdx, 0, 1))
                        return;

                    final ByteBuffer locBuf = localBuff.get();

                    assert locBuf.capacity() == store.getPageSize();

                    locBuf.clear();

                    if (store.readPage(pageId, locBuf, true) < 0)
                        return;

                    locBuf.flip();

                    writePage0(pageId, locBuf);
                }
                else {
                    // Direct buffre is needs to be written, associated checkpoint not finished yet.
                    writePage0(pageId, buf);

                    buf.rewind();
                }
            }
            catch (Throwable t) {
                backupFut.onDone(t);
            }
        }

        /**
         * @param pageId Page ID.
         * @param pageBuf Page buffer to write.
         * @throws IOException If page writing failed (IO error occurred).
         */
        private void writePage0(long pageId, ByteBuffer pageBuf) throws IOException {
            lock.readLock().lock();

            try {
                assert fileIo != null : "Delta pages storage is not inited: " + this;
                assert pageBuf.position() == 0;
                assert pageBuf.order() == ByteOrder.nativeOrder() : "Page buffer order " + pageBuf.order()
                    + " should be same with " + ByteOrder.nativeOrder();

                int crc = PageIO.getCrc(pageBuf);
                int crc32 = FastCrc.calcCrc(new CRC32(), pageBuf, pageBuf.limit());

                if (log.isDebugEnabled()) {
                    log.debug("onPageWrite [pageId=" + pageId +
                        ", pageIdBuff=" + PageIO.getPageId(pageBuf) +
                        ", fileSize=" + fileIo.size() +
                        ", crcBuff=" + crc32 +
                        ", crcPage=" + crc + ']');
                }

                pageBuf.rewind();

                // Write buffer to the end of the file.
                fileIo.writeFully(pageBuf);
            }
            finally {
                lock.readLock().unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void close() {
            lock.writeLock().lock();

            try {
                U.closeQuiet(fileIo);

                fileIo = null;
            }
            finally {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     *
     */
    private static class BackupContext {
        /** Unique identifier of snapshot process. */
        private final String snapshotName;

        /** Absolute backup storage path. */
        private final File snapshotDir;

        /** Service to perform partitions copy. */
        private final Executor exec;

        /**
         * The length of file size per each cache partiton file.
         * Partition has value greater than zero only for partitons in OWNING state.
         * Information collected under checkpoint write lock.
         */
        private final Map<GroupPartitionId, Long> partFileLengths = new HashMap<>();

        /**
         * Map of partitions to backup and theirs corresponding delta PageStores.
         * Writers are pinned to the backup context due to controlling partition
         * processing supplier.
         */
        private final Map<GroupPartitionId, PageStoreSerialWriter> partDeltaWriters = new HashMap<>();

        /** Future of result completion. */
        @GridToStringExclude
        private final GridFutureAdapter<String> backupFut = new GridFutureAdapter<>();

        /** Snapshot data receiver. */
        @GridToStringExclude
        private final Supplier<PartitionSnapshotReceiver> rcvFactory;

        /** Collection of partition to be backuped. */
        private final List<GroupPartitionId> parts = new ArrayList<>();

        /** Checkpoint end future. */
        private final CompletableFuture<Boolean> cpEndFut = new CompletableFuture<>();

        /** Flag idicates that this backup is start copying partitions. */
        private volatile boolean started;

        /**
         * @param snapshotName Unique identifier of backup process.
         * @param snapshotDir Backup storage directory.
         * @param exec Service to perform partitions copy.
         */
        public BackupContext(
            String snapshotName,
            File snapshotDir,
            Map<Integer, Set<Integer>> parts,
            Executor exec,
            Supplier<PartitionSnapshotReceiver> rcvFactory
        ) {
            A.notNull(snapshotName, "Backup name cannot be empty or null");
            A.notNull(snapshotDir, "You must secify correct backup directory");
            A.ensure(snapshotDir.isDirectory(), "Specified path is not a directory");
            A.notNull(exec, "Executor service must be not null");
            A.notNull(rcvFactory, "Snapshot receiver which handles execution tasks must be not null");

            this.snapshotName = snapshotName;
            this.snapshotDir = snapshotDir;
            this.exec = exec;
            this.rcvFactory = rcvFactory;

            for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet()) {
                for (Integer partId : e.getValue())
                    this.parts.add(new GroupPartitionId(e.getKey(), partId));
            }
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            BackupContext ctx = (BackupContext)o;

            return snapshotName.equals(ctx.snapshotName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(snapshotName);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(BackupContext.class, this);
        }
    }

    /**
     *
     */
    private static class SerialExecutor implements Executor {
        /** */
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        /** */
        private final Executor executor;

        /** */
        private volatile Runnable active;

        /**
         * @param executor Executor to run tasks on.
         */
        public SerialExecutor(Executor executor) {
            this.executor = executor;
        }

        /** {@inheritDoc} */
        @Override public synchronized void execute(final Runnable r) {
            tasks.offer(new Runnable() {
                /** {@inheritDoc} */
                @Override public void run() {
                    try {
                        r.run();
                    }
                    finally {
                        scheduleNext();
                    }
                }
            });

            if (active == null) {
                scheduleNext();
            }
        }

        /**
         *
         */
        protected synchronized void scheduleNext() {
            if ((active = tasks.poll()) != null) {
                executor.execute(active);
            }
        }
    }

    /**
     *
     */
    private static class RemotePartitionSnapshotReceiver implements PartitionSnapshotReceiver {
        /** Ignite logger to use. */
        private final IgniteLogger log;

        /** The sender which sends files to remote node. */
        private final GridIoManager.TransmissionSender sndr;

        /**
         * @param log Ignite logger.
         * @param sndr File sender instance.
         */
        public RemotePartitionSnapshotReceiver(
            IgniteLogger log,
            GridIoManager.TransmissionSender sndr
        ) {
            this.log = log.getLogger(RemotePartitionSnapshotReceiver.class);
            this.sndr = sndr;
        }

        /** {@inheritDoc} */
        @Override public void receivePart(File part, String cacheDirName, GroupPartitionId pair, Long length) {
            try {
                Map<String, Serializable> params = new HashMap<>();

                params.put(String.valueOf(pair.getGroupId()), String.valueOf(pair.getGroupId()));
                params.put(String.valueOf(pair.getPartitionId()), String.valueOf(pair.getPartitionId()));
                params.put(cacheDirName, cacheDirName);

                sndr.send(part, 0, length, params, TransmissionPolicy.FILE);

                if (log.isInfoEnabled()) {
                    log.info("Partition file has been send [part=" + part.getName() + ", pair=" + pair +
                        ", length=" + length + ']');
                }
            }
            catch (IgniteCheckedException | InterruptedException | IOException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void receiveDelta(File delta, GroupPartitionId pair) {
            try {
                Map<String, Serializable> params = new HashMap<>();

                params.put(String.valueOf(pair.getGroupId()), String.valueOf(pair.getGroupId()));
                params.put(String.valueOf(pair.getPartitionId()), String.valueOf(pair.getPartitionId()));

                sndr.send(delta, params, TransmissionPolicy.CHUNK);

                if (log.isInfoEnabled())
                    log.info("Delta pages storage has been send [part=" + delta.getName() + ", pair=" + pair + ']');
            }
            catch (IgniteCheckedException | InterruptedException | IOException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            U.closeQuiet(sndr);
        }
    }

    /**
     *
     */
    private static class LocalPartitionSnapshotReceiver implements PartitionSnapshotReceiver {
        /** Ignite logger to use. */
        private final IgniteLogger log;

        /** Local node snapshot directory. */
        private final File snapshotDir;

        /** Facotry to produce IO interface over a file. */
        private final FileIOFactory ioFactory;

        /** Factory to create page store for restore. */
        private final FilePageStoreFactory storeFactory;

        /** Size of page. */
        private final int pageSize;

        /** Raw received partition file during the first stage. */
        private File snapshotPart;

        /**
         * @param log Ignite logger to use.
         * @param snapshotDir Local node snapshot directory.
         * @param ioFactory Facotry to produce IO interface over a file.
         * @param storeFactory Factory to create page store for restore.
         * @param pageSize Size of page.
         */
        public LocalPartitionSnapshotReceiver(
            IgniteLogger log,
            File snapshotDir,
            FileIOFactory ioFactory,
            FilePageStoreFactory storeFactory,
            int pageSize
        ) {
            this.log = log.getLogger(LocalPartitionSnapshotReceiver.class);
            this.snapshotDir = snapshotDir;
            this.ioFactory = ioFactory;
            this.storeFactory = storeFactory;
            this.pageSize = pageSize;
        }

        /** {@inheritDoc} */
        @Override public void receivePart(File part, String cacheDirName, GroupPartitionId pair, Long length) {
            snapshotPart = new File(cacheWorkDir(snapshotDir, cacheDirName), part.getName());

            try {
                if (!snapshotPart.exists() || snapshotPart.delete())
                    snapshotPart.createNewFile();

                if (length == 0)
                    return;

                try (FileIO src = ioFactory.create(part);
                     FileChannel dest = new FileOutputStream(snapshotPart).getChannel()) {
                    src.position(0);

                    long written = 0;

                    while (written < length)
                        written += src.transferTo(written, length - written, dest);
                }

                if (log.isInfoEnabled()) {
                    log.info("Partition has been snapshotted [snapshotDir=" + snapshotDir.getAbsolutePath() +
                        ", cacheDirName=" + cacheDirName + ", part=" + part.getName() +
                        ", length=" + part.length() + ", snapshot=" + snapshotPart.getName() + ']');
                }
            }
            catch (IOException ex) {
                throw new IgniteException(ex);
            }
        }

        /** {@inheritDoc} */
        @Override public void receiveDelta(File delta, GroupPartitionId pair) {
            assert snapshotPart != null;

            U.log(log, "Start partition backup recovery with the given delta page file [part=" + snapshotPart +
                ", delta=" + delta + ']');

            try (FileIO fileIo = ioFactory.create(delta, READ);
                 FilePageStore store = (FilePageStore)storeFactory
                     .createPageStore(pair.partType(),
                         snapshotPart::toPath,
                         new LongAdderMetric("NO_OP", null))
            ) {
                ByteBuffer pageBuf = ByteBuffer.allocate(pageSize)
                    .order(ByteOrder.nativeOrder());

                long totalBytes = fileIo.size();

                assert totalBytes % pageSize == 0 : "Given file with delta pages has incorrect size: " + fileIo.size();

                store.beginRecover();

                for (long pos = 0; pos < totalBytes; pos += pageSize) {
                    long read = fileIo.readFully(pageBuf, pos);

                    assert read == pageBuf.capacity();

                    pageBuf.flip();

                    long pageId = PageIO.getPageId(pageBuf);

                    int crc32 = FastCrc.calcCrc(new CRC32(), pageBuf, pageBuf.limit());

                    int crc = PageIO.getCrc(pageBuf);

                    U.log(log, "Read page given delta file [path=" + delta.getName() +
                        ", pageId=" + pageId + ", pos=" + pos + ", pages=" + (totalBytes / pageSize) +
                        ", crcBuff=" + crc32 + ", crcPage=" + crc + ']');

                    pageBuf.rewind();

                    store.write(PageIO.getPageId(pageBuf), pageBuf, 0, false);

                    pageBuf.flip();
                }

                store.finishRecover();
            }
            catch (IOException | IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {

        }
    }
}
