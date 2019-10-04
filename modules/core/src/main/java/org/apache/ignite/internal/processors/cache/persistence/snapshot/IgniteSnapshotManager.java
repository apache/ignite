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
public class IgniteSnapshotManager extends GridCacheSharedManagerAdapter {
    /** File with delta pages suffix. */
    public static final String DELTA_SUFFIX = ".delta";

    /** File name template consists of delta pages. */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + DELTA_SUFFIX;

    /** File name template for index delta pages. */
    public static final String INDEX_DELTA_NAME = INDEX_FILE_NAME + DELTA_SUFFIX;

    /** The reason of checkpoint start for needs of bakcup. */
    public static final String SNAPSHOT_CP_REASON = "Wakeup for checkpoint to take snapshot [name=%s]";

    /** Default working directory for snapshot temporary files. */
    public static final String DFLT_SNAPSHOT_DIRECTORY = "snapshots";

    /** Prefix for snapshot threads. */
    private static final String SNAPSHOT_RUNNER_THREAD_PREFIX = "snapshot-runner";

    /** Total number of thread to perform local snapshot. */
    private static final int SNAPSHOT_THEEAD_POOL_SIZE = 4;

    /** Map of registered cache snapshot processes and their corresponding contexts. */
    private final ConcurrentMap<String, SnapshotContext> snpCtxs = new ConcurrentHashMap<>();

    /** All registered page writers of all running snapshot processes. */
    private final ConcurrentMap<GroupPartitionId, List<PageStoreSerialWriter>> partWriters = new ConcurrentHashMap<>();

    /** Lock to protect the resources is used. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** Main snapshot directory to store files. */
    private File snpWorkDir;

    /** Factory to working with delta as file storage. */
    private volatile FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** snapshot thread pool. */
    private IgniteThreadPoolExecutor snpRunner;

    /** Checkpoint listener to handle scheduled snapshot requests. */
    private DbCheckpointListener cpLsnr;

    /** Database manager for enabled persistence. */
    private GridCacheDatabaseSharedManager dbMgr;

    /** Configured data storage page size. */
    private int pageSize;

    /**
     * @param ctx Kernal context.
     */
    public IgniteSnapshotManager(GridKernalContext ctx) {
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
            snpRunner = new IgniteThreadPoolExecutor(
                SNAPSHOT_RUNNER_THREAD_PREFIX,
                cctx.igniteInstanceName(),
                SNAPSHOT_THEEAD_POOL_SIZE,
                SNAPSHOT_THEEAD_POOL_SIZE,
                30_000,
                new LinkedBlockingQueue<>(),
                SYSTEM_POOL,
                (t, e) -> cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e)));
        }

        IgnitePageStoreManager store = cctx.pageStore();

        assert store instanceof FilePageStoreManager : "Invalid page store manager was created: " + store;

        snpWorkDir = U.resolveWorkDirectory(((FilePageStoreManager)store).workDir().getAbsolutePath(),
            DFLT_SNAPSHOT_DIRECTORY, false);

        dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

        dbMgr.addCheckpointListener(cpLsnr = new DbCheckpointListener() {
            @Override public void beforeCheckpointBegin(Context ctx) {
                for (SnapshotContext bctx0 : snpCtxs.values()) {
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
                for (SnapshotContext bctx0 : snpCtxs.values()) {
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
                        bctx0.snpFut.onDone(e);
                    }
                }

                // Remove not used delta stores.
                for (List<PageStoreSerialWriter> list0 : partWriters.values())
                    list0.removeIf(PageStoreSerialWriter::stopped);
            }

            @Override public void onCheckpointBegin(Context ctx) {
                for (SnapshotContext bctx0 : snpCtxs.values()) {
                    if (bctx0.started || bctx0.snpFut.isDone())
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

        for (SnapshotContext ctx : snpCtxs.values())
            closeSnapshotResources(ctx);

        partWriters.clear();
        snpRunner.shutdown();
    }

    /**
     * @param snpWorkDir Current snapshot working directory.
     * @param snapshotName snapshot name.
     * @return snapshot directory.
     */
    public static File snapshotDir(File snpWorkDir, String snapshotName) {
        return new File(snpWorkDir, snapshotName);
    }

    /**
     * @return Snapshot directory used by manager.
     */
    public File snapshotWorkDir() {
        assert snpWorkDir != null;

        return snpWorkDir;
    }

    /**
     * @param snapshotName Unique snapshot name.
     * @return Future which will be completed when snapshot is done.
     * @throws IgniteCheckedException If initialiation fails.
     */
    public IgniteInternalFuture<String> createLocalSnapshot(
        String snapshotName,
        List<Integer> grpIds
    ) throws IgniteCheckedException {
        // Collection of pairs group and appropratate cache partition to be snapshotted.
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

        File snapshotDir0 = snapshotDir(snpWorkDir, snapshotName);

        return scheduleSnapshot(snapshotName,
            parts,
            snapshotDir0,
            snpRunner,
            localSnapshotReceiver(snapshotDir0));
    }

    /**
     * @param parts Collection of pairs group and appropratate cache partition to be snapshotted.
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

        File snapshotDir0 = snapshotDir(snpWorkDir, snapshotName);

        IgniteInternalFuture<?> fut = scheduleSnapshot(snapshotName,
            parts,
            snapshotDir0,
            new SerialExecutor(cctx.kernalContext()
                .pools()
                .poolForPolicy(plc)),
            remoteSnapshotReceiver(rmtNodeId, topic));

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
     * @param snpName Unique snapshot name.
     * @param parts Collection of pairs group and appropratate cache partition to be snapshotted.
     * @param snpDir Local directory to save cache partition deltas and snapshots to.
     * @param snpRcv Factory which produces snapshot receiver instance.
     * @return Future which will be completed when snapshot is done.
     * @throws IgniteCheckedException If initialiation fails.
     */
    IgniteInternalFuture<String> scheduleSnapshot(
        String snpName,
        Map<Integer, Set<Integer>> parts,
        File snpDir,
        Executor exec,
        SnapshotReceiver snpRcv
    ) throws IgniteCheckedException {
        if (snpCtxs.containsKey(snpName))
            throw new IgniteCheckedException("snapshot with requested name is already scheduled: " + snpName);

        SnapshotContext bctx = null;

        try {
            // Atomic operation, fails with exception if not.
            Files.createDirectory(snpDir.toPath());

            bctx = new SnapshotContext(snpName,
                snpDir,
                parts,
                exec,
                snpRcv);

            final SnapshotContext bctx0 = bctx;

            bctx.snpFut.listen(f -> {
                snpCtxs.remove(snpName);

                closeSnapshotResources(bctx0);
            });

            for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet()) {
                final CacheGroupContext gctx = cctx.cache().cacheGroup(e.getKey());

                // Create cache snapshot directory if not.
                File grpDir = U.resolveWorkDirectory(bctx.snpDir.getAbsolutePath(),
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
                            bctx.snpFut,
                            getPartionDeltaFile(grpDir, partId),
                            ioFactory,
                            pageSize));
                }
            }

            SnapshotContext ctx0 = snpCtxs.putIfAbsent(snpName, bctx);

            assert ctx0 == null : ctx0;

            CheckpointFuture cpFut = dbMgr.forceCheckpoint(String.format(SNAPSHOT_CP_REASON, snpName));

            SnapshotContext finalBctx = bctx;

            cpFut.finishFuture()
                .listen(f -> {
                    if (f.error() == null)
                        finalBctx.cpEndFut.complete(true);
                    else
                        finalBctx.cpEndFut.completeExceptionally(f.error());
                });

            cpFut.beginFuture()
                .get();

            U.log(log, "snapshot operation scheduled with the following context: " + bctx);
        }
        catch (IOException e) {
            closeSnapshotResources(bctx);

            try {
                Files.delete(snpDir.toPath());
            }
            catch (IOException ioe) {
                throw new IgniteCheckedException("Error deleting snapshot directory during context initialization " +
                    "failed: " + snpName, e);
            }

            throw new IgniteCheckedException(e);
        }

        return bctx.snpFut;
    }

    /**
     *
     * @param snapshotDir Snapshot directory.
     * @return Snapshot receiver instance.
     */
    SnapshotReceiver localSnapshotReceiver(File snapshotDir) {
        return new LocalSnapshotReceiver(log,
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
    SnapshotReceiver remoteSnapshotReceiver(UUID rmtNodeId, Object topic) {
        return new RemoteSnapshotReceiver(log, cctx.gridIO().openTransmissionSender(rmtNodeId, topic));
    }

    /**
     * @return The executor service used to run snapshot tasks.
     */
    ExecutorService snapshotExecutorService() {
        assert snpRunner != null;

        return snpRunner;
    }

    /**
     * @param sctx Context to clouse all resources.
     */
    private void closeSnapshotResources(SnapshotContext sctx) {
        if (sctx == null)
            return;

        for (PageStoreSerialWriter writer : sctx.partDeltaWriters.values())
            U.closeQuiet(writer);

        U.closeQuiet(sctx.snpRcv);
    }

    /**
     * @param bctx Context to handle.
     */
    private void submitTasks(SnapshotContext bctx) {
        List<CompletableFuture<Void>> futs = new ArrayList<>(bctx.parts.size());
        File workDir = ((FilePageStoreManager) cctx.pageStore()).workDir();

        U.log(log, "Partition allocated lengths: " + bctx.partFileLengths);

        for (GroupPartitionId pair : bctx.parts) {
            CacheConfiguration ccfg = cctx.cache().cacheGroup(pair.getGroupId()).config();
            String cacheDirName = cacheDirName(ccfg);

            CompletableFuture<Void> fut0 = CompletableFuture.runAsync(() ->
                    bctx.snpRcv.receivePart(
                        getPartitionFileEx(
                            workDir,
                            cacheDirName,
                            pair.getPartitionId()),
                        cacheDirName,
                        pair,
                        bctx.partFileLengths.get(pair)),
                bctx.exec)
                .thenRun(() -> bctx.partDeltaWriters.get(pair).partProcessed = true)
                // Wait for the completion of both futures - checkpoint end, copy partition
                .runAfterBothAsync(bctx.cpEndFut,
                    () -> bctx.snpRcv.receiveDelta(
                        getPartionDeltaFile(
                            cacheWorkDir(bctx.snpDir, cacheDirName),
                            pair.getPartitionId()),
                        cacheDirName,
                        pair),
                    bctx.exec);

            futs.add(fut0);
        }

        CompletableFuture.allOf(futs.toArray(new CompletableFuture[bctx.parts.size()]))
             .whenComplete(new BiConsumer<Void, Throwable>() {
                 @Override public void accept(Void res, Throwable t) {
                     if (t == null)
                         bctx.snpFut.onDone(bctx.snpName);
                     else
                         bctx.snpFut.onDone(t);
                 }
             });
    }

    /**
     * @param snpName Unique snapshot name.
     */
    public void stopCacheSnapshot(String snpName) {

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

        /** If snapshot has been stopped due to an error. */
        private final GridFutureAdapter<?> snpFut;

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
            GridFutureAdapter<?> snpFut,
            File cfgFile,
            FileIOFactory factory,
            int pageSize
        ) throws IOException {
            this.checkpointComplete = checkpointComplete;
            this.snpFut = snpFut;
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
            return (checkpointComplete.getAsBoolean() && partProcessed) || snpFut.isDone();
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

                    // Page out of snapshot scope.
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
                snpFut.onDone(t);
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
    private static class SnapshotContext {
        /** Unique identifier of snapshot process. */
        private final String snpName;

        /** Absolute snapshot storage path. */
        private final File snpDir;

        /** Service to perform partitions copy. */
        private final Executor exec;

        /**
         * The length of file size per each cache partiton file.
         * Partition has value greater than zero only for partitons in OWNING state.
         * Information collected under checkpoint write lock.
         */
        private final Map<GroupPartitionId, Long> partFileLengths = new HashMap<>();

        /**
         * Map of partitions to snapshot and theirs corresponding delta PageStores.
         * Writers are pinned to the snapshot context due to controlling partition
         * processing supplier.
         */
        private final Map<GroupPartitionId, PageStoreSerialWriter> partDeltaWriters = new HashMap<>();

        /** Future of result completion. */
        @GridToStringExclude
        private final GridFutureAdapter<String> snpFut = new GridFutureAdapter<>();

        /** Snapshot data receiver. */
        @GridToStringExclude
        private final SnapshotReceiver snpRcv;

        /** Collection of partition to be snapshotted. */
        private final List<GroupPartitionId> parts = new ArrayList<>();

        /** Checkpoint end future. */
        private final CompletableFuture<Boolean> cpEndFut = new CompletableFuture<>();

        /** Flag idicates that this snapshot is start copying partitions. */
        private volatile boolean started;

        /**
         * @param snpName Unique identifier of snapshot process.
         * @param snpDir snapshot storage directory.
         * @param exec Service to perform partitions copy.
         */
        public SnapshotContext(
            String snpName,
            File snpDir,
            Map<Integer, Set<Integer>> parts,
            Executor exec,
            SnapshotReceiver snpRcv
        ) {
            A.notNull(snpName, "snapshot name cannot be empty or null");
            A.notNull(snpDir, "You must secify correct snapshot directory");
            A.ensure(snpDir.isDirectory(), "Specified path is not a directory");
            A.notNull(exec, "Executor service must be not null");
            A.notNull(snpRcv, "Snapshot receiver which handles execution tasks must be not null");

            this.snpName = snpName;
            this.snpDir = snpDir;
            this.exec = exec;
            this.snpRcv = snpRcv;

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

            SnapshotContext ctx = (SnapshotContext)o;

            return snpName.equals(ctx.snpName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(snpName);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SnapshotContext.class, this);
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
    private static class RemoteSnapshotReceiver implements SnapshotReceiver {
        /** Ignite logger to use. */
        private final IgniteLogger log;

        /** The sender which sends files to remote node. */
        private final GridIoManager.TransmissionSender sndr;

        /**
         * @param log Ignite logger.
         * @param sndr File sender instance.
         */
        public RemoteSnapshotReceiver(
            IgniteLogger log,
            GridIoManager.TransmissionSender sndr
        ) {
            this.log = log.getLogger(RemoteSnapshotReceiver.class);
            this.sndr = sndr;
        }

        /** {@inheritDoc} */
        @Override public void receivePart(File part, String cacheDirName, GroupPartitionId pair, Long length) {
            try {
                sndr.send(part, 0, length, transmissionParams(cacheDirName, pair), TransmissionPolicy.FILE);

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
        @Override public void receiveDelta(File delta, String cacheDirName, GroupPartitionId pair) {
            try {

                sndr.send(delta, transmissionParams(cacheDirName, pair), TransmissionPolicy.CHUNK);

                if (log.isInfoEnabled())
                    log.info("Delta pages storage has been send [part=" + delta.getName() + ", pair=" + pair + ']');
            }
            catch (IgniteCheckedException | InterruptedException | IOException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * @param cacheDirName Cache directory name.
         * @param pair Cache group id with corresponding partition id.
         * @return Map of params.
         */
        private Map<String, Serializable> transmissionParams(String cacheDirName, GroupPartitionId pair) {
            Map<String, Serializable> params = new HashMap<>();

            params.put(String.valueOf(pair.getGroupId()), String.valueOf(pair.getGroupId()));
            params.put(String.valueOf(pair.getPartitionId()), String.valueOf(pair.getPartitionId()));
            params.put(cacheDirName, cacheDirName);

            return params;
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            U.closeQuiet(sndr);
        }
    }

    /**
     *
     */
    private static class LocalSnapshotReceiver implements SnapshotReceiver {
        /** Ignite logger to use. */
        private final IgniteLogger log;

        /** Local node snapshot directory. */
        private final File snpDir;

        /** Facotry to produce IO interface over a file. */
        private final FileIOFactory ioFactory;

        /** Factory to create page store for restore. */
        private final FilePageStoreFactory storeFactory;

        /** Size of page. */
        private final int pageSize;

        /**
         * @param log Ignite logger to use.
         * @param snpDir Local node snapshot directory.
         * @param ioFactory Facotry to produce IO interface over a file.
         * @param storeFactory Factory to create page store for restore.
         * @param pageSize Size of page.
         */
        public LocalSnapshotReceiver(
            IgniteLogger log,
            File snpDir,
            FileIOFactory ioFactory,
            FilePageStoreFactory storeFactory,
            int pageSize
        ) {
            this.log = log.getLogger(LocalSnapshotReceiver.class);
            this.snpDir = snpDir;
            this.ioFactory = ioFactory;
            this.storeFactory = storeFactory;
            this.pageSize = pageSize;
        }

        /** {@inheritDoc} */
        @Override public void receivePart(File part, String cacheDirName, GroupPartitionId pair, Long length) {
            File snpPart = new File(cacheWorkDir(snpDir, cacheDirName), part.getName());

            try {
                if (!snpPart.exists() || snpPart.delete())
                    snpPart.createNewFile();

                if (length == 0)
                    return;

                try (FileIO src = ioFactory.create(part);
                     FileChannel dest = new FileOutputStream(snpPart).getChannel()) {
                    src.position(0);

                    long written = 0;

                    while (written < length)
                        written += src.transferTo(written, length - written, dest);
                }

                if (log.isInfoEnabled()) {
                    log.info("Partition has been snapshotted [snapshotDir=" + snpDir.getAbsolutePath() +
                        ", cacheDirName=" + cacheDirName + ", part=" + part.getName() +
                        ", length=" + part.length() + ", snapshot=" + snpPart.getName() + ']');
                }
            }
            catch (IOException ex) {
                throw new IgniteException(ex);
            }
        }

        /** {@inheritDoc} */
        @Override public void receiveDelta(File delta, String cacheDirName, GroupPartitionId pair) {
            File snpPart = getPartitionFileEx(snpDir, cacheDirName, pair.getPartitionId());

            U.log(log, "Start partition snapshot recovery with the given delta page file [part=" + snpPart +
                ", delta=" + delta + ']');

            try (FileIO fileIo = ioFactory.create(delta, READ);
                 FilePageStore store = (FilePageStore)storeFactory
                     .createPageStore(pair.partType(),
                         snpPart::toPath,
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
