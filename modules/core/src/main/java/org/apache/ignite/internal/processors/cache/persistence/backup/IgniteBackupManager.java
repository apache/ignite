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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
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

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
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
    private File backupDir;

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
     * @param dir Backup directory to store files.
     * @param partId Cache partition identifier.
     * @return A file representation.
     */
    public static File getPartionDeltaFile(File dir, int partId) {
        return new File(dir, getPartitionDeltaFileName(partId));
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

        backupDir = U.resolveWorkDirectory(((FilePageStoreManager)store).workDir().getAbsolutePath(),
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
                        bctx0.result.onDone(e);
                    }
                }

                // Remove not used delta stores.
                for (List<PageStoreSerialWriter> list0 : partWriters.values())
                    list0.removeIf(PageStoreSerialWriter::stopped);
            }

            @Override public void onCheckpointBegin(Context ctx) {
                final FilePageStoreManager storeMgr = (FilePageStoreManager)cctx.pageStore();

                for (BackupContext bctx0 : backupCtxs.values()) {
                    if (bctx0.started || bctx0.result.isDone())
                        continue;

                    // Submit all tasks for partitions and deltas processing.
                    submitTasks(bctx0, storeMgr.workDir());

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
     * @param name Unique backup name.
     * @param parts Collection of pairs group and appropratate cache partition to be backuped.
     * @param dir Local directory to save cache partition deltas to.
     * @return Future which will be completed when backup is done.
     * @throws IgniteCheckedException If initialiation fails.
     */
    public IgniteInternalFuture<?> createLocalBackup(
        String name,
        Map<Integer, Set<Integer>> parts,
        File dir
    ) throws IgniteCheckedException {
        return scheduleBackup(name, parts, dir, backupRunner, partWorkerFactory(), deltaWorkerFactory());
    }

    /**
     * @param name Unique backup name.
     * @param parts Collection of pairs group and appropratate cache partition to be backuped.
     * @param remoteId The remote node to connect to.
     * @param topic The remote topic to connect to.
     * @throws IgniteCheckedException If initialiation fails.
     */
    public void sendBackup(
        String name,
        Map<Integer, Set<Integer>> parts,
        ExecutorService execSvc,
        UUID remoteId,
        Object topic
    ) throws IgniteCheckedException {
        File backupDir0 = new File(backupDir, name);

        GridIoManager.TransmissionSender sndr = cctx.gridIO().openTransmissionSender(remoteId, topic);

        IgniteInternalFuture<?> fut = scheduleBackup(name,
            parts,
            backupDir0,
            execSvc,
            partSenderFactory(sndr),
            deltaSenderFactory(sndr));

        fut.listen(f -> {
            if (log.isInfoEnabled()) {
                log.info("The requested bakcup has been send [result=" + (f.error() == null) +
                    ", name=" + name + ']');
            }
        });
    }

    /**
     * @param name Unique backup name.
     * @param parts Collection of pairs group and appropratate cache partition to be backuped.
     * @param dir Local directory to save cache partition deltas to.
     * @param partWorkerFactory Factory which produces partition suppliers.
     * @param deltaWorkerFactory Factory which produces partition delta suppliers.
     * @return Future which will be completed when backup is done.
     * @throws IgniteCheckedException If initialiation fails.
     */
    IgniteInternalFuture<?> scheduleBackup(
        String name,
        Map<Integer, Set<Integer>> parts,
        File dir,
        ExecutorService execSvc,
        Supplier<IgniteTriConsumer<File, File, Long>> partWorkerFactory,
        Supplier<BiConsumer<File, GroupPartitionId>> deltaWorkerFactory
    ) throws IgniteCheckedException {
        if (backupCtxs.containsKey(name))
            throw new IgniteCheckedException("Backup with requested name is already scheduled: " + name);

        BackupContext bctx = null;
        File backupDir0 = new File(dir, name);

        try {
            // Atomic operation, fails with exception if not.
            Files.createDirectory(backupDir0.toPath());

            bctx = new BackupContext(name,
                backupDir0,
                parts,
                execSvc,
                partWorkerFactory,
                deltaWorkerFactory);

            for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet()) {
                final CacheGroupContext gctx = cctx.cache().cacheGroup(e.getKey());

                // Create cache backup directory if not.
                File grpDir = U.resolveWorkDirectory(bctx.backupDir.getAbsolutePath(),
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
                            bctx.result,
                            () -> getPartionDeltaFile(grpDir, partId).toPath(),
                            ioFactory,
                            pageSize));
                }
            }

            BackupContext ctx0 = backupCtxs.putIfAbsent(name, bctx);

            assert ctx0 == null : ctx0;

            CheckpointFuture cpFut = dbMgr.forceCheckpoint(String.format(BACKUP_CP_REASON, name));

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
                Files.delete(backupDir0.toPath());
            }
            catch (IOException ioe) {
                throw new IgniteCheckedException("Error deleting backup directory during context initialization " +
                    "failed: " + name, e);
            }

            throw new IgniteCheckedException(e);
        }

        return bctx.result;
    }

    /**
     * @return Partition supplier factory.
     */
    Supplier<IgniteTriConsumer<File, File, Long>> partWorkerFactory() {
        return () -> new PartitionCopyConsumer(ioFactory, log);
    }

    /**
     * @return Factory which procudes workers for backup partition recovery.
     */
    Supplier<BiConsumer<File, GroupPartitionId>> deltaWorkerFactory() {
        return () -> new BiConsumer<File, GroupPartitionId>() {
            @Override public void accept(File dir, GroupPartitionId pair) {
                File delta = getPartionDeltaFile(dir, pair.getPartitionId());

                partitionRecovery(getPartitionFileEx(dir, pair.getPartitionId()),delta);

                delta.delete();
            }
        };
    }

    /**
     * @return Factory which procudes senders of partition files.
     */
    Supplier<IgniteTriConsumer<File, File, Long>> partSenderFactory(GridIoManager.TransmissionSender sndr) {
        return () -> new PartitionCopyConsumer(ioFactory, log);
    }

    /**
     * @return Factory which procudes senders of partition deltas.
     */
    Supplier<BiConsumer<File, GroupPartitionId>> deltaSenderFactory(GridIoManager.TransmissionSender sndr) {
        return () -> new BiConsumer<File, GroupPartitionId>() {
            @Override public void accept(File dir, GroupPartitionId pair) {
                File delta = getPartionDeltaFile(dir, pair.getPartitionId());

                partitionRecovery(getPartitionFileEx(dir, pair.getPartitionId()),delta);

                delta.delete();
            }
        };
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
    private static void closeBackupResources(BackupContext bctx) {
        if (bctx == null)
            return;

        for (PageStoreSerialWriter writer : bctx.partDeltaWriters.values())
            U.closeQuiet(writer);
    }

    /**
     * @param bctx Context to handle.
     */
    private void submitTasks(BackupContext bctx, File workDir) {
        List<CompletableFuture<Void>> futs = new ArrayList<>(bctx.parts.size());

        U.log(log, "Partition allocated lengths: " + bctx.partFileLengths);

        for (GroupPartitionId pair : bctx.parts) {
            CacheConfiguration ccfg = cctx.cache().cacheGroup(pair.getGroupId()).config();
            File cacheBackupDir = new File(bctx.backupDir, cacheDirName(ccfg));

            CompletableFuture<Void> fut0 = CompletableFuture.runAsync(() ->
                    bctx.partWorkerFactory.get()
                        .accept(getPartitionFileEx(cacheWorkDir(workDir, ccfg), pair.getPartitionId()),
                            cacheBackupDir,
                            bctx.partFileLengths.get(pair)),
                bctx.execSvc)
                .thenRun(() -> bctx.partDeltaWriters.get(pair).partProcessed = true)
                // Wait for the completion of both futures - checkpoint end, copy partition
                .runAfterBothAsync(bctx.cpEndFut,
                    () -> bctx.deltaWorkerFactory.get().accept(cacheBackupDir, pair),
                    bctx.execSvc);

            futs.add(fut0);
        }

        CompletableFuture.allOf(futs.toArray(new CompletableFuture[bctx.parts.size()]))
             .whenComplete(new BiConsumer<Void, Throwable>() {
                 @Override public void accept(Void res, Throwable t) {
                     if (t == null)
                         bctx.result.onDone();
                     else
                         bctx.result.onDone(t);
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
     * @param partStore Partition file previously backuped.
     * @param deltaStore File with delta pages.
     */
    public void partitionRecovery(File partStore, File deltaStore) {
        U.log(log, "Start partition backup recovery with the given delta page file [part=" + partStore +
            ", delta=" + deltaStore + ']');

        byte type = INDEX_FILE_NAME.equals(partStore.getName()) ? FLAG_IDX : FLAG_DATA;

        try (FileIO fileIo = ioFactory.create(deltaStore);
             FilePageStore store = (FilePageStore)((FilePageStoreManager)cctx.pageStore())
                 .getFilePageStoreFactory()
                 .createPageStore(type,
                     partStore::toPath,
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

                U.log(log, "Read page given delta file [path=" + deltaStore.getName() +
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

    /**
     *
     */
    private static class PartitionCopyConsumer implements IgniteTriConsumer<File, File, Long> {
        /** Ignite logger to use. */
        private final IgniteLogger log;

        /** Factory to produce IO channels. */
        private final FileIOFactory ioFactory;


        /**
         * @param log Ignite logger to use.
         */
        public PartitionCopyConsumer(FileIOFactory ioFactory, IgniteLogger log) {
            this.log = log.getLogger(PartitionCopyConsumer.class);
            this.ioFactory = ioFactory;
        }

        @Override public void accept(File part, File backupDir, Long length) {
            assert backupDir.isDirectory() : "Destination path must be a directory";

            File to = new File(backupDir, part.getName());

            try {
                if (!to.exists() || to.delete())
                    to.createNewFile();

                if (length == 0)
                    return;

                try (FileIO src = ioFactory.create(part);
                     FileChannel dest = new FileOutputStream(to).getChannel()) {
                    src.position(0);

                    long written = 0;

                    while (written < length)
                        written += src.transferTo(written, length - written, dest);
                }

                U.log(log, "Partition file has been copied [from=" + part.getAbsolutePath() +
                    ", fromSize=" + part.length() + ", to=" + to.getAbsolutePath() + ']');
            }
            catch (IOException ex) {
                throw new IgniteException(ex);
            }
        }
    }

    /**
     *
     */
    private static class PageStoreSerialWriter implements Closeable {
        /** Ignite logger to use. */
        @GridToStringExclude
        private final IgniteLogger log;

        /** Configuration file path provider. */
        private final Supplier<Path> cfgPath;

        /** Buse lock to perform write opertions. */
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        /** Local buffer to perpform copy-on-write operations. */
        private final ThreadLocal<ByteBuffer> localBuff;

        /** {@code true} if need the original page from PageStore instead of given buffer. */
        private final BooleanSupplier checkpointComplete;

        /** If backup has been stopped due to an error. */
        private final GridFutureAdapter<Void> backupFut;

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
         * @param cfgPath Configuration file path provider.
         * @param factory Factory to produce an IO interface over underlying file.
         */
        public PageStoreSerialWriter(
            IgniteLogger log,
            BooleanSupplier checkpointComplete,
            GridFutureAdapter<Void> backupFut,
            Supplier<Path> cfgPath,
            FileIOFactory factory,
            int pageSize
        ) throws IOException {
            this.checkpointComplete = checkpointComplete;
            this.backupFut = backupFut;
            this.log = log.getLogger(PageStoreSerialWriter.class);
            this.cfgPath = cfgPath;

            localBuff = ThreadLocal.withInitial(() ->
                ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder()));

            fileIo = factory.create(cfgPath.get().toFile());
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
                        ", part=" + cfgPath.get().toAbsolutePath() +
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
        /** Unique identifier of backup process. */
        private final String name;

        /** Absolute backup storage path. */
        private final File backupDir;

        /** Service to perform partitions copy. */
        private final ExecutorService execSvc;

        /**
         * The length of file size per each cache partiton file.
         * Partition has value greater than zero only for partitons in OWNING state.
         * Information collected under checkpoint write lock.
         */
        private final Map<GroupPartitionId, Long> partFileLengths = new HashMap<>();

        /**
         * Map of partitions to backup and theirs corresponding delta PageStores.
         * Writers are pinned to the backup context due to controlling partition
         * processing supplier (see {@link PartitionCopyConsumer}).
         */
        private final Map<GroupPartitionId, PageStoreSerialWriter> partDeltaWriters = new HashMap<>();

        /** Future of result completion. */
        @GridToStringExclude
        private final GridFutureAdapter<Void> result = new GridFutureAdapter<>();

        /** Factory to create executable tasks for partition processing. */
        @GridToStringExclude
        private final Supplier<IgniteTriConsumer<File, File, Long>> partWorkerFactory;

        /** Factory to create executable tasks for partition delta pages processing. */
        @GridToStringExclude
        private final Supplier<BiConsumer<File, GroupPartitionId>> deltaWorkerFactory;

        /** Collection of partition to be backuped. */
        private final List<GroupPartitionId> parts = new ArrayList<>();

        /** Checkpoint end future. */
        private final CompletableFuture<Boolean> cpEndFut = new CompletableFuture<>();

        /** Flag idicates that this backup is start copying partitions. */
        private volatile boolean started;

        /**
         * @param name Unique identifier of backup process.
         * @param backupDir Backup storage directory.
         * @param execSvc Service to perform partitions copy.
         * @param partWorkerFactory Factory to create executable tasks for partition processing.
         */
        public BackupContext(
            String name,
            File backupDir,
            Map<Integer, Set<Integer>> parts,
            ExecutorService execSvc,
            Supplier<IgniteTriConsumer<File, File, Long>> partWorkerFactory,
            Supplier<BiConsumer<File, GroupPartitionId>> deltaWorkerFactory
        ) {
            A.notNull(name, "Backup name cannot be empty or null");
            A.notNull(backupDir, "You must secify correct backup directory");
            A.ensure(backupDir.isDirectory(), "Specified path is not a directory");
            A.notNull(execSvc, "Executor service must be not null");
            A.notNull(partWorkerFactory, "Factory which procudes backup tasks to execute must be not null");
            A.notNull(deltaWorkerFactory, "Factory which processes delta pages storage must be not null");

            this.name = name;
            this.backupDir = backupDir;
            this.execSvc = execSvc;
            this.partWorkerFactory = partWorkerFactory;
            this.deltaWorkerFactory = deltaWorkerFactory;

            result.listen(f -> {
                if (f.error() != null)
                    closeBackupResources(this);
            });

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

            return name.equals(ctx.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(name);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(BackupContext.class, this);
        }
    }
}
