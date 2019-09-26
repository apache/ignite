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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointFuture;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PagesAllocationRange;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
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
    /** */
    public static final String DELTA_SUFFIX = ".delta";

    /** */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + DELTA_SUFFIX;

    /** */
    public static final String INDEX_DELTA_NAME = INDEX_FILE_NAME + DELTA_SUFFIX;

    /** */
    public static final String BACKUP_CP_REASON = "Wakeup for checkpoint to take backup [name=%s]";

    /** Prefix for backup threads. */
    private static final String BACKUP_RUNNER_THREAD_PREFIX = "backup-runner";

    /** Total number of thread to perform local backup. */
    private static final int BACKUP_POOL_SIZE = 4;

    /** Map of registered cache backup processes and their corresponding contexts. */
    private final ConcurrentMap<String, BackupContext> backupCtxs = new ConcurrentHashMap<>();

    /** All registered page writers of all running backup processes. */
    private final ConcurrentMap<GroupPartitionId, List<PageStoreSerialWriter>> partWriters = new ConcurrentHashMap<>();

    /** Factory to working with {@link FileDeltaPageStore} as file storage. */
    private volatile FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** Backup thread pool. */
    private IgniteThreadPoolExecutor backupRunner;

    /** Checkpoint listener to handle scheduled backup requests. */
    private DbCheckpointListener cpLsnr;

    /** Database manager for enabled persistence. */
    private GridCacheDatabaseSharedManager dbMgr;

    /** Configured data storage page size. */
    private int pageSize;

    //// BELOW IS NOT USED

    /** Keep only the first page error. */
    private final ConcurrentMap<GroupPartitionId, IgniteCheckedException> pageTrackErrors = new ConcurrentHashMap<>();

    /** */
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
        return createLocalBackup(name, parts, dir, this::partSupplierFactory, this::deltaSupplierFactory);
    }

    /**
     * @return Partition supplier factory.
     */
    Supplier<File> partSupplierFactory(File from, File to, long length) {
        return new PartitionCopySupplier(log, from, to, length);
    }

    /**
     * @return Delta supplier factory.
     */
    Supplier<File> deltaSupplierFactory(File from, FileDeltaPageStore delta) {
        return new PartitionDeltaSupplier(log,
            ((FilePageStoreManager)cctx.pageStore())
                .getFilePageStoreFactory(),
            from,
            delta);
    }

    /**
     * @param name Unique backup name.
     * @param parts Collection of pairs group and appropratate cache partition to be backuped.
     * @param dir Local directory to save cache partition deltas to.
     * @param partSuppFactory Factory which produces partition suppliers.
     * @param deltaSuppFactory Factory which produces partition delta suppliers.
     * @return Future which will be completed when backup is done.
     * @throws IgniteCheckedException If initialiation fails.
     */
    IgniteInternalFuture<?> createLocalBackup(
        String name,
        Map<Integer, Set<Integer>> parts,
        File dir,
        IgniteTriClosure<File, File, Long, Supplier<File>> partSuppFactory,
        IgniteBiClosure<File, FileDeltaPageStore, Supplier<File>> deltaSuppFactory
    ) throws IgniteCheckedException {
        if (backupCtxs.containsKey(name))
            throw new IgniteCheckedException("Backup with requested name is already scheduled: " + name);

        BackupContext bctx = null;
        File backupDir = new File(dir, name);

        try {
            // Atomic operation, fails with exception if not.
            Files.createDirectory(backupDir.toPath());

            bctx = new BackupContext(name,
                backupDir,
                parts,
                backupRunner,
                partSuppFactory,
                deltaSuppFactory);

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
                        new PageStoreSerialWriter(
                            new FileDeltaPageStore(log,
                                () -> getPartionDeltaFile(grpDir, partId).toPath(),
                                ioFactory,
                                pageSize),
                            () -> cpEndFut0.isDone() && !cpEndFut0.isCompletedExceptionally(),
                            bctx.result,
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
                Files.delete(backupDir.toPath());
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
    private void submitTasks(BackupContext bctx, File cacheWorkDir) {
        List<CompletableFuture<File>> futs = new ArrayList<>(bctx.parts.size());

        U.log(log, "Partition allocated lengths: " + bctx.partFileLengths);

        for (GroupPartitionId pair : bctx.parts) {
            CacheConfiguration ccfg = cctx.cache().cacheGroup(pair.getGroupId()).config();

            CompletableFuture<File> fut0 = CompletableFuture.supplyAsync(
                bctx.partSuppFactory.apply(
                    getPartitionFileEx(
                        cacheWorkDir(cacheWorkDir, ccfg),
                        pair.getPartitionId()),
                    new File(bctx.backupDir,
                        cacheDirName(ccfg)),
                    bctx.partFileLengths.get(pair)),
                bctx.execSvc)
                .thenApply(file -> {
                    bctx.partDeltaWriters.get(pair).partProcessed = true;

                    return file;
                })
                .thenCombineAsync(bctx.cpEndFut,
                    (from, res) -> {
                        assert res;

                        // Call the factory which creates tasks for page delta processing.
                        return bctx.deltaSuppFactory.apply(from,
                            bctx.partDeltaWriters
                                .get(pair)
                                .deltaStore)
                            .get();
                    },
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
     *
     */
    private static class PartitionDeltaSupplier implements Supplier<File> {
        /** Ignite logger to use. */
        private final IgniteLogger log;

        /** File page store factory */
        private final FilePageStoreFactory factory;

        /** Copied partition file to apply delta pages to. */
        private final File from;

        /** Delta pages storage for the given partition. */
        private final FileDeltaPageStore deltaStore;

        /**
         * @param deltaStore Storage with delta pages.
         */
        public PartitionDeltaSupplier(
            IgniteLogger log,
            FilePageStoreFactory factory,
            File from,
            FileDeltaPageStore deltaStore
        ) {
            this.log = log.getLogger(PartitionDeltaSupplier.class);
            this.factory = factory;
            this.from = from;
            this.deltaStore = deltaStore;
        }

        /** {@inheritDoc} */
        @Override public File get() {
            try {
                byte type = INDEX_FILE_NAME.equals(from.getName()) ? FLAG_IDX : FLAG_DATA;

                FilePageStore store = (FilePageStore)factory.createPageStore(type,
                    from::toPath,
                    new LongAdderMetric("NO_OP", null));

                store.doRecover(deltaStore);

                U.log(log, "Partition delta storage applied to: " + from.getName());

                deltaStore.delete();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            return from;
        }
    }

    /**
     *
     */
    private static class PartitionCopySupplier implements Supplier<File> {
        /** Ignite logger to use. */
        private final IgniteLogger log;

        /** Partition file. */
        private final File from;

        /** Destination copy file to copy partition to. */
        private final File to;

        /** Size of partition. */
        private final long length;

        /**
         * @param log Ignite logger to use.
         * @param from Partition file.
         * @param dir Destination copy file.
         * @param length Size of partition.
         */
        public PartitionCopySupplier(
            IgniteLogger log,
            File from,
            File dir,
            long length
        ) {
            A.ensure(dir.isDirectory(), "Destination path must be a directory");

            this.log = log.getLogger(PartitionCopySupplier.class);
            this.from = from;
            this.length = length;
            to = new File(dir, from.getName());
        }

        /** {@inheritDoc} */
        @Override public File get() {
            try {
                if (!to.exists() || to.delete())
                    to.createNewFile();

                if (length == 0)
                    return to;

                try (FileChannel src = new FileInputStream(from).getChannel();
                     FileChannel dest = new FileOutputStream(to).getChannel()) {
                    src.position(0);

                    long written = 0;

                    while (written < length)
                        written += src.transferTo(written, length - written, dest);
                }

                U.log(log, "Partition file has been copied [from=" + from.getAbsolutePath() +
                    ", fromSize=" + from.length() + ", to=" + to.getAbsolutePath() + ']');
            }
            catch (IOException ex) {
                throw new IgniteException(ex);
            }

            return to;
        }
    }

    /**
     *
     */
    private static class PageStoreSerialWriter implements Closeable {
        /** Storage to write pages to. */
        private final FileDeltaPageStore deltaStore;

        /** Local buffer to perpform copy-on-write operations. */
        private final ThreadLocal<ByteBuffer> localBuff;

        /** {@code true} if need the original page from PageStore instead of given buffer. */
        private final BooleanSupplier checkpointComplete;

        /** If backup has been stopped due to an error. */
        private final GridFutureAdapter<Void> backupFut;

        /** {@code true} if current writer is stopped. */
        private volatile boolean partProcessed;

        /**
         * Array of bits. 1 - means pages written, 0 - the otherwise.
         * Size of array can be estimated only under checkpoint write lock.
         */
        private volatile AtomicIntegerArray pagesWrittenBits;

        /**
         * @param deltaStore Serial storage to write to.
         * @param checkpointComplete Checkpoint finish flag.
         * @param pageSize Size of page to use for local buffer.
         */
        public PageStoreSerialWriter(
            FileDeltaPageStore deltaStore,
            BooleanSupplier checkpointComplete,
            GridFutureAdapter<Void> backupFut,
            int pageSize
        ) {
            this.deltaStore = deltaStore;
            this.checkpointComplete = checkpointComplete;
            this.backupFut = backupFut;

            localBuff = ThreadLocal.withInitial(() ->
                ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder()));
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

                    deltaStore.writePage(pageId, locBuf);
                }
                else {
                    // Direct buffre is needs to be written, associated checkpoint not finished yet.
                    deltaStore.writePage(pageId, buf);

                    buf.rewind();
                }
            }
            catch (Throwable t) {
                backupFut.onDone(t);
            }
        }

        /** {@inheritDoc} */
        @Override public void close() {
            U.closeQuiet(deltaStore);
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
         * processing supplier (see {@link PartitionCopySupplier}).
         */
        private final Map<GroupPartitionId, PageStoreSerialWriter> partDeltaWriters = new HashMap<>();

        /** Future of result completion. */
        @GridToStringExclude
        private final GridFutureAdapter<Void> result = new GridFutureAdapter<>();

        /** Factory to create executable tasks for partition processing. */
        @GridToStringExclude
        private final IgniteTriClosure<File, File, Long, Supplier<File>> partSuppFactory;

        /** Factory to create executable tasks for partition delta pages processing. */
        @GridToStringExclude
        private final IgniteBiClosure<File, FileDeltaPageStore, Supplier<File>> deltaSuppFactory;

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
         * @param partSuppFactory Factory to create executable tasks for partition processing.
         */
        public BackupContext(
            String name,
            File backupDir,
            Map<Integer, Set<Integer>> parts,
            ExecutorService execSvc,
            IgniteTriClosure<File, File, Long, Supplier<File>> partSuppFactory,
            IgniteBiClosure<File, FileDeltaPageStore, Supplier<File>> deltaSuppFactory
        ) {
            A.notNull(name, "Backup name cannot be empty or null");
            A.notNull(backupDir, "You must secify correct backup directory");
            A.ensure(backupDir.isDirectory(), "Specified path is not a directory");
            A.notNull(execSvc, "Executor service must be not null");
            A.notNull(partSuppFactory, "Factory which procudes backup tasks to execute must be not null");
            A.notNull(deltaSuppFactory, "Factory which processes delta pages storage must be not null");

            this.name = name;
            this.backupDir = backupDir;
            this.execSvc = execSvc;
            this.partSuppFactory = partSuppFactory;
            this.deltaSuppFactory = deltaSuppFactory;

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
