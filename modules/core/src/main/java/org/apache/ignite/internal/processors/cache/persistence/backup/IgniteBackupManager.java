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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.store.PageStoreListener;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointFuture;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PagesAllocationRange;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.GridBusyLock;
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
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;

/** */
public class IgniteBackupManager extends GridCacheSharedManagerAdapter {
    /** */
    public static final String DELTA_SUFFIX = ".delta";

    /** */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + DELTA_SUFFIX;

    /** */
    public static final String BACKUP_CP_REASON = "Wakeup for checkpoint to take backup [name=%s]";

    /** Prefix for backup threads. */
    private static final String BACKUP_RUNNER_THREAD_PREFIX = "backup-runner";

    /** Total number of thread to perform local backup. */
    private static final int BACKUP_POOL_SIZE = 4;

    /** Factory to working with {@link DeltaPagesStorage} as file storage. */
    private static final FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** Read-write lock to handle managers operations. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** Map of registered cache backup processes and their corresponding contexts. */
    private final ConcurrentMap<String, BackupContext> backupCtxs = new ConcurrentHashMap<>();

    /** TODO: CAS on list with temporary page stores */
    private final ConcurrentMap<GroupPartitionId, List<DeltaPagesStorage>> processingParts = new ConcurrentHashMap<>();

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

    /** Base working directory for saving copied pages. */
    private File backupWorkDir;

    /** */
    public IgniteBackupManager(GridKernalContext ctx) throws IgniteCheckedException {
        assert CU.isPersistenceEnabled(ctx.config());

    }

    /**
     * @param tmpDir Temporary directory to store files.
     * @param partId Cache partition identifier.
     * @return A file representation.
     */
    private static File getPartionDeltaFile(File tmpDir, int partId) {
        return new File(tmpDir, String.format(PART_DELTA_TEMPLATE, partId));
    }

    /**
     * @param ccfg Cache configuration.
     * @param partId Partiton identifier.
     * @return The cache partiton file.
     */
    private static File resolvePartitionFileCfg(
        FilePageStoreManager storeMgr,
        CacheConfiguration ccfg,
        int partId
    ) {
        File cacheDir = storeMgr.cacheWorkDir(ccfg);

        return getPartitionFile(cacheDir, partId);
    }

    /**
     * @param ccfg Cache configuration.
     * @param partId Partiton identifier.
     * @return The cache partiton delta file.
     */
    private File resolvePartitionDeltaFileCfg(CacheConfiguration ccfg, int partId) {
        File cacheTempDir = cacheWorkDir(backupWorkDir, ccfg);

        return getPartionDeltaFile(cacheTempDir, partId);
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        backupWorkDir = U.resolveWorkDirectory(cctx.kernalContext().config().getWorkDirectory(),
            DataStorageConfiguration.DFLT_BACKUP_DIRECTORY,
            true);

        U.ensureDirectory(backupWorkDir, "backup store working directory", log);

        pageSize = cctx.kernalContext().config().getDataStorageConfiguration().getPageSize();

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
                // Under the write lock here. It's safe to add new stores
                for (BackupContext bctx0 : backupCtxs.values()) {
                    if (bctx0.started)
                        continue;

                    for (Map.Entry<GroupPartitionId, DeltaPagesStorage> e : bctx0.partDeltaStores.entrySet()) {
                        processingParts.computeIfAbsent(e.getKey(), p -> new LinkedList<>())
                            .add(e.getValue());
                    }
                }

                // Remove not used delta stores.
                for (List<DeltaPagesStorage> list0 : processingParts.values())
                    list0.removeIf(store -> !store.writable());
            }

            @Override public void onCheckpointBegin(Context ctx) {
                final FilePageStoreManager pageMgr = (FilePageStoreManager)cctx.pageStore();

                for (BackupContext bctx0 : backupCtxs.values()) {
                    if (bctx0.started)
                        continue;

                    try {
                        PartitionAllocationMap allocationMap = ctx.partitionStatMap();
                        allocationMap.prepareForSnapshot();

                        assert !allocationMap.isEmpty() : "Partitions statistics has not been gathered: " + bctx0;

                        for (GroupPartitionId pair : bctx0.partAllocLengths.keySet()) {
                            PagesAllocationRange allocRange = allocationMap.get(pair);

                            assert allocRange != null : "Pages not allocated [pairId=" + pair + ", ctx=" + bctx0 + ']';

                            PageStore store = pageMgr.getStore(pair.getGroupId(), pair.getPartitionId());

                            bctx0.partAllocLengths.put(pair,
                                allocRange.getCurrAllocatedPageCnt() == 0 ? 0L :
                                    (long)allocRange.getCurrAllocatedPageCnt() * pageSize + store.headerSize());
                        }

                        submitTasks(bctx0, pageMgr);

                        bctx0.started = true;
                    }
                    catch (IgniteCheckedException e) {
                        bctx0.result.onDone(e);
                    }
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        dbMgr.removeCheckpointListener(cpLsnr);

        for (Collection<DeltaPagesStorage> deltas : processingParts.values()) {
            for (DeltaPagesStorage s : deltas)
                U.closeQuiet(s);
        }

        processingParts.clear();
        backupRunner.shutdown();
    }

    /**
     * @param name Unique backup name.
     * @param parts Collection of pairs group and appropratate cache partition to be backuped.
     * @param dir Local directory to save cache partition deltas to.
     * @return Future which will be completed when backup is done.
     * @throws IgniteCheckedException If initialiation fails.
     * @throws IOException If fails.
     */
    public IgniteInternalFuture<?> createLocalBackup(
        String name,
        Map<Integer, Set<Integer>> parts,
        File dir
    ) throws IgniteCheckedException, IOException {
        if (backupCtxs.containsKey(name))
            throw new IgniteCheckedException("Backup with requested name is already scheduled: " + name);

        File backupDir = new File(dir, name);

        // Atomic operation, fails with ex if not.
        Files.createDirectory(backupDir.toPath());

        final BackupContext bctx0 = new BackupContext(name,
            backupDir,
            parts,
            backupRunner,
            (from, to, partSize) ->
                new PartitionCopySupplier(log, from, to, partSize),
            (from, delta) ->
                new PartitionDeltaSupplier(log,
                    ((FilePageStoreManager)cctx.pageStore())
                        .getFilePageStoreFactory(),
                    from,
                    delta));

        // Stop all corresponding storages.
        bctx0.cpEndFut.thenRun(() -> {
            for (DeltaPagesStorage s : bctx0.partDeltaStores.values())
                s.writable(false);

            U.log(log, "All partition delta storages are closed to write after checkpoint finished");
        });

        try {
            for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet()) {
                final CacheGroupContext gctx = cctx.cache().cacheGroup(e.getKey());

                // Create cache backup directory if not.
                File grpDir = U.resolveWorkDirectory(bctx0.backupDir.getAbsolutePath(),
                    cacheDirName(gctx.config()), false);

                U.ensureDirectory(grpDir,
                    "temporary directory for cache group: " + gctx.groupId(),
                    null);

                for (int partId : e.getValue()) {
                    final GroupPartitionId pair = new GroupPartitionId(e.getKey(), partId);

                    bctx0.partAllocLengths.put(pair, 0L);
                    bctx0.partDeltaStores.put(pair,
                        new DeltaPagesStorage(log,
                            () -> getPartionDeltaFile(grpDir, partId)
                                .toPath(),
                            ioFactory,
                            cctx.gridConfig()
                                .getDataStorageConfiguration()
                                .getPageSize())
                            .init());
                }
            }
        }
        catch (IgniteCheckedException e) {
            try {
                Files.delete(bctx0.backupDir.toPath());
            }
            catch (IOException ioe) {
                throw new IgniteCheckedException("Error deleting backup directory during context initialization " +
                    "failed: " + name, e);
            }

            bctx0.result.onDone(e);

            throw e;
        }

        BackupContext tctx = backupCtxs.putIfAbsent(name, bctx0);

        assert tctx == null : tctx;

        CheckpointFuture cpFut = dbMgr.forceCheckpoint(String.format(BACKUP_CP_REASON, name));

        cpFut.finishFuture()
            .listen(f -> {
                if (f.error() == null)
                    bctx0.cpEndFut.complete(true);
                else
                    bctx0.cpEndFut.completeExceptionally(f.error());
            });

        cpFut.beginFuture()
            .get();

        U.log(log, "Backup operation scheduled with the following context: " + bctx0);

        return bctx0.result;
    }

    /**
     * @param bctx Context to clouse all resources.
     */
    private static void closeBackupResources(BackupContext bctx) {
        if (bctx == null)
            return;

        for (DeltaPagesStorage storage : bctx.partDeltaStores.values())
            U.closeQuiet(storage);
    }

    /**
     * @param bctx Context to handle.
     */
    private void submitTasks(BackupContext bctx, FilePageStoreManager pageMgr) throws IgniteCheckedException {
        List<CompletableFuture<File>> futs = new ArrayList<>(bctx.partAllocLengths.size());

        U.log(log, "Partition allocated lengths: " + bctx.partAllocLengths);

        for (Map.Entry<GroupPartitionId, Long> e : bctx.partAllocLengths.entrySet()) {
            GroupPartitionId pair = e.getKey();

            CacheConfiguration ccfg = cctx.cache().cacheGroup(pair.getGroupId()).config();

            CompletableFuture<File> fut0 = CompletableFuture.supplyAsync(
                bctx.partSuppFactory
                    .apply(
                        getPartitionFile(
                            cacheWorkDir(pageMgr.workDir(), ccfg),
                            pair.getPartitionId()),
                        new File(
                            bctx.backupDir,
                            cacheDirName(ccfg)),
                        bctx.partAllocLengths.get(pair)),
                bctx.execSvc)
                .thenCombineAsync(bctx.cpEndFut,
                    new BiFunction<File, Boolean, File>() {
                        @Override public File apply(File from, Boolean cp) {
                            assert cp;

                            return bctx.deltaTaskFactory.apply(from, bctx.partDeltaStores.get(pair))
                                .get();
                        }
                    },
                    bctx.execSvc);

            futs.add(fut0);
        }

        CompletableFuture.allOf(futs.toArray(new CompletableFuture[bctx.partAllocLengths.size()]))
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
    public void beforeStoreWrite(GroupPartitionId pairId, long pageId, ByteBuffer buf, long off) {
        assert buf.position() == 0 : buf.position();
        assert buf.order() == ByteOrder.nativeOrder() : buf.order();

        try {
            List<DeltaPagesStorage> deltas = processingParts.get(pairId);

            if (deltas == null || deltas.isEmpty())
                return;

            for (DeltaPagesStorage delta : deltas) {
                if (!delta.writable())
                    continue;

                delta.write(pageId, buf, off);
                buf.rewind();
            }
        }
        catch (Exception e) {
            U.error(log, "An error occured in the process of page backup " +
                "[pairId=" + pairId + ", pageId=" + pageId + ']');

            pageTrackErrors.putIfAbsent(pairId,
                new IgniteCheckedException("Partition backup processing error [pageId=" + pageId + ']', e));
        }
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
        private final DeltaPagesStorage delta;

        /**
         * @param delta Storage with delta pages.
         */
        public PartitionDeltaSupplier(
            IgniteLogger log,
            FilePageStoreFactory factory,
            File from,
            DeltaPagesStorage delta
        ) {
            this.log = log.getLogger(PartitionDeltaSupplier.class);
            this.factory = factory;
            this.from = from;
            this.delta = delta;
        }

        /** {@inheritDoc} */
        @Override public File get() {
            try {
                PageStore store = factory.createPageStore(FLAG_DATA,
                    from::toPath,
                    new LongAdderMetric("NO_OP", null),
                    PageStoreListener.NO_OP);

                store.beginRecover();
                delta.apply(store);
                store.stop(false);

                U.log(log, "Partition delta storage applied to: " + from.getName());
            }
            catch (IOException | IgniteCheckedException e) {
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
        private final long partSize;

        /**
         * @param log Ignite logger to use.
         * @param from Partition file.
         * @param dir Destination copy file.
         * @param partSize Size of partition.
         */
        public PartitionCopySupplier(
            IgniteLogger log,
            File from,
            File dir,
            long partSize
        ) {
            A.ensure(dir.isDirectory(), "Destination path must be a directory");

            this.log = log.getLogger(PartitionCopySupplier.class);
            this.from = from;
            this.partSize = partSize;
            to = new File(dir, from.getName());
        }

        /** {@inheritDoc} */
        @Override public File get() {
            try {
                if (!to.exists() || to.delete())
                    to.createNewFile();

                if (partSize == 0)
                    return to;

                try (FileChannel src = new FileInputStream(from).getChannel();
                     FileChannel dest = new FileOutputStream(to).getChannel()) {
                    src.position(0);

                    long written = 0;

                    while (written < partSize)
                        written += src.transferTo(written, partSize - written, dest);
                }

                U.log(log, "Partition has been copied [from=" + from.getAbsolutePath() + ", fromSize=" + from.length() +
                    ", to=" + to.getAbsolutePath() + ']');
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
        private final Map<GroupPartitionId, Long> partAllocLengths = new HashMap<>();

        /** Map of partitions to backup and theirs corresponding delta PageStores. */
        private final Map<GroupPartitionId, DeltaPagesStorage> partDeltaStores = new HashMap<>();

        /** Future of result completion. */
        @GridToStringExclude
        private final GridFutureAdapter<Void> result = new GridFutureAdapter<>();

        /** Factory to create executable tasks for partition processing. */
        @GridToStringExclude
        private final IgniteTriClosure<File, File, Long, Supplier<File>> partSuppFactory;

        /** Factory to create executable tasks for partition delta pages processing. */
        @GridToStringExclude
        private final IgniteBiClosure<File, DeltaPagesStorage, Supplier<File>> deltaTaskFactory;

        /** Collection of partition to be backuped. */
        private final Map<Integer, Set<Integer>> parts;

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
            IgniteBiClosure<File, DeltaPagesStorage, Supplier<File>> deltaTaskFactory
        ) {
            A.notNull(name, "Backup name cannot be empty or null");
            A.notNull(backupDir, "You must secify correct backup directory");
            A.ensure(backupDir.isDirectory(), "Specified path is not a directory");
            A.notNull(execSvc, "Executor service must be not null");
            A.notNull(partSuppFactory, "Factory which procudes backup tasks to execute must be not null");
            A.notNull(deltaTaskFactory, "Factory which processes delta pages storage must be not null");

            this.name = name;
            this.backupDir = backupDir;
            this.parts = parts;
            this.execSvc = execSvc;
            this.partSuppFactory = partSuppFactory;
            this.deltaTaskFactory = deltaTaskFactory;

            result.listen(f -> {
                if (f.error() != null)
                    closeBackupResources(this);
            });
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
