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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointFuture;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PagesAllocationRange;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationAdapter;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerFuture;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

import static java.util.Optional.ofNullable;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;
import static org.apache.ignite.internal.util.io.GridFileUtils.copy;

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

    /** Factory to working with {@link PartitionDeltaPageStore} as file storage. */
    private static final FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** Map of registered cache backup processes and their corresponding contexts. */
    private final ConcurrentMap<String, BackupContext2> backupCtxs = new ConcurrentHashMap<>();

    /** TODO: CAS on list with temporary page stores */
    private final ConcurrentMap<GroupPartitionId, List<PartitionDeltaPageStore>> processingParts = new ConcurrentHashMap<>();

    /** Backup thread pool. */
    private IgniteThreadPoolExecutor backupRunner;

    //// BELOW IS NOT USED

    /** Collection of backup stores indexed by [grpId, partId] key. */
    private final Map<GroupPartitionId, PartitionDeltaPageStore> backupStores = new ConcurrentHashMap<>();

    /** Map of registered cache backup processes and their corresponding contexts. */
    private final ConcurrentMap<String, BackupContext> backupMap = new ConcurrentHashMap<>();

    /** Tracking partition files over all running snapshot processes. */
    private final ConcurrentMap<GroupPartitionId, AtomicInteger> trackMap = new ConcurrentHashMap<>();

    /** Keep only the first page error. */
    private final ConcurrentMap<GroupPartitionId, IgniteCheckedException> pageTrackErrors = new ConcurrentHashMap<>();

    /** Checkpoint listener to handle scheduled backup requests. */
    private DbCheckpointListener cpLsnr;

    /** Database manager for enabled persistence. */
    private GridCacheDatabaseSharedManager dbMgr;

    /** Configured data storage page size. */
    private int pageSize;

    /** Thread local with buffers for handling copy-on-write over {@link PageStore} events. */
    private ThreadLocal<ByteBuffer> threadPageBuff;

    /** */
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();

    /** Base working directory for saving copied pages. */
    private File backupWorkDir;

    /** A byte array to store intermediate calculation results of process handling page writes. */
    private ThreadLocal<byte[]> threadTempArr;

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

        setThreadPageBuff(ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder())));

        threadTempArr = ThreadLocal.withInitial(() -> new byte[pageSize]);

        dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

        dbMgr.addCheckpointListener(cpLsnr = new DbCheckpointListener() {
            @Override public void beforeCheckpointBegin(Context ctx) {
                for (BackupContext2 bctx0 : backupCtxs.values()) {
                    if (bctx0.started)
                        continue;

                    // Gather partitions metainfo for thouse which will be copied.
                    ctx.gatherPartStats(bctx0.parts);
                }
            }

            @Override public void onMarkCheckpointBegin(Context ctx) {
                // Under the write lock here. It's safe to add new stores
                for (BackupContext2 bctx0 : backupCtxs.values()) {
                    if (bctx0.started)
                        continue;

                    for (Map.Entry<GroupPartitionId, PartitionDeltaPageStore> e : bctx0.partDeltaStores.entrySet()) {
                        processingParts.computeIfAbsent(e.getKey(), p -> new LinkedList<>())
                            .add(e.getValue());
                    }
                }

                // Remove not used delta stores.
                for (List<PartitionDeltaPageStore> list0 : processingParts.values())
                    list0.removeIf(store -> !store.writable());
            }

            @Override public void onCheckpointBegin(Context ctx) {
                final FilePageStoreManager pageMgr = (FilePageStoreManager)cctx.pageStore();

                for (BackupContext2 bctx0 : backupCtxs.values()) {
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

                        submitPartitionsTask(bctx0, pageMgr.workDir());

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

        for (Collection<PartitionDeltaPageStore> deltas : processingParts.values()) {
            for (PartitionDeltaPageStore s : deltas)
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

        final BackupContext2 bctx0 = new BackupContext2(name,
            backupDir,
            parts,
            backupRunner,
            (storeWorkDir, ccfg, partId, partSize, delta) ->
                new PartitionCopyWorker(cctx.igniteInstanceName(),
                    log,
                    backupDir,
                    storeWorkDir,
                    ccfg,
                    partId,
                    partSize,
                    delta));

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
                        new PartitionDeltaPageStore(getPartionDeltaFile(grpDir, partId),
                            ioFactory,
                            cctx.gridConfig().getDataStorageConfiguration().getPageSize()));
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

            throw e;
        }

        BackupContext2 tctx = backupCtxs.putIfAbsent(name, bctx0);

        dbMgr.forceCheckpoint(String.format(BACKUP_CP_REASON, name))
            .beginFuture()
            .get();

        assert tctx == null : tctx;

        return bctx0.result;
    }

    /**
     * @param bctx Context to handle.
     */
    private void submitPartitionsTask(BackupContext2 bctx, File storeWorkDir) {
        GridWorkerFuture<Boolean> wrapFut;
        GridWorker w0;

        for (Map.Entry<GroupPartitionId, Long> e : bctx.partAllocLengths.entrySet()) {
            GroupPartitionId pair = e.getKey();

            bctx.execSvc.submit(
                U.wrapIgniteFuture(
                        w0 = bctx.factory
                        .createTask(storeWorkDir,
                            cctx.cache()
                                .cacheGroup(pair.getGroupId())
                                .config(),
                            pair.getPartitionId(),
                            bctx.partAllocLengths.get(pair),
                            bctx.partDeltaStores.get(pair)),
                    wrapFut = new GridWorkerFuture<>()));

            // To be able to cancel all tasks.
            wrapFut.setWorker(w0);
            bctx.result.add(wrapFut);
        }

        bctx.result.markInitialized();
    }

    /**
     * @param backupName Unique backup name.
     */
    public void stopCacheBackup(String backupName) {

    }

    /**
     * @param backupName Unique backup identifier.
     * @param parts Collection of pairs group and appropratate cache partition to be backuped.
     * @param closure Partition backup handling closure.
     * @throws IgniteCheckedException If fails.
     */
    public void backup(
        String backupName,
        Map<Integer, Set<Integer>> parts,
        PageStoreInClosure closure
    ) throws IgniteCheckedException {
        if (!(cctx.database() instanceof GridCacheDatabaseSharedManager))
            return;

        final GridFutureAdapter<Boolean> doneFut = new GridFutureAdapter<>();
        final NavigableSet<GroupPartitionId> grpPartIdSet = new TreeSet<>();

        for (Map.Entry<Integer, Set<Integer>> backupEntry : parts.entrySet()) {
            for (Integer partId : backupEntry.getValue())
                grpPartIdSet.add(new GroupPartitionId(backupEntry.getKey(), partId));
        }

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

        final BackupContext bctx = new BackupContext(backupName);
        DbCheckpointListener dbLsnr = null;

        try {
            // Init stores if not created yet.
            initTemporaryStores(grpPartIdSet);

            dbMgr.addCheckpointListener(dbLsnr = new BackupCheckpointListener(bctx, grpPartIdSet));

            CheckpointFuture cpFut = dbMgr.wakeupForCheckpointOperation(
                new SnapshotOperationAdapter() {
                    @Override public Set<Integer> cacheGroupIds() {
                        return new HashSet<>(parts.keySet());
                    }
                },
                String.format(BACKUP_CP_REASON, backupName)
            );

            A.notNull(cpFut, "Checkpoint thread is not running.");

            cpFut.finishFuture().listen(f -> {
                assert bctx.inited.get() : "Backup context must be initialized: " + bctx;
            });

            cpFut.finishFuture().get();

            U.log(log, "Start backup operation [grps=" + parts + ']');

            // Use sync mode to execute provided task over partitons and corresponding deltas.
            for (GroupPartitionId grpPartId : grpPartIdSet) {
                IgniteCheckedException pageErr = pageTrackErrors.get(grpPartId);

                if (pageErr != null)
                    throw pageErr;

                final CacheConfiguration grpCfg = cctx.cache()
                    .cacheGroup(grpPartId.getGroupId())
                    .config();

                final PageStore store = ((FilePageStoreManager)cctx.pageStore())
                    .getStore(grpPartId.getGroupId(), grpPartId.getPartitionId());

                final long partSize = bctx.partAllocatedPages.get(grpPartId) * pageSize + store.headerSize();

                closure.accept(grpPartId,
                    PageStoreType.MAIN,
                    resolvePartitionFileCfg((FilePageStoreManager)cctx.pageStore(),
                        grpCfg,
                        grpPartId.getPartitionId()),
                    0,
                    partSize);

                // Stop page delta tracking for particular pair id.
                ofNullable(trackMap.get(grpPartId))
                    .ifPresent(AtomicInteger::decrementAndGet);

                if (log.isDebugEnabled())
                    log.debug("Partition handled successfully [pairId" + grpPartId + ']');

                final Map<GroupPartitionId, Integer> offsets = bctx.deltaOffsetMap;
                final int deltaOffset = offsets.get(grpPartId);
                final long deltaSize = backupStores.get(grpPartId).writtenPagesCount() * pageSize;

                closure.accept(grpPartId,
                    PageStoreType.TEMP,
                    resolvePartitionDeltaFileCfg(grpCfg, grpPartId.getPartitionId()),
                    deltaOffset,
                    deltaSize);

                // Finish partition backup task.
                bctx.remainPartIds.remove(grpPartId);

                if (log.isDebugEnabled())
                    log.debug("Partition delta handled successfully [pairId" + grpPartId + ']');
            }

            doneFut.onDone(true);
        }
        catch (Exception e) {
            for (GroupPartitionId key : grpPartIdSet) {
                AtomicInteger keyCnt = trackMap.get(key);

                if (keyCnt != null && (keyCnt.decrementAndGet() == 0))
                    U.closeQuiet(backupStores.get(key));
            }

            throw new IgniteCheckedException(e);
        }
        finally {
            dbMgr.removeCheckpointListener(dbLsnr);
        }
    }

    /**
     * @param pairId Cache group, partition identifiers pair.
     * @param store Store to handle operatwion at.
     * @param pageId Tracked page id.
     */
    public void beforeStoreWrite(GroupPartitionId pairId, PageStore store, long pageId) {
        AtomicInteger trackCnt = trackMap.get(pairId);

        if (trackCnt == null || trackCnt.get() <= 0)
            return;

        final ByteBuffer tmpPageBuff = threadPageBuff.get();

        assert tmpPageBuff.capacity() == store.getPageSize();

        tmpPageBuff.clear();

        try {
            store.read(pageId, tmpPageBuff, true);

            tmpPageBuff.flip();

            // We can read a page with zero bytes as it isn't exist in the store (e.g. on first write request).
            // Check the buffer contains only zero bytes and exit.
            if (isNewPage(tmpPageBuff))
                return;

            PartitionDeltaPageStore tempStore = backupStores.get(pairId);

            assert tempStore != null;

            tempStore.write(pageId, tmpPageBuff);

            tmpPageBuff.clear();
        }
        catch (IgniteDataIntegrityViolationException e) {
            // The page can be readed with zero bytes only if it allocated but not changed yet.
            U.warn(log, "Ignore integrity violation checks [pairId=" + pairId + ", pageId=" + pageId + ']');
        }
        catch (Exception e) {
            U.error(log, "An error occured in the process of page backup " +
                "[pairId=" + pairId + ", pageId=" + pageId + ']');

            pageTrackErrors.putIfAbsent(pairId,
                new IgniteCheckedException("Partition backup processing error [pageId=" + pageId + ']', e));
        }
    }

    /**
     * @param buff Input array to check.
     * @return {@code True} if contains only zero bytes.
     */
    private boolean isNewPage(ByteBuffer buff) {
        assert buff.position() == 0 : buff.position();
        assert buff.limit() == pageSize : buff.limit();

        byte[] array = threadTempArr.get();

        buff.get(array);

        buff.rewind();

        int sum = 0;

        for (byte b : array)
            sum |= b;

        return sum == 0;
    }

    /**
     * @param grpPartIdSet Collection of pairs cache group and partition ids.
     * @throws IgniteCheckedException If fails.
     */
    public void initTemporaryStores(Set<GroupPartitionId> grpPartIdSet) throws IgniteCheckedException {
        U.log(log, "Resolve temporary directories: " + grpPartIdSet);

        for (GroupPartitionId grpPartId : grpPartIdSet) {
            CacheConfiguration ccfg = cctx.cache().cacheGroup(grpPartId.getGroupId()).config();

            // Create cache temporary directory if not.
            File tempGroupDir = U.resolveWorkDirectory(backupWorkDir.getAbsolutePath(), cacheDirName(ccfg), false);

            U.ensureDirectory(tempGroupDir, "temporary directory for grpId: " + grpPartId.getGroupId(), null);

            backupStores.putIfAbsent(grpPartId,
                new PartitionDeltaPageStore(getPartionDeltaFile(tempGroupDir,
                    grpPartId.getPartitionId()),
                    ioFactory,
                    pageSize));
        }
    }

    /**
     * @param buf Buffer to set.
     */
    public void setThreadPageBuff(final ThreadLocal<ByteBuffer> buf) {
        threadPageBuff = buf;
    }

    /**
     *
     */
    private class BackupCheckpointListener implements DbCheckpointListener {
        /** */
        private final BackupContext ctx;

        /** */
        private final Collection<GroupPartitionId> grpPartIdSet;

        /**
         * @param ctx Backup context handler associate with.
         * @param parts Colleciton of partitions to handle.
         */
        public BackupCheckpointListener(
            BackupContext ctx,
            Collection<GroupPartitionId> parts) {
            this.ctx = ctx;
            this.grpPartIdSet = parts;
        }

        // #onMarkCheckpointBegin() is used to save meta information of partition (e.g. updateCounter, size).
        // To get consistent partition state we should start to track all corresponding pages updates
        // before GridCacheOffheapManager will saves meta to the #partitionMetaPageId() page.
        // TODO shift to the second checkpoint begin.
        //
        /** {@inheritDoc} */
        @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
            // Start tracking writes over remaining parts only from the next checkpoint.
            if (this.ctx.tracked.compareAndSet(false, true)) {
                this.ctx.remainPartIds = new CopyOnWriteArraySet<>(grpPartIdSet);

                for (GroupPartitionId key : this.ctx.remainPartIds) {
                    // Start track.
                    AtomicInteger cnt = trackMap.putIfAbsent(key, new AtomicInteger(1));

                    if (cnt != null)
                        cnt.incrementAndGet();

                    // Update offsets.
                    this.ctx.deltaOffsetMap.put(key, pageSize * backupStores.get(key).writtenPagesCount());
                }
            }
        }

        /** {@inheritDoc */
        @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
            // No-op.
        }

        /** {@inheritDoc */
        @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
            // Will skip the other #onCheckpointBegin() checkpoint. We should wait for the next
            // checkpoint and if it occurs must start to track writings of remaining in context partitions.
            // Suppose there are no store writings between the end of last checkpoint and the start on new one.
            if (this.ctx.inited.compareAndSet(false, true)) {
                rwlock.readLock().lock();

                try {
                    PartitionAllocationMap allocationMap = ctx.partitionStatMap();

                    allocationMap.prepareForSnapshot();

                    for (GroupPartitionId key : grpPartIdSet) {
                        PagesAllocationRange allocRange = allocationMap.get(key);

                        assert allocRange != null :
                            "Pages not allocated [pairId=" + key + ", ctx=" + this.ctx + ']';

                        this.ctx.partAllocatedPages.put(key, allocRange.getCurrAllocatedPageCnt());

                        // Set offsets with default zero values.
                        this.ctx.deltaOffsetMap.put(key, 0);
                    }
                }
                finally {
                    rwlock.readLock().unlock();
                }
            }
        }
    }

    /**
     *
     */
    @FunctionalInterface
    private static interface PartitionTaskFactory {
        /**
         * @param storeWorkDir File page storage working directory.
         * @param ccfg Cache group configuration.
         * @param partId Partition id to process.
         * @param partSize Partition lenght on checkpoint time.
         * @param delta Storage with delta pages.
         * @return Executable partition prccessing task.
         */
        public GridWorker createTask(
            File storeWorkDir,
            CacheConfiguration ccfg,
            int partId,
            long partSize,
            PartitionDeltaPageStore delta
        );
    }

    /**
     *
     */
    private static class PartitionCopyWorker extends GridWorker {
        /** */
        private final File backupDir;

        /** */
        private final File storeWorkDir;

        /** */
        private final CacheConfiguration ccfg;

        /** */
        private final int partId;

        /** */
        private final long partSize;

        /** */
        private final PartitionDeltaPageStore delta;

        /**
         * @param backupDir Backup directory.
         * @param storeWorkDir File page storage working directory.
         * @param ccfg Cache group configuration.
         * @param partId Partition id to process.
         * @param partSize Partition lenght on checkpoint time.
         * @param delta Storage with delta pages.
         */
        public PartitionCopyWorker(
            String igniteInstanceName,
            IgniteLogger log,
            File backupDir,
            File storeWorkDir,
            CacheConfiguration ccfg,
            int partId,
            long partSize,
            PartitionDeltaPageStore delta
        ) {
            super(igniteInstanceName, "part-backup-worker-", log.getLogger(PartitionCopyWorker.class));

            this.backupDir = backupDir;
            this.storeWorkDir = storeWorkDir;
            this.ccfg = ccfg;
            this.partId = partId;
            this.partSize = partSize;
            this.delta = delta;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            if (partSize == 0)
                return;

            try {
                File part = getPartitionFile(cacheWorkDir(storeWorkDir, ccfg), partId);
                File partCopy = new File(backupDir, cacheDirName(ccfg));

                copy(part, 0, partSize, partCopy);

                U.log(log, "Partition has been copied [from=" + part.getAbsolutePath() +
                    ", to=" + partCopy.getAbsolutePath() + ']');

                // Partition copied, stop recording deltas
                delta.writable(false);

                // TODO that merge with deltas
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteException(ex);
            }
        }
    }

    /**
     *
     */
    private static class BackupContext2 {
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
        private final Map<GroupPartitionId, PartitionDeltaPageStore> partDeltaStores = new HashMap<>();

        /** Future of result completion. */
        @GridToStringExclude
        private final GridCompoundFuture<Boolean, Boolean> result = new GridCompoundFuture<>();

        /** Factory to create executable tasks for partition processing. */
        @GridToStringExclude
        private final PartitionTaskFactory factory;

        /** Collection of partition to be backuped. */
        private final Map<Integer, Set<Integer>> parts;

        /** Flag idicates that this backup is start copying partitions. */
        private volatile boolean started;

        /**
         * @param name Unique identifier of backup process.
         * @param backupDir Backup storage directory.
         * @param execSvc Service to perform partitions copy.
         * @param factory Factory to create executable tasks for partition processing.
         */
        public BackupContext2(
            String name,
            File backupDir,
            Map<Integer, Set<Integer>> parts,
            ExecutorService execSvc,
            PartitionTaskFactory factory
        ) {
            A.ensure(name != null, "Backup name cannot be empty or null");
            A.ensure(backupDir != null && backupDir.isDirectory(), "You must secify correct backup directory");
            A.ensure(execSvc != null, "Executor service must be not null");
            A.ensure(factory != null, "Factory which procudes backup tasks to execute must be not null");

            this.name = name;
            this.backupDir = backupDir;
            this.parts = parts;
            this.execSvc = execSvc;
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            BackupContext2 context2 = (BackupContext2)o;

            return name.equals(context2.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(name);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(BackupContext2.class, this);
        }
    }

    /**
     *
     */
    private static class BackupContext {
        /** Unique identifier of backup process. */
        private final String name;

        /** */
        private final AtomicBoolean inited = new AtomicBoolean();

        /** */
        private final AtomicBoolean tracked = new AtomicBoolean();

        /** */
        private final GridFutureAdapter<?> result = new GridFutureAdapter<>();

        /**
         * The length of partition file sizes up to each cache partiton file.
         * Partition has value greater than zero only for OWNING state partitons.
         */
        private final Map<GroupPartitionId, Integer> partAllocatedPages = new HashMap<>();

        /** The offset from which reading of delta partition file should be started. */
        private final ConcurrentMap<GroupPartitionId, Integer> deltaOffsetMap = new ConcurrentHashMap<>();

        /** Left partitions to be processed. */
        private CopyOnWriteArraySet<GroupPartitionId> remainPartIds;

        /**
         * @param name Unique backup process name.
         */
        public BackupContext(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(BackupContext.class, this);
        }
    }
}
