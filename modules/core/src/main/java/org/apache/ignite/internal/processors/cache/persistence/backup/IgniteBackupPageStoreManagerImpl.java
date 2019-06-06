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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
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
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.Optional.ofNullable;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;

/** */
public class IgniteBackupPageStoreManagerImpl extends GridCacheSharedManagerAdapter
    implements IgniteBackupPageStoreManager {
    /** */
    public static final String DELTA_SUFFIX = ".delta";

    /** */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + DELTA_SUFFIX;

    /** */
    public static final String BACKUP_CP_REASON = "Wakeup for checkpoint to take backup [name=%s]";

    /** */
    private final ConcurrentMap<String, GridFutureAdapter<?>> scheduledBackups = new ConcurrentHashMap<>();

    /** TODO: CAS on list with temporary page stores */
    private final ConcurrentMap<GroupPartitionId, List<TempPageStore>> processingParts = new ConcurrentHashMap<>();

    /** Factory to working with {@link TempPageStore} as file storage. */
    private final FileIOFactory ioFactory;

    /** Tracking partition files over all running snapshot processes. */
    private final ConcurrentMap<GroupPartitionId, AtomicInteger> trackMap = new ConcurrentHashMap<>();

    /** Keep only the first page error. */
    private final ConcurrentMap<GroupPartitionId, IgniteCheckedException> pageTrackErrors = new ConcurrentHashMap<>();

    /** Collection of backup stores indexed by [grpId, partId] key. */
    private final Map<GroupPartitionId, TempPageStore> backupStores = new ConcurrentHashMap<>();

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
    public IgniteBackupPageStoreManagerImpl(GridKernalContext ctx) throws IgniteCheckedException {
        assert CU.isPersistenceEnabled(ctx.config());

        ioFactory = new RandomAccessFileIOFactory();
    }

    /**
     * @param tmpDir Temporary directory to store files.
     * @param partId Cache partition identifier.
     * @return A file representation.
     */
    public static File getPartionDeltaFile(File tmpDir, int partId) {
        return new File(tmpDir, String.format(PART_DELTA_TEMPLATE, partId));
    }

    /**
     * @param ccfg Cache configuration.
     * @param partId Partiton identifier.
     * @return The cache partiton file.
     */
    private File resolvePartitionFileCfg(CacheConfiguration ccfg, int partId) {
        File cacheDir = ((FilePageStoreManager)cctx.pageStore()).cacheWorkDir(ccfg);

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

        setThreadPageBuff(ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder())));

        threadTempArr = ThreadLocal.withInitial(() -> new byte[pageSize]);

        dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

        dbMgr.addCheckpointListener(cpLsnr = new DbCheckpointListener() {
            @Override public void beforeMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {

            }

            @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {

            }

            @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {

            }

            @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {

            }
        });
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        dbMgr.removeCheckpointListener(cpLsnr);
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        // Nothing to do. Backups are created on demand.
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        for (TempPageStore store : backupStores.values())
            U.closeQuiet(store);

        backupStores.clear();
        trackMap.clear();
        pageTrackErrors.clear();
    }

    /** {@inheritDoc} */
    @Override public CheckpointFuture forceStart() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Set<CompletableBackup>> scheduleCacheBackup(
        String backupName,
        GridCacheContext cctx,
        Set<Integer> parts,
        File dir
    ) {
        scheduledBackups.putIfAbsent(backupName, new GridFutureAdapter<>());

        return null;
    }

    /** {@inheritDoc} */
    @Override public void stopCacheBackup(String backupName, GridCacheContext cctx) {

    }

    /** {@inheritDoc} */
    @Override public void backup(
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
                    resolvePartitionFileCfg(grpCfg, grpPartId.getPartitionId()),
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

    /** {@inheritDoc} */
    @Override public void beforeStoreWrite(GroupPartitionId pairId, PageStore store, long pageId) {
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

            TempPageStore tempStore = backupStores.get(pairId);

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
                new FileTempPageStore(getPartionDeltaFile(tempGroupDir,
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
        /** {@inheritDoc */
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
    private static class PartitionCompletableBackup implements CompletableBackup {
        /** {@inheritDoc} */
        @Override public File getPartition() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public long getPartitionSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public File getPartitionDelta() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void complete() {

        }
    }

    /**
     *
     */
    private static class CacheBackupContext {
        /** Cache group id. */
        private final int grpId;

        /** Unique backup name. */
        private final String backupName;

        /** Set of partitions to process. */
        private final Set<Integer> partIds = new HashSet<>();

        /**
         * The length of file size per each cache partiton file.
         * Partition has value greater than zero only for partitons in OWNING state.
         * Information collected under checkpoint write lock.
         */
        private final Map<File, Integer> partSizesMap = new HashMap<>();

        /** Map of partitions to backup and theirs corresponding PageStores. */
        private final Map<Integer, TempPageStore> partTempStoreMap = new HashMap<>();

        /** Ready to start partitions copy process future. */
        private final IgniteInternalFuture<?> beginFut = new GridFutureAdapter<>();

        /**
         * @param grpId Backup cache group id.
         * @param backupName Unique backup name.
         */
        public CacheBackupContext(int grpId, String backupName) {
            this.grpId = grpId;
            this.backupName = backupName;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheBackupContext.class, this);
        }
    }

    /** */
    private static class BackupContext {
        /** */
        private final AtomicBoolean inited = new AtomicBoolean();

        /** */
        private final AtomicBoolean tracked = new AtomicBoolean();

        /** Unique identifier of backup process. */
        private final String name;

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
