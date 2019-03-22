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
import java.util.HashMap;
import java.util.HashSet;
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
public class GridBackupPageStoreManager extends GridCacheSharedManagerAdapter
    implements IgniteBackupPageStoreManager {
    /** */
    public static final String DELTA_SUFFIX = ".delta";

    /** */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + DELTA_SUFFIX;

    /** */
    public static final String BACKUP_CP_REASON = "Wakeup for checkpoint to take backup [id=%s]";

    /** Factory to working with {@link TemporaryStore} as file storage. */
    private final FileIOFactory ioFactory;

    /** Tracking partition files over all running snapshot processes. */
    private final ConcurrentMap<GroupPartitionId, AtomicInteger> trackMap = new ConcurrentHashMap<>();

    /** Keep only the first page error. */
    private final ConcurrentMap<GroupPartitionId, IgniteCheckedException> pageTrackErrors = new ConcurrentHashMap<>();

    /** Collection of backup stores indexed by [grpId, partId] key. */
    private final Map<GroupPartitionId, TemporaryStore> backupStores = new ConcurrentHashMap<>();

    /** */
    private int pageSize;

    /** */
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();

    /** Base working directory for saving copied pages. */
    private File backupWorkDir;

    /** Thread local with buffers for handling copy-on-write over {@link PageStore} events. */
    private ThreadLocal<ByteBuffer> threadPageBuff;

    /** A byte array to store intermediate calculation results of process handling page writes. */
    private ThreadLocal<byte[]> threadTempArr;

    /** */
    public GridBackupPageStoreManager(GridKernalContext ctx) throws IgniteCheckedException {
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
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        // Nothing to do. Backups are created on demand.
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        for (TemporaryStore store : backupStores.values())
            U.closeQuiet(store);

        backupStores.clear();
        trackMap.clear();
        pageTrackErrors.clear();
    }

    /** {@inheritDoc} */
    @Override public void backup(
        long idx,
        Map<Integer, Set<Integer>> grpsBackup,
        BackupProcessSupplier task,
        IgniteInternalFuture<Boolean> fut
    ) throws IgniteCheckedException {
        if (!(cctx.database() instanceof GridCacheDatabaseSharedManager))
            return;

        final NavigableSet<GroupPartitionId> grpPartIdSet = new TreeSet<>();

        for (Map.Entry<Integer, Set<Integer>> backupEntry : grpsBackup.entrySet()) {
            for (Integer partId : backupEntry.getValue())
                grpPartIdSet.add(new GroupPartitionId(backupEntry.getKey(), partId));
        }

        // Init stores if not created yet.
        initTemporaryStores(grpPartIdSet);

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

        final BackupContext backupCtx = new BackupContext();

        DbCheckpointListener dbLsnr = new DbCheckpointListener() {
            // #onMarkCheckpointBegin() is used to save meta information of partition (e.g. updateCounter, size).
            // To get consistent partition state we should start to track all corresponding pages updates
            // before GridCacheOffheapManager will saves meta to the #partitionMetaPageId() page.
            // TODO shift to the second checkpoint begin.
            @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
                // Start tracking writes over remaining parts only from the next checkpoint.
                if (backupCtx.tracked.compareAndSet(false, true)) {
                    backupCtx.remainPartIds = new CopyOnWriteArraySet<>(grpPartIdSet);

                    for (GroupPartitionId key : backupCtx.remainPartIds) {
                        // Start track.
                        AtomicInteger cnt = trackMap.putIfAbsent(key, new AtomicInteger(1));

                        if (cnt != null)
                            cnt.incrementAndGet();

                        // Update offsets.
                        backupCtx.deltaOffsetMap.put(key, pageSize * backupStores.get(key).writtenPagesCount());
                    }
                }
            }

            @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
                // No-op.
            }

            @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
                // Will skip the other #onCheckpointBegin() checkpoint. We should wait for the next
                // checkpoint and if it occurs must start to track writings of remaining in context partitions.
                // Suppose there are no store writings between the end of last checkpoint and the start on new one.
                if (backupCtx.inited.compareAndSet(false, true)) {
                    rwlock.readLock().lock();

                    try {
                        PartitionAllocationMap allocationMap = ctx.partitionStatMap();

                        allocationMap.prepareForSnapshot();

                        backupCtx.idx = idx;

                        for (GroupPartitionId key : grpPartIdSet) {
                            PagesAllocationRange allocRange = allocationMap.get(key);

                            assert allocRange != null :
                                "Pages not allocated [pairId=" + key + ", backupCtx=" + backupCtx + ']';

                            backupCtx.partAllocatedPages.put(key, allocRange.getCurrAllocatedPageCnt());

                            // Set offsets with default zero values.
                            backupCtx.deltaOffsetMap.put(key, 0);
                        }
                    }
                    finally {
                        rwlock.readLock().unlock();
                    }
                }
            }
        };

        try {
            if (fut.isCancelled())
                return;

            dbMgr.addCheckpointListener(dbLsnr);

            CheckpointFuture cpFut = dbMgr.wakeupForCheckpointOperation(
                new SnapshotOperationAdapter() {
                    @Override public Set<Integer> cacheGroupIds() {
                        return new HashSet<>(grpsBackup.keySet());
                    }
                },
                String.format(BACKUP_CP_REASON, idx)
            );

            A.notNull(cpFut, "Checkpoint thread is not running.");

            cpFut.finishFuture().listen(f -> {
                assert backupCtx.inited.get() : "Backup context must be initialized: " + backupCtx;
            });

            cpFut.finishFuture().get();

            U.log(log, "Start backup operation [grps=" + grpsBackup + ']');

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

                final long partSize = backupCtx.partAllocatedPages.get(grpPartId) * pageSize + store.headerSize();

                if (fut.isCancelled())
                    return;

                task.supplyPartition(grpPartId,
                    resolvePartitionFileCfg(grpCfg, grpPartId.getPartitionId()),
                    partSize);

                // Stop page delta tracking for particular pair id.
                ofNullable(trackMap.get(grpPartId))
                    .ifPresent(AtomicInteger::decrementAndGet);

                if (log.isDebugEnabled())
                    log.debug("Partition handled successfully [pairId" + grpPartId + ']');

                final Map<GroupPartitionId, Integer> offsets = backupCtx.deltaOffsetMap;
                final int deltaOffset = offsets.get(grpPartId);
                final long deltaSize = backupStores.get(grpPartId).writtenPagesCount() * pageSize;

                if (fut.isCancelled())
                    return;

                task.supplyDelta(grpPartId,
                    resolvePartitionDeltaFileCfg(grpCfg, grpPartId.getPartitionId()),
                    deltaOffset,
                    deltaSize);

                // Finish partition backup task.
                backupCtx.remainPartIds.remove(grpPartId);

                if (log.isDebugEnabled())
                    log.debug("Partition delta handled successfully [pairId" + grpPartId + ']');
            }
        }
        catch (Exception e) {
            U.error(log, "An error occured while handling partition files.", e);

            for (GroupPartitionId key : grpPartIdSet) {
                AtomicInteger keyCnt = trackMap.get(key);

                if (keyCnt != null && (keyCnt.decrementAndGet() == 0))
                    U.closeQuiet(backupStores.get(key));
            }

            fut.cancel();

            throw new IgniteCheckedException(e);
        }
        finally {
            dbMgr.removeCheckpointListener(dbLsnr);
        }
    }

    /** {@inheritDoc} */
    @Override public void handleWritePageStore(GroupPartitionId pairId, PageStore store, long pageId) {
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

            TemporaryStore tempStore = backupStores.get(pairId);

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

    /** {@inheritDoc} */
    @Override public void initTemporaryStores(Set<GroupPartitionId> grpPartIdSet) throws IgniteCheckedException {
        U.log(log, "Resolve temporary directories: " + grpPartIdSet);

        for (GroupPartitionId grpPartId : grpPartIdSet) {
            CacheConfiguration ccfg = cctx.cache().cacheGroup(grpPartId.getGroupId()).config();

            // Create cache temporary directory if not.
            File tempGroupDir = U.resolveWorkDirectory(backupWorkDir.getAbsolutePath(), cacheDirName(ccfg), false);

            U.ensureDirectory(tempGroupDir, "temporary directory for grpId: " + grpPartId.getGroupId(), null);

            backupStores.putIfAbsent(grpPartId,
                new FileTemporaryStore(getPartionDeltaFile(tempGroupDir,
                    grpPartId.getPartitionId()),
                    ioFactory,
                    pageSize));
        }
    }

    /** */
    public void setThreadPageBuff(final ThreadLocal<ByteBuffer> buf) {
        threadPageBuff = buf;
    }

    /** */
    private static class BackupContext {
        /** */
        private final AtomicBoolean inited = new AtomicBoolean();

        /** */
        private final AtomicBoolean tracked = new AtomicBoolean();

        /** Unique identifier of backup process. */
        private long idx;

        /**
         * The length of partition file sizes up to each cache partiton file.
         * Partition has value greater than zero only for OWNING state partitons.
         */
        private Map<GroupPartitionId, Integer> partAllocatedPages = new HashMap<>();

        /** The offset from which reading of delta partition file should be started. */
        private ConcurrentMap<GroupPartitionId, Integer> deltaOffsetMap = new ConcurrentHashMap<>();

        /** Left partitions to be processed. */
        private CopyOnWriteArraySet<GroupPartitionId> remainPartIds;

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(BackupContext.class, this);
        }
    }
}
