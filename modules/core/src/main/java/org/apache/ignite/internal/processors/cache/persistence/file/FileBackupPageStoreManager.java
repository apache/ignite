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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointFuture;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.backup.BackupProcessTask;
import org.apache.ignite.internal.processors.cache.persistence.backup.FileTemporaryStore;
import org.apache.ignite.internal.processors.cache.persistence.backup.IgniteBackupPageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.backup.TemporaryStore;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationAdapter;
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
public class FileBackupPageStoreManager extends GridCacheSharedManagerAdapter
    implements IgniteBackupPageStoreManager {
    /** */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + ".delta";

    /** */
    private static final String BACKUP_CP_REASON = "Wakeup for checkpoint to take backup [id=%s, grpId=%s, parts=%s]";

    /** Base working directory for saving copied pages. */
    private final File backupWorkDir;

    /** Factory to working with {@link TemporaryStore} as file storage. */
    private final FileIOFactory ioFactory;

    /** Tracking partition files over all running snapshot processes. */
    private final ConcurrentMap<GroupPartitionId, AtomicInteger> trackMap = new ConcurrentHashMap<>();

    /** Collection of backup stores indexed by [grpId, partId] key. */
    private final Map<GroupPartitionId, TemporaryStore> backupStores = new ConcurrentHashMap<>();

    /** */
    private final IgniteLogger log;

    /** */
    private final int pageSize;

    /** */
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();

    /** Thread local with buffers for handling copy-on-write over {@link PageStore} events. */
    private ThreadLocal<ByteBuffer> threadPageBuf;

    /** */
    public FileBackupPageStoreManager(GridKernalContext ctx) throws IgniteCheckedException {
        assert CU.isPersistenceEnabled(ctx.config());

        log = ctx.log(getClass());
        pageSize = ctx.config().getDataStorageConfiguration().getPageSize();

        backupWorkDir = U.resolveWorkDirectory(ctx.config().getWorkDirectory(),
            DataStorageConfiguration.DFLT_BACKUP_DIRECTORY,
            true);

        U.ensureDirectory(backupWorkDir, "backup store working directory", log);

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

        setThreadPageBuf(ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder())));
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
    }

    /** {@inheritDoc} */
    @Override public void backup(
        int idx,
        int grpId,
        Set<Integer> parts,
        BackupProcessTask task
    ) throws IgniteCheckedException {
        if (!(cctx.database() instanceof GridCacheDatabaseSharedManager) || parts == null || parts.isEmpty())
            return;

        final SortedSet<GroupPartitionId> grpPartIdSet = parts.stream()
            .map(p -> new GroupPartitionId(grpId, p))
            .collect(Collectors.toCollection(TreeSet::new));

        // Init stores if not created yet.
        initTemporaryStores(grpPartIdSet);

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

        DbCheckpointListener dbLsnr = null;

        final BackupContext backupCtx = new BackupContext();

        try {
            dbLsnr = new DbCheckpointListener() {
                @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
                    // Start tracking writes over remaining parts only from the next checkpoint.
                    if (backupCtx.inited.get() && backupCtx.tracked.compareAndSet(false, true)) {
                        // Safe iteration over copy-on-write collection.
                        CopyOnWriteArraySet<GroupPartitionId> leftParts = backupCtx.remainPartIds;

                        for (GroupPartitionId key : leftParts) {
                            // Start track.
                            AtomicInteger cnt = trackMap.putIfAbsent(key, new AtomicInteger(1));

                            if (cnt != null)
                                cnt.incrementAndGet();

                            // Update offsets.
                            backupCtx.deltaOffsetMap.put(key, backupStores.get(key).writtenPagesCount());
                        }
                    }
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

                            backupCtx.partAllocatedPages = grpPartIdSet.stream()
                                .collect(Collectors.toMap(Function.identity(),
                                    p -> allocationMap.get(p).getCurrAllocatedPageCnt()));

                            // Set offsets with default zero values.
                            backupCtx.deltaOffsetMap = grpPartIdSet.stream()
                                .collect(Collectors.toConcurrentMap(Function.identity(), o -> 0));

                            backupCtx.remainPartIds = new CopyOnWriteArraySet<>(grpPartIdSet);
                        }
                        finally {
                            rwlock.readLock().unlock();
                        }
                    }
                }
            };

            dbMgr.addCheckpointListener(dbLsnr);

            CheckpointFuture cpFut = dbMgr.wakeupForCheckpointOperation(
                new SnapshotOperationAdapter() {
                    @Override public Set<Integer> cacheGroupIds() {
                        return new HashSet<>(Collections.singletonList(grpId));
                    }
                },
                String.format(BACKUP_CP_REASON, idx, grpId, S.compact(parts))
            );

            A.notNull(cpFut, "Checkpoint thread is not running.");

            cpFut.finishFuture().listen(f -> {
                assert backupCtx.inited.get() : "Backup context must be initialized: " + backupCtx;
            });

            cpFut.finishFuture().get();

            U.log(log, "Start snapshot operation over files [grpId=" + grpId + ", parts=" + S.compact(parts) +
                ", context=" + backupCtx + ']');

            // Use sync mode to execute provided task over partitons and corresponding deltas.
            for (GroupPartitionId grpPartId : grpPartIdSet) {
                final CacheConfiguration grpCfg = cctx.cache()
                    .cacheGroup(grpPartId.getGroupId())
                    .config();
                final long size = backupCtx.partAllocatedPages.get(grpPartId) * pageSize;

                task.handlePartition(grpPartId,
                    resolvePartitionFileCfg(grpCfg, grpPartId.getPartitionId()),
                    0,
                    size);

                // Stop page delta tracking for particular pair id.
                ofNullable(trackMap.get(grpPartId))
                    .ifPresent(AtomicInteger::decrementAndGet);
                U.log(log, "Partition file handled [pairId" + grpPartId + ']');

                final Map<GroupPartitionId, Integer> offsets = backupCtx.deltaOffsetMap;
                final int deltaOffset = offsets.get(grpPartId);
                final long deltaSize = backupStores.get(grpPartId).writtenPagesCount() * pageSize;

                task.handleDelta(grpPartId,
                    resolvePartitionDeltaFileCfg(grpCfg, grpPartId.getPartitionId()),
                    deltaOffset,
                    deltaSize);

                // Finish partition backup task.
                backupCtx.remainPartIds.remove(grpPartId);

                U.log(log, "Partition delta handled [pairId" + grpPartId + ']');
            }
        }
        catch (IgniteCheckedException e) {
            U.log(log, "An error occured while handling partition files.", e);

            for (GroupPartitionId key : grpPartIdSet) {
                AtomicInteger keyCnt = trackMap.get(key);

                if (keyCnt != null && (keyCnt.decrementAndGet() == 0))
                    U.closeQuiet(backupStores.get(key));
            }
        }
        finally {
            dbMgr.removeCheckpointListener(dbLsnr);
        }

        // Remove all tracked partitions.
    }

    /** {@inheritDoc} */
    @Override public void handleWritePageStore(GroupPartitionId pairId, PageStore store, long pageId) {
        AtomicInteger trackCnt = trackMap.get(pairId);

        if (trackCnt == null || trackCnt.get() <= 0)
            return;

        final ByteBuffer tmpPageBuff = threadPageBuf.get();

        tmpPageBuff.clear();

        try {
            store.read(pageId, tmpPageBuff, true);

            tmpPageBuff.rewind();

            TemporaryStore tempStore = backupStores.get(pairId);

            assert tempStore != null;

            tempStore.write(pageId, tmpPageBuff);
        }
        catch (IgniteCheckedException e) {
            U.log(log, "An error occured while backuping page. Please, check configured backup store " +
                "[pairId=" + pairId + ", pageId=" + pageId + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void initTemporaryStores(Set<GroupPartitionId> grpPartIdSet) throws IgniteCheckedException {
        for (GroupPartitionId grpPartId : grpPartIdSet) {
            CacheConfiguration ccfg = cctx.cache().cacheGroup(grpPartId.getGroupId()).config();

            // Create cache temporary directory if not.
            File tempGroupDir = U.resolveWorkDirectory(backupWorkDir.getAbsolutePath(), cacheDirName(ccfg), false);

            U.ensureDirectory(tempGroupDir, "temporary directory for grpId: " + grpPartId.getGroupId(), log);

            backupStores.putIfAbsent(grpPartId,
                new FileTemporaryStore(getPartionDeltaFile(tempGroupDir, grpPartId.getPartitionId()), ioFactory));
        }
    }

    /** */
    public void setThreadPageBuf(final ThreadLocal<ByteBuffer> buf) {
        threadPageBuf = buf;
    }

    /** */
    private static class BackupContext {
        /** */
        private final AtomicBoolean inited = new AtomicBoolean();

        /** */
        private final AtomicBoolean tracked = new AtomicBoolean();

        /** Unique identifier of backup process. */
        private int idx;

        /** The length of partition file sizes up to each cache partiton file. */
        private Map<GroupPartitionId, Integer> partAllocatedPages;

        /** The offset from which reading of delta partition file should be started. */
        private ConcurrentMap<GroupPartitionId, Integer> deltaOffsetMap;

        /** Left partitions to be processed. */
        private CopyOnWriteArraySet<GroupPartitionId> remainPartIds;

        /** {@inheritDoc} */
        @Override public String toString() {
            return "BackupContext {" +
                "inited=" + inited +
                ", tracked=" + tracked +
                ", idx=" + idx +
                ", partAllocatedPages=" + partAllocatedPages +
                ", deltaOffsetMap=" + deltaOffsetMap +
                ", remainPartIds=" + remainPartIds +
                '}';
        }
    }
}
