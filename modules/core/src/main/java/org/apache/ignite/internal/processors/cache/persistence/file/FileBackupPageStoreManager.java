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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.util.GridConcurrentHashSet;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointFuture;
import org.apache.ignite.internal.processors.cache.persistence.backup.BackupProcessTask;
import org.apache.ignite.internal.processors.cache.persistence.backup.FileTemporaryStore;
import org.apache.ignite.internal.processors.cache.persistence.backup.IgniteBackupPageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.backup.TemporaryStore;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;

/** */
public class FileBackupPageStoreManager extends GridCacheSharedManagerAdapter
    implements IgniteBackupPageStoreManager<FileBackupDescriptor> {
    /** */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + ".delta";
    /** */
    private static final String BACKUP_CP_REASON = "Wakeup for checkpoint to take backup [id=%s, grpId=%s, parts=%s]";
    /** Base working directory for saving copied pages. */
    private final File backupWorkDir;

    /** Factory to working with {@link TemporaryStore} as file storage. */
    private final FileIOFactory ioFactory;

    /**
     * Scheduled snapshot processes:
     * idx
     * grpId
     * partId
     * partiton fixed size
     * delta offset
     */

    /** Tracking partition files over all running snapshot processes. */
    private final Set<GroupPartitionId> trackList = new GridConcurrentHashSet<>();

    /** Collection of backup stores indexed by [grpId, partId] key. */
    private final Map<GroupPartitionId, TemporaryStore> backupStores = new ConcurrentHashMap<>();

    /** */
    private final IgniteLogger log;

    /** */
    private final int pageSize;

    /** */
    private final ReadWriteLock mgrLock = new ReentrantReadWriteLock();

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
        trackList.clear();
    }

    /** {@inheritDoc} */
    @Override public void backup(
        int idx,
        int grpId,
        Set<Integer> parts,
        BackupProcessTask<FileBackupDescriptor> task
    ) throws IgniteCheckedException {
        initTemporaryStores(grpId, parts);

        CheckpointFuture cpFut = cctx.database().forceCheckpoint(String.format(BACKUP_CP_REASON, idx, grpId, S.compact(parts)));

        if (cpFut == null)
            throw new IgniteCheckedException("Checkpoint thread is not running.");

        cpFut.finishFuture().listen(new IgniteInClosure<IgniteInternalFuture<Object>>() {
            @Override public void apply(IgniteInternalFuture<Object> future) {
                // Set each partitoin file size after checkpoint.
                // Set each start position offset of delta files.
                // Begin to track writes.
            }
        });

        // 1. Check for the last checkpoint and run if not.
        // 2. Wait when checkpoint process ends.
        // 2. Fix the partition files sizes.
        // 3. Start tracking all incoming updates for all cache group files.

        // Use sync mode to execute provided task over partitons and corresponding deltas.
        try {
            task.handlePartition(new FileBackupDescriptor(null, 0, 0, 0));

            task.handleDelta(new FileBackupDescriptor(null, 1, 0, 0));
        }
        catch (IgniteCheckedException e) {
            U.log(log, "An error occured while handling partition files.", e);
        }

        // Remove all tracked partitions.
    }

    /** {@inheritDoc} */
    @Override public void handleWritePageStore(GroupPartitionId pairId, PageStore store, long pageId) {
        if (!trackList.contains(pairId))
            return;

        ByteBuffer tmpPageBuff = threadPageBuf.get();

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
    @Override public void initTemporaryStores(int grpId, Set<Integer> partIds) throws IgniteCheckedException {
        if (partIds == null || partIds.isEmpty())
            return;

        CacheConfiguration ccfg = cctx.cache().cacheGroup(grpId).config();

        assert ccfg != null;

        // Create cache temporary directory if not.
        File tempGrpDir = U.resolveWorkDirectory(backupWorkDir.getAbsolutePath(), cacheDirName(ccfg), false);

        U.ensureDirectory(tempGrpDir, "temporary directory for grpId: " + grpId, log);

        for (Integer partId : partIds) {
            backupStores.putIfAbsent(new GroupPartitionId(grpId, partId),
                new FileTemporaryStore(getPartionDeltaFile(tempGrpDir, partId), ioFactory));
        }
    }

    /**
     * @param tmpDir Temporary directory to store files.
     * @param partId Cache partition identifier.
     * @return A file representation.
     */
    public static File getPartionDeltaFile(File tmpDir, int partId) {
        return new File(tmpDir, String.format(PART_DELTA_TEMPLATE, partId));
    }

    /** */
    public void setThreadPageBuf(final ThreadLocal<ByteBuffer> buf) {
        threadPageBuf = buf;
    }

    /** */
    private static class BackupContext {
        /** Unique identifier of backup process. */
        private final int idx;

        /** The length of partition file sizes up to each cache partiton file. */
        private final Map<GroupPartitionId, Long> partitionSizeMap;

        /** The offset from which reading of delta partition file should be started. */
        private final Map<GroupPartitionId, Long> deltaStartOffsetMap;

        /** Left partitions to be processed. */
        private final Set<GroupPartitionId> remainPartIds;

        /** */
        public BackupContext(int idx,
            Map<GroupPartitionId, Long> partitionSizeMap,
            Map<GroupPartitionId, Long> deltaStartOffsetMap,
            Set<GroupPartitionId> remainPartIds) {
            this.idx = idx;
            this.partitionSizeMap = partitionSizeMap;
            this.deltaStartOffsetMap = deltaStartOffsetMap;
            this.remainPartIds = remainPartIds;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            BackupContext ctx = (BackupContext)o;

            return idx == ctx.idx;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return idx;
        }
    }
}
