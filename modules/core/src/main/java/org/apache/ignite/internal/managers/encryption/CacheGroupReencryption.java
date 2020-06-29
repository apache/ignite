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

package org.apache.ignite.internal.managers.encryption;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.apache.ignite.thread.OomExceptionHandler;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_BATCH_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_THREAD_POOL_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_THROTTLE;

/**
 * Cache group reencryption manager.
 */
public class CacheGroupReencryption implements DbCheckpointListener {
    /** Thread prefix for reencryption tasks. */
    private static final String REENCRYPT_THREAD_PREFIX = "reencrypt";

    /** Max amount of pages that will be read into memory under checkpoint lock. */
    private final int batchSize = IgniteSystemProperties.getInteger(IGNITE_REENCRYPTION_BATCH_SIZE, 1_000);

    /** Timeout between batches. */
    private final long timeoutBetweenBatches = IgniteSystemProperties.getLong(IGNITE_REENCRYPTION_THROTTLE, 0);

    /** Disable background reencryption. */
    private final boolean disabled = IgniteSystemProperties.getBoolean(IGNITE_REENCRYPTION_DISABLED, false);

    /** Number of threads for partition scanning. */
    private final int threadsCnt = IgniteSystemProperties.getInteger(IGNITE_REENCRYPTION_THREAD_POOL_SIZE,
        Runtime.getRuntime().availableProcessors());

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Lock. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Mapping of cache group ID to reencryption context. */
    private final Map<Integer, GroupReencryptionContext> grps = new ConcurrentHashMap<>();

    /** Queue of groups waiting for a checkpoint. */
    private final Queue<Integer> cpWaitGrps = new ConcurrentLinkedQueue<>();

    /** Executor to start partition scan tasks. */
    private final IgniteThreadPoolExecutor execSvc;

    /** Stop flag. */
    private boolean stopped;

    /**
     * @param ctx Grid kernal context.
     */
    public CacheGroupReencryption(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        execSvc = new IgniteThreadPoolExecutor(REENCRYPT_THREAD_PREFIX,
            ctx.igniteInstanceName(),
            threadsCnt,
            threadsCnt,
            IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.SYSTEM_POOL,
            new OomExceptionHandler(ctx));

        execSvc.allowCoreThreadTimeOut(true);
    }

    /**
     * Shutdown reencryption and disable new tasks scheduling.
     */
    public void stop() throws IgniteCheckedException {
        lock.lock();

        try {
            stopped = true;

            for (GroupReencryptionContext ctx0 : grps.values())
                ctx0.finishFuture().cancel();

            execSvc.shutdown();
        } finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context cpCtx) {
        Set<Integer> completeCandidates = new HashSet<>();

        Integer grpId;

        while ((grpId = cpWaitGrps.poll()) != null)
            completeCandidates.add(grpId);

        cpCtx.finishedStateFut().listen(
            f -> {
                if (f.error() != null || f.isCancelled()) {
                    cpWaitGrps.addAll(completeCandidates);

                    return;
                }

                lock.lock();

                try {
                    for (int groupId : completeCandidates) {
                        GroupReencryptionContext encrCtx = grps.remove(groupId);

                        boolean finished = encrCtx.finish();

                        assert finished : groupId;

                        if (log.isInfoEnabled())
                            log.info("Cache group reencryption is finished [grpId=" + groupId + "]");
                    }

                    if (!grps.isEmpty())
                        return;

                    ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).
                        removeCheckpointListener(this);
                }
                finally {
                    lock.unlock();
                }
            }
        );
    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context cpCtx) {
        for (GroupReencryptionContext encrCtx : grps.values()) {
            if (encrCtx.skipDirty())
                return;

            cpCtx.finishedStateFut().listen(f -> {
                if (f.error() == null && !f.isCancelled())
                    encrCtx.skipDirty(true);
            });
        }
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) {
        // No-op.
    }

    /**
     * @return {@code True} If reencryption is disabled.
     */
    public boolean disabled() {
        return disabled;
    }

    /**
     * @param grpId Cache group ID.
     * @param skipDirty Dirty page skip flag.
     */
    public IgniteInternalFuture schedule(int grpId, boolean skipDirty) throws IgniteCheckedException {
        if (disabled)
            throw new IgniteCheckedException("Reencryption is disabled.");

        CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

        if (grp == null) {
            if (log.isDebugEnabled())
                log.debug("Skip reencryption, group was destroyed [grp=" + grpId + "]");

            return new GridFinishedFuture();
        }

        lock.lock();

        try {
            if (stopped)
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            if (grps.isEmpty())
                ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).addCheckpointListener(this);

            GroupReencryptionContext prevState = grps.get(grpId);

            if (prevState != null) {
                if (log.isDebugEnabled())
                    log.debug("Reencryption already scheduled [grpId=" + grpId + "]");

                return prevState.finishFuture();
            }

            GroupReencryptionContext ctx0 = new GroupReencryptionContext(grpId, skipDirty);

            IgnitePageStoreManager pageStoreMgr = ctx.cache().context().pageStore();

            forEachPartition(grp.affinity().partitions(), new IgniteInClosureX<Integer>() {
                @Override public void applyx(Integer partId) throws IgniteCheckedException {
                    if (!pageStoreMgr.exists(grpId, partId))
                        return;

                    PageStore pageStore = ((FilePageStoreManager)pageStoreMgr).getStore(grpId, partId);

                    if (pageStore.encryptedPageCount() == 0) {
                        if (log.isDebugEnabled())
                            log.debug("Skipping partition reencryption [grp=" + grpId + ", p=" + partId + "]");

                        return;
                    }

                    PageStoreScanTask scan = new PageStoreScanTask(ctx0, partId, pageStore);

                    ctx0.add(partId, scan);

                    execSvc.submit(scan);
                }
            });

            ctx0.initialize().listen(f -> {
                Throwable t = f.error();

                if (t != null) {
                    log.error("Reencryption is failed [grpId=" + grpId + "]", t);

                    ctx0.cpFut.onDone(t);

                    return;
                }

                boolean added = cpWaitGrps.offer(grpId);

                assert added;
            });

            if (log.isInfoEnabled())
                log.info("Scheduled reencryption [grpId=" + grpId + "]");

            grps.put(grpId, ctx0);

            return ctx0.finishFuture();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @param grpId Cache group ID.
     * @return Future that will be completed when the reencryption of the specified group ends.
     */
    public IgniteInternalFuture<Void> statusFuture(int grpId) {
        GroupReencryptionContext state = grps.get(grpId);

        return state == null ? new GridFinishedFuture<>() : state.cpFut;
    }

    /**
     * Stop reencryption of the specified partition.
     *
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @return {@code True} if reencryption was cancelled.
     * @throws IgniteCheckedException If failed.
     */
    public boolean cancel(int grpId, int partId) throws IgniteCheckedException {
        GroupReencryptionContext ctx = grps.get(grpId);

        if (ctx == null)
            return false;

        return ctx.cancel(partId);
    }

    /**
     * Save current pages count for reencryption.
     *
     * @param grpId Cache group ID.
     * @return Map of partitions with current page count.
     * @throws IgniteCheckedException If failed.
     */
    public Map<Integer, Integer> storePagesCount(int grpId) throws IgniteCheckedException {
        Map<Integer, Integer> offsets = new HashMap<>();

        CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

        FilePageStoreManager pageStoreMgr = (FilePageStoreManager)ctx.cache().context().pageStore();

        ctx.cache().context().database().checkpointReadLock();

        try {
            forEachPartition(grp.affinity().partitions(), new IgniteInClosureX<Integer>() {
                @Override public void applyx(Integer p) throws IgniteCheckedException {
                    if (!pageStoreMgr.exists(grpId, p))
                        return;

                    PageStore pageStore = pageStoreMgr.getStore(grpId, p);

                    int pagesCnt = pageStore.pages();

                    pageStore.encryptedPageCount(pagesCnt);
                    pageStore.encryptedPageIndex(0);

                    storePagesCountOnMetaPage(grp, p, pagesCnt);

                    offsets.put(p, pagesCnt);
                }
            });
        } finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }

        return offsets;
    }

    /**
     * Save number of pages on meta page.
     *
     * @param grp Cache group.
     * @param partId Partition ID.
     * @param cnt Number of pages to reencrypt.
     * @throws IgniteCheckedException If failed.
     */
    public void storePagesCountOnMetaPage(CacheGroupContext grp, int partId, int cnt) throws IgniteCheckedException {
        assert ctx.cache().context().database().checkpointLockIsHeldByThread();

        PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();

        long pageId = partId == PageIdAllocator.INDEX_PARTITION ?
            pageMem.metaPageId(grp.groupId()) : pageMem.partitionMetaPageId(grp.groupId(), partId);

        long page = pageMem.acquirePage(grp.groupId(), pageId, grp.statisticsHolderData());

        try {
            long pageAddr = pageMem.writeLock(grp.groupId(), pageId, page);

            boolean changed = false;

            try {
                PageMetaIO metaIO = partId == PageIdAllocator.INDEX_PARTITION ?
                    PageMetaIO.VERSIONS.forPage(pageAddr) : PagePartitionMetaIO.VERSIONS.forPage(pageAddr);

                changed |= metaIO.setEncryptedPageCount(pageAddr, cnt);
                changed |= metaIO.setEncryptedPageIndex(pageAddr, 0);

                IgniteWriteAheadLogManager wal = ctx.cache().context().wal();

                // Save snapshot if page is dirty.
                if (PageHandler.isWalDeltaRecordNeeded(pageMem, grp.groupId(), pageId, page, wal, null)) {
                    byte[] payload = PageUtils.getBytes(pageAddr, 0, pageMem.realPageSize(grp.groupId()));

                    FullPageId fullPageId = new FullPageId(pageId, grp.groupId());

                    wal.log(new PageSnapshot(fullPageId, payload, pageMem.realPageSize(grp.groupId())));
                }
            }
            finally {
                pageMem.writeUnlock(grp.groupId(), pageId, page, null, changed, true);
            }
        }
        finally {
            pageMem.releasePage(grp.groupId(), pageId, page);
        }
    }

    /**
     * @param partCnt Parttiion count.
     * @param hnd Handler.
     */
    private void forEachPartition(int partCnt, IgniteInClosureX<Integer> hnd) throws IgniteCheckedException {
        for (int p = 0; p < partCnt; p++)
            hnd.applyx(p);

        hnd.applyx(PageIdAllocator.INDEX_PARTITION);
    }

    /**
     * Cache group reencryption context.
     */
    private static class GroupReencryptionContext {
        /** Partition scanning futures. */
        private final Map<Integer, IgniteInternalFuture<Void>> futMap = new ConcurrentHashMap<>();

        /** Compound future, that will be completed when all partitions scanned. */
        private final GridCompoundFuture<Void, Void> compFut = new GridCompoundFuture<>();

        /** Cache group ID. */
        private final int grpId;

        /** Dirty page skip flag. */
        private volatile boolean skipDirty;

        /** Future that ends after all partitions are done and a checkpoint is finished. */
        private final GridFutureAdapter<Void> cpFut = new GridFutureAdapter<Void>() {
            @Override public boolean cancel() throws IgniteCheckedException {
                compFut.cancel();

                return onDone(null, null, true);
            }
        };

        /**
         * @param grpId Cache group ID.
         * @param skipDirty Dirty page skip flag.
         */
        public GroupReencryptionContext(int grpId, boolean skipDirty) {
            this.grpId = grpId;
            this.skipDirty = skipDirty;
        }

        /**
         * @return Cache group ID.
         */
        public int groupId() {
            return grpId;
        }

        /**
         * @return Dirty page skip flag.
         */
        public boolean skipDirty() {
            return skipDirty;
        }

        /**
         * @param skipDirty Dirty page skip flag.
         */
        public void skipDirty(boolean skipDirty) {
            this.skipDirty = skipDirty;
        }

        /**
         * @param partId Partition ID.
         * @param fut Partition scanning future.
         */
        public void add(int partId, IgniteInternalFuture<Void> fut) {
            compFut.add(fut);

            futMap.put(partId, fut);
        }

        /**
         * @return Compound future, that will be completed when all partitions scanned.
         */
        public IgniteInternalFuture<Void> initialize() {
            return compFut.markInitialized();
        }

        /**
         * @return Future that ends after all partitions are done and a checkpoint is finished.
         */
        public IgniteInternalFuture<Void> finishFuture() {
            return cpFut;
        }

        /**
         * Finish reencryption future.
         *
         * @return {@code True} if the future was finished by this call.
         */
        public boolean finish() {
            return cpFut.onDone(compFut.result());
        }

        /**
         * Stop reencryption of the specified partition.
         *
         * @param partId Partition ID.
         * @return {@code True} if reencryption was cancelled.
         * @throws IgniteCheckedException If failed.
         */
        public boolean cancel(int partId) throws IgniteCheckedException {
            IgniteInternalFuture<Void> fut = futMap.get(partId);

            if (fut == null)
                return false;

            return fut.cancel();
        }
    }

    /**
     * Page store scanning task.
     */
    private class PageStoreScanTask extends GridFutureAdapter<Void> implements Runnable {
        /** Partiion ID. */
        private final int partId;

        /** Page store. */
        private final PageStore store;

        /** Cache group reencryption context. */
        private final GroupReencryptionContext encrCtx;

        /**
         * @param encrCtx Cache group reencryption context.
         * @param partId Partition ID.
         * @param store Page store.
         */
        public PageStoreScanTask(GroupReencryptionContext encrCtx, int partId, PageStore store) {
            this.encrCtx = encrCtx;
            this.partId = partId;
            this.store = store;
        }

        /** {@inheritDoc} */
        @Override public synchronized boolean cancel() throws IgniteCheckedException {
            return onDone();
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                int grpId = encrCtx.groupId();

                CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

                if (grp == null) {
                    onDone();

                    return;
                }

                IgniteWriteAheadLogManager wal = ctx.cache().context().wal();
                PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();
                long metaPageId = pageMem.partitionMetaPageId(grpId, partId);
                int pageSize = pageMem.realPageSize(grpId);
                int pageNum = store.encryptedPageIndex();
                int cnt = store.encryptedPageCount();

                assert cnt <= store.pages() : "cnt=" + cnt + ", max=" + store.pages();

                if (log.isDebugEnabled()) {
                    log.debug("Partition reencryption is started [" +
                        "p=" + partId + ", remain=" + (cnt - pageNum) + ", total=" + cnt + "]");
                }

                while (pageNum < cnt) {
                    synchronized (this) {
                        ctx.cache().context().database().checkpointReadLock();

                        try {
                            if (isDone() || store.encryptedPageCount() == 0)
                                break;

                            int end = Math.min(pageNum + batchSize, cnt);

                            do {
                                long pageId = metaPageId + pageNum;

                                pageNum += 1;

                                long page = pageMem.acquirePage(grpId, pageId);

                                boolean skipDirty = encrCtx.skipDirty();

                                try {
                                    // Can skip rewriting a dirty page if the checkpoint has been completed.
                                    if (skipDirty && pageMem.isDirty(grpId, pageId, page))
                                        continue;

                                    long pageAddr = pageMem.writeLock(grpId, pageId, page, true);

                                    try {
                                        if (skipDirty ||
                                            !PageHandler.isWalDeltaRecordNeeded(pageMem, grpId, pageId, page, wal, null))
                                            continue;

                                        // If checkpoint has not been completed and the page is dirty - save snapshot.
                                        byte[] payload = PageUtils.getBytes(pageAddr, 0, pageSize);

                                        FullPageId fullPageId = new FullPageId(pageId, grpId);

                                        wal.log(new PageSnapshot(fullPageId, payload, pageSize));
                                    }
                                    finally {
                                        pageMem.writeUnlock(grpId, pageId, page, null, true, false);
                                    }
                                }
                                finally {
                                    pageMem.releasePage(grpId, pageId, page);
                                }
                            }
                            while (pageNum < end);
                        }
                        finally {
                            ctx.cache().context().database().checkpointReadUnlock();
                        }
                    }

                    store.encryptedPageIndex(pageNum);

                    if (timeoutBetweenBatches != 0 && !isDone())
                        U.sleep(timeoutBetweenBatches);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Partition reencryption is finished " +
                        "[p=" + partId +
                        ", remain=" + (cnt - pageNum) +
                        ", total=" + cnt +
                        ", cancelled=" + isCancelled() +
                        ", failed=" + isFailed() + "]");
                }

                onDone();
            }
            catch (Throwable t) {
                if (X.hasCause(t, NodeStoppingException.class))
                    onCancelled();
                else
                    onDone(t);
            }
        }
    }
}
