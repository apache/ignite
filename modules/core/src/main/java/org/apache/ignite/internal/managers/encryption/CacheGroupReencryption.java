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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.apache.ignite.thread.OomExceptionHandler;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_BATCH_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_THREAD_POOL_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REENCRYPTION_THROTTLE;

/**
 *
 */
public class CacheGroupReencryption implements DbCheckpointListener {
    /** Thread prefix for reencryption tasks. */
    private static final String REENCRYPT_THREAD_PREFIX = "reencrypt";

    /** Max amount of pages that will be read into memory under checkpoint lock. */
    private final int batchSize = IgniteSystemProperties.getInteger(IGNITE_REENCRYPTION_BATCH_SIZE, 1_000);

    /** Timeout between batches. */
    private final long timeoutBetweenBatches = IgniteSystemProperties.getLong(IGNITE_REENCRYPTION_THROTTLE, 0);

    /** Disable background re-encryption. */
    private final boolean disabled = IgniteSystemProperties.getBoolean(IGNITE_REENCRYPTION_DISABLED, false);

    /** */
    private final int threadsCnt = IgniteSystemProperties.getInteger(IGNITE_REENCRYPTION_THREAD_POOL_SIZE,
        Runtime.getRuntime().availableProcessors());

    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ReentrantLock initLock = new ReentrantLock();

    /** */
    private final Map<Integer, GroupReencryptionContext> grps = new ConcurrentHashMap<>();

    /** */
    private final Queue<Integer> completedGrps = new ConcurrentLinkedQueue<>();

    /** */
    private final IgniteThreadPoolExecutor execSvc;

    /** */
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
     * Shutdown re-encryption and disable new tasks scheduling.
     */
    public void stop() throws IgniteCheckedException {
        initLock.lock();

        try {
            stopped = true;

            for (GroupReencryptionContext state : grps.values())
                state.fut.cancel();

            execSvc.shutdown();
        } finally {
            initLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx0) {
        Set<Integer> completeCandidates = new HashSet<>();

        Integer grpId;

        while ((grpId = completedGrps.poll()) != null)
            completeCandidates.add(grpId);

        ctx0.finishedStateFut().listen(
            f -> {
                if (f.error() != null || f.isCancelled()) {
                    completedGrps.addAll(completeCandidates);

                    return;
                }

                initLock.lock();

                try {
                    for (int groupId : completeCandidates) {
                        GroupReencryptionContext scanCtx = grps.remove(groupId);

                        scanCtx.finish();

                        if (log.isInfoEnabled())
                            log.info("Cache group re-encryption is finished [grpId=" + groupId + "]");
                    }

                    if (!grps.isEmpty())
                        return;

                    ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).
                        removeCheckpointListener(this);
                }
                finally {
                    initLock.unlock();
                }
            }
        );
    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context ctx) {
        for (GroupReencryptionContext scanCtx : grps.values()) {
            if (scanCtx.skipDirty())
                return;

            ctx.finishedStateFut().listen(f -> {
                if (f.error() == null && !f.isCancelled())
                    scanCtx.skipDirty(true);
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
     * @param skipDirty Skip dirty pages.
     */
    public IgniteInternalFuture schedule(int grpId, boolean skipDirty) throws IgniteCheckedException {
        if (disabled)
            throw new IgniteCheckedException("Re-encryption is disabled.");

        CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

        if (grp == null) {
            if (log.isDebugEnabled())
                log.debug("Skip re-encryption, group was destroyed [grp=" + grpId + "]");

            return new GridFinishedFuture();
        }

        initLock.lock();

        try {
            if (stopped)
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            if (grps.isEmpty())
                ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).addCheckpointListener(this);

            GroupReencryptionContext prevState = grps.get(grpId);

            if (prevState != null) {
                if (log.isDebugEnabled())
                    log.debug("Re-encryption already scheduled [grpId=" + grpId + "]");

                return prevState.finishFuture();
            }

            GroupReencryptionContext ctx0 = new GroupReencryptionContext(grpId, skipDirty);

            IgnitePageStoreManager pageStoreMgr = ctx.cache().context().pageStore();

            forEachPartition(grp.affinity().partitions(), new IgniteInClosureX<Integer>() {
                @Override public void applyx(Integer partId) throws IgniteCheckedException {
                    int grpId = ctx0.groupId();

                    if (!pageStoreMgr.exists(grpId, partId))
                        return;

                    PageStore pageStore = ((FilePageStoreManager)pageStoreMgr).getStore(grpId, partId);

                    if (pageStore.encryptPageCount() == 0) {
                        if (log.isDebugEnabled())
                            log.debug("Skipping partition re-encryption [grp=" + grpId + ", p=" + partId + "]");

                        return;
                    }

                    PageStoreScanner scan = new PageStoreScanner(ctx0, partId, pageStore);

                    ctx0.add(partId, scan);

                    execSvc.submit(scan);
                }
            });

            ctx0.initialize().listen(f -> {
                Throwable t = f.error();

                if (t != null) {
                    log.error("Re-encryption is failed [grpId=" + grpId + "]", t);

                    ctx0.cpFut.onDone(t);

                    return;
                }

                boolean added = completedGrps.offer(grpId);

                assert added;
            });

            if (log.isInfoEnabled())
                log.info("Scheduled re-encryption [grpId=" + grpId + "]");

            grps.put(grpId, ctx0);

            return ctx0.finishFuture();
        }
        finally {
            initLock.unlock();
        }
    }

    public IgniteInternalFuture<Void> statusFuture(int grpId) {
        GroupReencryptionContext state = grps.get(grpId);

        return state == null ? new GridFinishedFuture<>() : state.cpFut;
    }

    public boolean cancel(int grpId, int partId) throws IgniteCheckedException {
        GroupReencryptionContext encryptCtx = grps.get(grpId);

        if (encryptCtx == null)
            return false;

        IgniteInternalFuture<Void> reencryptFut = encryptCtx.futMap.get(partId);

        if (reencryptFut == null)
            return false;

        return reencryptFut.cancel();
    }

    /**
     * Save current pages count for reencryption.
     *
     * @param grpId Cache group ID.
     * @return List of partitions with current page count.
     * @throws IgniteCheckedException If failed.
     */
    public List<T2<Integer, Integer>> storePagesCount(int grpId) throws IgniteCheckedException {
        List<T2<Integer, Integer>> offsets = new ArrayList<>();

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

                    pageStore.encryptPageCount(pagesCnt);
                    pageStore.encryptPageIndex(0);

                    storePagesCountOnMetaPage(grp, p, pagesCnt);

                    offsets.add(new T2<>(p, pagesCnt));
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

                changed |= metaIO.setEncryptPageCount(pageAddr, cnt);
                changed |= metaIO.setEncryptPageIndex(pageAddr, 0);

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

    private static class GroupReencryptionContext {
        private final Map<Integer, IgniteInternalFuture<Void>> futMap = new ConcurrentHashMap<>();

        private final GridCompoundFuture<Void, Void> fut = new GridCompoundFuture<>();

        private final int grpId;

        private volatile boolean skipDirty;

        private final GridFutureAdapter<Void> cpFut = new GridFutureAdapter<Void>() {
            @Override public boolean cancel() throws IgniteCheckedException {
                fut.cancel();

                return onDone(null, null, true);
            }
        };

        public GroupReencryptionContext(int grpId, boolean skipDirty) {
            this.grpId = grpId;
            this.skipDirty = skipDirty;
        }

        public int groupId() {
            return grpId;
        }

        public boolean skipDirty() {
            return skipDirty;
        }

        public void skipDirty(boolean skipDirty) {
            this.skipDirty = skipDirty;
        }

        public IgniteInternalFuture<Void> finishFuture() {
            return cpFut;
        }

        public boolean finish() {
            return cpFut.onDone(fut.result());
        }

        public void add(int partId, IgniteInternalFuture<Void> fut0) {
            fut.add(fut0);

            futMap.put(partId, fut0);
        }

        public IgniteInternalFuture<Void> initialize() {
            return fut.markInitialized();
        }
    }

    private class PageStoreScanner extends GridFutureAdapter<Void> implements Runnable {
        private final int partId;

        private final Object cancelMux = new Object();

        private final PageStore store;

        private final GroupReencryptionContext scanCtx;

        public PageStoreScanner(GroupReencryptionContext scanCtx, int partId, PageStore store) {
            this.scanCtx = scanCtx;
            this.partId = partId;
            this.store = store;
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() throws IgniteCheckedException {
            synchronized (cancelMux) {
                // todo cancel properly
                return onDone();
            }
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                int grpId = scanCtx.groupId();
                CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

                if (grp == null) {
                    onDone();

                    return;
                }

                IgniteWriteAheadLogManager wal = ctx.cache().context().wal();
                PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();
                long metaPageId = pageMem.partitionMetaPageId(grpId, partId);
                int pageSize = pageMem.realPageSize(grpId);
                int pageNum = store.encryptPageIndex();
                int cnt = store.encryptPageCount();

                assert cnt <= store.pages() : "cnt=" + cnt + ", max=" + store.pages();

                if (log.isDebugEnabled()) {
                    log.debug("Partition re-encryption is started [" +
                        "p=" + partId + ", remain=" + (cnt - pageNum) + ", total=" + cnt + "]");
                }

                while (pageNum < cnt) {
                    synchronized (cancelMux) {
                        ctx.cache().context().database().checkpointReadLock();

                        try {
                            if (isDone() || store.encryptPageCount() == 0)
                                break;

                            int end = Math.min(pageNum + batchSize, cnt);

                            do {
                                long pageId = metaPageId + pageNum;

                                pageNum += 1;

                                long page = pageMem.acquirePage(grpId, pageId);

                                try {
                                    if (scanCtx.skipDirty() && pageMem.isDirty(grpId, pageId, page))
                                        continue;

                                    long pageAddr = pageMem.writeLock(grpId, pageId, page, true);

                                    try {
                                        if (PageHandler.isWalDeltaRecordNeeded(pageMem, grpId, pageId, page, wal, null)) {
                                            byte[] payload = PageUtils.getBytes(pageAddr, 0, pageSize);

                                            FullPageId fullPageId = new FullPageId(pageId, grpId);

                                            wal.log(new PageSnapshot(fullPageId, payload, pageSize));
                                        }
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

                    store.encryptPageIndex(pageNum);

                    if (timeoutBetweenBatches != 0 && !isDone())
                        U.sleep(timeoutBetweenBatches);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Partition re-encryption is finished " +
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

