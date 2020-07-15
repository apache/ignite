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
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
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
 * Cache group page stores scanner.
 */
public class CacheGroupPageScanner implements DbCheckpointListener {
    /** Thread prefix for scanning tasks. */
    private static final String REENCRYPT_THREAD_PREFIX = "reencrypt";

    /** Max amount of pages that will be read into memory under checkpoint lock. */
    private final int batchSize = IgniteSystemProperties.getInteger(IGNITE_REENCRYPTION_BATCH_SIZE, 1_000);

    /** Timeout between batches. */
    private final long timeoutBetweenBatches = IgniteSystemProperties.getLong(IGNITE_REENCRYPTION_THROTTLE, 0);

    /** Disable background pages scanning. */
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

    /** Mapping of cache group ID to scanning context. */
    private final Map<Integer, GroupScanContext> grps = new ConcurrentHashMap<>();

    /** Queue of groups waiting for a checkpoint. */
    private final Queue<Integer> cpWaitGrps = new ConcurrentLinkedQueue<>();

    /** Executor to start partition scan tasks. */
    private final IgniteThreadPoolExecutor execSvc;

    /** Stop flag. */
    private boolean stopped;

    /**
     * @param ctx Grid kernal context.
     */
    public CacheGroupPageScanner(GridKernalContext ctx) {
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
     * Shutdown scanning and disable new tasks scheduling.
     */
    public void stop() throws IgniteCheckedException {
        lock.lock();

        try {
            stopped = true;

            for (GroupScanContext ctx0 : grps.values())
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
                        GroupScanContext scanCtx = grps.remove(groupId);

                        boolean finished = scanCtx.finish();

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
        for (GroupScanContext scanCtx : grps.values()) {
            if (scanCtx.skipDirty())
                return;

            cpCtx.finishedStateFut().listen(f -> {
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

            GroupScanContext prevState = grps.get(grpId);

            if (prevState != null) {
                if (log.isDebugEnabled())
                    log.debug("Reencryption already scheduled [grpId=" + grpId + "]");

                return prevState.finishFuture();
            }

            GroupScanContext ctx0 = new GroupScanContext(grpId, skipDirty);

            forEachPageStore(grp, new IgniteInClosureX<Integer>() {
                @Override public void applyx(Integer partId) {
                    if (ctx.encryption().getEncryptionState(grpId, partId) == 0) {
                        if (log.isDebugEnabled())
                            log.debug("Skipping partition reencryption [grp=" + grpId + ", p=" + partId + "]");

                        return;
                    }

                    PageStoreScanTask scan = new PageStoreScanTask(ctx0, partId);

                    ctx0.add(partId, scan);

                    execSvc.submit(scan);
                }
            });

            ctx0.initialize().listen(f -> {
                Throwable t = f.error();

                if (t != null) {
                    log.error("Reencryption is failed [grpId=" + grpId + "]", t);

                    ctx0.fail(t);

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
        GroupScanContext ctx0 = grps.get(grpId);

        return ctx0 == null ? new GridFinishedFuture<>() : ctx0.finishFuture();
    }

    /**
     * Stop scannig the specified partition.
     *
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @return {@code True} if reencryption was cancelled.
     * @throws IgniteCheckedException If failed.
     */
    public boolean cancel(int grpId, int partId) throws IgniteCheckedException {
        GroupScanContext ctx = grps.get(grpId);

        if (ctx == null)
            return false;

        return ctx.cancel(partId);
    }

    /**
     * Collect current pages count for reencryption.
     *
     * @param grp Cache group.
     * @return Map of partitions with current page count.
     * @throws IgniteCheckedException If failed.
     */
    public Map<Integer, Long> pagesCount(CacheGroupContext grp) throws IgniteCheckedException {
        Map<Integer, Long> partStates = new HashMap<>();

        ctx.cache().context().database().checkpointReadLock();

        try {
            forEachPageStore(grp, new IgniteInClosureX<Integer>() {
                @Override public void applyx(Integer partId) throws IgniteCheckedException {
                    int pagesCnt = ctx.cache().context().pageStore().pages(grp.groupId(), partId);

                    partStates.put(partId, (long)pagesCnt);
                }
            });
        } finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }

        return partStates;
    }

    /**
     * @param grp Cache group context.
     * @param hnd Page store handler.
     */
    private void forEachPageStore(CacheGroupContext grp, IgniteInClosureX<Integer> hnd) throws IgniteCheckedException {
        int parts = grp.affinity().partitions();

        IgnitePageStoreManager pageStoreMgr = ctx.cache().context().pageStore();

        for (int p = 0; p < parts; p++) {
            if (!pageStoreMgr.exists(grp.groupId(), p))
                continue;

            hnd.applyx(p);
        }

        hnd.applyx(PageIdAllocator.INDEX_PARTITION);
    }

    /**
     * Cache group reencryption context.
     */
    private static class GroupScanContext {
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
        public GroupScanContext(int grpId, boolean skipDirty) {
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

        /**
         * @param t Throwable.
         */
        public void fail(Throwable t) {
            cpFut.onDone(t);
        }
    }

    /**
     * Page store scanning task.
     */
    private class PageStoreScanTask extends GridFutureAdapter<Void> implements Runnable {
        /** Partiion ID. */
        private final int partId;

        /** Cache group scan context. */
        private final GroupScanContext scanCtx;

        /**
         * @param scanCtx Cache group reencryption context.
         * @param partId Partition ID.
         */
        public PageStoreScanTask(GroupScanContext scanCtx, int partId) {
            this.scanCtx = scanCtx;
            this.partId = partId;
        }

        /** {@inheritDoc} */
        @Override public synchronized boolean cancel() throws IgniteCheckedException {
            return onDone();
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

                PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();
                long metaPageId = pageMem.partitionMetaPageId(grpId, partId);
                long state = ctx.encryption().getEncryptionState(grpId, partId);

                int off = (int)(state >> 32);
                int cnt = (int)state;

                if (log.isDebugEnabled()) {
                    log.debug("Partition reencryption is started [grpId=" + grpId +
                        ", p=" + partId + ", remain=" + (cnt - off) + ", total=" + cnt + "]");
                }

                while (off < cnt) {
                    synchronized (this) {
                        ctx.cache().context().database().checkpointReadLock();

                        try {
                            if (isDone() || ctx.encryption().getEncryptionState(grpId, partId) == 0)
                                break;

                            off += scanPages(pageMem, metaPageId + off, Math.min(batchSize, cnt - off));
                        }
                        finally {
                            ctx.cache().context().database().checkpointReadUnlock();
                        }
                    }

                    ctx.encryption().setEncryptionState(grpId, partId, off, cnt);

                    if (timeoutBetweenBatches != 0 && !isDone())
                        U.sleep(timeoutBetweenBatches);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Partition reencryption is finished " +
                        "[grpId=" + grpId +
                        ", p=" + partId +
                        ", remain=" + (cnt - off) +
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

        /**
         * @param pageMem Page memory.
         * @param startPageId Start page ID.
         * @param cnt Count of pages to scan.
         * @return Count of scanned pages.
         * @throws IgniteCheckedException If failed.
         */
        private int scanPages(PageMemoryEx pageMem, long startPageId, int cnt) throws IgniteCheckedException {
            for (long pageId = startPageId; pageId < startPageId + cnt; pageId++) {
                long page = pageMem.acquirePage(scanCtx.groupId(), pageId);

                try {
                    // Can skip rewriting a dirty page if the checkpoint has been completed.
                    if (scanCtx.skipDirty() && pageMem.isDirty(scanCtx.groupId(), pageId, page))
                        continue;

                    pageMem.writeLock(scanCtx.groupId(), pageId, page, true);
                    pageMem.writeUnlock(scanCtx.groupId(), pageId, page, null, true);
                }
                finally {
                    pageMem.releasePage(scanCtx.groupId(), pageId, page);
                }
            }

            return cnt;
        }
    }
}
