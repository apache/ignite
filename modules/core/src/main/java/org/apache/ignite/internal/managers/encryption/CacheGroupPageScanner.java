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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.BasicRateLimiter;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.apache.ignite.thread.OomExceptionHandler;

import static org.apache.ignite.internal.util.IgniteUtils.MB;

/**
 * Cache group page stores scanner.
 * Scans a range of pages and marks them as dirty to re-encrypt them with the last encryption key on disk.
 */
public class CacheGroupPageScanner implements DbCheckpointListener {
    /** Thread prefix for scanning tasks. */
    private static final String REENCRYPT_THREAD_PREFIX = "reencrypt";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Lock. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Mapping of cache group ID to group scanning task. */
    private final Map<Integer, GroupScanTask> grps = new ConcurrentHashMap<>();

    /** Collection of groups waiting for a checkpoint. */
    private final Collection<GroupScanTask> cpWaitGrps = new ConcurrentLinkedQueue<>();

    /** Page scanning speed limiter. */
    private final BasicRateLimiter limiter;

    /** Single-threaded executor to run cache group scan task. */
    private final ThreadPoolExecutor singleExecSvc;

    /** Number of pages that is scanned during reencryption under checkpoint lock. */
    private final int batchSize;

    /** Stop flag. */
    private boolean stopped;

    /**
     * @param ctx Grid kernal context.
     */
    public CacheGroupPageScanner(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        DataStorageConfiguration dsCfg = ctx.config().getDataStorageConfiguration();

        if (!CU.isPersistenceEnabled(dsCfg)) {
            batchSize = -1;
            limiter = null;
            singleExecSvc = null;

            return;
        }

        double rateLimit = dsCfg.getEncryptionConfiguration().getReencryptionRateLimit();

        limiter = rateLimit > 0 ? new BasicRateLimiter(rateLimit * MB /
            (dsCfg.getPageSize() == 0 ? DataStorageConfiguration.DFLT_PAGE_SIZE : dsCfg.getPageSize())) : null;

        batchSize = dsCfg.getEncryptionConfiguration().getReencryptionBatchSize();

        singleExecSvc = new IgniteThreadPoolExecutor(REENCRYPT_THREAD_PREFIX,
            ctx.igniteInstanceName(),
            1,
            1,
            IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.SYSTEM_POOL,
            new OomExceptionHandler(ctx));

        singleExecSvc.allowCoreThreadTimeOut(true);
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context cpCtx) {
        Set<GroupScanTask> completeCandidates = new HashSet<>();

        cpWaitGrps.removeIf(completeCandidates::add);

        cpCtx.finishedStateFut().listen(
            f -> {
                // Retry if error occurs.
                if (f.error() != null || f.isCancelled()) {
                    cpWaitGrps.addAll(completeCandidates);

                    return;
                }

                lock.lock();

                try {
                    for (GroupScanTask grpScanTask : completeCandidates) {
                        grps.remove(grpScanTask.groupId());

                        grpScanTask.onDone();

                        if (log.isInfoEnabled())
                            log.info("Cache group reencryption is finished [grpId=" + grpScanTask.groupId() + "]");
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
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) {
        // No-op.
    }

    /**
     * Schedule scanning partitions.
     *
     * @param grpId Cache group ID.
     */
    public IgniteInternalFuture<Void> schedule(int grpId) throws IgniteCheckedException {
        CacheGroupContext grp = ctx.cache().cacheGroup(grpId);

        if (grp == null || !grp.affinityNode()) {
            if (log.isInfoEnabled())
                log.info("Skip reencryption, cache group doesn't exist on the local node [grp=" + grpId + "]");

            return new GridFinishedFuture<>();
        }

        lock.lock();

        try {
            if (stopped)
                throw new NodeStoppingException("Operation has been cancelled (node is stopping).");

            if (grps.isEmpty())
                ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).addCheckpointListener(this);

            GroupScanTask prevState = grps.get(grpId);

            if (prevState != null && !prevState.isDone()) {
                if (log.isDebugEnabled())
                    log.debug("Reencryption already scheduled [grpId=" + grpId + "]");

                return prevState;
            }

            Set<Integer> parts = new HashSet<>();

            forEachPageStore(grp, new IgniteInClosureX<Integer>() {
                @Override public void applyx(Integer partId) {
                    if (ctx.encryption().getEncryptionState(grpId, partId) == 0) {
                        if (log.isDebugEnabled())
                            log.debug("Skipping partition reencryption [grp=" + grpId + ", p=" + partId + "]");

                        return;
                    }

                    parts.add(partId);
                }
            });

            GroupScanTask grpScan = new GroupScanTask(grp, parts);

            singleExecSvc.submit(grpScan);

            if (log.isInfoEnabled())
                log.info("Scheduled reencryption [grpId=" + grpId + "]");

            grps.put(grpId, grpScan);

            return grpScan;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @param grpId Cache group ID.
     * @return Future that will be completed when all partitions have been scanned and pages have been written to disk.
     */
    public IgniteInternalFuture<Void> statusFuture(int grpId) {
        GroupScanTask grpScanTask = grps.get(grpId);

        return grpScanTask == null ? new GridFinishedFuture<>() : grpScanTask;
    }

    /**
     * Shutdown scanning and disable new tasks scheduling.
     */
    public void stop() throws IgniteCheckedException {
        lock.lock();

        try {
            stopped = true;

            for (GroupScanTask grpScanTask : grps.values())
                grpScanTask.cancel();

            if (singleExecSvc != null)
                singleExecSvc.shutdownNow();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stop scannig the specified partition.
     *
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @return {@code True} if reencryption was cancelled.
     */
    public boolean excludePartition(int grpId, int partId) {
        GroupScanTask grpScanTask = grps.get(grpId);

        if (grpScanTask == null)
            return false;

        return grpScanTask.excludePartition(partId);
    }

    /**
     * Collect current number of pages in the specified cache group.
     *
     * @param grp Cache group.
     * @return Partitions with current page count.
     * @throws IgniteCheckedException If failed.
     */
    public long[] pagesCount(CacheGroupContext grp) throws IgniteCheckedException {
        // The last element of the array is used to store the status of the index partition.
        long[] partStates = new long[grp.affinity().partitions() + 1];

        ctx.cache().context().database().checkpointReadLock();

        try {
            forEachPageStore(grp, new IgniteInClosureX<Integer>() {
                @Override public void applyx(Integer partId) throws IgniteCheckedException {
                    int pagesCnt = ctx.cache().context().pageStore().pages(grp.groupId(), partId);

                    // The last element of the array is used to store the status of the index partition.
                    partStates[Math.min(partId, partStates.length - 1)] = pagesCnt;
                }
            });
        } finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }

        return partStates;
    }

    /**
     * @param grp Cache group.
     * @param hnd Partition handler.
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
     * Cache group partition scanning task.
     */
    private class GroupScanTask extends GridFutureAdapter<Void> implements Runnable {
        /** Cache group ID. */
        private final CacheGroupContext grp;

        /** Partition IDs. */
        private final Set<Integer> parts;

        /** Page memory. */
        private final PageMemoryEx pageMem;

        /**
         * @param grp Cache group.
         */
        public GroupScanTask(CacheGroupContext grp, Set<Integer> parts) {
            this.grp = grp;
            this.parts = new GridConcurrentHashSet<>(parts);

            pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();
        }

        /** {@inheritDoc} */
        @Override public synchronized boolean cancel() throws IgniteCheckedException {
            return onCancelled();
        }

        /**
         * Stop reencryption of the specified partition.
         *
         * @param partId Partition ID.
         * @return {@code True} if reencryption was cancelled.
         */
        public synchronized boolean excludePartition(int partId) {
            return parts.remove(partId);
        }

        /**
         * @return Cache group ID.
         */
        public int groupId() {
            return grp.groupId();
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                for (int partId : parts) {
                    long state = ctx.encryption().getEncryptionState(grp.groupId(), partId);

                    if (state == 0)
                        continue;

                    scanPartition(partId, ReencryptStateUtils.pageIndex(state), ReencryptStateUtils.pageCount(state));

                    if (isDone())
                        return;
                }

                boolean added = cpWaitGrps.add(this);

                assert added;
            }
            catch (Throwable t) {
                if (X.hasCause(t, NodeStoppingException.class))
                    onCancelled();
                else
                    onDone(t);
            }
        }

        /**
         * @param partId Partition ID.
         * @param off Start page offset.
         * @param cnt Count of pages to scan.
         */
        private void scanPartition(int partId, int off, int cnt) throws IgniteCheckedException {
            if (log.isDebugEnabled()) {
                log.debug("Partition reencryption is started [grpId=" + grp.groupId() +
                    ", p=" + partId + ", remain=" + (cnt - off) + ", total=" + cnt + "]");
            }

            while (off < cnt) {
                int pagesCnt = Math.min(batchSize, cnt - off);

                if (limiter != null)
                    limiter.acquire(pagesCnt);

                synchronized (this) {
                    if (isDone() || !parts.contains(partId))
                        break;

                    ctx.cache().context().database().checkpointReadLock();

                    try {
                        off += scanPages(partId, off, pagesCnt);
                    }
                    finally {
                        ctx.cache().context().database().checkpointReadUnlock();
                    }
                }

                ctx.encryption().setEncryptionState(grp, partId, off, cnt);
            }

            if (log.isDebugEnabled()) {
                log.debug("Partition reencryption is finished " +
                    "[grpId=" + grp.groupId() +
                    ", p=" + partId +
                    ", remain=" + (cnt - off) +
                    ", total=" + cnt + "]");
            }
        }

        /**
         * @param off Start page offset.
         * @param cnt Count of pages to scan.
         * @return Count of scanned pages.
         * @throws IgniteCheckedException If failed.
         */
        private int scanPages(int partId, int off, int cnt) throws IgniteCheckedException {
            int grpId = grp.groupId();
            byte flag = GroupPartitionId.getFlagByPartId(partId);

            for (int pageIdx = off; pageIdx < off + cnt; pageIdx++) {
                long pageId = PageIdUtils.pageId(partId, flag, pageIdx);
                long page = pageMem.acquirePage(grpId, pageId);

                try {
                    if (pageMem.isDirty(grpId, pageId, page))
                        continue;

                    pageMem.writeLock(grpId, pageId, page, true);
                    pageMem.writeUnlock(grpId, pageId, page, null, true);
                }
                finally {
                    pageMem.releasePage(grpId, pageId, page);
                }
            }

            return cnt;
        }
    }
}
