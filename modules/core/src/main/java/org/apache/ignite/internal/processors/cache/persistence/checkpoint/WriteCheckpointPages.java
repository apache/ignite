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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.PageStoreWriter;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.CheckpointMetricsTracker;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridConcurrentMultiPairQueue;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.lang.IgniteThrowableFunction;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getType;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getVersion;
import static org.apache.ignite.internal.util.IgniteUtils.hexLong;

/**
 * Implementation of page writer which able to store pages to disk during checkpoint.
 */
public class WriteCheckpointPages implements Runnable {
    /** */
    private final CheckpointMetricsTracker tracker;

    /** Collection of page IDs to write under this task. Overall pages to write may be greater than this collection */
    private final GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> writePageIds;

    /** */
    private final ConcurrentLinkedHashMap<PageStore, LongAdder> updStores;

    /** */
    private final CountDownFuture doneFut;

    /** Total pages to write, counter may be greater than {@link #writePageIds} size */
    private final int totalPagesToWrite;

    /** */
    private final Runnable beforePageWrite;

    /** Snapshot manager. */
    private final IgniteCacheSnapshotManager snapshotMgr;

    /** */
    private final IgniteLogger log;

    /** */
    private final DataStorageMetricsImpl persStoreMetrics;

    /** Thread local with buffers for the checkpoint threads. Each buffer represent one page for durable memory. */
    private final ThreadLocal<ByteBuffer> threadBuf;

    /** Throttling policy according to the settings. */
    private final PageMemoryImpl.ThrottlingPolicy throttlingPolicy;

    /** Resolver of page memory by group id. */
    private final IgniteThrowableFunction<Integer, PageMemoryEx> pageMemoryGroupResolver;

    /** Current checkpoint. This field is updated only by checkpoint thread. */
    private final CheckpointProgressImpl curCpProgress;

    /** */
    private final FilePageStoreManager storeMgr;

    /** Shutdown now. */
    private final BooleanSupplier shutdownNow;

    /**
     * Creates task for write pages
     *
     * @param tracker Checkpoint metrics tracker.
     * @param writePageIds Collection of page IDs to write.
     * @param updStores Updating storage.
     * @param doneFut Done future.
     * @param totalPagesToWrite Total pages to be written under this checkpoint.
     * @param beforePageWrite Action to be performed before every page write.
     * @param snapshotManager Snapshot manager.
     * @param log Logger.
     * @param dsMetrics Data storage metrics.
     * @param buf Thread local byte buffer.
     * @param throttlingPolicy Throttling policy.
     * @param pageMemoryGroupResolver Resolver of page memory by group id.
     * @param progress Checkpoint progress.
     * @param storeMgr File page store manager.
     * @param shutdownNow Shutdown supplier.
     */
    WriteCheckpointPages(
        final CheckpointMetricsTracker tracker,
        final GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> writePageIds,
        final ConcurrentLinkedHashMap<PageStore, LongAdder> updStores,
        final CountDownFuture doneFut,
        final int totalPagesToWrite,
        final Runnable beforePageWrite,
        IgniteCacheSnapshotManager snapshotManager,
        IgniteLogger log,
        DataStorageMetricsImpl dsMetrics,
        ThreadLocal<ByteBuffer> buf,
        PageMemoryImpl.ThrottlingPolicy throttlingPolicy,
        IgniteThrowableFunction<Integer, PageMemoryEx> pageMemoryGroupResolver,
        CheckpointProgressImpl progress,
        FilePageStoreManager storeMgr,
        BooleanSupplier shutdownNow
    ) {
        this.tracker = tracker;
        this.writePageIds = writePageIds;
        this.updStores = updStores;
        this.doneFut = doneFut;
        this.totalPagesToWrite = totalPagesToWrite;
        this.beforePageWrite = beforePageWrite;
        this.snapshotMgr = snapshotManager;
        this.log = log;
        this.persStoreMetrics = dsMetrics;
        this.threadBuf = buf;
        this.throttlingPolicy = throttlingPolicy;
        this.pageMemoryGroupResolver = pageMemoryGroupResolver;
        this.curCpProgress = progress;
        this.storeMgr = storeMgr;
        this.shutdownNow = shutdownNow;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        snapshotMgr.beforeCheckpointPageWritten();

        GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> writePageIds = this.writePageIds;

        try {
            GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> pagesToRetry = writePages(writePageIds);

            if (pagesToRetry.isEmpty())
                doneFut.onDone();
            else {
                LT.warn(log, pagesToRetry.initialSize() + " checkpoint pages were not written yet due to unsuccessful " +
                    "page write lock acquisition and will be retried");

                while (!pagesToRetry.isEmpty())
                    pagesToRetry = writePages(pagesToRetry);

                doneFut.onDone();
            }
        }
        catch (Throwable e) {
            doneFut.onDone(e);
        }
    }

    /**
     * @param writePageIds Collections of pages to write.
     * @return pagesToRetry Pages which should be retried.
     */
    private GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> writePages(
        GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> writePageIds
    ) throws IgniteCheckedException {
        Map<PageMemoryEx, List<FullPageId>> pagesToRetry = new HashMap<>();

        CheckpointMetricsTracker tracker = persStoreMetrics.metricsEnabled() ? this.tracker : null;

        PageStoreWriter pageStoreWriter = createPageStoreWriter(pagesToRetry);

        ByteBuffer tmpWriteBuf = threadBuf.get();

        boolean throttlingEnabled = throttlingPolicy != PageMemoryImpl.ThrottlingPolicy.DISABLED;

        GridConcurrentMultiPairQueue.Result<PageMemoryEx, FullPageId> res =
            new GridConcurrentMultiPairQueue.Result<>();

        while (writePageIds.next(res)) {
            if (shutdownNow.getAsBoolean())
                break;

            beforePageWrite.run();

            FullPageId fullId = res.getValue();

            PageMemoryEx pageMem = res.getKey();

            snapshotMgr.beforePageWrite(fullId);

            tmpWriteBuf.rewind();

            pageMem.checkpointWritePage(fullId, tmpWriteBuf, pageStoreWriter, tracker);

            if (throttlingEnabled) {
                while (pageMem.shouldThrottle()) {
                    FullPageId cpPageId = pageMem.pullPageFromCpBuffer();

                    if (cpPageId.equals(FullPageId.NULL_PAGE))
                        break;

                    snapshotMgr.beforePageWrite(cpPageId);

                    tmpWriteBuf.rewind();

                    pageMem.checkpointWritePage(cpPageId, tmpWriteBuf, pageStoreWriter, tracker);
                }
            }
        }

        return pagesToRetry.isEmpty() ?
            GridConcurrentMultiPairQueue.EMPTY :
            new GridConcurrentMultiPairQueue<>(pagesToRetry);
    }

    /**
     * Factory method for create {@link PageStoreWriter}.
     *
     * @param pagesToRetry List pages for retry.
     * @return Checkpoint page write context.
     */
    private PageStoreWriter createPageStoreWriter(Map<PageMemoryEx, List<FullPageId>> pagesToRetry) {
        return new PageStoreWriter() {
            /** {@inheritDoc} */
            @Override public void writePage(FullPageId fullPageId, ByteBuffer buf,
                int tag) throws IgniteCheckedException {
                if (tag == PageMemoryImpl.TRY_AGAIN_TAG) {
                    PageMemoryEx pageMem = pageMemoryGroupResolver.apply(fullPageId.groupId());

                    pagesToRetry.computeIfAbsent(pageMem, k -> new ArrayList<>()).add(fullPageId);

                    return;
                }

                int groupId = fullPageId.groupId();
                long pageId = fullPageId.pageId();

                assert getType(buf) != 0 : "Invalid state. Type is 0! pageId = " + hexLong(pageId);
                assert getVersion(buf) != 0 : "Invalid state. Version is 0! pageId = " + hexLong(pageId);

                if (persStoreMetrics.metricsEnabled()) {
                    int pageType = getType(buf);

                    if (PageIO.isDataPageType(pageType))
                        tracker.onDataPageWritten();
                }

                curCpProgress.updateWrittenPages(1);

                PageStore store = storeMgr.writeInternal(groupId, pageId, buf, tag, true);

                updStores.computeIfAbsent(store, k -> new LongAdder()).increment();
            }
        };
    }
}
