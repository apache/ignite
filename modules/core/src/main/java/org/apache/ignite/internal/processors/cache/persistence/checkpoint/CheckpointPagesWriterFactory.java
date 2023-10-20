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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.PageStoreWriter;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.CheckpointMetricsTracker;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.util.GridConcurrentMultiPairQueue;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.lang.IgniteThrowableFunction;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.WorkProgressDispatcher;
import org.jsr166.ConcurrentLinkedHashMap;

/**
 * Factory class for checkpoint pages writer.
 *
 * It holds all dependency which is needed for creation of checkpoint writer and recovery checkpoint writer.
 */
public class CheckpointPagesWriterFactory {
    /** Context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Data storage metrics. */
    private final DataStorageMetricsImpl persStoreMetrics;

    /** Thread local with buffers for the checkpoint threads. Each buffer represent one page for durable memory. */
    private volatile ThreadLocal<ByteBuffer> threadBuf;

    /** Throttling policy according to the settings. */
    private final PageMemoryImpl.ThrottlingPolicy throttlingPolicy;

    /** Resolver of page memory by group id. */
    private final IgniteThrowableFunction<Integer, PageMemoryEx> pageMemoryGroupResolver;

    /** Writer which writes pages to page store during the checkpoint. */
    private final CheckpointPagesWriter.CheckpointPageWriter checkpointPageWriter;

    /**
     * @param ctx Context.
     * @param logger Logger.
     * @param checkpointPageWriter Checkpoint page writer.
     * @param persStoreMetrics Persistence metrics.
     * @param throttlingPolicy Throttling policy.
     * @param threadBuf Thread write buffer.
     * @param pageMemoryGroupResolver Page memory resolver.
     */
    CheckpointPagesWriterFactory(
        GridKernalContext ctx,
        Function<Class<?>, IgniteLogger> logger,
        CheckpointPagesWriter.CheckpointPageWriter checkpointPageWriter,
        DataStorageMetricsImpl persStoreMetrics,
        PageMemoryImpl.ThrottlingPolicy throttlingPolicy,
        ThreadLocal<ByteBuffer> threadBuf,
        IgniteThrowableFunction<Integer, PageMemoryEx> pageMemoryGroupResolver
    ) {
        this.ctx = ctx;
        this.log = logger.apply(getClass());
        this.persStoreMetrics = persStoreMetrics;
        this.threadBuf = threadBuf;
        this.throttlingPolicy = throttlingPolicy;
        this.pageMemoryGroupResolver = pageMemoryGroupResolver;
        this.checkpointPageWriter = checkpointPageWriter;
    }

    /**
     * @param tracker Checkpoint metrics tracker.
     * @param cpPages List of pages to write.
     * @param updStores Updated page store storage.
     * @param doneWriteFut Write done future.
     * @param beforePageWrite Before page write callback.
     * @param curCpProgress Current checkpoint data.
     * @param shutdownNow Checker of stop operation.
     * @return Instance of page checkpint writer.
     */
    Runnable buildCheckpointPagesWriter(
        CheckpointMetricsTracker tracker,
        GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> cpPages,
        ConcurrentLinkedHashMap<PageStore, LongAdder> updStores,
        CountDownFuture doneWriteFut,
        Runnable beforePageWrite,
        CheckpointProgressImpl curCpProgress,
        BooleanSupplier shutdownNow
    ) {
        return new CheckpointPagesWriter(
            tracker,
            cpPages,
            updStores,
            doneWriteFut,
            beforePageWrite,
            log,
            persStoreMetrics,
            threadBuf,
            throttlingPolicy,
            pageMemoryGroupResolver,
            curCpProgress,
            checkpointPageWriter,
            shutdownNow
        );
    }

    /**
     * @param recoveryDataFile File to write recovery data.
     * @param cpPages List of pages to write.
     * @param cacheGrpIds Set of cache groups to process (cache groups with WAL enabled).
     * @param doneWriteFut Write done future.
     * @param workProgressDispatcher Work progress dispatcher.
     * @param curCpProgress Current checkpoint data.
     * @param shutdownNow Checker of stop operation.
     * @return Instance of page checkpint writer.
     */
    Runnable buildRecoveryDataWriter(
        CheckpointRecoveryFile recoveryDataFile,
        GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> cpPages,
        Set<Integer> cacheGrpIds,
        CountDownFuture doneWriteFut,
        WorkProgressDispatcher workProgressDispatcher,
        CheckpointProgressImpl curCpProgress,
        BooleanSupplier shutdownNow
    ) {
        return new RecoveryDataWriter(
            ctx,
            recoveryDataFile,
            cpPages,
            cacheGrpIds,
            doneWriteFut,
            workProgressDispatcher,
            log,
            curCpProgress,
            shutdownNow
        );
    }

    /**
     * @param pages List of pages to write.
     * @param updStores Updated page store storage.
     * @param writePagesError Error storage.
     * @param cpPagesCnt Count of checkpointed pages.
     * @return Instance of page checkpint writer.
     */
    Runnable buildRecoveryFinalizer(
        GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> pages,
        Collection<PageStore> updStores,
        AtomicReference<Throwable> writePagesError,
        AtomicInteger cpPagesCnt
    ) {
        return () -> {
            GridConcurrentMultiPairQueue.Result<PageMemoryEx, FullPageId> res =
                new GridConcurrentMultiPairQueue.Result<>();

            int pagesWritten = 0;
            ByteBuffer tmpWriteBuf = threadBuf.get();

            Map<PageMemoryEx, PageStoreWriter> pageStoreWriters = new HashMap<>();
            try {
                while (pages.next(res)) {
                    // Fail-fast break if some exception occurred.
                    if (writePagesError.get() != null)
                        break;

                    PageMemoryEx pageMem = res.getKey();

                    PageStoreWriter pageStoreWriter = pageStoreWriters.computeIfAbsent(
                        pageMem,
                        (pageMemEx) -> (fullPageId, buf, tag) -> {
                            assert tag != PageMemoryImpl.TRY_AGAIN_TAG : "Lock is held by other thread for page " + fullPageId;

                            // Write buf to page store.
                            PageStore store = checkpointPageWriter.write(pageMemEx, fullPageId, buf, tag);

                            // Save store for future fsync.
                            updStores.add(store);
                        }
                    );

                    // Write page content to page store via pageStoreWriter.
                    // Tracker is null, because no need to track checkpoint metrics on recovery.
                    pageMem.checkpointWritePage(res.getValue(), tmpWriteBuf, pageStoreWriter, null, false);

                    // Add number of handled pages.
                    pagesWritten++;
                }
            }
            catch (Throwable e) {
                U.error(log, "Failed to write page to pageStore: " + res);

                writePagesError.compareAndSet(null, e);

                if (e instanceof Error)
                    throw (Error)e;
            }

            cpPagesCnt.addAndGet(pagesWritten);
        };
    }

    /**
     * @param threadBuf Thread local byte buffer.
     */
    public void threadBuf(ThreadLocal<ByteBuffer> threadBuf) {
        this.threadBuf = threadBuf;
    }
}
