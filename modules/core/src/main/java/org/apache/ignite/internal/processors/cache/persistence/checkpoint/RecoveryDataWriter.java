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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.PageStoreWriter;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.util.GridConcurrentMultiPairQueue;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.worker.WorkProgressDispatcher;

import static org.apache.ignite.configuration.DataStorageConfiguration.MAX_PAGE_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getType;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getVersion;
import static org.apache.ignite.internal.util.IgniteUtils.hexLong;

/**
 * Implementation of writer which able to store recovery data to disk during checkpoint.
 */
public class RecoveryDataWriter implements Runnable {
    /** File to write recovery data. */
    private final CheckpointRecoveryFile file;

    /** Context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Collection of page IDs to write under this task. Overall pages to write may be greater than this collection. */
    private final GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> writePageIds;

    /** Set of cache groups to process. */
    private final Set<Integer> cacheGrpIds;

    /** Future which should be finished when all pages would be written. */
    private final CountDownFuture doneFut;

    /** Work progress dispatcher. */
    private final WorkProgressDispatcher workProgressDispatcher;

    /** Current checkpoint. This field is updated only by checkpoint thread. */
    private final CheckpointProgressImpl curCpProgress;

    /** Shutdown now. */
    private final BooleanSupplier shutdownNow;

    /** Page data buffer. */
    private final ByteBuffer pageBuf = ByteBuffer.allocateDirect(MAX_PAGE_SIZE).order(ByteOrder.nativeOrder());

    /** */
    private final DiskPageCompression compressionType;

    /** */
    private final int compressionLevel;

    /**
     * Creates task for write recovery data.
     *
     * @param ctx Context.
     * @param file Checkpoint recovery file.
     * @param writePageIds Collection of page IDs to write.
     * @param doneFut Done future.
     * @param workProgressDispatcher Work progress dispatcher.
     * @param log Logger.
     * @param curCpProgress Checkpoint progress.
     * @param shutdownNow Shutdown supplier.
     */
    RecoveryDataWriter(
        GridKernalContext ctx,
        CheckpointRecoveryFile file,
        GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> writePageIds,
        Set<Integer> cacheGrpIds,
        CountDownFuture doneFut,
        WorkProgressDispatcher workProgressDispatcher,
        IgniteLogger log,
        CheckpointProgressImpl curCpProgress,
        BooleanSupplier shutdownNow
    ) {
        this.ctx = ctx;
        this.file = file;
        this.writePageIds = writePageIds;
        this.cacheGrpIds = cacheGrpIds;
        this.doneFut = doneFut;
        this.workProgressDispatcher = workProgressDispatcher;
        this.log = log;
        this.curCpProgress = curCpProgress;
        this.shutdownNow = shutdownNow;

        DataStorageConfiguration dsCfg = ctx.config().getDataStorageConfiguration();
        compressionType = dsCfg.getCheckpointRecoveryDataCompression();
        compressionLevel = CompressionProcessor.getCompressionLevel(dsCfg.getCheckpointRecoveryDataCompressionLevel(),
            compressionType);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> writePageIds = this.writePageIds;

        Throwable err = null;

        try {
            GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> pagesToRetry = writePages(writePageIds);

            if (!pagesToRetry.isEmpty()) {
                if (log.isInfoEnabled()) {
                    log.info(pagesToRetry.initialSize() + " recovery pages were not written yet due to " +
                        "unsuccessful page write lock acquisition and will be retried");
                }

                while (!pagesToRetry.isEmpty())
                    pagesToRetry = writePages(pagesToRetry);
            }

            if (shutdownNow.getAsBoolean()) {
                doneFut.onDone(new NodeStoppingException("Node is stopping."));

                return;
            }

            workProgressDispatcher.blockingSectionBegin();

            try {
                file.fsync();
            }
            finally {
                workProgressDispatcher.blockingSectionEnd();
            }
        }
        catch (Throwable e) {
            err = e;
        }
        finally {
            try {
                file.close();
            }
            catch (Exception e) {
                if (err == null)
                    err = e;
                else
                    err.addSuppressed(e);
            }
            doneFut.onDone(err);
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

        Map<PageMemoryEx, PageStoreWriter> pageStoreWriters = new HashMap<>();

        GridConcurrentMultiPairQueue.Result<PageMemoryEx, FullPageId> res =
            new GridConcurrentMultiPairQueue.Result<>();

        while (writePageIds.next(res)) {
            if (shutdownNow.getAsBoolean())
                break;

            workProgressDispatcher.updateHeartbeat();

            FullPageId fullId = res.getValue();

            PageMemoryEx pageMem = res.getKey();

            if (!cacheGrpIds.contains(fullId.groupId()))
                continue;

            pageBuf.rewind();
            pageBuf.limit(pageMem.pageSize());

            PageStoreWriter pageStoreWriter =
                pageStoreWriters.computeIfAbsent(pageMem, pageMemEx -> createPageStoreWriter(pageMemEx, pagesToRetry));

            pageMem.checkpointWritePage(fullId, pageBuf, pageStoreWriter, null, true);
        }

        return pagesToRetry.isEmpty() ?
            GridConcurrentMultiPairQueue.EMPTY :
            new GridConcurrentMultiPairQueue<>(pagesToRetry);
    }

    /**
     * Factory method for create {@link PageStoreWriter}.
     *
     * @param pageMemEx Page memory.
     * @param pagesToRetry List pages for retry.
     * @return Checkpoint page write context.
     */
    private PageStoreWriter createPageStoreWriter(
        PageMemoryEx pageMemEx,
        Map<PageMemoryEx, List<FullPageId>> pagesToRetry
    ) {
        return new PageStoreWriter() {
            /** {@inheritDoc} */
            @Override public void writePage(FullPageId fullPageId, ByteBuffer buf, int tag) throws IgniteCheckedException {
                if (tag == PageMemoryImpl.TRY_AGAIN_TAG) {
                    pagesToRetry.computeIfAbsent(pageMemEx, k -> new ArrayList<>()).add(fullPageId);

                    return;
                }

                long pageId = fullPageId.pageId();

                assert getType(buf) != 0 : "Invalid state. Type is 0! pageId = " + hexLong(pageId);
                assert getVersion(buf) != 0 : "Invalid state. Version is 0! pageId = " + hexLong(pageId);

                buf.limit(pageMemEx.realPageSize(fullPageId.groupId()));

                if (compressionType != DiskPageCompression.DISABLED) {
                    buf = ctx.compress().compressPage(buf, buf.remaining(), 1,
                        compressionType, compressionLevel);
                }

                try {
                    file.writePage(fullPageId, buf);
                }
                catch (IOException e) {
                    throw new StorageException("Failed to write page to recovery file [file=" + file.file() + ']', e);
                }

                curCpProgress.updateWrittenRecoveryPages(1);
            }
        };
    }
}
