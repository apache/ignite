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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertFragmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageRemoveRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageUpdateRecord;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.evict.PageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.LongListReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.GridCursorIteratorWrapper;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl.SEG_CNT;

/**
 */
public abstract class AbstractFreeList<T extends Storable> extends PagesList implements FreeList<T>, ReuseList {
    /** */
    private static final int BUCKETS = 256; // Must be power of 2.

    /** */
    private static final int REUSE_BUCKET = BUCKETS - 1;

    /** */
    private static final Integer COMPLETE = Integer.MAX_VALUE;

    /** */
    private static final Integer FAIL_I = Integer.MIN_VALUE;

    /** */
    private static final Long FAIL_L = Long.MAX_VALUE;

    /** */
    private static final int MIN_PAGE_FREE_SPACE = 8;

    /** Initial backoff (nanoseconds) between consecutive no-progress eviction attempts. */
    private static final long EVICTION_BACKOFF_START_NANOS = 50_000L;

    /** Upper bound (nanoseconds) for the backoff between no-progress eviction attempts. */
    private static final long EVICTION_BACKOFF_MAX_NANOS = 1_000_000L;

    /**
     * Maximum time (milliseconds) the size-aware eviction guard is willing to wait without either an eviction or a
     * new highest empty-pages count before failing with an out-of-memory.
     */
    private static final long EVICTION_NO_PROGRESS_TIMEOUT_MILLIS = 1_000L;

    /**
     * Hard upper bound (milliseconds) on the total duration of one size-aware eviction cycle, regardless of
     * per-iteration progress.
     */
    private static final long EVICTION_MAX_CYCLE_TIME_MILLIS = 5 * EVICTION_NO_PROGRESS_TIMEOUT_MILLIS;

    /**
     * Number of compare-and-set attempts in {@link #tryReserve(int)}. A deficit (the space is genuinely unavailable)
     * defers the row immediately; only a lost CAS race (concurrent reservers) is retried, up to this many attempts,
     * before the row is deferred to the blocking context as well.
     */
    private static final int TRY_RESERVE_CAS_ATTEMPTS = 4;

    /**
     * Step between buckets in free list, measured in powers of two.
     * For example, for page size 4096 and 256 buckets, shift is 4 and step is 16 bytes.
     */
    private final int shift;

    /** */
    private final AtomicReferenceArray<Stripe[]> buckets = new AtomicReferenceArray<>(BUCKETS);

    /** Onheap bucket page list caches. */
    private final AtomicReferenceArray<PagesCache> bucketCaches = new AtomicReferenceArray<>(BUCKETS);

    /** */
    private final int MIN_SIZE_FOR_DATA_PAGE;

    /** */
    private final PageHandler<T, Boolean> updateRow = new UpdateRowHandler();

    /** */
    private final DataRegionMetricsImpl memMetrics;

    /** */
    private final PageEvictionTracker evictionTracker;

    /** Whether the page reservation is engaged for this free list: only for in-memory regions with enabled page eviction. */
    private final boolean reserveEnabled;

    /** Data region this free list belongs to. */
    private final DataRegion dataRegion;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Pages reserved by in-flight inserts (see {@link #reserve(int)}). */
    private final AtomicLong reservedPages = new AtomicLong();

    /** Page list cache limit. */
    private final AtomicLong pageListCacheLimit;

    /**
     *
     */
    private final class UpdateRowHandler extends PageHandler<T, Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            T row,
            int itemId,
            IoStatisticsHolder statHolder)
            throws IgniteCheckedException {
            AbstractDataPageIO<T> io = (AbstractDataPageIO<T>)iox;

            int rowSize = row.size();

            boolean updated = io.updateRow(pageAddr, itemId, pageSize(), null, row, rowSize);

            evictionTracker.touchPage(pageId);

            if (updated && needWalDeltaRecord(pageId, page, walPlc)) {
                // TODO This record must contain only a reference to a logical WAL record with the actual data.
                byte[] payload = new byte[rowSize];

                DataPagePayload data = io.readPayload(pageAddr, itemId, pageSize());

                assert data.payloadSize() == rowSize;

                PageUtils.getBytes(pageAddr, data.offset(), payload, 0, rowSize);

                statHolder.trackPageRemoveData(rowSize);
                statHolder.trackPageInsertData(rowSize);

                wal.log(new DataPageUpdateRecord(
                    cacheId,
                    pageId,
                    itemId,
                    payload));
            }

            return updated;
        }
    }

    /** Write a single row on a single page. */
    private final WriteRowHandler writeRowHnd = new WriteRowHandler();

    /** Write multiple rows on a single page. */
    private final WriteRowsHandler writeRowsHnd = new WriteRowsHandler();

    /**  */
    private class WriteRowHandler extends PageHandler<T, Integer> {
        /** {@inheritDoc} */
        @Override public Integer run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            T row,
            int written,
            IoStatisticsHolder statHolder
        ) throws IgniteCheckedException {
            written = addRow(pageId, page, pageAddr, iox, row, written, statHolder);

            putPage(((AbstractDataPageIO)iox).getFreeSpace(pageAddr), pageId, page, pageAddr, statHolder);

            return written;
        }

        /**
         * @param pageId Page ID.
         * @param page Page absolute pointer.
         * @param pageAddr Page address.
         * @param iox IO.
         * @param row Row to write.
         * @param written Written size.
         * @param statHolder IO statistics holder.
         * @return Number of bytes written, {@link #COMPLETE} if the row was fully written.
         * @throws IgniteCheckedException If failed.
         */
        protected Integer addRow(
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            T row,
            int written,
            IoStatisticsHolder statHolder
        ) throws IgniteCheckedException {
            AbstractDataPageIO<T> io = (AbstractDataPageIO<T>)iox;

            long lastLink = row.link();

            int rowSize = row.size();
            int oldFreeSpace = io.getFreeSpace(pageAddr);

            assert oldFreeSpace > 0 : oldFreeSpace;

            // If the full row does not fit into this page write only a fragment.
            written = (written == 0 && oldFreeSpace >= rowSize) ? addRowFull(pageId, page, pageAddr, io, row, rowSize) :
                addRowFragment(pageId, page, pageAddr, io, row, written, rowSize);

            if (written == rowSize)
                evictionTracker.touchPage(pageId);

            evictionTracker.trackFragmentPage(pageId, lastLink, written == rowSize);

            statHolder.trackPageInsertData(oldFreeSpace - io.getFreeSpace(pageAddr));

            // Avoid boxing with garbage generation for usual case.
            return written == rowSize ? COMPLETE : written;
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param io IO.
         * @param row Row.
         * @param rowSize Row size.
         * @return Written size which is always equal to row size here.
         * @throws IgniteCheckedException If failed.
         */
        protected int addRowFull(
            long pageId,
            long page,
            long pageAddr,
            AbstractDataPageIO<T> io,
            T row,
            int rowSize
        ) throws IgniteCheckedException {
            io.addRow(pageId, pageAddr, row, rowSize, pageSize());

            if (needWalDeltaRecord(pageId, page, null)) {
                // TODO IGNITE-5829 This record must contain only a reference to a logical WAL record with the actual data.
                byte[] payload = new byte[rowSize];

                DataPagePayload data = io.readPayload(pageAddr, PageIdUtils.itemId(row.link()), pageSize());

                assert data.payloadSize() == rowSize;

                PageUtils.getBytes(pageAddr, data.offset(), payload, 0, rowSize);

                wal.log(new DataPageInsertRecord(
                    grpId,
                    pageId,
                    payload));
            }

            return rowSize;
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param io IO.
         * @param row Row.
         * @param written Written size.
         * @param rowSize Row size.
         * @return Updated written size.
         * @throws IgniteCheckedException If failed.
         */
        protected int addRowFragment(
            long pageId,
            long page,
            long pageAddr,
            AbstractDataPageIO<T> io,
            T row,
            int written,
            int rowSize
        ) throws IgniteCheckedException {
            // Read last link before the fragment write, because it will be updated there.
            long lastLink = row.link();

            int payloadSize = io.addRowFragment(pageMem, pageId, pageAddr, row, written, rowSize, pageSize());

            assert payloadSize > 0 : payloadSize;

            if (needWalDeltaRecord(pageId, page, null)) {
                // TODO IGNITE-5829 This record must contain only a reference to a logical WAL record with the actual data.
                byte[] payload = new byte[payloadSize];

                DataPagePayload data = io.readPayload(pageAddr, PageIdUtils.itemId(row.link()), pageSize());

                PageUtils.getBytes(pageAddr, data.offset(), payload, 0, payloadSize);

                wal.log(new DataPageInsertFragmentRecord(grpId, pageId, payload, lastLink));
            }

            return written + payloadSize;
        }

        /**
         * Put page into the free list if needed.
         *
         * @param freeSpace Page free space.
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param statHolder Statistics holder to track IO operations.
         */
        protected void putPage(int freeSpace, long pageId, long page, long pageAddr, IoStatisticsHolder statHolder)
            throws IgniteCheckedException {
            if (freeSpace > MIN_PAGE_FREE_SPACE) {
                int bucket = bucket(freeSpace, false);

                put(null, pageId, page, pageAddr, bucket, statHolder);
            }
        }
    }

    /**
     * Per-call write context of {@link #insertDataRows}: the rows cursor plus the reservation accounting of the row
     * currently being written, shared between {@link WriteRowsHandler} and the outer loop of {@link #insertDataRows}.
     */
    private static final class RowsWriteContext<T extends Storable> {
        /** Rows cursor. */
        private GridCursor<T> cur;

        /** Reservation made by {@link #reserve(int)} for the current row. */
        private long reserved;

        /** Number of the current row's pages that have already consumed the reservation. */
        private int consumed;

        /**
         * Set when {@link WriteRowsHandler} could not reserve space for a row without eviction (forbidden under the
         * page write lock); the cursor is positioned on that row, and the outer loop must continue without advancing.
         */
        private boolean pending;
    }

    /** */
    private final class WriteRowsHandler extends PageHandler<RowsWriteContext<T>, Integer> {
        /** {@inheritDoc} */
        @Override public Integer run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            RowsWriteContext<T> ctx,
            int written,
            IoStatisticsHolder statHolder
        ) throws IgniteCheckedException {
            AbstractDataPageIO<T> io = (AbstractDataPageIO<T>)iox;

            // Fill the page up to the end.
            while (written != COMPLETE || (!evictionTracker.evictionRequired() && ctx.cur.next())) {
                T row = ctx.cur.get();

                if (written == COMPLETE) {
                    long tryClaim = tryReserve(row.size());

                    if (tryClaim < 0) {
                        ctx.pending = true;

                        break;
                    }

                    claim(ctx, tryClaim);

                    written = writeWholePages(ctx, row, statHolder);

                    // If the data row was completely written without remainder, proceed to the next.
                    if (written == COMPLETE)
                        continue;

                    if (io.getFreeSpace(pageAddr) < row.size() - written)
                        break;
                }

                written = writeRowHnd.addRow(pageId, page, pageAddr, io, row, written, statHolder);

                assert written == COMPLETE;
            }

            writeRowHnd.putPage(io.getFreeSpace(pageAddr), pageId, page, pageAddr, statHolder);

            return written;
        }
    }

    /** */
    private final PageHandler<ReuseBag, Long> rmvRow;

    /**
     *
     */
    private final class RemoveRowHandler extends PageHandler<ReuseBag, Long> {
        /** Indicates whether partition ID should be masked from page ID. */
        private final boolean maskPartId;

        /** */
        RemoveRowHandler(boolean maskPartId) {
            this.maskPartId = maskPartId;
        }

        /** {@inheritDoc} */
        @Override public Long run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            ReuseBag reuseBag,
            int itemId,
            IoStatisticsHolder statHolder
        ) throws IgniteCheckedException {
            AbstractDataPageIO<T> io = (AbstractDataPageIO<T>)iox;

            int oldFreeSpace = io.getFreeSpace(pageAddr);

            assert oldFreeSpace >= 0 : oldFreeSpace;

            long nextLink = io.removeRow(pageAddr, itemId, pageSize());

            if (needWalDeltaRecord(pageId, page, walPlc))
                wal.log(new DataPageRemoveRecord(cacheId, pageId, itemId));

            int newFreeSpace = io.getFreeSpace(pageAddr);

            if (newFreeSpace > MIN_PAGE_FREE_SPACE) {
                int newBucket = bucket(newFreeSpace, false);

                boolean putIsNeeded = oldFreeSpace <= MIN_PAGE_FREE_SPACE;

                if (!putIsNeeded) {
                    int oldBucket = bucket(oldFreeSpace, false);

                    if (oldBucket != newBucket) {
                        // It is possible that page was concurrently taken for put, in this case put will handle bucket change.
                        pageId = maskPartId ? PageIdUtils.maskPartitionId(pageId) : pageId;

                        putIsNeeded = removeDataPage(pageId, page, pageAddr, io, oldBucket, statHolder);
                    }
                }

                if (io.isEmpty(pageAddr)) {
                    evictionTracker.forgetPage(pageId);

                    if (putIsNeeded)
                        reuseBag.addFreePage(recyclePage(pageId, page, pageAddr, null));
                }
                else if (putIsNeeded)
                    put(null, pageId, page, pageAddr, newBucket, statHolder);
            }

            statHolder.trackPageRemoveData(newFreeSpace - oldFreeSpace);

            // For common case boxed 0L will be cached inside of Long, so no garbage will be produced.
            return nextLink;
        }
    }

    /**
     * @param cacheGrpId Cache group ID.
     * @param name Name (for debug purpose).
     * @param dataRegion Data region.
     * @param reuseList Reuse list or {@code null} if this free list will be a reuse list for itself.
     * @param wal Write ahead log manager.
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new metadata should be initialized.
     * @param pageLockTrackerManager Page lock tracker manager.
     * @param pageFlag Default flag value for allocated pages.
     * @throws IgniteCheckedException If failed.
     */
    public AbstractFreeList(
        int cacheGrpId,
        String name,
        DataRegion dataRegion,
        @Nullable ReuseList reuseList,
        @Nullable IgniteWriteAheadLogManager wal,
        long metaPageId,
        boolean initNew,
        PageLockTrackerManager pageLockTrackerManager,
        GridKernalContext ctx,
        @Nullable AtomicLong pageListCacheLimit,
        byte pageFlag
    ) throws IgniteCheckedException {
        super(cacheGrpId, name, dataRegion.pageMemory(), BUCKETS, wal, metaPageId, pageLockTrackerManager, ctx, pageFlag);

        rmvRow = new RemoveRowHandler(cacheGrpId == 0);

        evictionTracker = dataRegion.evictionTracker();

        reserveEnabled = !dataRegion.config().isPersistenceEnabled() &&
            dataRegion.config().getPageEvictionMode() != DataPageEvictionMode.DISABLED;

        this.dataRegion = dataRegion;
        this.ctx = ctx;
        this.reuseList = reuseList == null ? this : reuseList;
        int pageSize = pageMem.pageSize();

        assert U.isPow2(pageSize) : "Page size must be a power of 2: " + pageSize;
        assert U.isPow2(BUCKETS);
        assert BUCKETS <= pageSize : pageSize;

        // TODO this constant is used because currently we cannot reuse data pages as index pages
        // TODO and vice-versa. It should be removed when data storage format is finalized.
        MIN_SIZE_FOR_DATA_PAGE = pageSize - AbstractDataPageIO.MIN_DATA_PAGE_OVERHEAD;

        int shift = 0;

        while (pageSize > BUCKETS) {
            shift++;
            pageSize >>>= 1;
        }

        this.shift = shift;

        memMetrics = dataRegion.metrics();

        this.pageListCacheLimit = pageListCacheLimit;

        init(metaPageId, initNew);
    }

    /** {@inheritDoc} */
    @Override public int getPageFreeSpace(int bucket) {
        return bucket << shift;
    }

    /**
     * Calculates free space tracked by this FreeListImpl instance.
     *
     * @return Free space available for use, in bytes.
     */
    public long freeSpace() {
        long freeSpace = 0;

        for (int b = BUCKETS - 2; b > 0; b--) {
            long perPageFreeSpace = b << shift;

            long pages = bucketsSize.get(b);

            freeSpace += pages * perPageFreeSpace;
        }

        return freeSpace;
    }

    /** {@inheritDoc} */
    @Override public void dumpStatistics(IgniteLogger log) {
        long dataPages = 0;

        final boolean dumpBucketsInfo = false;

        for (int b = 0; b < BUCKETS; b++) {
            long size = bucketsSize.get(b);

            if (!isReuseBucket(b))
                dataPages += size;

            if (dumpBucketsInfo) {
                Stripe[] stripes = getBucket(b);

                boolean empty = true;

                if (stripes != null) {
                    for (Stripe stripe : stripes) {
                        if (!stripe.empty) {
                            empty = false;

                            break;
                        }
                    }
                }

                if (log.isInfoEnabled())
                    log.info("Bucket [b=" + b +
                        ", size=" + size +
                        ", stripes=" + (stripes != null ? stripes.length : 0) +
                        ", stripesEmpty=" + empty + ']');
            }
        }

        if (dataPages > 0) {
            if (log.isInfoEnabled())
                log.info("FreeList [name=" + name() +
                    ", buckets=" + BUCKETS +
                    ", dataPages=" + dataPages +
                    ", reusePages=" + bucketsSize.get(REUSE_BUCKET) + "]");
        }
    }

    /**
     * @param freeSpace Page free space.
     * @param allowReuse {@code True} if it is allowed to get reuse bucket.
     * @return Bucket.
     */
    private int bucket(int freeSpace, boolean allowReuse) {
        assert freeSpace > 0 : freeSpace;

        int bucket = freeSpace >>> shift;

        assert bucket >= 0 && bucket < BUCKETS : bucket;

        if (!allowReuse && isReuseBucket(bucket))
            bucket--;

        return bucket;
    }

    /** {@inheritDoc} */
    @Override protected int getBucketIndex(int freeSpace) {
        return freeSpace > MIN_PAGE_FREE_SPACE ? bucket(freeSpace, false) : -1;
    }

    /** {@inheritDoc} */
    @Override public void insertDataRow(T row, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        int written = 0;

        try {
            do {
                if (written != 0)
                    memMetrics.incrementLargeEntriesPages();

                written = writeSinglePage(row, written, statHolder);
            }
            while (written != COMPLETE);
        }
        catch (IgniteCheckedException | Error | IgniteOutOfMemoryException e) {
            throw e;
        }
        catch (Throwable t) {
            throw new CorruptedFreeListException("Failed to insert data row", t, grpId);
        }
    }

    /**
     * Reduces the workload on the free list by writing multiple rows into a single memory page at once.<br>
     * <br>
     * Rows are sequentially added to the page as long as there is enough free space on it. If the row is large then
     * those fragments that occupy the whole memory page are written to other pages, and the remainder is added to the
     * current one.
     *
     * @param rows Rows.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteCheckedException If failed.
     */
    @Override public void insertDataRows(Collection<T> rows, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        try {
            RowsWriteContext<T> ctx = new RowsWriteContext<>();

            ctx.cur = new GridCursorIteratorWrapper<>(rows.iterator());

            int written = COMPLETE;

            try {
                while (written != COMPLETE || ctx.pending || ctx.cur.next()) {
                    ctx.pending = false;

                    T row = ctx.cur.get();

                    if (written == COMPLETE) {
                        written = reserveAndWriteWholePages(ctx, row, statHolder);

                        continue;
                    }

                    AbstractDataPageIO initIo = null;

                    long pageId = takePage(row.size() - written, row, statHolder);

                    if (pageId == 0L) {
                        pageId = allocateDataPage(row.partition());

                        initIo = row.ioVersions().latest();
                    }

                    if (reserveEnabled) {
                        reservedPages.decrementAndGet();

                        ctx.consumed++;
                    }

                    written = write(pageId, writeRowsHnd, initIo, ctx, written, FAIL_I, statHolder);

                    assert written != FAIL_I; // We can't fail here.
                }
            }
            finally {
                releaseReserved(ctx.reserved - ctx.consumed);
            }
        }
        catch (IgniteOutOfMemoryException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new CorruptedFreeListException("Failed to insert data rows", e, grpId);
        }
    }

    /**
     * Reserves the space for the next row (releasing the leftover reservation of the previous one) and writes its
     * whole-page fragments, each consuming the reservation.
     *
     * @param ctx Write context holding the reservation accounting of the current row.
     * @param row Row to process.
     * @param statHolder Statistics holder to track IO operations.
     * @return Number of bytes written, {@link #COMPLETE} if the row was fully written by whole-page fragments.
     * @throws IgniteCheckedException If failed.
     */
    private int reserveAndWriteWholePages(RowsWriteContext<T> ctx, T row, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        claim(ctx, reserve(row.size()));

        return writeWholePages(ctx, row, statHolder);
    }

    /**
     * Releases the unconsumed remainder of the previous row's reservation and stores the new claim for the current row.
     *
     * @param ctx Write context holding the reservation accounting of the current row.
     * @param claimPages Pages claimed for the current row.
     */
    private void claim(RowsWriteContext<T> ctx, long claimPages) {
        releaseReserved(ctx.reserved - ctx.consumed);

        ctx.reserved = claimPages;
        ctx.consumed = 0;
    }

    /**
     * Write fragments of the row, which occupy the whole memory page. A data row is ignored if it is less than the max
     * payload of an empty data page. Every written fragment consumes one reserved page (accounted in {@code ctx}
     * right next to the {@code reservedPages} decrement), so the accounting is exact on any outcome.
     *
     * @param ctx Write context holding the reservation accounting of the current row.
     * @param row Row to process.
     * @param statHolder Statistics holder to track IO operations.
     * @return Number of bytes written, {@link #COMPLETE} if the row was fully written, {@code 0} if data row was
     * ignored because it is less than the max payload of an empty data page.
     * @throws IgniteCheckedException If failed.
     */
    private int writeWholePages(RowsWriteContext<T> ctx, T row, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        assert row.link() == 0 : row.link();

        int written = 0;
        int rowSize = row.size();

        while (rowSize - written >= MIN_SIZE_FOR_DATA_PAGE) {
            written = writeSinglePage(row, written, statHolder);

            if (reserveEnabled) {
                reservedPages.decrementAndGet();

                ctx.consumed++;
            }

            memMetrics.incrementLargeEntriesPages();
        }

        return written;
    }

    /**
     * Take a page and write row on it. The reservation made by {@link #reserve(int)} (when engaged for the row's
     * insert) is released as a whole by the caller once the insert completes, so pages taken here are not
     * individually accounted against it.
     *
     * @param row Row to write.
     * @param written Written size.
     * @param statHolder Statistics holder to track IO operations.
     * @return Number of bytes written, {@link #COMPLETE} if the row was fully written.
     * @throws IgniteCheckedException If failed.
     */
    private int writeSinglePage(T row, int written, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        AbstractDataPageIO initIo = null;

        long pageId = takePage(row.size() - written, row, statHolder);

        if (pageId == 0L) {
            pageId = allocateDataPage(row.partition());

            initIo = row.ioVersions().latest();
        }

        written = write(pageId, writeRowHnd, initIo, row, written, FAIL_I, statHolder);

        assert written != FAIL_I; // We can't fail here.

        return written;
    }

    /**
     * Take page from free list.
     *
     * @param size Required free space on page.
     * @param row Row to write.
     * @param statHolder Statistics holder to track IO operations.
     * @return Page identifier or 0 if no page found in free list.
     * @throws IgniteCheckedException If failed.
     */
    private long takePage(int size, T row, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        long pageId = 0;

        if (size < MIN_SIZE_FOR_DATA_PAGE) {
            for (int b = bucket(size, false) + 1; b < REUSE_BUCKET; b++) {
                pageId = takeEmptyPage(b, row.ioVersions(), statHolder);

                if (pageId != 0L)
                    break;
            }
        }

        if (pageId == 0L) { // Handle reuse bucket.
            if (reuseList == this)
                pageId = takeEmptyPage(REUSE_BUCKET, row.ioVersions(), statHolder);
            else {
                pageId = reuseList.takeRecycledPage();

                if (pageId != 0)
                    pageId = reuseList.initRecycledPage(pageId, FLAG_DATA, row.ioVersions().latest());
            }
        }

        if (pageId == 0L)
            return 0;

        assert PageIdUtils.flag(pageId) == FLAG_DATA
            : "rowVersions=" + row.ioVersions() + ", pageId=" + PageIdUtils.toDetailString(pageId);

        return PageIdUtils.changePartitionId(pageId, row.partition());
    }

    /** {@inheritDoc} */
    @Override public boolean updateDataRow(long link, T row,
        IoStatisticsHolder statHolder) throws IgniteCheckedException {
        assert link != 0;

        try {
            long pageId = PageIdUtils.pageId(link);
            int itemId = PageIdUtils.itemId(link);

            Boolean updated = write(pageId, updateRow, row, itemId, null, statHolder);

            assert updated != null; // Can't fail here.

            return updated;
        }
        catch (AssertionError e) {
            throw corruptedFreeListException(e);
        }
        catch (IgniteCheckedException | Error e) {
            throw e;
        }
        catch (Throwable t) {
            throw new CorruptedFreeListException("Failed to update data row", t, grpId);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeDataRowByLink(long link, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        assert link != 0;

        try {
            long pageId = PageIdUtils.pageId(link);
            int itemId = PageIdUtils.itemId(link);

            ReuseBag bag = new LongListReuseBag();

            long nextLink = write(pageId, rmvRow, bag, itemId, FAIL_L, statHolder);

            assert nextLink != FAIL_L; // Can't fail here.

            while (nextLink != 0L) {
                memMetrics.decrementLargeEntriesPages();

                itemId = PageIdUtils.itemId(nextLink);
                pageId = PageIdUtils.pageId(nextLink);

                nextLink = write(pageId, rmvRow, bag, itemId, FAIL_L, statHolder);

                assert nextLink != FAIL_L; // Can't fail here.
            }

            reuseList.addForRecycle(bag);
        }
        catch (AssertionError e) {
            throw corruptedFreeListException(e);
        }
        catch (IgniteCheckedException | Error e) {
            throw e;
        }
        catch (Throwable t) {
            throw new CorruptedFreeListException("Failed to remove data by link", t, grpId);
        }
    }

    /** {@inheritDoc} */
    @Override protected Stripe[] getBucket(int bucket) {
        return buckets.get(bucket);
    }

    /** {@inheritDoc} */
    @Override protected boolean casBucket(int bucket, Stripe[] exp, Stripe[] upd) {
        boolean res = buckets.compareAndSet(bucket, exp, upd);

        if (log.isDebugEnabled()) {
            log.debug("CAS bucket [list=" + name() + ", bucket=" + bucket + ", old=" + Arrays.toString(exp) +
                ", new=" + Arrays.toString(upd) + ", res=" + res + ']');
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override protected boolean isReuseBucket(int bucket) {
        return bucket == REUSE_BUCKET;
    }

    /** {@inheritDoc} */
    @Override protected PagesCache getBucketCache(int bucket, boolean create) {
        PagesCache pagesCache = bucketCaches.get(bucket);

        if (pagesCache == null && create &&
            !bucketCaches.compareAndSet(bucket, null, pagesCache = new PagesCache(pageListCacheLimit)))
            pagesCache = bucketCaches.get(bucket);

        return pagesCache;
    }

    /**
     * @return Number of empty data pages in free list.
     */
    public int emptyDataPages() {
        return (int)bucketsSize.get(REUSE_BUCKET);
    }

    /** {@inheritDoc} */
    @Override public void addForRecycle(ReuseBag bag) throws IgniteCheckedException {
        assert reuseList == this : "not allowed to be a reuse list";

        try {
            put(bag, 0, 0, 0L, REUSE_BUCKET, IoStatisticsHolderNoOp.INSTANCE);
        }
        catch (AssertionError e) {
            throw corruptedFreeListException(e);
        }
        catch (IgniteCheckedException | Error e) {
            throw e;
        }
        catch (Throwable t) {
            throw new CorruptedFreeListException("Failed to add page for recycle", t, grpId);
        }
    }

    /** {@inheritDoc} */
    @Override public long takeRecycledPage() throws IgniteCheckedException {
        assert reuseList == this : "not allowed to be a reuse list";

        try {
            return takeEmptyPage(REUSE_BUCKET, null, IoStatisticsHolderNoOp.INSTANCE);
        }
        catch (AssertionError e) {
            throw corruptedFreeListException(e);
        }
        catch (IgniteCheckedException | Error e) {
            throw e;
        }
        catch (Throwable t) {
            throw new CorruptedFreeListException("Failed to take recycled page", t, grpId);
        }
    }

    /** {@inheritDoc} */
    @Override public long initRecycledPage(long pageId, byte flag, PageIO initIO) throws IgniteCheckedException {
        return initRecycledPage0(pageId, flag, initIO);
    }

    /** {@inheritDoc} */
    @Override public long recycledPagesCount() throws IgniteCheckedException {
        assert reuseList == this : "not allowed to be a reuse list";

        try {
            return storedPagesCount(REUSE_BUCKET);
        }
        catch (AssertionError e) {
            throw corruptedFreeListException(e);
        }
        catch (IgniteCheckedException | Error e) {
            throw e;
        }
        catch (Throwable t) {
            throw new CorruptedFreeListException("Failed to count recycled pages", t, grpId);
        }
    }

    /** {@inheritDoc} */
    @Override public long reserve(int rowSize) throws IgniteOutOfMemoryException, IgniteCheckedException {
        if (!reserveEnabled) {
            checkDisabledEvictionOom(rowSize);

            return 0;
        }

        long claimPages = pagesToReserve(rowSize);

        long backoffNanos = EVICTION_BACKOFF_START_NANOS;

        long lastProgressNanos = System.nanoTime();

        long cycleDeadlineNanos = lastProgressNanos + TimeUnit.MILLISECONDS.toNanos(EVICTION_MAX_CYCLE_TIME_MILLIS);

        long totalPages = totalPages();

        while (true) {
            long deficit = claimPages - (freePages(totalPages) - reservedPages.get());

            if (deficit <= 0) {
                long reserved = reservedPages.get();

                if (reservedPages.compareAndSet(reserved, reserved + claimPages))
                    return claimPages;

                continue;
            }

            if (memMetrics.onPageEvictionsStarted()) {
                U.warn(log, "Page-based evictions started." +
                    " Consider increasing 'maxSize' on Data Region configuration: " + dataRegion.config().getName());
            }

            boolean evicted = evictionTracker.evictDataPage();

            memMetrics.updateEvictionRate();

            long curDeficit = claimPages - (freePages(totalPages) - reservedPages.get());

            if (evicted || curDeficit < deficit) {
                lastProgressNanos = System.nanoTime();

                backoffNanos = EVICTION_BACKOFF_START_NANOS;
            }
            else {
                LockSupport.parkNanos(backoffNanos);

                backoffNanos = Math.min(backoffNanos << 1, EVICTION_BACKOFF_MAX_NANOS);
            }

            long nowNanos = System.nanoTime();

            if (nowNanos - lastProgressNanos > TimeUnit.MILLISECONDS.toNanos(EVICTION_NO_PROGRESS_TIMEOUT_MILLIS) ||
                nowNanos > cycleDeadlineNanos)
                throw outOfMemory(dataRegion.config());
        }
    }

    /** {@inheritDoc} */
    @Override public void releaseReserved(long pages) {
        assert pages >= 0;

        if (reserveEnabled && pages != 0)
            reservedPages.addAndGet(-pages);
    }

    /** {@inheritDoc} */
    @Override public void checkFitsForInsert(int rowSize) throws IgniteOutOfMemoryException {
        checkDisabledEvictionOom(rowSize);
    }

    /**
     * Attempts to reserve space for a row without blocking on eviction. Unlike {@link #reserve(int)}, this method
     * never calls {@code evictionTracker.evictDataPage()} (which would take an entry lock), so it is safe to call
     * while holding a page write lock.
     *
     * @param rowSize Row size in bytes.
     * @return The number of claimed pages (>= 0) if the reservation succeeded without eviction, or {@code -1} if
     *     the claim would require eviction (the caller should defer the row to a context where blocking is safe).
     * @throws IgniteOutOfMemoryException If the row is larger than the region or the region is exhausted (disabled
     *     eviction in-memory OOM check).
     */
    private long tryReserve(int rowSize) throws IgniteOutOfMemoryException {
        if (!reserveEnabled) {
            checkDisabledEvictionOom(rowSize);

            return 0;
        }

        long claimPages = pagesToReserve(rowSize);
        long totalPages = totalPages();

        for (int i = 0; i < TRY_RESERVE_CAS_ATTEMPTS; i++) {
            long reserved = reservedPages.get();

            if (claimPages - (freePages(totalPages) - reserved) > 0)
                return -1;

            if (reservedPages.compareAndSet(reserved, reserved + claimPages))
                return claimPages;
        }

        return -1;
    }

    /**
     * Computes the number of pages to reserve for a row, validating that the row can fit into the data region.
     *
     * @param rowSize Row size in bytes.
     * @return Number of pages to claim: the pages required for the row plus the empty pages pool, capped at the region size.
     * @throws IgniteOutOfMemoryException If the row is larger than the whole data region.
     */
    private long pagesToReserve(int rowSize) throws IgniteOutOfMemoryException {
        long totalPages = totalPages();

        long requiredPages = (rowSize + MIN_SIZE_FOR_DATA_PAGE - 1) / MIN_SIZE_FOR_DATA_PAGE;

        if (requiredPages > totalPages)
            throw outOfMemory(dataRegion.config());

        return Math.min(requiredPages + dataRegion.config().getEmptyPagesPoolSize(), totalPages);
    }

    /** @return Total number of pages in the data region (maxSize / systemPageSize). */
    private long totalPages() {
        return dataRegion.config().getMaxSize() / pageMem.systemPageSize();
    }

    /**
     * OOM threshold check for the disabled-eviction in-memory case (previously in {@code ensureFreeSpace}).
     *
     * @param rowSize Row size in bytes.
     * @throws IgniteOutOfMemoryException If the region is near exhaustion.
     */
    private void checkDisabledEvictionOom(int rowSize) throws IgniteOutOfMemoryException {
        if (!dataRegion.config().isPersistenceEnabled() && dataRegion.config().getPageEvictionMode() == DataPageEvictionMode.DISABLED) {
            long nonEmptyPages = pageMem.loadedPages() - emptyDataPages();

            boolean oomThreshold =
                totalPages() < ((double)rowSize / pageMem.pageSize() + nonEmptyPages * (8.0 * 1.5 / pageMem.pageSize() + 1) + 256);

            if (oomThreshold)
                throw outOfMemory(dataRegion.config());
        }
    }

    /**
     * @param part Partition.
     * @return Page ID.
     * @throws IgniteCheckedException If failed.
     */
    private long allocateDataPage(int part) throws IgniteCheckedException {
        assert part <= PageIdAllocator.MAX_PARTITION_ID;

        return pageMem.allocatePage(grpId, part, FLAG_DATA);
    }

    /**
     * @param totalPages Total pages in the region (maxSize / systemPageSize).
     * @return Free pages of the region: empty pages in this free list plus unallocated headroom. Effective headroom
     * accounts for per-segment allocation overhead (up to one page per segment), so it does not trust headroom
     * that cannot actually be allocated.
     */
    private long freePages(long totalPages) {
        return emptyDataPages() + effectiveHeadroom(totalPages, pageMem.loadedPages());
    }

    /**
     * Computes the effective headroom (unallocated pages that can actually be allocated), accounting for per-segment
     * allocation overhead (up to one page per segment).
     *
     * @param totalPages Total pages in the region (maxSize / systemPageSize).
     * @param loadedPages Currently loaded pages.
     * @return Effective headroom, never negative.
     */
    private static long effectiveHeadroom(long totalPages, long loadedPages) {
        return Math.max(0, (totalPages > SEG_CNT ? totalPages - SEG_CNT : 0) - loadedPages);
    }

    /**
     * @param cfg Data region configuration.
     * @return New {@link IgniteOutOfMemoryException} (also reported as a critical failure) for the given region.
     */
    private IgniteOutOfMemoryException outOfMemory(DataRegionConfiguration cfg) {
        IgniteOutOfMemoryException oom = new IgniteOutOfMemoryException("Out of memory in data region [" +
            "name=" + cfg.getName() +
            ", initSize=" + U.readableSize(cfg.getInitialSize(), false) +
            ", maxSize=" + U.readableSize(cfg.getMaxSize(), false) +
            ", persistenceEnabled=" + cfg.isPersistenceEnabled() + "] Try the following:" + U.nl() +
            "  ^-- Increase maximum off-heap memory size (DataRegionConfiguration.maxSize)" + U.nl() +
            "  ^-- Enable Ignite persistence (DataRegionConfiguration.persistenceEnabled)" + U.nl() +
            "  ^-- Enable eviction or expiration policies"
        );

        ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, oom));

        return oom;
    }
}
