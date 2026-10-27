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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataRegionConfiguration;
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
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageUpdateResult;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.LongListReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.GridCursorIteratorWrapper;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PAGE_EVICTION_NO_PROGRESS_TIMEOUT_MILLIS;
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
     * Default maximum time (milliseconds) the size-aware eviction guard is willing to wait without the region's free
     * page count increasing (the deficit shrinking) before giving up (see {@link #reserve(long, boolean)}). Configurable via
     * {@link IgniteSystemProperties#IGNITE_PAGE_EVICTION_NO_PROGRESS_TIMEOUT_MILLIS}.
     */
    public static final long DFLT_EVICTION_NO_PROGRESS_TIMEOUT_MILLIS = 5_000L;

    /**
     * Maximum time (milliseconds) the size-aware eviction guard is willing to wait without the region's free page
     * count increasing (the deficit shrinking) before giving up. This is a liveness guard: a
     * {@code evictDataPage()} call can free no page (for example when the sampled rows are not actually reclaimable),
     * so progress is measured by a real deficit decrease.
     * An eviction that is merely slow but still frees pages keeps shrinking the deficit and thus keeps resetting the
     * timer; a stall that frees nothing for the whole window gives up instead of spinning.
     */
    private static final long EVICTION_NO_PROGRESS_TIMEOUT_MILLIS = IgniteSystemProperties.getLong(
        IGNITE_PAGE_EVICTION_NO_PROGRESS_TIMEOUT_MILLIS, DFLT_EVICTION_NO_PROGRESS_TIMEOUT_MILLIS);

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
    private final PageHandler<T, Boolean> updateSinglePageRow = new UpdateSinglePageRowHandler();

    /** */
    private final DataRegionMetricsImpl memMetrics;

    /** */
    private final PageEvictionTracker evictionTracker;

    /** Whether the page reservation is engaged for this free list: only for in-memory regions with enabled page eviction. */
    private final boolean reserveEnabled;

    /** Data region this free list belongs to. */
    private final DataRegion dataRegion;

    /**
     * Pages reserved by in-flight inserts (see {@link #reserve(long, boolean)}): only the rows' own pages, the empty pages pool
     * is a separate region-wide margin. A reservation is released as a whole once its row (or batch of rows) is written.
     */
    private final AtomicLong reservedPages = new AtomicLong();

    /**
     * Whether the eviction is stalled: set by a best-effort reservation that gave up because the eviction made no
     * progress within {@link #EVICTION_NO_PROGRESS_TIMEOUT_MILLIS}, cleared by any reservation that observes progress or
     * reserves pages. While set, best-effort reservations (including the already waiting ones) give up after an
     * eviction attempt without progress, so a series of inserts does not wait for the timeout one by one.
     */
    private volatile boolean stalled;

    /** Page list cache limit. */
    private final AtomicLong pageListCacheLimit;

    /**
     *
     */
    private final class UpdateSinglePageRowHandler extends PageHandler<T, Boolean> {
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
            IoStatisticsHolder statHolder
        ) throws IgniteCheckedException {
            AbstractDataPageIO<T> io = (AbstractDataPageIO<T>)iox;

            int rowSize = row.size();

            boolean updated = io.updateRow(pageAddr, itemId, pageSize(), row, rowSize);

            evictionTracker.touchPage(pageId);

            if (updated) {
                statHolder.trackPageRemoveData(rowSize);
                statHolder.trackPageInsertData(rowSize);

                if (needWalDeltaRecord(pageId, page, walPlc)) {
                    // TODO IGNITE-5829 This record must contain only a reference to a logical WAL record with the actual data.
                    byte[] payload = new byte[rowSize];

                    DataPagePayload data = io.readPayload(pageAddr, itemId, pageSize());

                    assert data.payloadSize() == rowSize;

                    PageUtils.getBytes(pageAddr, data.offset(), payload, 0, rowSize);

                    wal.log(new DataPageUpdateRecord(
                        cacheId,
                        pageId,
                        itemId,
                        payload));
                }
            }

            return updated;
        }
    }

    /** Current state (for last processed page) of partially written row. */
    private final class PartiallyWritten {
        /** */
        private final T row;

        /** */
        private long nextLink;

        /** */
        private int written;

        /** */
        private boolean modified;

        /** */
        public PartiallyWritten(T row) {
            this.row = row;
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

            // A non-head fragment (the row is not complete after the fragment write) always occupies the whole
            // initially empty page (see takePage), so a page can be shared between rows only as a head fragment page.
            // Deadlock freedom of the fragmented row lock coupling (see updateFragmentedRow) relies on this.
            assert written == rowSize || io.getDirectCount(pageAddr) == 1 :
                "Non-head fragment is written to a shared page [pageId=" + U.hexLong(pageId) +
                    ", items=" + io.getDirectCount(pageAddr) + ']';

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

    /** */
    private final class WriteRowsHandler extends PageHandler<GridCursor<T>, Integer> {
        /** {@inheritDoc} */
        @Override public Integer run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            GridCursor<T> cur,
            int written,
            IoStatisticsHolder statHolder
        ) throws IgniteCheckedException {
            AbstractDataPageIO<T> io = (AbstractDataPageIO<T>)iox;

            // Fill the page up to the end.
            while (written != COMPLETE || cur.next()) {
                T row = cur.get();

                if (written == COMPLETE) {
                    // If the data row was completely written without remainder, proceed to the next.
                    if ((written = writeWholePages(row, statHolder)) == COMPLETE)
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

        this.evictionTracker = dataRegion.evictionTracker();

        reserveEnabled = ctx.cache().context().database().isReserveEnabled(dataRegion);

        this.dataRegion = dataRegion;
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

        this.memMetrics = dataRegion.metrics();

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

    /**
     * @param part Partition.
     * @return Page ID.
     * @throws IgniteCheckedException If failed.
     */
    private long allocateDataPage(int part) throws IgniteCheckedException {
        assert part <= PageIdAllocator.MAX_PARTITION_ID;

        return pageMem.allocatePage(grpId, part, FLAG_DATA);
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
        catch (IgniteCheckedException | Error e) {
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
     * <p>
     * The space of the whole batch is reserved upfront, before any page lock is taken: the eviction is not allowed
     * under a page write lock, and a batch (e.g. of rebalancing) is small, so a pessimistic reservation is cheap.
     *
     * @param rows Rows.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteCheckedException If failed.
     */
    @Override public void insertDataRows(Collection<T> rows,
        IoStatisticsHolder statHolder) throws IgniteCheckedException {
        try {
            // Best-effort: rows of a batch (e.g. of rebalancing) can not be safely refused.
            long reserved = reserveEnabled ? reserve(batchSize(rows), true) : 0;

            try {
                GridCursor<T> cur = new GridCursorIteratorWrapper<>(rows.iterator());

                int written = COMPLETE;

                while (written != COMPLETE || cur.next()) {
                    T row = cur.get();

                    if (written == COMPLETE) {
                        written = writeWholePages(row, statHolder);

                        continue;
                    }

                    AbstractDataPageIO initIo = null;

                    long pageId = takePage(row.size() - written, row, statHolder);

                    if (pageId == 0L) {
                        pageId = allocateDataPage(row.partition());

                        initIo = row.ioVersions().latest();
                    }

                    written = write(pageId, writeRowsHnd, initIo, cur, written, FAIL_I, statHolder);

                    assert written != FAIL_I; // We can't fail here.
                }
            }
            finally {
                releaseReserved(reserved);
            }
        }
        catch (RuntimeException e) {
            throw new CorruptedFreeListException("Failed to insert data rows", e, grpId);
        }
    }

    /**
     * Computes a pessimistic size of a batch of rows: every row is rounded up to whole data pages, as if no rows
     * shared a page.
     *
     * @param rows Rows.
     * @return Size of the batch in bytes.
     * @throws IgniteCheckedException If failed.
     */
    private long batchSize(Collection<T> rows) throws IgniteCheckedException {
        long size = 0;

        for (T row : rows)
            size += requiredPages(row.size()) * MIN_SIZE_FOR_DATA_PAGE;

        return size;
    }

    /**
     * Write fragments of the row, which occupy the whole memory page. A data row is ignored if it is less than the max
     * payload of an empty data page.
     *
     * @param row Row to process.
     * @param statHolder Statistics holder to track IO operations.
     * @return Number of bytes written, {@link #COMPLETE} if the row was fully written, {@code 0} if data row was
     * ignored because it is less than the max payload of an empty data page.
     * @throws IgniteCheckedException If failed.
     */
    private int writeWholePages(T row, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        assert row.link() == 0 : row.link();

        int written = 0;
        int rowSize = row.size();

        while (rowSize - written >= MIN_SIZE_FOR_DATA_PAGE) {
            written = writeSinglePage(row, written, statHolder);

            memMetrics.incrementLargeEntriesPages();
        }

        return written;
    }

    /**
     * Take a page and write row on it.
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
    @Override public void updateDataRow(
        T oldRow,
        T newRow,
        IoStatisticsHolder statHolder
    ) throws IgniteCheckedException {
        long link = oldRow.link();
        int size = newRow.size();

        assert link != 0;
        assert oldRow.size() == size :
            "Unexpected row size on update [oldSize=" + oldRow.size() + ", newSize=" + size + ']';

        try {
            long pageId = PageIdUtils.pageId(link);
            int itemId = PageIdUtils.itemId(link);

            if (size <= pageSize() - AbstractDataPageIO.MIN_DATA_PAGE_OVERHEAD) {
                Boolean updated = write(pageId, updateSinglePageRow, newRow, itemId, null, statHolder);

                assert updated != null; // Can't fail here.

                if (updated)
                    return;

                // Fallback if row is fragmented.
            }

            PartiallyWritten updateRes = updateFragmentedRow(link, newRow, statHolder);

            statHolder.trackPageRemoveData(size);
            statHolder.trackPageInsertData(size);

            assert updateRes.written == size :
                "Unexpected written row size [written=" + updateRes.written + ", rowSize=" + size + ']';

            assert updateRes.nextLink == 0 :
                "Unexpected next page link [nextLink=" + Long.toHexString(updateRes.nextLink) + ']';
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

    /**
     * Updates fragmented row in-place.
     * <p>
     * Fragments are updated in the order of fragment links (from the head fragment to the tail one) using lock
     * coupling: the write lock on the next fragment page is acquired before the write lock on the current fragment
     * page is released. Readers of fragmented rows traverse fragments in the same order with the same lock coupling
     * (see {@code CacheDataRowAdapter#doInitFromLink}), so a concurrent update can neither overtake a reader in the
     * middle of the row nor be overtaken by it, and every reader observes either the entirely old or the entirely new
     * row.
     * <p>
     * Lock coupling is deadlock-free: a page can be shared between rows only as a head fragment page, since non-head
     * fragments always occupy the whole page (see {@link #takePage}, {@code WriteRowHandler#addRow}), and the head
     * page is never requested while a lock on another page of the same or another row is held.
     *
     * @param link Row link.
     * @param row Row with the new data.
     * @param statHolder Statistics holder.
     * @return Final state of the update.
     * @throws IgniteCheckedException If failed.
     */
    private PartiallyWritten updateFragmentedRow(
        long link, T row,
        IoStatisticsHolder statHolder
    ) throws IgniteCheckedException {
        PartiallyWritten state = new PartiallyWritten(row);

        int size = row.size();

        long pageId = PageIdUtils.pageId(link);
        int itemId = PageIdUtils.itemId(link);
        long page = 0L;
        long pageAddr = 0L;
        boolean dirty = false;

        try {
            page = acquirePage(pageId, statHolder);
            pageAddr = lockFragmentPage(pageId, page, link);

            while (true) {
                updateFragmentPage(pageId, page, pageAddr, pageIoRslvr.resolve(pageAddr), state, itemId);

                dirty = state.modified;

                assert PageIO.getCrc(pageAddr) == 0; // See PageHandler#writePage.

                if (state.written >= size)
                    return state;

                assert state.nextLink != 0L : "Unexpected end of the fragmented row [link=" + U.hexLong(link) +
                    ", written=" + state.written + ", rowSize=" + size + ']';

                long nextPageId = PageIdUtils.pageId(state.nextLink);
                int nextItemId = PageIdUtils.itemId(state.nextLink);

                // Lock coupling: acquire the write lock on the next fragment page before releasing the current one.
                long nextPage = acquirePage(nextPageId, statHolder);
                long nextPageAddr;

                try {
                    nextPageAddr = lockFragmentPage(nextPageId, nextPage, state.nextLink);
                }
                catch (Throwable t) {
                    releasePage(nextPageId, nextPage);

                    throw t;
                }

                try {
                    writeUnlock(pageId, page, pageAddr, null, dirty);
                    releasePage(pageId, page);
                }
                finally {
                    pageId = nextPageId;
                    itemId = nextItemId;
                    page = nextPage;
                    pageAddr = nextPageAddr;
                    dirty = false;
                }
            }
        }
        finally {
            if (pageAddr != 0L)
                writeUnlock(pageId, page, pageAddr, null, dirty);

            if (page != 0L)
                releasePage(pageId, page);
        }
    }

    /**
     * Acquires the write lock on the fragment page of the live (referenced from the data tree) row.
     *
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param link Fragment link (for diagnostics).
     * @return Page address.
     */
    private long lockFragmentPage(long pageId, long page, long link) {
        long pageAddr = writeLock(pageId, page);

        // Pages of the live row can't be recycled, so the lock can't fail here.
        assert pageAddr != 0L : "Failed to lock fragment page of the live row [link=" + U.hexLong(link) +
            ", tag=" + PageIdUtils.tag(pageId) + ']';

        return pageAddr;
    }

    /**
     * Updates locked fragment page of the row.
     *
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param iox Page IO
     * @param fragment Partially written fragment state.
     * @param itemId Item ID.
     */
    private void updateFragmentPage(
        long pageId,
        long page,
        long pageAddr,
        PageIO iox,
        PartiallyWritten fragment,
        int itemId
    ) throws IgniteCheckedException {
        AbstractDataPageIO<T> io = (AbstractDataPageIO<T>)iox;

        boolean walEnabled = wal != null && !wal.pageRecordsDisabled(grpId, pageId);
        // Never need payload for in-memory. For persistence need payload if wal is enabled and need to compare
        // page to mark it dirty if it's not dirty yet even if wal is disabled.
        boolean needPayload = pageMem instanceof PageMemoryEx && (walEnabled || !pageMem.isDirty(grpId, pageId, page));

        DataPageUpdateResult updateRes = io.updateRowFragment(pageMem, pageAddr, itemId, pageSize(),
            fragment.row, fragment.written, needPayload);

        evictionTracker.touchPage(pageId);

        if (updateRes != null) {
            if (updateRes.modifiedPayload() != null && walEnabled && needWalDeltaRecord(pageId, page, null)) {
                wal.log(new DataPageUpdateRecord(
                    grpId,
                    pageId,
                    itemId,
                    updateRes.modifiedPayload()));
            }

            fragment.modified = !needPayload || updateRes.modifiedPayload() != null;
            fragment.nextLink = updateRes.nextLink();
            fragment.written += updateRes.payloadSize();
        }
        else {
            fragment.modified = true;
            fragment.nextLink = 0L;
            fragment.written = fragment.row.size();
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
    @Override public long reserve(long rowSize, boolean bestEffort) throws IgniteOutOfMemoryException, IgniteCheckedException {
        // Nothing to insert (e.g. an empty batch): no eviction is needed.
        if (!reserveEnabled || rowSize <= 0)
            return 0;

        long claimPages = requiredPages(rowSize);

        if (!fitsRegion(claimPages)) {
            if (bestEffort)
                return 0; // Let the page memory decide.

            throw outOfMemory();
        }

        long limitPages = limitPages(claimPages);

        long backoffNanos = EVICTION_BACKOFF_START_NANOS;

        long lastProgressMillis = U.currentTimeMillis();

        // Deficit before the last eviction attempt, 0 until the first one.
        long prevDeficit = 0;

        while (true) {
            long reserved = reservedPages.get();
            long deficit = deficit(claimPages, limitPages, reserved);

            if (deficit <= 0) {
                if (reservedPages.compareAndSet(reserved, reserved + claimPages)) {
                    clearStalled();

                    return claimPages;
                }

                continue;
            }

            if (prevDeficit > 0) {
                if (deficit < prevDeficit) {
                    clearStalled();

                    lastProgressMillis = U.currentTimeMillis();

                    backoffNanos = EVICTION_BACKOFF_START_NANOS;
                }
                else if (bestEffort && stalled)
                    return 0;
                else if (U.currentTimeMillis() - lastProgressMillis > EVICTION_NO_PROGRESS_TIMEOUT_MILLIS) {
                    if (!bestEffort)
                        throw outOfMemory();

                    stalled = true;

                    LT.warn(log, "Page eviction made no progress, the row is inserted without a page reservation " +
                        "[region=" + dataRegion.config().getName() + ", timeout=" + EVICTION_NO_PROGRESS_TIMEOUT_MILLIS + "ms]");

                    return 0;
                }
                else {
                    LockSupport.parkNanos(backoffNanos);

                    backoffNanos = Math.min(backoffNanos << 1, EVICTION_BACKOFF_MAX_NANOS);
                }
            }

            prevDeficit = deficit;

            if (memMetrics.onPageEvictionsStarted()) {
                U.warn(log, "Page-based evictions started." +
                    " Consider increasing 'maxSize' on Data Region configuration: " + dataRegion.config().getName());
            }

            evictionTracker.evictDataPage();

            memMetrics.updateEvictionRate();
        }
    }

    /**
     * Clears the {@link #stalled} flag once a reservation observes progress or reserves pages. The flag is read first,
     * so the common path does not write to the shared field.
     */
    private void clearStalled() {
        if (stalled)
            stalled = false;
    }

    /** {@inheritDoc} */
    @Override public void releaseReserved(long pages) {
        assert pages >= 0;

        if (reserveEnabled && pages != 0)
            reservedPages.addAndGet(-pages);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "FreeList [name=" + name() + ']';
    }

    /**
     * Computes the number of pages to claim for a row. The empty pages pool is not a part of the claim: it is a single
     * region-wide margin that every reservation must leave free (see {@link #deficit}), so concurrent writers do not
     * multiply it.
     *
     * @param rowSize Row size in bytes.
     * @return Number of pages required for the row.
     */
    private long requiredPages(long rowSize) {
        return (rowSize + MIN_SIZE_FOR_DATA_PAGE - 1) / MIN_SIZE_FOR_DATA_PAGE;
    }

    /**
     * @param requiredPages Pages required for a row.
     * @return {@code True} if the row together with the empty pages pool fits into the whole data region.
     */
    private boolean fitsRegion(long requiredPages) {
        return requiredPages + dataRegion.config().getEmptyPagesPoolSize() <= totalPages();
    }

    /** @return Total number of pages in the data region. */
    private long totalPages() {
        return dataRegion.config().getMaxSize() / pageMem.systemPageSize();
    }

    /**
     * Computes the number of loaded pages up to which the free space of the region is counted for a row. Normally it
     * is the eviction threshold ({@link DataRegionConfiguration#getEvictionThreshold()}), so every insert path starts
     * evicting at the same fill level. A row that does not fit below the threshold together with the empty pages pool
     * may use the space above it, up to the whole region.
     *
     * @param requiredPages Pages required for the row.
     * @return Limit of loaded pages for the row.
     */
    private long limitPages(long requiredPages) {
        long totalPages = totalPages();

        long hardPages = totalPages > SEG_CNT ? totalPages - SEG_CNT : 0;

        long thresholdPages = Math.min((long)(dataRegion.config().getEvictionThreshold() * totalPages), hardPages);

        return requiredPages + dataRegion.config().getEmptyPagesPoolSize() > thresholdPages ? hardPages : thresholdPages;
    }

    /**
     * Computes how many pages are missing to admit a claim: the claim, the empty pages pool and the reservations of
     * other in-flight inserts must fit into the free pages of the region (empty pages in this free list plus the
     * headroom up to the limit).
     *
     * @param claimPages Pages to claim.
     * @param limitPages Limit of loaded pages (see {@link #limitPages(long)}).
     * @param reserved Pages reserved by other in-flight inserts.
     * @return Number of missing pages, a non-positive value if the claim can be admitted.
     */
    private long deficit(long claimPages, long limitPages, long reserved) {
        long freePages = emptyDataPages() + Math.max(0, limitPages - pageMem.loadedPages());

        return claimPages + dataRegion.config().getEmptyPagesPoolSize() + reserved - freePages;
    }

    /**
     * Creates an out-of-memory exception for a refused reservation. It is thrown only for inserts that can be refused
     * (see {@link #reserve(long, boolean)}) and is not reported to the failure handler: nothing has been changed yet.
     *
     * @return New {@link IgniteOutOfMemoryException} for the data region of this free list.
     */
    private IgniteOutOfMemoryException outOfMemory() {
        DataRegionConfiguration cfg = dataRegion.config();

        return new IgniteOutOfMemoryException("Out of memory in data region [" +
            "name=" + cfg.getName() +
            ", initSize=" + U.readableSize(cfg.getInitialSize(), false) +
            ", maxSize=" + U.readableSize(cfg.getMaxSize(), false) +
            ", persistenceEnabled=" + cfg.isPersistenceEnabled() + "] Try the following:" + U.nl() +
            "  ^-- Increase maximum off-heap memory size (DataRegionConfiguration.maxSize)" + U.nl() +
            "  ^-- Enable Ignite persistence (DataRegionConfiguration.persistenceEnabled)" + U.nl() +
            "  ^-- Enable eviction or expiration policies"
        );
    }
}
