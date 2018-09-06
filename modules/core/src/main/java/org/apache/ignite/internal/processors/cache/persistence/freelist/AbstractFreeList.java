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

import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
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
import org.apache.ignite.internal.processors.cache.persistence.evict.PageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.LongListReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.typedef.internal.U;

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

    /**
     * Step between buckets in free list, measured in powers of two.
     * For example, for page size 4096 and 256 buckets, shift is 4 and step is 16 bytes.
     */
    private final int shift;

    /** */
    private final AtomicReferenceArray<Stripe[]> buckets = new AtomicReferenceArray<>(BUCKETS);

    /** */
    private final int MIN_SIZE_FOR_DATA_PAGE;

    /** */
    private final PageHandler<T, Boolean> updateRow = new UpdateRowHandler();

    /** */
    private final DataRegionMetricsImpl memMetrics;

    /** */
    private final PageEvictionTracker evictionTracker;

    /**
     *
     */
    private final class UpdateRowHandler extends PageHandler<T, Boolean> {
        @Override public Boolean run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            T row,
            int itemId)
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

                wal.log(new DataPageUpdateRecord(
                    cacheId,
                    pageId,
                    itemId,
                    payload));
            }

            return updated;
        }
    }

    /** */
    private final PageHandler<T, Integer> writeRow = new WriteRowHandler();

    /**
     *
     */
    private final class WriteRowHandler extends PageHandler<T, Integer> {
        @Override public Integer run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            T row,
            int written)
            throws IgniteCheckedException {
            AbstractDataPageIO<T> io = (AbstractDataPageIO<T>)iox;

            int rowSize = row.size();
            int oldFreeSpace = io.getFreeSpace(pageAddr);

            assert oldFreeSpace > 0 : oldFreeSpace;

            // If the full row does not fit into this page write only a fragment.
            written = (written == 0 && oldFreeSpace >= rowSize) ? addRow(pageId, page, pageAddr, io, row, rowSize) :
                addRowFragment(pageId, page, pageAddr, io, row, written, rowSize);

            // Reread free space after update.
            int newFreeSpace = io.getFreeSpace(pageAddr);

            if (newFreeSpace > MIN_PAGE_FREE_SPACE) {
                int bucket = bucket(newFreeSpace, false);

                put(null, pageId, page, pageAddr, bucket);
            }

            if (written == rowSize)
                evictionTracker.touchPage(pageId);

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
        private int addRow(
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
        private int addRowFragment(
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

        @Override public Long run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            ReuseBag reuseBag,
            int itemId)
            throws IgniteCheckedException {
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

                        putIsNeeded = removeDataPage(pageId, page, pageAddr, io, oldBucket);
                    }
                }

                if (io.isEmpty(pageAddr)) {
                    evictionTracker.forgetPage(pageId);

                    if (putIsNeeded)
                        reuseBag.addFreePage(recyclePage(pageId, page, pageAddr, null));
                }
                else if (putIsNeeded)
                    put(null, pageId, page, pageAddr, newBucket);
            }

            // For common case boxed 0L will be cached inside of Long, so no garbage will be produced.
            return nextLink;
        }
    }

    /**
     * @param cacheId Cache ID.
     * @param name Name (for debug purpose).
     * @param memMetrics Memory metrics.
     * @param memPlc Data region.
     * @param reuseList Reuse list or {@code null} if this free list will be a reuse list for itself.
     * @param wal Write ahead log manager.
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new metadata should be initialized.
     * @throws IgniteCheckedException If failed.
     */
    public AbstractFreeList(
        int cacheId,
        String name,
        DataRegionMetricsImpl memMetrics,
        DataRegion memPlc,
        ReuseList reuseList,
        IgniteWriteAheadLogManager wal,
        long metaPageId,
        boolean initNew) throws IgniteCheckedException {
        super(cacheId, name, memPlc.pageMemory(), BUCKETS, wal, metaPageId);

        rmvRow = new RemoveRowHandler(cacheId == 0);

        this.evictionTracker = memPlc.evictionTracker();
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

        this.memMetrics = memMetrics;

        init(metaPageId, initNew);
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

            long pages = bucketsSize[b].longValue();

            freeSpace += pages * perPageFreeSpace;
        }

        return freeSpace;
    }

    /** {@inheritDoc} */
    @Override public void dumpStatistics(IgniteLogger log) {
        long dataPages = 0;

        final boolean dumpBucketsInfo = false;

        for (int b = 0; b < BUCKETS; b++) {
            long size = bucketsSize[b].longValue();

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
                log.info("FreeList [name=" + name +
                    ", buckets=" + BUCKETS +
                    ", dataPages=" + dataPages +
                    ", reusePages=" + bucketsSize[REUSE_BUCKET].longValue() + "]");
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

    /**
     * @param part Partition.
     * @return Page ID.
     * @throws IgniteCheckedException If failed.
     */
    private long allocateDataPage(int part) throws IgniteCheckedException {
        assert part <= PageIdAllocator.MAX_PARTITION_ID;
        assert part != PageIdAllocator.INDEX_PARTITION;

        return pageMem.allocatePage(grpId, part, PageIdAllocator.FLAG_DATA);
    }

    /** {@inheritDoc} */
    @Override public void insertDataRow(T row) throws IgniteCheckedException {
        int rowSize = row.size();

        int written = 0;

        do {
            if (written != 0)
                memMetrics.incrementLargeEntriesPages();

            int remaining = rowSize - written;

            long pageId = 0L;

            for (int b = remaining < MIN_SIZE_FOR_DATA_PAGE ? bucket(remaining, false) + 1 : REUSE_BUCKET; b < BUCKETS; b++) {
                pageId = takeEmptyPage(b, ioVersions());

                if (pageId != 0L)
                    break;
            }

            AbstractDataPageIO<T> initIo = null;

            if (pageId == 0L) {
                pageId = allocateDataPage(row.partition());

                initIo = ioVersions().latest();
            }
            else if (PageIdUtils.tag(pageId) != PageIdAllocator.FLAG_DATA)
                pageId = initReusedPage(pageId, row.partition());
            else
                pageId = PageIdUtils.changePartitionId(pageId, (row.partition()));

            written = write(pageId, writeRow, initIo, row, written, FAIL_I);

            assert written != FAIL_I; // We can't fail here.
        }
        while (written != COMPLETE);
    }

    /**
     * @param reusedPageId Reused page id.
     * @param partId Partition id.
     * @return Prepared page id.
     *
     * @see PagesList#initReusedPage(long, long, long, int, byte, PageIO)
     */
    private long initReusedPage(long reusedPageId, int partId) throws IgniteCheckedException {
        long reusedPage = acquirePage(reusedPageId);
        try {
            long reusedPageAddr = writeLock(reusedPageId, reusedPage);

            assert reusedPageAddr != 0;

            try {
                return initReusedPage(reusedPageId, reusedPage, reusedPageAddr,
                    partId, PageIdAllocator.FLAG_DATA, ioVersions().latest());
            }
            finally {
                writeUnlock(reusedPageId, reusedPage, reusedPageAddr, true);
            }
        }
        finally {
            releasePage(reusedPageId, reusedPage);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean updateDataRow(long link, T row) throws IgniteCheckedException {
        assert link != 0;

        long pageId = PageIdUtils.pageId(link);
        int itemId = PageIdUtils.itemId(link);

        Boolean updated = write(pageId, updateRow, row, itemId, null);

        assert updated != null; // Can't fail here.

        return updated;
    }

    /** {@inheritDoc} */
    @Override public <S, R> R updateDataRow(long link, PageHandler<S, R> pageHnd, S arg) throws IgniteCheckedException {
        assert link != 0;

        long pageId = PageIdUtils.pageId(link);
        int itemId = PageIdUtils.itemId(link);

        R updRes = write(pageId, pageHnd, arg, itemId, null);

        assert updRes != null; // Can't fail here.

        return updRes;
    }

    /** {@inheritDoc} */
    @Override public void removeDataRowByLink(long link) throws IgniteCheckedException {
        assert link != 0;

        long pageId = PageIdUtils.pageId(link);
        int itemId = PageIdUtils.itemId(link);

        ReuseBag bag = new LongListReuseBag();

        long nextLink = write(pageId, rmvRow, bag, itemId, FAIL_L);

        assert nextLink != FAIL_L; // Can't fail here.

        while (nextLink != 0L) {
            memMetrics.decrementLargeEntriesPages();

            itemId = PageIdUtils.itemId(nextLink);
            pageId = PageIdUtils.pageId(nextLink);

            nextLink = write(pageId, rmvRow, bag, itemId, FAIL_L);

            assert nextLink != FAIL_L; // Can't fail here.
        }

        reuseList.addForRecycle(bag);
    }

    /** {@inheritDoc} */
    @Override protected Stripe[] getBucket(int bucket) {
        return buckets.get(bucket);
    }

    /** {@inheritDoc} */
    @Override protected boolean casBucket(int bucket, Stripe[] exp, Stripe[] upd) {
        return buckets.compareAndSet(bucket, exp, upd);
    }

    /** {@inheritDoc} */
    @Override protected boolean isReuseBucket(int bucket) {
        return bucket == REUSE_BUCKET;
    }

    /**
     * @return Number of empty data pages in free list.
     */
    public int emptyDataPages() {
        return bucketsSize[REUSE_BUCKET].intValue();
    }

    /** {@inheritDoc} */
    @Override public void addForRecycle(ReuseBag bag) throws IgniteCheckedException {
        assert reuseList == this : "not allowed to be a reuse list";

        put(bag, 0, 0, 0L, REUSE_BUCKET);
    }

    /** {@inheritDoc} */
    @Override public long takeRecycledPage() throws IgniteCheckedException {
        assert reuseList == this : "not allowed to be a reuse list";

        return takeEmptyPage(REUSE_BUCKET, null);
    }

    /** {@inheritDoc} */
    @Override public long recycledPagesCount() throws IgniteCheckedException {
        assert reuseList == this : "not allowed to be a reuse list";

        return storedPagesCount(REUSE_BUCKET);
    }

    /**
     * @return IOVersions.
     */
    public abstract IOVersions<? extends AbstractDataPageIO<T>> ioVersions();

    /** {@inheritDoc} */
    @Override public String toString() {
        return "FreeList [name=" + name + ']';
    }
}
