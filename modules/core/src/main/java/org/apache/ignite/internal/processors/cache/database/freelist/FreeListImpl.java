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

package org.apache.ignite.internal.processors.cache.database.freelist;

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
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.MemoryMetricsImpl;
import org.apache.ignite.internal.processors.cache.database.MemoryPolicy;
import org.apache.ignite.internal.processors.cache.database.evict.PageEvictionTracker;
import org.apache.ignite.internal.processors.cache.database.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 */
public class FreeListImpl extends PagesList implements FreeList, ReuseList {
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

    /** */
    private final int shift;

    /** */
    private final AtomicReferenceArray<Stripe[]> buckets = new AtomicReferenceArray<>(BUCKETS);

    /** */
    private final int MIN_SIZE_FOR_DATA_PAGE;

    /** */
    private final int emptyDataPagesBucket;

    /** */
    private final PageHandler<CacheDataRow, Boolean> updateRow = new UpdateRowHandler();

    /** */
    private final MemoryMetricsImpl memMetrics;

    /** */
    private final PageEvictionTracker evictionTracker;

    /**
     *
     */
    private final class UpdateRowHandler extends PageHandler<CacheDataRow, Boolean> {
        @Override public Boolean run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            CacheDataRow row,
            int itemId)
            throws IgniteCheckedException {
            DataPageIO io = (DataPageIO)iox;

            int rowSize = getRowSize(row);

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
    private final PageHandler<CacheDataRow, Integer> writeRow = new WriteRowHandler();

    /**
     *
     */
    private final class WriteRowHandler extends PageHandler<CacheDataRow, Integer> {
        @Override public Integer run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            CacheDataRow row,
            int written)
            throws IgniteCheckedException {
            DataPageIO io = (DataPageIO)iox;

            int rowSize = getRowSize(row);
            int oldFreeSpace = io.getFreeSpace(pageAddr);

            assert oldFreeSpace > 0 : oldFreeSpace;

            // If the full row does not fit into this page write only a fragment.
            written = (written == 0 && oldFreeSpace >= rowSize) ? addRow(pageId, page, pageAddr, io, row, rowSize):
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
            DataPageIO io,
            CacheDataRow row,
            int rowSize
        ) throws IgniteCheckedException {
            io.addRow(pageAddr, row, rowSize, pageSize());

            if (needWalDeltaRecord(pageId, page, null)) {
                // TODO This record must contain only a reference to a logical WAL record with the actual data.
                byte[] payload = new byte[rowSize];

                DataPagePayload data = io.readPayload(pageAddr, PageIdUtils.itemId(row.link()), pageSize());

                assert data.payloadSize() == rowSize;

                PageUtils.getBytes(pageAddr, data.offset(), payload, 0, rowSize);

                wal.log(new DataPageInsertRecord(
                    cacheId,
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
            DataPageIO io,
            CacheDataRow row,
            int written,
            int rowSize
        ) throws IgniteCheckedException {
            // Read last link before the fragment write, because it will be updated there.
            long lastLink = row.link();

            int payloadSize = io.addRowFragment(pageMem, pageAddr, row, written, rowSize, pageSize());

            assert payloadSize > 0 : payloadSize;

            if (needWalDeltaRecord(pageId, page, null)) {
                // TODO This record must contain only a reference to a logical WAL record with the actual data.
                byte[] payload = new byte[payloadSize];

                DataPagePayload data = io.readPayload(pageAddr, PageIdUtils.itemId(row.link()), pageSize());

                PageUtils.getBytes(pageAddr, data.offset(), payload, 0, payloadSize);

                wal.log(new DataPageInsertFragmentRecord(cacheId, pageId, payload, lastLink));
            }

            return written + payloadSize;
        }
    }


    /** */
    private final PageHandler<Void, Long> rmvRow = new RemoveRowHandler();

    /**
     *
     */
    private final class RemoveRowHandler extends PageHandler<Void, Long> {
        @Override public Long run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            Void ignored,
            int itemId)
            throws IgniteCheckedException {
            DataPageIO io = (DataPageIO)iox;

            int oldFreeSpace = io.getFreeSpace(pageAddr);

            assert oldFreeSpace >= 0: oldFreeSpace;

            long nextLink = io.removeRow(pageAddr, itemId, pageSize());

            if (needWalDeltaRecord(pageId, page, walPlc))
                wal.log(new DataPageRemoveRecord(cacheId, pageId, itemId));

            int newFreeSpace = io.getFreeSpace(pageAddr);

            if (newFreeSpace > MIN_PAGE_FREE_SPACE) {
                int newBucket = bucket(newFreeSpace, false);

                if (oldFreeSpace > MIN_PAGE_FREE_SPACE) {
                    int oldBucket = bucket(oldFreeSpace, false);

                    if (oldBucket != newBucket) {
                        // It is possible that page was concurrently taken for put, in this case put will handle bucket change.
                        if (removeDataPage(pageId, page, pageAddr, io, oldBucket))
                            put(null, pageId, page, pageAddr, newBucket);
                    }
                }
                else
                    put(null, pageId, page, pageAddr, newBucket);

                if (io.isEmpty(pageAddr))
                    evictionTracker.forgetPage(pageId);
            }

            // For common case boxed 0L will be cached inside of Long, so no garbage will be produced.
            return nextLink;
        }
    }

    /**
     * @param cacheId Cache ID.
     * @param name Name (for debug purpose).
     * @param memMetrics Memory metrics.
     * @param memPlc Memory policy.
     * @param reuseList Reuse list or {@code null} if this free list will be a reuse list for itself.
     * @param wal Write ahead log manager.
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new metadata should be initialized.
     * @throws IgniteCheckedException If failed.
     */
    public FreeListImpl(
        int cacheId,
        String name,
        MemoryMetricsImpl memMetrics,
        MemoryPolicy memPlc,
        ReuseList reuseList,
        IgniteWriteAheadLogManager wal,
        long metaPageId,
        boolean initNew) throws IgniteCheckedException {
        super(cacheId, name, memPlc.pageMemory(), BUCKETS, wal, metaPageId);
        this.evictionTracker = memPlc.evictionTracker();
        this.reuseList = reuseList == null ? this : reuseList;
        int pageSize = pageMem.pageSize();

        assert U.isPow2(pageSize) : "Page size must be a power of 2: " + pageSize;
        assert U.isPow2(BUCKETS);
        assert BUCKETS <= pageSize : pageSize;

        // TODO this constant is used because currently we cannot reuse data pages as index pages
        // TODO and vice-versa. It should be removed when data storage format is finalized.
        MIN_SIZE_FOR_DATA_PAGE = pageSize - DataPageIO.MIN_DATA_PAGE_OVERHEAD;

        int shift = 0;

        while (pageSize > BUCKETS) {
            shift++;
            pageSize >>>= 1;
        }

        this.shift = shift;

        this.memMetrics = memMetrics;

        emptyDataPagesBucket = bucket(MIN_SIZE_FOR_DATA_PAGE, false);

        init(metaPageId, initNew);
    }

    /**
     * Calculates average fill factor over FreeListImpl instance.
     */
    public float fillFactor() {
        long pageSize = pageSize();

        long totalSize = 0;
        long loadSize = 0;

        for (int b = BUCKETS - 2; b > 0; b--) {
            long bsize = pageSize - ((REUSE_BUCKET - b) << shift);

            long pages = bucketsSize[b].longValue();

            loadSize += pages * (pageSize - bsize);

            totalSize += pages * pageSize;
        }

        return totalSize == 0 ? -1L : ((float) loadSize / totalSize);
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

                log.info("Bucket [b=" + b +
                    ", size=" + size +
                    ", stripes=" + (stripes != null ? stripes.length : 0) +
                    ", stripesEmpty=" + empty + ']');
            }
        }

        if (dataPages > 0) {
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

        return pageMem.allocatePage(cacheId, part, PageIdAllocator.FLAG_DATA);
    }

    /** {@inheritDoc} */
    @Override public void insertDataRow(CacheDataRow row) throws IgniteCheckedException {
        int rowSize = getRowSize(row);

        int written = 0;

        do {
            if (written != 0)
                memMetrics.incrementLargeEntriesPages();

            int freeSpace = Math.min(MIN_SIZE_FOR_DATA_PAGE, rowSize - written);

            long pageId = 0L;

            if (freeSpace == MIN_SIZE_FOR_DATA_PAGE)
                pageId = takeEmptyPage(emptyDataPagesBucket, DataPageIO.VERSIONS);

            boolean reuseBucket = false;

            // TODO: properly handle reuse bucket.
            if (pageId == 0L) {
                for (int b = bucket(freeSpace, false) + 1; b < BUCKETS - 1; b++) {
                    pageId = takeEmptyPage(b, DataPageIO.VERSIONS);

                    if (pageId != 0L) {
                        reuseBucket = isReuseBucket(b);

                        break;
                    }
                }
            }

            boolean allocated = pageId == 0L;

            if (allocated)
                pageId = allocateDataPage(row.partition());

            DataPageIO init = reuseBucket || allocated ? DataPageIO.VERSIONS.latest() : null;

            written = write(pageId, writeRow, init, row, written, FAIL_I);

            assert written != FAIL_I; // We can't fail here.
        }
        while (written != COMPLETE);
    }

    /** {@inheritDoc} */
    @Override public boolean updateDataRow(long link, CacheDataRow row) throws IgniteCheckedException {
        assert link != 0;

        long pageId = PageIdUtils.pageId(link);
        int itemId = PageIdUtils.itemId(link);

        Boolean updated = write(pageId, updateRow, row, itemId, null);

        assert updated != null; // Can't fail here.

        return updated;
    }

    /** {@inheritDoc} */
    @Override public void removeDataRowByLink(long link) throws IgniteCheckedException {
        assert link != 0;

        long pageId = PageIdUtils.pageId(link);
        int itemId = PageIdUtils.itemId(link);

        long nextLink = write(pageId, rmvRow, itemId, FAIL_L);

        assert nextLink != FAIL_L; // Can't fail here.

        while (nextLink != 0L) {
            memMetrics.decrementLargeEntriesPages();

            itemId = PageIdUtils.itemId(nextLink);
            pageId = PageIdUtils.pageId(nextLink);

            nextLink = write(pageId, rmvRow, itemId, FAIL_L);

            assert nextLink != FAIL_L; // Can't fail here.
        }
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
        return bucketsSize[emptyDataPagesBucket].intValue();
    }

    /** {@inheritDoc} */
    @Override public void addForRecycle(ReuseBag bag) throws IgniteCheckedException {
        assert reuseList == this: "not allowed to be a reuse list";

        put(bag, 0, 0, 0L, REUSE_BUCKET);
    }

    /** {@inheritDoc} */
    @Override public long takeRecycledPage() throws IgniteCheckedException {
        assert reuseList == this: "not allowed to be a reuse list";

        return takeEmptyPage(REUSE_BUCKET, null);
    }

    /** {@inheritDoc} */
    @Override public long recycledPagesCount() throws IgniteCheckedException {
        assert reuseList == this: "not allowed to be a reuse list";

        return storedPagesCount(REUSE_BUCKET);
    }

    /**
     * @param row Row.
     * @return Entry size on page.
     * @throws IgniteCheckedException If failed.
     */
    private static int getRowSize(CacheDataRow row) throws IgniteCheckedException {
        int keyLen = row.key().valueBytesLength(null);
        int valLen = row.value().valueBytesLength(null);

        return keyLen + valLen + CacheVersionIO.size(row.version(), false) + 8 + (row.cacheId() == 0 ? 0 : 4);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "FreeList [name=" + name + ']';
    }
}
