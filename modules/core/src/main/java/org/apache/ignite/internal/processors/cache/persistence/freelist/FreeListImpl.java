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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

import lib.llpl.Heap;
import lib.llpl.MemoryBlock;
import lib.llpl.Transactional;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertFragmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageRemoveRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageUpdateRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.evict.PageEvictionTracker;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.T2;
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
    private final DataRegionMetricsImpl memMetrics;

    /** */
    private final PageEvictionTracker evictionTracker;

    /** Holds FreeListImpl buckets. Key: data region id, Value: buckets. */
    public static final ConcurrentHashMap<Integer, AtomicReferenceArray<Stripe[]>> bucketsMap = new ConcurrentHashMap<>();

    private static PersistentLinkedList<Transactional, Long> plist;

    static {

        if (Ignition.isAepEnabled()) {

            plist = Ignition.UNSAFE.getPersistentList();

            assert plist != null;

            int size = plist.size();

            // We skip index 0 (the schema registry region).
            for (int i = 1; i < size; i++) {
                Long base = plist.get(i);
                assert base != null;

                MemoryBlock<Transactional> block = Ignition.getAepHeap().memoryBlockFromAddress(Transactional.class, base);

                assert block != null;

                if (block.getInt(block.size() - 2 * Integer.BYTES) == AepUnsafe.BlockType.BUCKET.ordinal()) {
                    PagesList.Stripe[] stripes;
                    int id = block.getInt(block.size() - Integer.BYTES);
                    bucketsMap.put(id, new AtomicReferenceArray<>(BUCKETS));

                    for (int j = 0; j < BUCKETS; j++) {
                        stripes = getPersistedStripes(id, j);
                        AtomicReferenceArray<Stripe[]> ara = bucketsMap.get(id);

                        assert ara != null;

                        ara.set(j, stripes);
                    }
                }
            }
        }
    }

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

            int rowSize = getRowSize(row, row.cacheId() != 0);

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

            int rowSize = getRowSize(row, row.cacheId() != 0);
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
    private final PageHandler<Void, Long> rmvRow;

    /**
     *
     */
    private final class RemoveRowHandler extends PageHandler<Void, Long> {
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

            // Ignition.print("OldFreeSpace = " + oldFreeSpace + ", " + newFreeSpace);

            if (newFreeSpace > MIN_PAGE_FREE_SPACE) {
                int newBucket = bucket(newFreeSpace, false);

                if (oldFreeSpace > MIN_PAGE_FREE_SPACE) {
                    int oldBucket = bucket(oldFreeSpace, false);

                    if (oldBucket != newBucket) {
                        // It is possible that page was concurrently taken for put, in this case put will handle bucket change.
                        pageId = maskPartId ? PageIdUtils.maskPartitionId(pageId) : pageId;
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
     * @param memPlc Data region.
     * @param reuseList Reuse list or {@code null} if this free list will be a reuse list for itself.
     * @param wal Write ahead log manager.
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new metadata should be initialized.
     * @throws IgniteCheckedException If failed.
     */
    public FreeListImpl(
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

        if (Ignition.isAepEnabled())
            addBuckets(memPlc.config().getName(), buckets);
    }

    /**
     * Calculates average fill factor over FreeListImpl instance.
     *
     * @return Tuple (numenator, denominator).
     */
    public T2<Long, Long> fillFactor() {
        long pageSize = pageSize();

        long totalSize = 0;
        long loadSize = 0;

        for (int b = BUCKETS - 2; b > 0; b--) {
            long bsize = pageSize - ((REUSE_BUCKET - b) << shift);

            long pages = bucketsSize[b].longValue();

            loadSize += pages * (pageSize - bsize);

            totalSize += pages * pageSize;
        }

        return totalSize == 0 ? new T2<>(0L, 0L) : new T2<>(loadSize, totalSize);
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
    @Override public void insertDataRow(CacheDataRow row) throws IgniteCheckedException {
        int rowSize = getRowSize(row, row.cacheId() != 0);

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
            else
                pageId = PageIdUtils.changePartitionId(pageId, (row.partition()));

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
        Stripe[] stripes = buckets.get(bucket);

        if (Ignition.isAepEnabled() && stripes == null)
            stripes = getStripes(pageMem.getDataRegionName(), bucket);

        return stripes;
    }

    /** {@inheritDoc} */
    @Override protected boolean casBucket(int bucket, Stripe[] exp, Stripe[] upd) {
        boolean updated = buckets.compareAndSet(bucket, exp, upd);

        if (Ignition.isAepEnabled() && updated)
            updateStripes(pageMem.getDataRegionName(), bucket, upd);

        return updated;
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
     * @param withCacheId If {@code true} adds cache ID size.
     * @return Entry size on page.
     * @throws IgniteCheckedException If failed.
     */
    public static int getRowSize(CacheDataRow row, boolean withCacheId) throws IgniteCheckedException {
        KeyCacheObject key = row.key();
        CacheObject val = row.value();

        int keyLen = key.valueBytesLength(null);
        int valLen = val.valueBytesLength(null);

        return keyLen + valLen + CacheVersionIO.size(row.version(), false) + 8 + (withCacheId ? 4 : 0);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "FreeList [name=" + name + ']';
    }


    private static void addBuckets(String regionName, AtomicReferenceArray<PagesList.Stripe[]> buckets) {
        int id = regionName.hashCode();
        if (!bucketsMap.containsKey(id))
            bucketsMap.put(id, buckets);
    }

    private static PagesList.Stripe[] getStripes(String regionName, int bucket) {
        for (Map.Entry<Integer, AtomicReferenceArray<PagesList.Stripe[]>> e : bucketsMap.entrySet()) {
            if (e.getKey() == regionName.hashCode())
                return e.getValue().get(bucket);
        }
        return null;
    }

    private void updateStripes(String regionName, int bucket, PagesList.Stripe[] upd) {
        for (Map.Entry<Integer, AtomicReferenceArray<PagesList.Stripe[]>> e : bucketsMap.entrySet()) {
            if (e.getKey() == regionName.hashCode()) {
                e.getValue().set(bucket, upd);
                break;
            }
        }
    }

    private static MemoryBlock<Transactional> getPersistedStripesBlock(int id, int bucket) {
        Heap heap = Ignition.getAepHeap();
        MemoryBlock<Transactional> block = null;

        // We skip index 0 (the schema registry region).
        int i = 1;
        int size = plist.size();
        for (; i < size; i++) {
            Long address = plist.get(i);
            if (address == null)
                continue;

            block = heap.memoryBlockFromAddress(Transactional.class, address);
            if (block == null)
                continue;

            if (block.getInt(block.size() - Integer.BYTES) == id &&
                    block.getInt(block.size() - 2 * Integer.BYTES) == bucket &&
                    block.getInt(block.size() - 3 * Integer.BYTES) == AepUnsafe.BlockType.BUCKET.ordinal())
                break;
        }

        if (i == size)
            return null;

        return block;
    }

    @SuppressWarnings("unchecked")
    private static PagesList.Stripe[] getPersistedStripes(int id, int bucket) {
        MemoryBlock<?> block = getPersistedStripesBlock(id, bucket);
        if (block == null)
            return null;

        byte[] stripes = new byte[(int) block.size() - 3 * Integer.BYTES];
        for (int j = 0; j < stripes.length; j++)
            stripes[j] = block.getByte(j);

        return (PagesList.Stripe[]) SerializationUtils.deserialize(stripes);
    }

    @SuppressWarnings("unchecked")
    public static void persistStripes(int id, int bucket, PagesList.Stripe[] upd) {
        if (upd == null)
            return;

        byte[] arr = SerializationUtils.serialize(upd);
        int len = arr.length;

        MemoryBlock<Transactional> block;
        PagesList.Stripe[] stripes = getPersistedStripes(id, bucket);

        if (stripes == null) {
            block = Ignition.getAepHeap().allocateMemoryBlock(Transactional.class, len + 3 * Integer.BYTES);
            block.setInt(len, AepUnsafe.BlockType.BUCKET.ordinal());
            block.setInt(len + Integer.BYTES, bucket);
            block.setInt(len + 2 * Integer.BYTES, id);
        }
        else
            block = getPersistedStripesBlock(id, bucket);

        assert block != null;

        block.copyFromArray(arr, 0, 0, len);

        if (stripes == null)
            plist.add(block.address());
    }

}
