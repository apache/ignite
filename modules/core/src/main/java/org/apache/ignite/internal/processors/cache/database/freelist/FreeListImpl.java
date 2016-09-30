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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertFragmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageRemoveRecord;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.writePage;

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
    private final int MIN_SIZE_FOR_BUCKET;

    /** */
    private final PageHandler<CacheDataRow, Integer> writeRow =
        new PageHandler<CacheDataRow, Integer>() {
            @Override public Integer run(Page page, PageIO iox, ByteBuffer buf, CacheDataRow row, int written)
                throws IgniteCheckedException {
                DataPageIO io = (DataPageIO)iox;

                int rowSize = getRowSize(row);
                int oldFreeSpace = io.getFreeSpace(buf);

                assert oldFreeSpace > 0 : oldFreeSpace;

                // If the full row does not fit into this page write only a fragment.
                written = (written == 0 && oldFreeSpace >= rowSize) ? addRow(page, buf, io, row, rowSize):
                    addRowFragment(page, buf, io, row, written, rowSize);

                // Reread free space after update.
                int newFreeSpace = io.getFreeSpace(buf);

                if (newFreeSpace > MIN_PAGE_FREE_SPACE) {
                    int bucket = bucket(newFreeSpace, false);

                    put(null, page, buf, bucket);
                }

                // Avoid boxing with garbage generation for usual case.
                return written == rowSize ? COMPLETE : written;
            }

            /**
             * @param page Page.
             * @param buf Buffer.
             * @param io IO.
             * @param row Row.
             * @param rowSize Row size.
             * @return Written size which is always equal to row size here.
             * @throws IgniteCheckedException If failed.
             */
            private int addRow(
                Page page,
                ByteBuffer buf,
                DataPageIO io,
                CacheDataRow row,
                int rowSize
            ) throws IgniteCheckedException {
                // TODO: context parameter.
                io.addRow(buf, row, rowSize);

                if (isWalDeltaRecordNeeded(wal, page)) {
                    // TODO This record must contain only a reference to a logical WAL record with the actual data.
                    byte[] payload = new byte[rowSize];

                    io.setPositionAndLimitOnPayload(buf, PageIdUtils.itemId(row.link()));

                    assert buf.remaining() == rowSize;

                    buf.get(payload);
                    buf.position(0);

                    wal.log(new DataPageInsertRecord(
                        cacheId,
                        page.id(),
                        payload));
                }

                return rowSize;
            }

            /**
             * @param page Page.
             * @param buf Buffer.
             * @param io IO.
             * @param row Row.
             * @param written Written size.
             * @param rowSize Row size.
             * @return Updated written size.
             * @throws IgniteCheckedException If failed.
             */
            private int addRowFragment(
                Page page,
                ByteBuffer buf,
                DataPageIO io,
                CacheDataRow row,
                int written,
                int rowSize
            ) throws IgniteCheckedException {
                // Read last link before the fragment write, because it will be updated there.
                long lastLink = row.link();

                int payloadSize = io.addRowFragment(buf, row, written, rowSize);

                assert payloadSize > 0: payloadSize;

                if (isWalDeltaRecordNeeded(wal, page)) {
                    // TODO This record must contain only a reference to a logical WAL record with the actual data.
                    byte[] payload = new byte[payloadSize];

                    io.setPositionAndLimitOnPayload(buf, PageIdUtils.itemId(row.link()));
                    buf.get(payload);
                    buf.position(0);

                    wal.log(new DataPageInsertFragmentRecord(cacheId, page.id(), payload, lastLink));
                }

                return written + payloadSize;
            }
        };

    /** */
    private final PageHandler<Void, Long> rmvRow = new PageHandler<Void, Long>() {
        @Override public Long run(Page page, PageIO iox, ByteBuffer buf, Void arg, int itemId)
            throws IgniteCheckedException {
            DataPageIO io = (DataPageIO)iox;

            int oldFreeSpace = io.getFreeSpace(buf);

            assert oldFreeSpace >= 0: oldFreeSpace;

            long nextLink = io.removeRow(buf, itemId);

            if (isWalDeltaRecordNeeded(wal, page))
                wal.log(new DataPageRemoveRecord(cacheId, page.id(), itemId));

            // TODO: properly handle reuse bucket.
//            if (io.isEmpty(buf)) {
//                int oldBucket = oldFreeSpace > MIN_PAGE_FREE_SPACE ? bucket(oldFreeSpace, false) : -1;
//
//                if (oldBucket == -1 || removeDataPage(page, buf, io, oldBucket))
//                    put(null, page, buf, REUSE_BUCKET);
//            }

            int newFreeSpace = io.getFreeSpace(buf);

            if (newFreeSpace > MIN_PAGE_FREE_SPACE) {
                int newBucket = bucket(newFreeSpace, false);

                if (oldFreeSpace > MIN_PAGE_FREE_SPACE) {
                    int oldBucket = bucket(oldFreeSpace, false);

                    if (oldBucket != newBucket) {
                        // It is possible that page was concurrently taken for put, in this case put will handle bucket change.
                        if (removeDataPage(page, buf, io, oldBucket))
                            put(null, page, buf, newBucket);
                    }
                }
                else
                    put(null, page, buf, newBucket);
            }

            // For common case boxed 0L will be cached inside of Long, so no garbage will be produced.
            return nextLink;
        }
    };

    /**
     * @param cacheId Cache ID.
     * @param name Name (for debug purpose).
     * @param pageMem Page memory.
     * @param reuseList Reuse list or {@code null} if this free list will be a reuse list for itself.
     * @param wal Write ahead log manager.
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new metadata should be initialized.
     * @throws IgniteCheckedException If failed.
     */
    public FreeListImpl(
        int cacheId,
        String name,
        PageMemory pageMem,
        ReuseList reuseList,
        IgniteWriteAheadLogManager wal,
        long metaPageId,
        boolean initNew) throws IgniteCheckedException {
        super(cacheId, name, pageMem, BUCKETS, wal, metaPageId);
        this.reuseList = reuseList == null ? this : reuseList;
        int pageSize = pageMem.pageSize();

        assert U.isPow2(pageSize) : "Page size must be a power of 2: " + pageSize;
        assert U.isPow2(BUCKETS);
        assert BUCKETS <= pageSize : pageSize;

        MIN_SIZE_FOR_BUCKET = pageSize - 1;

        int shift = 0;

        while (pageSize > BUCKETS) {
            shift++;
            pageSize >>>= 1;
        }

        this.shift = shift;

        init(metaPageId, initNew);
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
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    private Page allocateDataPage(int part) throws IgniteCheckedException {
        assert part <= PageIdAllocator.MAX_PARTITION_ID;
        assert part != PageIdAllocator.INDEX_PARTITION;

        long pageId = pageMem.allocatePage(cacheId, part, PageIdAllocator.FLAG_DATA);

        return pageMem.page(cacheId, pageId);
    }

    /** {@inheritDoc} */
    @Override public void insertDataRow(CacheDataRow row) throws IgniteCheckedException {
        int rowSize = getRowSize(row);

        int written = 0;

        do {
            int freeSpace = Math.min(MIN_SIZE_FOR_BUCKET, rowSize - written);

            int bucket = bucket(freeSpace, false);

            long pageId = 0;
            boolean reuseBucket = false;

            // TODO: properly handle reuse bucket.
            for (int b = bucket; b < BUCKETS - 1; b++) {
                pageId = takeEmptyPage(b, DataPageIO.VERSIONS);

                if (pageId != 0L) {
                    reuseBucket = isReuseBucket(b);

                    break;
                }
            }

            try (Page page = pageId == 0 ? allocateDataPage(row.partition()) : pageMem.page(cacheId, pageId)) {
                // If it is an existing page, we do not need to initialize it.
                DataPageIO init = reuseBucket || pageId == 0L ? DataPageIO.VERSIONS.latest() : null;

                written = writePage(page, this, writeRow, init, wal, row, written, FAIL_I);

                assert written != FAIL_I; // We can't fail here.
            }
        }
        while (written != COMPLETE);
    }

    /** {@inheritDoc} */
    @Override public void removeDataRowByLink(long link) throws IgniteCheckedException {
        assert link != 0;

        long pageId = PageIdUtils.pageId(link);
        int itemId = PageIdUtils.itemId(link);

        long nextLink;

        try (Page page = pageMem.page(cacheId, pageId)) {
            nextLink = writePage(page, this, rmvRow, null, itemId, FAIL_L);

            assert nextLink != FAIL_L; // Can't fail here.
        }

        while (nextLink != 0L) {
            itemId = PageIdUtils.itemId(nextLink);
            pageId = PageIdUtils.pageId(nextLink);

            try (Page page = pageMem.page(cacheId, pageId)) {
                nextLink = writePage(page, this, rmvRow, null, itemId, FAIL_L);

                assert nextLink != FAIL_L; // Can't fail here.
            }
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

    /** {@inheritDoc} */
    @Override public void addForRecycle(ReuseBag bag) throws IgniteCheckedException {
        assert reuseList == this: "not allowed to be a reuse list";

        put(bag, null, null, REUSE_BUCKET);
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

        return keyLen + valLen + CacheVersionIO.size(row.version(), false) + 8;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "FreeList [name=" + name + ']';
    }
}
