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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertFragmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageRemoveRecord;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.writePage;

/**
 */
public final class FreeListNew extends PagesList implements FreeList, ReuseList {
    /** */
    private static final int BUCKETS = 256; // Must be power of 2.

    /** */
    private static final int REUSE_BUCKET = BUCKETS - 1; // TODO or 0?

    /** */
    private static final Integer COMPLETE = Integer.MAX_VALUE;

    /** */
    private final int shift;

    /** */
    private final AtomicReferenceArray<long[]> buckets = new AtomicReferenceArray<>(BUCKETS);

    /** */
    private final int MIN_SIZE_FOR_BUCKET;

    /**
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param reuseList Reuse list or {@code null} if this free list will be a reuse list for itself.
     * @param wal Write ahead log manager.
     * @throws IgniteCheckedException If failed.
     */
    public FreeListNew(
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

        assert U.isPow2(pageSize) : pageSize;
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
     * @return Bucket.
     */
    private int bucket(int freeSpace) {
        assert freeSpace > 0 : freeSpace;

        int bucket = freeSpace >>> shift;

        if (bucket == BUCKETS - 1)
            bucket--;

        assert bucket >= 0 && bucket < BUCKETS : bucket;

        return bucket;
    }

    /**
     * @param part Partition.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    private Page allocateDataPage(int part) throws IgniteCheckedException {
        long pageId = pageMem.allocatePage(cacheId, part, PageIdAllocator.FLAG_DATA);

        return pageMem.page(cacheId, pageId);
    }

    /** {@inheritDoc} */
    @Override public void insertDataRow(CacheDataRow row) throws IgniteCheckedException {
        int rowSize = getRowSize(null, row);

        int written = 0;

        do {
            int freeSpace = Math.min(MIN_SIZE_FOR_BUCKET, rowSize - written);

            int bucket = bucket(freeSpace);

            long pageId = 0;
            boolean newPage = true;

            for (int i = bucket; i < BUCKETS - 1; i++) {
                pageId = takeEmptyPage(i, DataPageIO.VERSIONS);

                if (pageId != 0L) {
                    newPage = false;

                    break;
                }
            }

            try (Page page = newPage ? allocateDataPage(row.partition()) : pageMem.page(cacheId, pageId)) {
                // If it is an existing page, we do not need to initialize it.
                DataPageIO init = newPage ? DataPageIO.VERSIONS.latest() : null;

                written = writePage(page.id(), page, writeRow, init, wal, row, written);
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
            nextLink = writePage(pageId, page, rmvRow, null, itemId);
        }

        while (nextLink != 0) {
            itemId = PageIdUtils.itemId(nextLink);
            pageId = PageIdUtils.pageId(nextLink);

            try (Page page = pageMem.page(cacheId, pageId)) {
                nextLink = writePage(pageId, page, rmvRow, null, itemId);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected long[] getBucket(int bucket) {
        return buckets.get(bucket);
    }

    /** {@inheritDoc} */
    @Override protected boolean casBucket(int bucket, long[] exp, long[] upd) {
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
     * @param coctx Cache object context.
     * @param row Row.
     * @return Entry size on page.
     * @throws IgniteCheckedException If failed.
     */
    private static int getRowSize(CacheObjectContext coctx, CacheDataRow row) throws IgniteCheckedException {
        int keyLen = row.key().valueBytesLength(coctx);
        int valLen = row.value().valueBytesLength(coctx);

        return keyLen + valLen + CacheVersionIO.size(row.version(), false) + 8;
    }

    /** */
    private final PageHandler<CacheDataRow, DataPageIO, Integer> writeRow =
        new PageHandler<CacheDataRow, DataPageIO, Integer>() {
            @Override public Integer run(long pageId, Page page, DataPageIO io, ByteBuffer buf, CacheDataRow row, int written)
                throws IgniteCheckedException {
                CacheObjectContext coctx = null;

                int rowSize = getRowSize(coctx, row);
                int oldFreeSpace = io.getFreeSpace(buf);

                assert oldFreeSpace > 0 : oldFreeSpace;

                // If the full row does not fit into this page write only a fragment.
                written = (written == 0 && oldFreeSpace >= rowSize) ? addRow(coctx, page, buf, io, row, rowSize):
                    addRowFragment(coctx, page, buf, io, row, written, rowSize);

                // Reread free space after update.
                int newFreeSpace = io.getFreeSpace(buf);

                if (newFreeSpace > 0) {
                    int bucket = bucket(newFreeSpace);

                    put(null, page, buf, bucket);
                }

                // Avoid boxing with garbage generation for usual case.
                return written == rowSize ? COMPLETE : written;
            }

            /**
             * @param coctx Cache object context.
             * @param page Page.
             * @param buf Buffer.
             * @param io IO.
             * @param row Row.
             * @param rowSize Row size.
             * @return Written size which is always equal to row size here.
             * @throws IgniteCheckedException If failed.
             */
            private int addRow(
                CacheObjectContext coctx,
                Page page,
                ByteBuffer buf,
                DataPageIO io,
                CacheDataRow row,
                int rowSize
            ) throws IgniteCheckedException {
                io.addRow(coctx, buf, row, rowSize);

                // TODO This record must contain only a reference to a logical WAL record with the actual data.
                if (isWalDeltaRecordNeeded(wal, page))
                    wal.log(new DataPageInsertRecord(cacheId, page.id(),
                            row.key(), row.value(), row.version(), row.expireTime(), rowSize));

                return rowSize;
            }

            /**
             * @param coctx Cache object context.
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
                CacheObjectContext coctx,
                Page page,
                ByteBuffer buf,
                DataPageIO io,
                CacheDataRow row,
                int written,
                int rowSize
            ) throws IgniteCheckedException {
                // Read last link before the fragment write, because it will be updated there.
                long lastLink = row.link();

                int payloadSize = io.addRowFragment(coctx, buf, row, written, rowSize);

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
    private final PageHandler<Void, DataPageIO, Long> rmvRow = new PageHandler<Void, DataPageIO, Long>() {
        @Override public Long run(long pageId, Page page, DataPageIO io, ByteBuffer buf, Void arg, int itemId)
                throws IgniteCheckedException {
            int oldFreeSpace = io.getFreeSpace(buf);

            assert oldFreeSpace >= 0: oldFreeSpace;

            long nextLink = io.removeRow(buf, (byte)itemId);

            if (isWalDeltaRecordNeeded(wal, page))
                wal.log(new DataPageRemoveRecord(cacheId, page.id(), itemId));

            int newFreeSpace = io.getFreeSpace(buf);

            if (newFreeSpace > 0) {
                int newBucket = bucket(newFreeSpace);

                if (oldFreeSpace > 0) {
                    int oldBucket = bucket(oldFreeSpace);

                    if (oldBucket != newBucket) {
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return "FreeList [name=" + name + ']';
    }
}
