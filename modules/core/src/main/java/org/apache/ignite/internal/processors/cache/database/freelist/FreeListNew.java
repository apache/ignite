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
import java.util.concurrent.atomic.AtomicLongArray;
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
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.DataStructure;
import org.apache.ignite.internal.processors.cache.database.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.getPageId;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.writePage;

/**
 */
public final class FreeListNew extends PagesList implements FreeList, ReuseList {
    /** */
    private static final int BUCKETS = 256; // Must be power of 2.

    /** */
    private static final int REUSE_BUCKET = BUCKETS - 1; // TODO or 0?

    /** */
    private final int shift;

    /** */
    private final AtomicReferenceArray<long[]> buckets = new AtomicReferenceArray<>(BUCKETS);

    /** */
    private final AtomicLongArray bitmap = new AtomicLongArray(BUCKETS / 64);

    /** */
    private final GridCacheContext<?, ?> cctx;

    /**
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param reuseList Reuse list or {@code null} if this free list will be a reuse list for itself.
     * @param wal Write ahead log.
     */
    public FreeListNew(int cacheId, PageMemory pageMem, ReuseList reuseList, GridCacheContext<?, ?> cctx, IgniteWriteAheadLogManager wal) {
        super(cacheId, pageMem, wal);

        this.reuseList = reuseList == null ? this : reuseList;
        this.cctx = cctx;

        int pageSize = pageMem.pageSize();

        assert U.isPow2(pageSize);
        assert U.isPow2(BUCKETS);
        assert BUCKETS <= pageSize;

        int shift = 0;

        while (pageSize > BUCKETS) {
            shift++;
            pageSize >>>= 1;
        }

        this.shift = shift;
    }

    private int bucket(int freeSpace) {
        assert freeSpace > 0: freeSpace;

        return freeSpace >>> shift;
    }
    /**
     * @param part Partition.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    private Page allocateDataPage(int part) throws IgniteCheckedException {
        long pageId = pageMem.allocatePage(cctx.cacheId(), part, PageIdAllocator.FLAG_DATA);

        return pageMem.page(cctx.cacheId(), pageId);
    }

    /** {@inheritDoc} */
    @Override public void insertDataRow(CacheDataRow row) throws IgniteCheckedException {
        int bucket = 1;

        System.out.println("insert, pages count before: " + storedPagesCount(bucket));

        int written = 0;

        do {
            long pageId = takeEmptyPage(bucket);

            boolean newPage = false;

            if (pageId == 0) {
                newPage = true;
            }
            else
                System.out.println("Take page: " + pageId);

            try (Page page = pageId == 0 ? allocateDataPage(row.partition()) : pageMem.page(cacheId, pageId)) {
                // If it is an existing page, we do not need to initialize it.
                DataPageIO init = newPage ? DataPageIO.VERSIONS.latest() : null;

                if (newPage)
                    System.out.println("Allocated page: " + page.id());

                written = writePage(page.id(), page, writeRow, init, wal, row, written);
            }
        }
        while (written != COMPLETE);

        System.out.println("insert, pages count after: " + storedPagesCount(bucket));
    }

    /** */
    private static final Integer COMPLETE = Integer.MAX_VALUE;

    /** {@inheritDoc} */
    @Override public void removeDataRowByLink(long link) throws IgniteCheckedException {
        // TODO port from old impl + use methods `put` and `removeDataPage` instead of trees
        System.out.println("remove, pages count before: " + storedPagesCount(1));

        assert link != 0;

        long pageId = PageIdUtils.pageId(link);
        int itemId = PageIdUtils.itemId(link);

        long nextLink;

        try (Page page = pageMem.page(cctx.cacheId(), pageId)) {
            nextLink = writePage(pageId, page, removeRow, null, itemId);
        }

        while (nextLink != 0) {
            itemId = PageIdUtils.itemId(nextLink);
            pageId = PageIdUtils.pageId(nextLink);

            try (Page page = pageMem.page(cctx.cacheId(), pageId)) {
                nextLink = writePage(pageId, page, removeRow, null, itemId);
            }
        }
        System.out.println("remove, pages count after: " + storedPagesCount(1));
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

        put(bag, null, 0);
    }

    /** {@inheritDoc} */
    @Override public long takeRecycledPage(DataStructure client, ReuseBag bag) throws IgniteCheckedException {
        assert reuseList == this: "not allowed to be a reuse list";

        if (bag.pollFreePage() != 0L) // TODO drop client and bag from the signature
            throw new IllegalStateException();

        return takeEmptyPage(0);
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
    private static int getRowSize(CacheObjectContext coctx, CacheDataRow row)
            throws IgniteCheckedException {
        int keyLen = row.key().valueBytesLength(coctx);
        int valLen = row.value().valueBytesLength(coctx);

        return keyLen + valLen + CacheVersionIO.size(row.version(), false);
    }

    /** */
    private final PageHandler<CacheDataRow, DataPageIO, Integer> writeRow =
            new PageHandler<CacheDataRow, DataPageIO, Integer>() {
                @Override public Integer run(long pageId, Page page, DataPageIO io, ByteBuffer buf, CacheDataRow row, int written)
                        throws IgniteCheckedException {
                    CacheObjectContext coctx = cctx.cacheObjectContext();

                    int rowSize = getRowSize(coctx, row);
                    int freeSpace0 = io.getFreeSpace(buf);

                    assert freeSpace0 > 0: freeSpace0;

                    // If the full row does not fit into this page write only a fragment.
                    written = (written == 0 && freeSpace0 >= rowSize) ? addRow(coctx, page, buf, io, row, rowSize):
                            addRowFragment(coctx, page, buf, io, row, written, rowSize);

                    // Reread free space after update.
                    int freeSpace = io.getFreeSpace(buf);

                    if (freeSpace > 0) {
                        long dataPageId = getPageId(buf);
                        System.out.println("Add data page: " + dataPageId + ", spaceBefore: " + freeSpace0 + ", freeSpace: " + freeSpace + " written  " + (freeSpace0 - freeSpace) + " row: " + rowSize);

                        put(null, buf, 1);
                    }
                    else {
                        long dataPageId = getPageId(buf);
                        System.out.println("No free space after add: " + dataPageId + ", spaceBefore: " + freeSpace0 + ", freeSpace: " + freeSpace + " written " + (freeSpace0 - freeSpace) + " row: " + rowSize);

                        io.setFreeListPageId(buf, 0);
                    }

                    // Put our free item back to tree.
                    // tree(row.partition()).put(new FreeItem(freeSpace, pageId));

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
                        wal.log(new DataPageInsertRecord(cctx.cacheId(), page.id(),
                                row.key(), row.value(), row.version(), rowSize));

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
    private final PageHandler<FreeTree, DataPageIO, Long> removeRow = new PageHandler<FreeTree, DataPageIO, Long>() {
        @Override public Long run(long pageId, Page page, DataPageIO io, ByteBuffer buf, FreeTree tree, int itemId)
                throws IgniteCheckedException {
            int oldFreeSpace = io.getFreeSpace(buf);

            assert oldFreeSpace >= 0: oldFreeSpace;

            long nextLink = io.removeRow(buf, (byte)itemId);

            if (isWalDeltaRecordNeeded(wal, page))
                wal.log(new DataPageRemoveRecord(cacheId, page.id(), itemId));

            int newFreeSpace = io.getFreeSpace(buf);

            if (newFreeSpace > 0) {
                long freeListPageId = io.getFreeListPageId(buf);

                if (freeListPageId == 0) {
                    System.out.println("Add page after remove: " + PageIO.getPageId(buf) + ", freeSpaceBefore: " + oldFreeSpace + ", freeSpace: " + newFreeSpace + " freed " + (newFreeSpace - oldFreeSpace ));

                    put(null, buf, 1);
                }
                else
                    System.out.println("After remove, page already added: " + PageIO.getPageId(buf) + ", freeSpaceBefore: " + oldFreeSpace + ", freeSpace: " + newFreeSpace + " freed " + (newFreeSpace - oldFreeSpace));
            }
            else
                System.out.println("No free space after remove: " + PageIO.getPageId(buf) + ", freeSpace: " + newFreeSpace + " freed " + (newFreeSpace - oldFreeSpace));

//             Move page to the new position with respect to the new free space.
//            FreeItem item = tree.remove(new FreeItem(oldFreeSpace, pageId));
//
//            // If item is null, then it was removed concurrently by insertRow, because
//            // in removeRow we own the write lock on this page. Thus we can be sure that
//            // insertRow will update position correctly after us.
//            if (item != null) {
//                FreeItem old = tree.put(new FreeItem(newFreeSpace, pageId));
//
//                assert old == null;
//            }

            // For common case boxed 0L will be cached inside of Long, so no garbage will be produced.
            return nextLink;
        }
    };
}
