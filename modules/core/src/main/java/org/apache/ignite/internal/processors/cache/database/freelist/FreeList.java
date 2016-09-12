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
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.ignite.internal.processors.cache.database.RootPage;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.writePage;

/**
 * Free data page list.
 */
public class FreeList {
    /** */
    private static final Integer COMPLETE = Integer.MAX_VALUE;

    /** */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final PageMemory pageMem;

    /** */
    private final ReuseList reuseList;

    /** */
    private final IgniteWriteAheadLogManager wal;

    /** */
    private final ConcurrentHashMap8<Integer, GridFutureAdapter<FreeTree>> trees = new ConcurrentHashMap8<>();

    /** */
    private final AtomicLong globalRmvId;

    /** */
    private final PageHandler<CacheDataRow, Integer> writeRow = new PageHandler<CacheDataRow, Integer>() {
        @Override public Integer run(long pageId, Page page, ByteBuffer buf, CacheDataRow row, int written)
            throws IgniteCheckedException {
            DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

            CacheObjectContext coctx = cctx.cacheObjectContext();

            int rowSize = getRowSize(coctx, row);
            int freeSpace = io.getFreeSpace(buf);

            assert freeSpace > 0: freeSpace;

            // If the full row does not fit into this page write only a fragment.
            written = freeSpace >= rowSize ? addRow(coctx, page, buf, io, row, rowSize):
                addRowFragment(coctx, page, buf, io, row, written, rowSize);

            // Reread free space after update.
            freeSpace = io.getFreeSpace(buf);

            // Put our free item back to tree.
            tree(row.partition()).put(new FreeItem(freeSpace, pageId));

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
            if (isWalDeltaRecordNeeded(wal, page)) {
                wal.log(new DataPageInsertRecord(cctx.cacheId(),
                    page.id(),
                    row.key(),
                    row.value(),
                    row.version(),
                    row.expireTime(),
                    rowSize));
            }

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

                wal.log(new DataPageInsertFragmentRecord(cctx.cacheId(), page.id(), payload, lastLink));
            }

            return written + payloadSize;
        }
    };

    /** */
    private final PageHandler<FreeTree, Long> removeRow = new PageHandler<FreeTree, Long>() {
        @Override public Long run(long pageId, Page page, ByteBuffer buf, FreeTree tree, int itemId) throws IgniteCheckedException {
            assert tree != null;

            DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

            int oldFreeSpace = io.getFreeSpace(buf);

            assert oldFreeSpace >= 0: oldFreeSpace;

            long nextLink = io.removeRow(buf, (byte)itemId);

            if (isWalDeltaRecordNeeded(wal, page))
                wal.log(new DataPageRemoveRecord(cctx.cacheId(), page.id(), itemId));

            int newFreeSpace = io.getFreeSpace(buf);

            // Move page to the new position with respect to the new free space.
            FreeItem item = tree.remove(new FreeItem(oldFreeSpace, pageId));

            // If item is null, then it was removed concurrently by insertRow, because
            // in removeRow we own the write lock on this page. Thus we can be sure that
            // insertRow will update position correctly after us.
            if (item != null) {
                FreeItem old = tree.put(new FreeItem(newFreeSpace, pageId));

                assert old == null;
            }

            // For common case boxed 0L will be cached inside of Long, so no garbage will be produced.
            return nextLink;
        }
    };

    /**
     * @param reuseList Reuse list.
     * @param cctx Cache context.
     */
    public FreeList(GridCacheContext<?, ?> cctx, ReuseList reuseList, AtomicLong globalRmvId) {
        assert cctx != null;

        this.cctx = cctx;

        wal = cctx.shared().wal();

        pageMem = cctx.shared().database().pageMemory();

        assert pageMem != null;

        this.reuseList = reuseList;
        this.globalRmvId = globalRmvId;
    }

    /**
     * @param tree Tree.
     * @param lookupItem Lookup item.
     * @return Free item or {@code null} if it was impossible to find one.
     * @throws IgniteCheckedException If failed.
     */
    private FreeItem take(FreeTree tree, FreeItem lookupItem) throws IgniteCheckedException {
        FreeItem res = tree.removeCeil(lookupItem, null);

        assert res == null || res.pageId() != 0: res;

        return res;
    }

    /**
     * @param partId Partition.
     * @return Tree.
     * @throws IgniteCheckedException If failed.
     */
    private FreeTree tree(Integer partId) throws IgniteCheckedException {
        assert partId >= 0 && partId < Short.MAX_VALUE : partId;

        GridFutureAdapter<FreeTree> fut = trees.get(partId);

        if (fut == null) {
            fut = new GridFutureAdapter<>();

            if (trees.putIfAbsent(partId, fut) != null)
                fut = trees.get(partId);
            else {
                // Index name will be the same across restarts.
                String idxName = BPlusTree.treeName("p" + partId, "Free");

                final RootPage rootPage = cctx.offheap().meta().getOrAllocateForTree(idxName);

                fut.onDone(new FreeTree(idxName, reuseList, cctx.cacheId(), partId, pageMem, wal, globalRmvId,
                    rootPage.pageId().pageId(), rootPage.isAllocated()));
            }
        }

        return fut.get();
    }

    /**
     * @param link Row link.
     * @throws IgniteCheckedException If failed.
     */
    public void removeRow(long link) throws IgniteCheckedException {
        assert link != 0;

        long pageId = PageIdUtils.pageId(link);
        int partId = PageIdUtils.partId(pageId);
        int itemId = PageIdUtils.itemId(link);

        FreeTree tree = tree(partId);

        long nextLink;

        try (Page page = pageMem.page(cctx.cacheId(), pageId)) {
            nextLink = writePage(pageId, page, removeRow, tree, itemId);
        }

        while (nextLink != 0) {
            itemId = PageIdUtils.itemId(nextLink);
            pageId = PageIdUtils.pageId(nextLink);

            try (Page page = pageMem.page(cctx.cacheId(), pageId)) {
                nextLink = writePage(pageId, page, removeRow, tree, itemId);
            }
        }
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

        return keyLen + valLen + CacheVersionIO.size(row.version(), false) + 8;
    }

    /**
     * @param row Row.
     * @throws IgniteCheckedException If failed.
     */
    public void insertRow(CacheDataRow row) throws IgniteCheckedException {
        assert row.link() == 0 : row.link();

        final int rowSize = getRowSize(cctx.cacheObjectContext(), row);

        // In case of a big entry we just ask for a page which is at least 90% empty,
        // because we have to support multiple data page versions at once and we do not know
        // in advance size of page header or max possible free space on data page.
        // We assume this 90% heuristic to be safe: even for small page size like 512,
        // it leaves 51 byte for a header, which must be enough.
        // We will have 1024 as min configurable page size.
        final int pageFreeSpace = (int) (0.9f * pageMem.pageSize());

        FreeTree tree = tree(row.partition());

        int written = 0;

        do {
            // TODO When the new version of FreeList will be ready, just ask for an empty page.
            int freeSpace = Math.min(rowSize - written, pageFreeSpace);

            FreeItem item = take(tree, new FreeItem(freeSpace, 0));

            try (Page page = item == null ?
                allocateDataPage(row.partition()) :
                pageMem.page(cctx.cacheId(), item.pageId())
            ) {
                // If it is an existing page, we do not need to initialize it.
                DataPageIO init = item == null ? DataPageIO.VERSIONS.latest() : null;

                written = writePage(page.id(), page, writeRow, init, wal, row, written);
            }
        }
        while (written != COMPLETE);
    }

    /**
     * @param collector List to add pages.
     * @throws IgniteCheckedException If failed.
     */
    public void pages(GridLongList collector) throws IgniteCheckedException {
        for (GridFutureAdapter<FreeTree> fut : trees.values()) {
            FreeTree tree = fut.get();

            GridCursor<FreeItem> cursor = tree.find(null, null);

            while (cursor.next())
                collector.add(cursor.get().pageId());
        }
    }

    /**
     * Destroys this FreeList.
     * @throws IgniteCheckedException If failed.
     */
    public void destroy() throws IgniteCheckedException {
        for (GridFutureAdapter<FreeTree> fut : trees.values()) {
            FreeTree tree = fut.get();

            tree.destroy();
        }
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
}
