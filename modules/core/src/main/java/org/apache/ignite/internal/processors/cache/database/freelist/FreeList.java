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
import org.apache.ignite.internal.processors.cache.database.CacheEntryFragmentContext;
import org.apache.ignite.internal.processors.cache.database.RootPage;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.writePage;

/**
 * Free data page list.
 */
public class FreeList {
    /** */
    private final GridCacheContext<?,?> cctx;

    /** */
    private final PageMemory pageMem;

    /** */
    private final ReuseList reuseList;

    /** */
    private final IgniteWriteAheadLogManager wal;

    /** */
    private final ConcurrentHashMap8<Integer,GridFutureAdapter<FreeTree>> trees = new ConcurrentHashMap8<>();

    /** */
    private final PageHandler<CacheDataRow, Void> writeRow = new PageHandler<CacheDataRow, Void>() {
        @Override public Void run(final long pageId, final Page page, final ByteBuffer buf, final CacheDataRow row,
            final int entrySize) throws IgniteCheckedException {
            DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

            int idx = io.addRow(cctx.cacheObjectContext(), buf, row, entrySize);

            assert idx >= 0 : idx;

            FreeTree tree = tree(row.partition());

            if (tree.needWalDeltaRecord(page))
                wal.log(new DataPageInsertRecord(cctx.cacheId(), page.id(), row.key(), row.value(),
                    row.version(), idx, entrySize));

            row.link(PageIdUtils.linkFromDwordOffset(pageId, idx));

            putInTree(pageId, buf, row, io);

            return null;
        }
    };

    /** */
    private final PageHandler<CacheEntryFragmentContext, Void> writeFragmentRow = new PageHandler<CacheEntryFragmentContext, Void>() {
        @Override public Void run(long pageId, Page page, ByteBuffer buf, CacheEntryFragmentContext fctx, int entrySize)
            throws IgniteCheckedException {
            assert fctx.chunks() > 1;

            final CacheDataRow row = fctx.dataRow();

            DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

            final int initWritten = fctx.written();

            fctx.pageBuffer(buf);

            io.addRowFragment(fctx);

            assert fctx.lastIndex() >= 0 : fctx.lastIndex();

            FreeTree tree = tree(row.partition());

            if (tree.needWalDeltaRecord(page)) {
                if (fctx.lastFragment()) {
                    // Write entry tail in WAL.
                    final byte[] frData = new byte[fctx.written() - initWritten];

                    buf.position(fctx.pageDataOffset());

                    buf.get(frData);

                    wal.log(new DataPageInsertFragmentRecord(
                        cctx.cacheId(),
                        pageId,
                        fctx.written(),
                        fctx.lastLink(),
                        frData));

                    buf.position(0);
                }
                else
                    // Just mark page to store in WAL, because all fragments
                    // except last one fill page fully.
                    page.forceFullPageWalRecord(true);
            }

            fctx.lastLink(PageIdUtils.linkFromDwordOffset(pageId, fctx.lastIndex()));

            putInTree(pageId, buf, row, io);

            row.link(fctx.lastLink());

            return null;
        }
    };

    /**
     * Return page in tree with updated free space value.
     *
     * @param pageId Page ID.
     * @param buf Page buffer.
     * @param row Cache data row.
     * @param io Data page IO.
     * @throws IgniteCheckedException
     */
    private void putInTree(final long pageId, final ByteBuffer buf, final CacheDataRow row,
        final DataPageIO io) throws IgniteCheckedException {
        final int freeSlots = io.getFreeItemSlots(buf);

        // If no free slots available then assume that page is full
        int freeSpace = freeSlots == 0 ? 0 : io.getFreeSpace(buf);

        // Put our free item.
        tree(row.partition()).put(new FreeItem(freeSpace, pageId, cctx.cacheId()));
    }

    /** */
    private final PageHandler<FreeTree, Long> removeRow = new PageHandler<FreeTree, Long>() {
        @Override public Long run(long pageId, Page page, ByteBuffer buf, FreeTree tree, int itemId) throws IgniteCheckedException {
            assert tree != null;

            DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

            assert DataPageIO.checkIndex(itemId): itemId;

            final int dataOff = io.getDataOffset(buf, itemId);

            final long nextLink = DataPageIO.getNextFragmentLink(buf, dataOff);

            int oldFreeSpace = io.getFreeSpace(buf);

            io.removeRow(buf, (byte)itemId);

            if (tree.needWalDeltaRecord(page))
                wal.log(new DataPageRemoveRecord(cctx.cacheId(), page.id(), itemId));

            int newFreeSpace = io.getFreeSpace(buf);

            // Move page to the new position with respect to the new free space.
            FreeItem item = tree.remove(new FreeItem(oldFreeSpace, pageId, cctx.cacheId()));

            // If item is null, then it was removed concurrently by insertRow, because
            // in removeRow we own the write lock on this page. Thus we can be sure that
            // insertRow will update position correctly after us.
            if (item != null) {
                FreeItem old = tree.put(new FreeItem(newFreeSpace, pageId, cctx.cacheId()));

                assert old == null;
            }

            return nextLink;
        }
    };

    /**
     * @param reuseList Reuse list.
     * @param cctx Cache context.
     */
    public FreeList(GridCacheContext<?,?> cctx, ReuseList reuseList) {
        assert cctx != null;

        this.cctx = cctx;

        wal = cctx.shared().wal();

        pageMem = cctx.shared().database().pageMemory();

        assert pageMem != null;

        this.reuseList = reuseList;
    }

    /**
     * @param tree Tree.
     * @param lookupItem Lookup item.
     * @return Free item or {@code null} if it was impossible to find one.
     * @throws IgniteCheckedException If failed.
     */
    private FreeItem take(FreeTree tree, FreeItem lookupItem) throws IgniteCheckedException {
        FreeItem res = tree.removeCeil(lookupItem, null);

        assert res == null || (res.pageId() != 0 && res.cacheId() == cctx.cacheId()): res;

        return res;
    }

    /**
     * @param partId Partition.
     * @return Tree.
     * @throws IgniteCheckedException If failed.
     */
    private FreeTree tree(Integer partId) throws IgniteCheckedException {
        assert partId >= 0 && partId < Short.MAX_VALUE: partId;

        GridFutureAdapter<FreeTree> fut = trees.get(partId);

        if (fut == null) {
            fut = new GridFutureAdapter<>();

            if (trees.putIfAbsent(partId, fut) != null)
                fut = trees.get(partId);
            else {
                // Index name will be the same across restarts.
                String idxName = BPlusTree.treeName("p" + partId, cctx.cacheId(), "Free");

                final RootPage rootPage = cctx.shared().database().meta()
                    .getOrAllocateForTree(cctx.cacheId(), idxName);

                fut.onDone(new FreeTree(idxName, reuseList, cctx.cacheId(), partId, pageMem, wal,
                    rootPage.pageId(), rootPage.isAllocated()));
            }
        }

        return fut.get();
    }

    /**
     * @param link Row link.
     * @throws IgniteCheckedException
     */
    public void removeRow(long link) throws IgniteCheckedException {
        assert link != 0;

        long pageId = PageIdUtils.pageId(link);
        int partId = PageIdUtils.partId(pageId);
        int itemId = PageIdUtils.dwordsOffset(link);

        FreeTree tree = tree(partId);

        long nextLink;

        try (Page page = pageMem.page(cctx.cacheId(), pageId)) {
            nextLink = writePage(pageId, page, removeRow, tree, itemId);
        }

        while (nextLink != 0) {
            itemId = PageIdUtils.dwordsOffset(nextLink);
            pageId = PageIdUtils.pageId(nextLink);

            try (Page page = pageMem.page(cctx.cacheId(), pageId)) {
                nextLink = writePage(pageId, page, removeRow, tree, itemId);
            }
        }
    }

    /**
     * @param row Row.
     * @throws IgniteCheckedException If failed.
     */
    public void insertRow(CacheDataRow row) throws IgniteCheckedException {
        assert row.link() == 0: row.link();

        final CacheObjectContext coctx = cctx.cacheObjectContext();

        final int entrySize = DataPageIO.getEntrySize(coctx, row.key(), row.value());

        assert entrySize > 0 : entrySize;

        final int availablePageSize = DataPageIO.getAvailablePageSize(coctx);

        FreeTree tree = tree(row.partition());

        // write fragmented entry
        final int chunks = DataPageIO.getChunksNum(availablePageSize, entrySize);

        assert chunks > 0;

        FragmentContext fctx = null;

        if (chunks > 1)
            fctx = new FragmentContext(entrySize, chunks, availablePageSize, row);

        for (int i = 0; i < chunks; i++) {
            int free = entrySize;

            if (fctx != null)
                free = i == 0 ? fctx.totalEntrySize() - fctx.chunkSize() * (fctx.chunks() - 1) : availablePageSize;

            FreeItem item = take(tree,
                new FreeItem(free, 0, cctx.cacheId()));

            try (Page page = item == null ?
                allocateDataPage(row.partition()) :
                pageMem.page(item.cacheId(), item.pageId())
            ) {
                if (item == null) {
                    DataPageIO io = DataPageIO.VERSIONS.latest();

                    ByteBuffer buf = page.getForWrite(); // Initial write.

                    try {
                        io.initNewPage(buf, page.id());

                        // It is a newly allocated page and we will not write record to WAL here.
                        assert !page.isDirty();

                        writeNewPage(page, buf, fctx, row, entrySize);
                    }
                    finally {
                        page.releaseWrite(true);
                    }
                }
                else
                    writeExistedPage(page, fctx, row, entrySize);
            }
        }
    }

    /**
     * @param page Data page.
     * @param fctx Fragment context.
     * @param row Cache data row.
     * @param entrySize Entry size.
     * @throws IgniteCheckedException
     */
    private void writeExistedPage(
        final Page page,
        final @Nullable CacheEntryFragmentContext fctx,
        final CacheDataRow row,
        final int entrySize
    ) throws IgniteCheckedException {
        if (fctx != null)
            writePage(page.id(), page, writeFragmentRow, fctx, entrySize);
        else
            writePage(page.id(), page, writeRow, row, entrySize);
    }

    /**
     * @param page Data page.
     * @param buf Data page buffer.
     * @param fctx Fragment context.
     * @param row Cache data row.
     * @param entrySize Entry size.
     * @throws IgniteCheckedException
     */
    private void writeNewPage(
        final Page page,
        final ByteBuffer buf,
        final @Nullable CacheEntryFragmentContext fctx,
        final CacheDataRow row,
        final int entrySize
    ) throws IgniteCheckedException {
        if (fctx != null)
            writeFragmentRow.run(page.id(), page, buf, fctx, entrySize);
        else
            writeRow.run(page.id(), page, buf, row, entrySize);
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
