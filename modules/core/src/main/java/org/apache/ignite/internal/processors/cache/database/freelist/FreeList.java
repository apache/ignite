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
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
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
    private final ConcurrentHashMap8<Integer,GridFutureAdapter<FreeTree>> trees = new ConcurrentHashMap8<>();

    /** */
    private final PageHandler<CacheDataRow, Void> writeRow = new PageHandler<CacheDataRow, Void>() {
        @Override public Void run(long pageId, Page page, ByteBuffer buf, CacheDataRow row, int entrySize)
            throws IgniteCheckedException {
            DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

            int idx = io.addRow(cctx.cacheObjectContext(), buf, row.key(), row.value(), row.version(), entrySize);

            assert idx >= 0 : idx;

            row.link(PageIdUtils.linkFromDwordOffset(pageId, idx));

            int freeSpace = io.getFreeSpace(buf);

            // Put our free item.
            tree(row.partition()).put(new FreeItem(freeSpace, pageId, cctx.cacheId()));

            return null;
        }
    };

    /** */
    private final PageHandler<FreeTree, Void> removeRow = new PageHandler<FreeTree, Void>() {
        @Override public Void run(long pageId, Page page, ByteBuffer buf, FreeTree tree, int itemId) throws IgniteCheckedException {
            assert tree != null;

            DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

            assert DataPageIO.check(itemId): itemId;

            int oldFreeSpace = io.getFreeSpace(buf);

            io.removeRow(buf, (byte)itemId);

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

            return null;
        }
    };

    /**
     * @param reuseList Reuse list.
     * @param cctx Cache context.
     */
    public FreeList(GridCacheContext<?,?> cctx, ReuseList reuseList) {
        assert cctx != null;

        this.cctx = cctx;

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

                IgniteBiTuple<FullPageId,Boolean> t = cctx.shared().database().meta()
                    .getOrAllocateForIndex(cctx.cacheId(), idxName);

                fut.onDone(new FreeTree(idxName, reuseList, cctx.cacheId(), partId, pageMem, t.get1(), t.get2()));
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

        try (Page page = pageMem.page(new FullPageId(pageId, cctx.cacheId()))) {
            writePage(pageId, page, removeRow, tree, itemId);
        }
    }

    /**
     * @param row Row.
     * @throws IgniteCheckedException If failed.
     */
    public void insertRow(CacheDataRow row) throws IgniteCheckedException {
        assert row.link() == 0: row.link();

        int entrySize = DataPageIO.getEntrySize(cctx.cacheObjectContext(), row.key(), row.value());

        assert entrySize > 0 && entrySize < Short.MAX_VALUE: entrySize;

        FreeTree tree = tree(row.partition());

        // TODO add random pageIndex here for lower contention?
        FreeItem item = take(tree, new FreeItem(entrySize, 0, cctx.cacheId()));

        try (Page page = item == null ?
            allocateDataPage(row.partition()) :
            pageMem.page(item)
        ) {
            if (item == null) {
                DataPageIO io = DataPageIO.VERSIONS.latest();

                ByteBuffer buf = page.getForInitialWrite();

                io.initNewPage(buf, page.id());

                writeRow.run(page.id(), page, buf, row, entrySize);
            }
            else
                writePage(page.id(), page, writeRow, row, entrySize);
        }
    }

    /**
     * @param part Partition.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    private Page allocateDataPage(int part) throws IgniteCheckedException {
        FullPageId pageId = pageMem.allocatePage(cctx.cacheId(), part, PageIdAllocator.FLAG_DATA);

        return pageMem.page(pageId);
    }
}
