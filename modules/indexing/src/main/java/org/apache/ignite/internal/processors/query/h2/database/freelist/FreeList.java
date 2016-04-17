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

package org.apache.ignite.internal.processors.query.h2.database.freelist;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
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
    private final ConcurrentHashMap8<Integer,GridFutureAdapter<FreeTree>> trees = new ConcurrentHashMap8<>();

    /** */
    private final PageHandler<GridH2Row> writeRow = new PageHandler<GridH2Row>() {
        @Override public int run(Page page, ByteBuffer buf, GridH2Row row, int entrySize)
            throws IgniteCheckedException {
            DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

            int idx = io.addRow(cctx.cacheObjectContext(), buf, row.key, row.val, row.ver, entrySize);

            assert idx >= 0;

            return io.getFreeSpace(buf);
        }
    };

    /**
     * @param cctx Cache context.
     */
    public FreeList(GridCacheContext<?,?> cctx) {
        assert cctx != null;

        this.cctx = cctx;

        pageMem = cctx.shared().database().pageMemory();

        assert pageMem != null;
    }

    /**
     * @param tree Tree.
     * @param neededSpace Needed free space.
     * @return Free item or {@code null} if it was impossible to find one.
     * @throws IgniteCheckedException If failed.
     */
    private FreeItem take(FreeTree tree, short neededSpace) throws IgniteCheckedException {
        assert neededSpace > 0 && neededSpace < Short.MAX_VALUE: neededSpace;

        FreeItem res = tree.removeCeil(new FreeItem(neededSpace, dispersion(), 0, 0));

        assert res == null || (res.pageId() != 0 && res.cacheId() == cctx.cacheId()): res;

        return res;
    }

    /**
     * @return Random dispersion value.
     */
    private static short dispersion() {
        return (short)ThreadLocalRandom.current().nextInt(Short.MIN_VALUE, Short.MAX_VALUE);
    }

    /**
     * @param part Partition.
     * @return Tree.
     * @throws IgniteCheckedException If failed.
     */
    private FreeTree tree(Integer part) throws IgniteCheckedException {
        assert part >= 0 && part < Short.MAX_VALUE: part;

        GridFutureAdapter<FreeTree> fut = trees.get(part);

        if (fut == null) {
            fut = new GridFutureAdapter<>();

            if (trees.putIfAbsent(part, fut) != null)
                fut = trees.get(part);
            else {
                // Index name will be the same across restarts.
                String idxName = part + "$$" + cctx.cacheId() + "_free";

                IgniteBiTuple<FullPageId,Boolean> t = cctx.shared().database().meta()
                    .getOrAllocateForIndex(cctx.cacheId(), idxName);

                fut.onDone(new FreeTree(pageMem, cctx.cacheId(), part, t.get1(), t.get2()));
            }
        }

        return fut.get();
    }

    /**
     * @param row Row.
     * @throws IgniteCheckedException If failed.
     */
    public void writeRowData(GridH2Row row) throws IgniteCheckedException {
        assert row.link == 0;

        int entrySize = DataPageIO.getEntrySize(cctx.cacheObjectContext(), row.key, row.val);

        assert entrySize > 0 && entrySize < Short.MAX_VALUE: entrySize;

        FreeTree tree = tree(row.partId);
        FreeItem item = take(tree, (short)entrySize);

        Page page = null;
        int freeSpace = -1;

        try {
            if (item == null) {
                DataPageIO io = DataPageIO.VERSIONS.latest();

                page = allocatePage(row.partId);

                ByteBuffer buf = page.getForInitialWrite();

                io.initNewPage(buf, page.id());

                freeSpace = writeRow.run(page, buf, row, entrySize);
            }
            else {
                page = pageMem.page(item);

                freeSpace = writePage(page, writeRow, row, entrySize, -1);
            }
        }
        finally {
            if (page != null) {
                page.close();

                if (freeSpace != -1) { // Put back to the tree.
                    assert freeSpace >= 0 && freeSpace < Short.MAX_VALUE: freeSpace;

                    if (item == null)
                        item = new FreeItem((short)freeSpace, dispersion(), page.id(), cctx.cacheId());
                    else {
                        item.freeSpace((short)freeSpace);
                        item.dispersion(dispersion());
                    }

                    FreeItem old = tree.put(item);

                    assert old == null;
                }
            }
        }
    }

    /**
     * @param part Partition.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    private Page allocatePage(int part) throws IgniteCheckedException {
        FullPageId pageId = pageMem.allocatePage(cctx.cacheId(), part, PageIdAllocator.FLAG_DATA);

        return pageMem.page(pageId);
    }
}
