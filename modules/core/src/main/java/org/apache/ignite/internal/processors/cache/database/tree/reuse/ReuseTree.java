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

package org.apache.ignite.internal.processors.cache.database.tree.reuse;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.io.ReuseInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.io.ReuseLeafIO;

/**
 * Reuse tree for index pages.
 */
public final class ReuseTree extends BPlusTree<Number, Long> {
    /**
     * @param name Tree name.
     * @param reuseList Reuse list.
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @param metaPageId Meta page ID.
     * @param initNew Initialize new index.
     * @throws IgniteCheckedException If failed.
     */
    public ReuseTree(
        String name,
        ReuseList reuseList,
        int cacheId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        long metaPageId,
        boolean initNew
    ) throws IgniteCheckedException {
        super(name, cacheId, pageMem, wal, metaPageId, reuseList, ReuseInnerIO.VERSIONS, ReuseLeafIO.VERSIONS);

        if (initNew)
            initNew();
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<Number> io, ByteBuffer buf, int idx, Number row) {
        int pageIdx = io.isLeaf() ?
            PageIdUtils.pageIndex(((ReuseLeafIO)io).getPageId(buf, idx)) :
            (((ReuseInnerIO)io).getPageIndex(buf, idx));

        // Compare as unsigned int.
        return Long.compare(pageIdx & 0xFFFFFFFFL, toPageIndex(row) & 0xFFFFFFFFL);
    }

    /** {@inheritDoc} */
    @Override protected long allocatePageForNew() throws IgniteCheckedException {
        // Do not call reuse list when allocating a new ReuseTree.
        return allocatePageNoReuse();
    }

    /**
     * @param row Lookup row.
     * @return Page index.
     */
    private static int toPageIndex(Number row) {
        if (row.getClass() == Integer.class)
            return row.intValue();

        return PageIdUtils.pageIndex(row.longValue());
    }

    /** {@inheritDoc} */
    @Override protected Long getRow(BPlusIO<Number> io, ByteBuffer buf, int idx) {
        assert io.isLeaf();

        return ((ReuseLeafIO)io).getPageId(buf, idx);
    }

    /** {@inheritDoc} */
    @Override public long destroy() throws IgniteCheckedException {
        if (!markDestroyed())
            return 0;

        DestroyBag bag = new DestroyBag();

        long pagesCnt = destroy(bag);

        for (long pageId = bag.pollFreePage(); pageId != 0; pageId = bag.pollFreePage())
            pageMem.freePage(getCacheId(), pageId);

        return pagesCnt;
    }
}
