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
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.io.ReuseInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.io.ReuseLeafIO;

/**
 * Reuse tree for index pages.
 */
public final class ReuseTree extends BPlusTree<Number, Long> {
    /**
     * @param reuseList Reuse list.
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param metaPageId Meta page ID.
     * @param initNew Initialize new index.
     * @throws IgniteCheckedException If failed.
     */
    public ReuseTree(ReuseList reuseList, int cacheId, PageMemory pageMem, FullPageId metaPageId, boolean initNew)
        throws IgniteCheckedException {
        super(cacheId, pageMem, metaPageId, reuseList, ReuseInnerIO.VERSIONS, ReuseLeafIO.VERSIONS);

        if (initNew)
            initNew();
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<Number> io, ByteBuffer buf, int idx, Number row)
        throws IgniteCheckedException {
        int pageIdx = io.isLeaf() ?
            PageIdUtils.pageIndex(((ReuseLeafIO)io).getPageId(buf, idx)) :
            (((ReuseInnerIO)io).getPageIndex(buf, idx));

        return Integer.compare(pageIdx, toPageIndex(row));
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
    @Override protected Long getRow(BPlusIO<Number> io, ByteBuffer buf, int idx)
        throws IgniteCheckedException {
        assert io.isLeaf();

        return ((ReuseLeafIO)io).getPageId(buf, idx);
    }
}
