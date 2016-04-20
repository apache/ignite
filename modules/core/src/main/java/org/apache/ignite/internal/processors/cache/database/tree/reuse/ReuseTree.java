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
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.io.ReuseInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.io.ReuseLeafIO;

/**
 * Reuse tree for index pages.
 */
public class ReuseTree extends BPlusTree<FullPageId, FullPageId> {
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
        super(cacheId, pageMem, metaPageId, reuseList);

        if (initNew)
            initNew();
    }

    /** {@inheritDoc} */
    @Override protected BPlusIO<FullPageId> io(int type, int ver) {
        if (type == PageIO.T_REUSE_INNER)
            return ReuseInnerIO.VERSIONS.forVersion(ver);

        assert type == PageIO.T_REUSE_LEAF: type;

        return ReuseLeafIO.VERSIONS.forVersion(ver);
    }

    /** {@inheritDoc} */
    @Override protected BPlusInnerIO<FullPageId> latestInnerIO() {
        return ReuseInnerIO.VERSIONS.latest();
    }

    /** {@inheritDoc} */
    @Override protected BPlusLeafIO<FullPageId> latestLeafIO() {
        return ReuseLeafIO.VERSIONS.latest();
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<FullPageId> io, ByteBuffer buf, int idx, FullPageId fullPageId)
        throws IgniteCheckedException {
        long pageIdx = io.isLeaf() ?
            PageIdUtils.pageIdx(((ReuseLeafIO)io).getPageId(buf, idx)) :
            (((ReuseInnerIO)io).getPageIndex(buf, idx) & 0xFFFFFFFFL);

        return Long.compare(pageIdx, PageIdUtils.pageIdx(fullPageId.pageId()));
    }

    /** {@inheritDoc} */
    @Override protected FullPageId getRow(BPlusIO<FullPageId> io, ByteBuffer buf, int idx)
        throws IgniteCheckedException {
        assert io.isLeaf();

        return new FullPageId(((ReuseLeafIO)io).getPageId(buf, idx) , cacheId);
    }
}
