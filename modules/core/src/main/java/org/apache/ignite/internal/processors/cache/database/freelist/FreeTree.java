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
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.freelist.io.FreeIO;
import org.apache.ignite.internal.processors.cache.database.freelist.io.FreeInnerIO;
import org.apache.ignite.internal.processors.cache.database.freelist.io.FreeLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;

/**
 * Data structure for data pages and their free spaces.
 */
public class FreeTree extends BPlusTree<FreeItem, FreeItem> {
    /** */
    private PageMemory pageMem;

    /** */
    private int cacheId;

    /** */
    private int part;

    /**
     * @param pageMem Page memory.
     * @param cacheId Cache ID.
     * @param part Partition.
     * @param metaPageId Meta page ID.
     * @param initNew    Initialize new index.
     * @throws IgniteCheckedException If failed.
     */
    public FreeTree(PageMemory pageMem, int cacheId, int part, FullPageId metaPageId, boolean initNew)
        throws IgniteCheckedException {
        super(metaPageId);

        assert pageMem != null;

        this.pageMem = pageMem;
        this.cacheId = cacheId;
        this.part = part;

        if (initNew)
            initNew();
    }

    /** {@inheritDoc} */
    @Override protected BPlusIO<FreeItem> io(int type, int ver) {
        if (type == PageIO.T_FREE_INNER)
            return FreeInnerIO.VERSIONS.forVersion(ver);

        assert type == PageIO.T_FREE_LEAF: type;

        return FreeLeafIO.VERSIONS.forVersion(ver);
    }

    /** {@inheritDoc} */
    @Override protected BPlusInnerIO<FreeItem> latestInnerIO() {
        return FreeInnerIO.VERSIONS.latest();
    }

    /** {@inheritDoc} */
    @Override protected BPlusLeafIO<FreeItem> latestLeafIO() {
        return FreeLeafIO.VERSIONS.latest();
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<FreeItem> io, ByteBuffer buf, int idx, FreeItem row)
        throws IgniteCheckedException {
        return Integer.compare(((FreeIO)io).dispersedFreeSpace(buf, idx), row.dispersedFreeSpace());
    }

    /** {@inheritDoc} */
    @Override protected FreeItem getRow(BPlusIO<FreeItem> io, ByteBuffer buf, int idx) throws IgniteCheckedException {
        assert io.isLeaf();

        FreeItem row = io.getLookupRow(this, buf, idx);

        assert row.pageId() != 0;
        assert row.cacheId() == cacheId;

        return row;
    }

    /** {@inheritDoc} */
    @Override protected Page page(long pageId) throws IgniteCheckedException {
        return pageMem.page(new FullPageId(pageId, cacheId));
    }

    /** {@inheritDoc} */
    @Override protected Page allocatePage() throws IgniteCheckedException {
        FullPageId pageId = pageMem.allocatePage(cacheId, part, PageIdAllocator.FLAG_IDX);

        return pageMem.page(pageId);
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }
}
