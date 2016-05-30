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
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.freelist.io.FreeIO;
import org.apache.ignite.internal.processors.cache.database.freelist.io.FreeInnerIO;
import org.apache.ignite.internal.processors.cache.database.freelist.io.FreeLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;

/**
 * Data structure for data pages and their free spaces.
 */
public class FreeTree extends BPlusTree<FreeItem, FreeItem> {
    /** */
    private final int partId;

    /**
     * @param name Tree name.
     * @param reuseList Reuse list.
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param metaPageId Meta page ID.
     * @param initNew    Initialize new index.
     * @throws IgniteCheckedException If failed.
     */
    public FreeTree(String name, ReuseList reuseList, int cacheId, int partId, PageMemory pageMem, FullPageId metaPageId, boolean initNew)
        throws IgniteCheckedException {
        super(name, cacheId, pageMem, metaPageId, reuseList, FreeInnerIO.VERSIONS, FreeLeafIO.VERSIONS);

        this.partId = partId;

        assert pageMem != null;

        if (initNew)
            initNew();
    }

    /**
     * @return Partition ID.
     */
    public int getPartId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<FreeItem> io, ByteBuffer buf, int idx, FreeItem row)
        throws IgniteCheckedException {
        int res = Short.compare(((FreeIO)io).getFreeSpace(buf, idx), row.freeSpace());

        if (res == 0)
            res = Integer.compare(((FreeIO)io).getPageIndex(buf, idx), PageIdUtils.pageIndex(row.pageId()));

        return res;
    }

    /** {@inheritDoc} */
    @Override protected FreeItem getRow(BPlusIO<FreeItem> io, ByteBuffer buf, int idx) throws IgniteCheckedException {
        assert io.isLeaf();

        FreeItem row = io.getLookupRow(this, buf, idx);

        assert row.pageId() != 0;
        assert row.cacheId() == getCacheId();

        return row;
    }
}
