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

package org.apache.ignite.internal.processors.cache.tree;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public class PendingEntriesTree extends BPlusTree<PendingRow, PendingRow> {
    /** */
    public static final Object WITHOUT_KEY = new Object();

    /** */
    private final CacheGroupContext grp;

    /**
     * @param grp Cache group.
     * @param name Tree name.
     * @param pageMem Page memory.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param initNew Initialize new index.
     * @throws IgniteCheckedException If failed.
     */
    public PendingEntriesTree(
        CacheGroupContext grp,
        String name,
        PageMemory pageMem,
        long metaPageId,
        ReuseList reuseList,
        boolean initNew)
        throws IgniteCheckedException {
        super(name,
            grp.groupId(),
            pageMem,
            grp.dataRegion().config().isPersistenceEnabled() ? grp.shared().wal() : null,
            grp.offheap().globalRemoveId(),
            metaPageId,
            reuseList,
            grp.sharedGroup() ? CacheIdAwarePendingEntryInnerIO.VERSIONS : PendingEntryInnerIO.VERSIONS,
            grp.sharedGroup() ? CacheIdAwarePendingEntryLeafIO.VERSIONS : PendingEntryLeafIO.VERSIONS,
            grp.shared().kernalContext().failure());

        this.grp = grp;

        assert !grp.dataRegion().config().isPersistenceEnabled()  || grp.shared().database().checkpointLockIsHeldByThread();

        initTree(initNew);
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<PendingRow> iox, long pageAddr, int idx, PendingRow row) {
        PendingRowIO io = (PendingRowIO)iox;

        int cmp;

        if (grp.sharedGroup()) {
            assert row.cacheId != CU.UNDEFINED_CACHE_ID : "Cache ID is not provided!";
            assert io.getCacheId(pageAddr, idx) != CU.UNDEFINED_CACHE_ID : "Cache ID is not stored!";

            cmp = Integer.compare(io.getCacheId(pageAddr, idx), row.cacheId);

            if (cmp != 0)
                return cmp;

            if (row.expireTime == 0 && row.link == 0) {
                // A search row with a cach ID only is used as a cache bound.
                // The found position will be shifted until the exact cache bound is found;
                // See for details:
                // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findLowerBound()
                // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findUpperBound()
                return cmp;
            }
        }

        long expireTime = io.getExpireTime(pageAddr, idx);

        cmp = Long.compare(expireTime, row.expireTime);

        if (cmp != 0)
            return cmp;

        if (row.link == 0L)
            return 0;

        long link = io.getLink(pageAddr, idx);

        return Long.compare(link, row.link);
    }

    /** {@inheritDoc} */
    @Override protected PendingRow getRow(BPlusIO<PendingRow> io, long pageAddr, int idx, Object flag)
        throws IgniteCheckedException {
        PendingRow row = io.getLookupRow(this, pageAddr, idx);

        return flag == WITHOUT_KEY ? row : row.initKey(grp);
    }
}
