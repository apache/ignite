/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Remove.
 */
public class RemoveRecord extends PageDeltaRecord {
    /** */
    private int idx;

    /** */
    private int cnt;

    /**
     * @param grpId Cache group ID.
     * @param pageId  Page ID.
     * @param idx Index.
     * @param cnt Count.
     */
    public RemoveRecord(int grpId, long pageId, int idx, int cnt) {
        super(grpId, pageId);

        this.idx = idx;
        this.cnt = cnt;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        BPlusIO<?> io = PageIO.getBPlusIO(pageAddr);

        if (io.getCount(pageAddr) != cnt)
            throw new DeltaApplicationException("Count is wrong [expCnt=" + cnt + ", actual=" + io.getCount(pageAddr) + ']');

        io.remove(pageAddr, idx, cnt);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_PAGE_REMOVE;
    }

    /**
     * @return Index.
     */
    public int index() {
        return idx;
    }

    /**
     * @return Count.
     */
    public int count() {
        return cnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RemoveRecord.class, this, "super", super.toString());
    }
}
