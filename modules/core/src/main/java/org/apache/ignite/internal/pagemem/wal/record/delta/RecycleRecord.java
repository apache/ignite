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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Recycle index page.
 */
public class RecycleRecord extends PageDeltaRecord {
    /** */
    @GridToStringExclude
    private long newPageId;

    /**
     * @param grpId Cache group ID.
     * @param pageId  Page ID.
     * @param newPageId New page ID.
     */
    public RecycleRecord(int grpId, long pageId, long newPageId) {
        super(grpId, pageId);

        this.newPageId = newPageId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        PageIO.setPageId(pageAddr, newPageId);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_PAGE_RECYCLE;
    }

    /**
     *
     */
    public long newPageId() {
        return newPageId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RecycleRecord.class, this,
            "newPageId", U.hexLong(newPageId),
            "super", super.toString()
        );
    }
}
