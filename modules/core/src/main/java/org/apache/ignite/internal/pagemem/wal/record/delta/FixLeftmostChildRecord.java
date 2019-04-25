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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Fix leftmost child.
 */
public class FixLeftmostChildRecord extends PageDeltaRecord {
    /** */
    private long rightId;

    /**
     * @param grpId Cache group ID.
     * @param pageId  Page ID.
     * @param rightId Right ID.
     */
    public FixLeftmostChildRecord(int grpId, long pageId, long rightId) {
        super(grpId, pageId);

        this.rightId = rightId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        BPlusInnerIO<?> io = PageIO.getBPlusIO(pageAddr);

        io.setLeft(pageAddr, 0, rightId);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_FIX_LEFTMOST_CHILD;
    }

    /**
     *
     */
    public long rightId() {
        return rightId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FixLeftmostChildRecord.class, this, "super", super.toString());
    }
}
