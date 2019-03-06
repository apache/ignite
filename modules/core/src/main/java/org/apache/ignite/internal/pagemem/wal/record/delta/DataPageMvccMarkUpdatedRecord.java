/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * MVCC mark updated record.
 */
public class DataPageMvccMarkUpdatedRecord extends PageDeltaRecord {
    /** */
    private int itemId;

    /** */
    private long newMvccCrd;

    /** */
    private long newMvccCntr;

    /** */
    private int newMvccOpCntr;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param itemId Item id.
     * @param newMvccCrd New MVCC coordinator version.
     * @param newMvccCntr New MVCC counter version.
     * @param newMvccOpCntr New MVCC operation counter.
     */
    public DataPageMvccMarkUpdatedRecord(int grpId, long pageId, int itemId, long newMvccCrd, long newMvccCntr, int newMvccOpCntr) {
        super(grpId, pageId);

        this.itemId = itemId;
        this.newMvccCrd = newMvccCrd;
        this.newMvccCntr = newMvccCntr;
        this.newMvccOpCntr = newMvccOpCntr;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        DataPageIO io = PageIO.getPageIO(pageAddr);

        io.updateNewVersion(pageAddr, itemId, pageMem.realPageSize(groupId()), newMvccCrd, newMvccCntr, newMvccOpCntr);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.MVCC_DATA_PAGE_MARK_UPDATED_RECORD;
    }

    /**
     * @return Item id.
     */
    public int itemId() {
        return itemId;
    }

    /**
     * @return New MVCC coordinator version.
     */
    public long newMvccCrd() {
        return newMvccCrd;
    }

    /**
     * @return New MVCC counter version.
     */
    public long newMvccCntr() {
        return newMvccCntr;
    }

    /**
     * @return New MVCC operation counter.
     */
    public int newMvccOpCntr() {
        return newMvccOpCntr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataPageMvccMarkUpdatedRecord.class, this, "super", super.toString());
    }
}
