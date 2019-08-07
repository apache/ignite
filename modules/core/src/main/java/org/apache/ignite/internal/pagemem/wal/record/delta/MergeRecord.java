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

package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Merge on remove.
 */
public class MergeRecord<L> extends PageDeltaRecord {
    /** */
    @GridToStringExclude
    private long prntId;

    /** */
    @GridToStringExclude
    private long rightId;

    /** */
    private int prntIdx;

    /** */
    private boolean emptyBranch;

    /**
     * @param grpId Cache group ID.
     * @param pageId  Page ID.
     * @param prntId Parent ID.
     * @param prntIdx Index in parent page.
     * @param rightId Right ID.
     * @param emptyBranch We are merging empty branch.
     */
    public MergeRecord(int grpId, long pageId, long prntId, int prntIdx, long rightId, boolean emptyBranch) {
        super(grpId, pageId);

        this.prntId = prntId;
        this.rightId = rightId;
        this.prntIdx = prntIdx;
        this.emptyBranch = emptyBranch;

        throw new IgniteException("Merge record should not be used directly (see GG-11640). " +
            "Clear the database directory and restart the node.");
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        throw new IgniteCheckedException("Merge record should not be logged.");
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_PAGE_MERGE;
    }

    public long parentId() {
        return prntId;
    }

    public int parentIndex() {
        return prntIdx;
    }

    public long rightId() {
        return rightId;
    }

    public boolean isEmptyBranch() {
        return emptyBranch;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MergeRecord.class, this, "prntId", U.hexLong(prntId), "rightId", U.hexLong(rightId),
            "super", super.toString());
    }
}
