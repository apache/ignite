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
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Split existing page.
 */
public class SplitExistingPageRecord extends PageDeltaRecord {
    /** */
    private int mid;

    /** */
    @GridToStringExclude
    private long fwdId;

    /**
     * @param grpId Cache group ID.
     * @param pageId  Page ID.
     * @param mid Bisection index.
     * @param fwdId New forward page ID.
     */
    public SplitExistingPageRecord(int grpId, long pageId, int mid, long fwdId) {
        super(grpId, pageId);

        this.mid = mid;
        this.fwdId = fwdId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        BPlusIO<?> io = PageIO.getBPlusIO(pageAddr);

        io.splitExistingPage(pageAddr, mid, fwdId);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_EXISTING_PAGE_SPLIT;
    }

    public int middleIndex() {
        return mid;
    }

    public long forwardId() {
        return fwdId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SplitExistingPageRecord.class, this, "fwId", U.hexLong(fwdId), "super", super.toString());
    }
}
