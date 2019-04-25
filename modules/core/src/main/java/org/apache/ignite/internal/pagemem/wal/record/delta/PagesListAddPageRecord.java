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
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class PagesListAddPageRecord extends PageDeltaRecord {
    /** */
    @GridToStringExclude
    private final long dataPageId;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param dataPageId Data page ID to add.
     */
    public PagesListAddPageRecord(int grpId, long pageId, long dataPageId) {
        super(grpId, pageId);

        this.dataPageId = dataPageId;
    }

    /**
     * @return Data page ID.
     */
    public long dataPageId() {
        return dataPageId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

        int cnt = io.addPage(pageAddr, dataPageId, pageMem.realPageSize(groupId()));

        assert cnt >= 0 : cnt;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PAGES_LIST_ADD_PAGE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PagesListAddPageRecord.class, this,
            "dataPageId", U.hexLong(dataPageId),
            "super", super.toString());
    }
}
