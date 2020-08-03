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
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class PagesListRemovePageRecord extends PageDeltaRecord {
    /** */
    @GridToStringExclude
    private final long rmvdPageId;

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param rmvdPageId Data page ID to remove.
     */
    public PagesListRemovePageRecord(int cacheId, long pageId, long rmvdPageId) {
        super(cacheId, pageId);

        this.rmvdPageId = rmvdPageId;
    }

    /**
     * @return Removed page ID.
     */
    public long removedPageId() {
        return rmvdPageId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

        boolean rmvd = io.removePage(pageAddr, rmvdPageId);

        assert rmvd;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PAGES_LIST_REMOVE_PAGE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PagesListRemovePageRecord.class, this,
            "rmvdPageId", U.hexLong(rmvdPageId),
            "pageId", U.hexLong(pageId()),
            "grpId", groupId(),
            "super", super.toString());
    }
}
