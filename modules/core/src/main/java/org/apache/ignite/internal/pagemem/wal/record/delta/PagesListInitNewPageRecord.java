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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class PagesListInitNewPageRecord extends InitNewPageRecord {
    /** */
    @GridToStringExclude
    private final long prevPageId;

    /** */
    @GridToStringExclude
    private final long addDataPageId;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param ioType IO type.
     * @param ioVer IO version.
     * @param newPageId New page ID.
     * @param prevPageId Previous page ID.
     * @param addDataPageId Optional page ID to add.
     */
    public PagesListInitNewPageRecord(
        int grpId,
        long pageId,
        int ioType,
        int ioVer,
        long newPageId,
        long prevPageId,
        long addDataPageId
    ) {
        this(grpId, pageId, ioType, ioVer, newPageId, prevPageId, addDataPageId, null);
    }

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param ioType IO type.
     * @param ioVer IO version.
     * @param newPageId New page ID.
     * @param prevPageId Previous page ID.
     * @param addDataPageId Optional page ID to add.
     * @param log Logger for case data is invalid. Can be {@code null}, but is needed when processing existing storage.
     */
    public PagesListInitNewPageRecord(
        int grpId,
        long pageId,
        int ioType,
        int ioVer,
        long newPageId,
        long prevPageId,
        long addDataPageId,
        @Nullable IgniteLogger log
    ) {
        super(grpId, pageId, ioType, ioVer, newPageId, log);

        this.prevPageId = prevPageId;
        this.addDataPageId = addDataPageId;
    }

    /**
     * @return Previous page ID.
     */
    public long previousPageId() {
        return prevPageId;
    }

    /**
     * @return Page ID to add.
     */
    public long dataPageId() {
        return addDataPageId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        PagesListNodeIO io = PageIO.getPageIO(PageIO.T_PAGE_LIST_NODE, ioVer);

        io.initNewPage(pageAddr, pageId(), pageMem.realPageSize(groupId()));
        io.setPreviousId(pageAddr, prevPageId);

        if (addDataPageId != 0L) {
            int cnt = io.addPage(pageAddr, addDataPageId, pageMem.realPageSize(groupId()));

            assert cnt == 0 : cnt;
        }
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PAGES_LIST_INIT_NEW_PAGE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PagesListInitNewPageRecord.class, this,
            "prevPageId", U.hexLong(prevPageId),
            "addDataPageId", U.hexLong(addDataPageId),
            "super", super.toString()
        );
    }
}
