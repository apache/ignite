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

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.database.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

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
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param prevPageId Previous page ID.
     * @param addDataPageId Optional page ID to add.
     */
    public PagesListInitNewPageRecord(
        int cacheId,
        long pageId,
        int ioType,
        int ioVer,
        long newPageId,
        long prevPageId,
        long addDataPageId
    ) {
        super(cacheId, pageId, ioType, ioVer, newPageId);

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
    @Override public void applyDelta(ByteBuffer buf) throws IgniteCheckedException {
        PagesListNodeIO io = PageIO.getPageIO(PageIO.T_PAGE_LIST_NODE, ioVer);

        io.initNewPage(buf, pageId());
        io.setPreviousId(buf, prevPageId);

        if (addDataPageId != 0L) {
            int cnt = io.addPage(buf, addDataPageId);

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
            "addDataPageId", U.hexLong(addDataPageId));
    }
}
