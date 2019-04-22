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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Initializes new page by calling {@link PageIO#initNewPage(long, long, int)}.
 */
public class InitNewPageRecord extends PageDeltaRecord {
    /** */
    protected int ioType;

    /** */
    protected int ioVer;

    /** */
    @GridToStringExclude
    protected long newPageId;

    /**
     * @param grpId Cache group ID.
     * @param pageId  Page ID.
     * @param ioType IO type.
     * @param ioVer IO version.
     * @param newPageId New page ID.
     */
    public InitNewPageRecord(int grpId, long pageId, int ioType, int ioVer, long newPageId) {
        super(grpId, pageId);

        this.ioType = ioType;
        this.ioVer = ioVer;
        this.newPageId = newPageId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        PageIO io = PageIO.getPageIO(ioType, ioVer);

        io.initNewPage(pageAddr, newPageId, pageMem.realPageSize(groupId()));
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.INIT_NEW_PAGE_RECORD;
    }

    /**
     * @return IO Version.
     */
    public int ioVersion() {
        return ioVer;
    }

    /**
     * @return IO Type.
     */
    public int ioType() {
        return ioType;
    }

    /**
     * @return New page ID.
     */
    public long newPageId() {
        return newPageId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(InitNewPageRecord.class, this,
            "newPageId", U.hexLong(newPageId),
            "super", super.toString());
    }
}

