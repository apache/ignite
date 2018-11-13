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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Insert fragment (part of big object which is bigger than page size) to data page record.
 */
public class DataPageInsertFragmentRecord extends PageDeltaRecord {
    /** Link to the last entry fragment. */
    private final long lastLink;

    /** Actual fragment data. */
    @GridToStringExclude
    private final byte[] payload;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param payload Fragment payload.
     * @param lastLink Link to the last entry fragment.
     */
    public DataPageInsertFragmentRecord(
        final int grpId,
        final long pageId,
        final byte[] payload,
        final long lastLink
    ) {
        super(grpId, pageId);

        this.lastLink = lastLink;
        this.payload = payload;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        AbstractDataPageIO io = PageIO.getPageIO(pageAddr);

        io.addRowFragment(PageIO.getPageId(pageAddr), pageAddr, payload, lastLink, pageMem.realPageSize(groupId()));
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.DATA_PAGE_INSERT_FRAGMENT_RECORD;
    }

    /**
     * @return Fragment payload size.
     */
    public int payloadSize() {
        return payload.length;
    }

    /**
     * @return Fragment payload.
     */
    public byte[] payload() {
        return payload;
    }

    /**
     * @return Link to the last entry fragment.
     */
    public long lastLink() {
        return lastLink;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataPageInsertFragmentRecord.class, this,
            "payloadSize", payload.length,
            "super", super.toString()
        );
    }
}
