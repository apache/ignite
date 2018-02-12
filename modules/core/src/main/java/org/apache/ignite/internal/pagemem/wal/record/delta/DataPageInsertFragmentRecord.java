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
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALReferenceAwareRecord;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Insert fragment (part of big object which is bigger than page size) to data page record.
 */
public class DataPageInsertFragmentRecord extends PageDeltaRecord implements WALReferenceAwareRecord {
    /** Link to the last entry fragment. */
    private final long lastLink;

    /** Fragment payload offset relatively to whole record payload. */
    private int offset;

    /** WAL reference to {@link DataRecord}. */
    private WALPointer reference;

    /** Actual fragment data. */
    @GridToStringExclude
    @Nullable private byte[] payload;

    /** Row associated with the current data fragment. */
    @Nullable private Storable row;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param offset Fragment data offset.
     * @param lastLink Link to the last entry fragment.
     * @param reference WAL reference to {@link DataRecord}.
     */
    public DataPageInsertFragmentRecord(
        int grpId,
        long pageId,
        int offset,
        long lastLink,
        WALPointer reference
    ) {
        super(grpId, pageId);

        this.lastLink = lastLink;
        this.offset = offset;
        this.reference = reference;
    }

    /**
     * Old constructor for backward compatibility.
     *
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param lastLink Link to the last entry fragment.
     * @param payload Fragment payload.
     */
    public DataPageInsertFragmentRecord(
            int grpId,
            long pageId,
            long lastLink,
            byte[] payload
    ) {
        super(grpId, pageId);

        this.lastLink = lastLink;
        this.payload = payload;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        assert payload != null || row != null : "Both explicit payload and row are null";

        AbstractDataPageIO<Storable> io = PageIO.getPageIO(pageAddr);

        if (payload != null) {
            io.addRowFragment(pageAddr, payload, lastLink, pageMem.pageSize());
        }
        else {
            io.addRowFragment(pageMem, pageAddr, row, offset, io.getRowSize(row), pageMem.pageSize());
        }
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.DATA_PAGE_INSERT_FRAGMENT_RECORD;
    }

    /**
     * @return Actual payload of data.
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

    /**
     * @return Fragment payload offset relatively to whole record payload.
     */
    public int offset() {
        return offset;
    }

    /** {@inheritDoc} */
    @Override public void row(Storable row) {
        this.row = row;
    }

    /** {@inheritDoc} */
    @Override public WALPointer reference() {
        return reference;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataPageInsertFragmentRecord.class, this,
                "offset", offset,
                "super", super.toString());
    }
}
