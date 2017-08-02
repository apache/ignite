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
import org.apache.ignite.internal.pagemem.wal.record.WALReferenceAwareRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;

/**
 * Insert fragment to data page record.
 */
public class DataPageInsertFragmentRecord extends PageDeltaRecord implements WALReferenceAwareRecord {
    /** Link to the last entry fragment. */
    private final long lastLink;

    /** Actual fragment data size. */
    private final int payloadSize;

    /** WAL reference to DataRecord. */
    private WALPointer reference;

    /** Actual fragment data. */
    private byte[] payload;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param payloadSize Fragment payload.
     * @param lastLink Link to the last entry fragment.
     */
    public DataPageInsertFragmentRecord(
        int grpId,
        long pageId,
        int payloadSize,
        long lastLink,
        WALPointer reference
    ) {
        super(grpId, pageId);

        this.lastLink = lastLink;
        this.payloadSize = payloadSize;
        this.reference = reference;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        DataPageIO io = DataPageIO.VERSIONS.forPage(pageAddr);

        io.addRowFragment(pageAddr, payload, lastLink, pageMem.pageSize());
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.DATA_PAGE_INSERT_FRAGMENT_RECORD;
    }

    /**
     * @return Link to the last entry fragment.
     */
    public long lastLink() {
        return lastLink;
    }

    /** {@inheritDoc} */
    @Override public int payloadSize() {
        return payloadSize;
    }

    /** {@inheritDoc} */
    @Override public void payload(byte[] payload) {
        this.payload = payload;
    }

    /** {@inheritDoc} */
    @Override public WALPointer reference() {
        return reference;
    }
}
