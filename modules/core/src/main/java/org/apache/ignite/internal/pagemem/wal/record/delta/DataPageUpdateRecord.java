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
 * Update existing record in data page.
 */
public class DataPageUpdateRecord extends PageDeltaRecord implements WALReferenceAwareRecord {
    /** */
    private int itemId;

    /** Actual fragment data size. */
    private final int payloadSize;

    /** WAL reference to DataRecord. */
    private WALPointer reference;

    /** Actual fragment data. */
    private byte[] payload;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param itemId Item ID.
     * @param payloadSize Record data size.
     */
    public DataPageUpdateRecord(
        int grpId,
        long pageId,
        int itemId,
        int payloadSize,
        WALPointer reference
    ) {
        super(grpId, pageId);

        this.payloadSize = payloadSize;
        this.itemId = itemId;
        this.reference = reference;
    }

    /**
     * @return Item ID.
     */
    public int itemId() {
        return itemId;
    }

    /**
     * @return Insert record payload.
     */
    public byte[] payload() {
        return payload;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        assert payload != null;

        DataPageIO io = DataPageIO.VERSIONS.forPage(pageAddr);

        io.updateRow(pageAddr, itemId, pageMem.pageSize(), payload, null, 0);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.DATA_PAGE_UPDATE_RECORD;
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
