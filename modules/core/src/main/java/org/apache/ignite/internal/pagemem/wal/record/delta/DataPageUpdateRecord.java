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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Update existing record in data page.
 */
public class DataPageUpdateRecord extends PageDeltaRecord {
    /** */
    private int itemId;

    /** */
    private byte[] payload;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param itemId Item ID.
     * @param payload Record data.
     */
    public DataPageUpdateRecord(
        int grpId,
        long pageId,
        int itemId,
        byte[] payload
    ) {
        super(grpId, pageId);

        this.payload = payload;
        this.itemId = itemId;
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

        AbstractDataPageIO io = PageIO.getPageIO(pageAddr);

        io.updateRow(pageAddr, itemId, pageMem.realPageSize(groupId()), payload, null, 0);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.DATA_PAGE_UPDATE_RECORD;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataPageUpdateRecord.class, this, "super", super.toString());
    }
}
