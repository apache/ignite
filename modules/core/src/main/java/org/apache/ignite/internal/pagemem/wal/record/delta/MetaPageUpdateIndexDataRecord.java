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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageMetaIOV2;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Meta page delta record, includes encryption status data.
 */
public class MetaPageUpdateIndexDataRecord extends PageDeltaRecord {
    /** Index of the last reencrypted page. */
    private int encryptPageIdx;

    /** Total pages to be reencrypted. */
    private int encryptPageCnt;

    /**
     * @param grpId  Cache group ID.
     * @param pageId Page ID.
     * @param encryptPageIdx Index of the last reencrypted page.
     * @param encryptPageCnt Total pages to be reencrypted.
     */
    public MetaPageUpdateIndexDataRecord(int grpId, long pageId, int encryptPageIdx, int encryptPageCnt) {
        super(grpId, pageId);

        this.encryptPageIdx = encryptPageIdx;
        this.encryptPageCnt = encryptPageCnt;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        if (PageIO.getVersion(pageAddr) < 2)
            ((PageMetaIOV2)PageMetaIOV2.VERSIONS.latest()).upgradePage(pageAddr);

        PageMetaIOV2 io = (PageMetaIOV2)PageMetaIOV2.VERSIONS.forPage(pageAddr);

        io.setEncryptedPageIndex(pageAddr, encryptPageIdx);
        io.setEncryptedPageCount(pageAddr, encryptPageCnt);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.INDEX_META_PAGE_DELTA_RECORD;
    }

    /**
     * @param in Input.
     */
    public MetaPageUpdateIndexDataRecord(DataInput in) throws IOException {
        super(in.readInt(), in.readLong());

        encryptPageIdx = in.readInt();
        encryptPageCnt = in.readInt();
    }

    /**
     * @param buf Buffer.
     */
    public void toBytes(ByteBuffer buf) {
        buf.putInt(groupId());
        buf.putLong(pageId());

        buf.putInt(encryptionPagesIndex());
        buf.putInt(encryptionPagesCount());
    }

    /**
     * @return Index of the last reencrypted page.
     */
    private int encryptionPagesIndex() {
        return encryptPageIdx;
    }

    /**
     * @return Total pages to be reencrypted.
     */
    private int encryptionPagesCount() {
        return encryptPageCnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetaPageUpdateIndexDataRecord.class, this, "partId",
            PageIdUtils.partId(pageId()), "super", super.toString());
    }
}
