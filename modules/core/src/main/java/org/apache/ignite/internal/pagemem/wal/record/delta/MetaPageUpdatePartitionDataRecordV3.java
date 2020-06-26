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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV3;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Partition meta page delta record includes encryption status data.
 */
public class MetaPageUpdatePartitionDataRecordV3 extends MetaPageUpdatePartitionDataRecordV2 {
    /** Index of the last reencrypted page. */
    private int encryptPageIdx;

    /** Total pages to be reencrypted. */
    private int encryptPageCnt;

    /**
     * @param grpId Group id.
     * @param pageId Page id.
     * @param updateCntr Update counter.
     * @param globalRmvId Global remove id.
     * @param partSize Partition size.
     * @param cntrsPageId Cntrs page id.
     * @param state State.
     * @param allocatedIdxCandidate Allocated index candidate.
     * @param link Link.
     * @param encryptPageIdx Index of the last reencrypted page.
     * @param encryptPageCnt Total pages to be reencrypted.
     */
    public MetaPageUpdatePartitionDataRecordV3(
        int grpId,
        long pageId,
        long updateCntr,
        long globalRmvId,
        int partSize,
        long cntrsPageId,
        byte state,
        int allocatedIdxCandidate,
        long link,
        int encryptPageIdx,
        int encryptPageCnt) {
        super(grpId, pageId, updateCntr, globalRmvId, partSize, cntrsPageId, state, allocatedIdxCandidate, link);

        this.encryptPageIdx = encryptPageIdx;
        this.encryptPageCnt = encryptPageCnt;
    }

    /**
     * @param in Input.
     */
    public MetaPageUpdatePartitionDataRecordV3(DataInput in) throws IOException {
        super(in);

        encryptPageIdx = in.readInt();
        encryptPageCnt = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        super.applyDelta(pageMem, pageAddr);

        PagePartitionMetaIOV3 io = (PagePartitionMetaIOV3)PagePartitionMetaIO.VERSIONS.forPage(pageAddr);

        io.setEncryptPageIndex(pageAddr, encryptPageIdx);
        io.setEncryptPageCount(pageAddr, encryptPageCnt);
    }

    /**
     * @return Index of the last reencrypted page.
     */
    public int encryptPageIndex() {
        return encryptPageIdx;
    }

    /**
     * @return Total pages to be reencrypted.
     */
    public int encryptPagesCount() {
        return encryptPageCnt;
    }

    /** {@inheritDoc} */
    @Override public void toBytes(ByteBuffer buf) {
        super.toBytes(buf);

        buf.putInt(encryptPageIndex());
        buf.putInt(encryptPagesCount());
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PARTITION_META_PAGE_UPDATE_COUNTERS_V3;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetaPageUpdatePartitionDataRecordV3.class, this, "partId",
            PageIdUtils.partId(pageId()), "super", super.toString());
    }
}
