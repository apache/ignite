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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV2;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Partition meta page delta record.
 * Contains information about tombstones count.
 */
public class MetaPageUpdatePartitionDataRecordV3 extends MetaPageUpdatePartitionDataRecordV2 {
    /** Tombstones count. */
    private long tombstonesCnt;

    /**
     * @param grpId Group id.
     * @param pageId Page id.
     * @param updateCntr Update counter.
     * @param globalRmvId Global remove id.
     * @param partSize Partition size.
     * @param cacheSizesPageId Cache sizes page id.
     * @param state State.
     * @param allocatedIdxCandidate Allocated index candidate.
     * @param gapsLink Gaps link.
     * @param tombstonesCnt Tombstones count.
     */
    public MetaPageUpdatePartitionDataRecordV3(
        int grpId,
        long pageId,
        long updateCntr,
        long globalRmvId,
        int partSize,
        long cacheSizesPageId,
        byte state,
        int allocatedIdxCandidate,
        long gapsLink,
        long tombstonesCnt
    ) {
        super(grpId, pageId, updateCntr, globalRmvId, partSize, cacheSizesPageId, state, allocatedIdxCandidate, gapsLink);
        this.tombstonesCnt = tombstonesCnt;
    }

    /**
     * @param in In.
     */
    public MetaPageUpdatePartitionDataRecordV3(DataInput in) throws IOException {
        super(in);

        this.tombstonesCnt = in.readLong();
    }

    /**
     * @return Tombstones count.
     */
    public long tombstonesCount() {
        return tombstonesCnt;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        super.applyDelta(pageMem, pageAddr);

        PagePartitionMetaIOV2 io = (PagePartitionMetaIOV2) PagePartitionMetaIO.VERSIONS.forPage(pageAddr);

        io.setTombstonesCount(pageAddr, tombstonesCnt);
    }

    /** {@inheritDoc} */
    @Override public void toBytes(ByteBuffer buf) {
        super.toBytes(buf);

        buf.putLong(tombstonesCnt);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PARTITION_META_PAGE_UPDATE_COUNTERS_V3;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetaPageUpdatePartitionDataRecordV2.class, this, "partId", PageIdUtils.partId(pageId()),
            "super", super.toString());
    }
}
