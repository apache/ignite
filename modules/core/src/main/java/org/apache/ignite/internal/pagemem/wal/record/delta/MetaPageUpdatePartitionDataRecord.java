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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class MetaPageUpdatePartitionDataRecord extends PageDeltaRecord {
    /** */
    private long updateCntr;

    /** */
    private long globalRmvId;

    /** TODO: Partition size may be long */
    private int partSize;

    /** */
    private byte state;

    /** */
    private int allocatedIdxCandidate;

    /** */
    private long cntrsPageId;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param allocatedIdxCandidate Page Allocated index candidate
     */
    public MetaPageUpdatePartitionDataRecord(
        int grpId,
        long pageId,
        long updateCntr,
        long globalRmvId,
        int partSize,
        long cntrsPageId,
        byte state,
        int allocatedIdxCandidate) {
        super(grpId, pageId);

        this.updateCntr = updateCntr;
        this.globalRmvId = globalRmvId;
        this.partSize = partSize;
        this.state = state;
        this.allocatedIdxCandidate = allocatedIdxCandidate;
        this.cntrsPageId = cntrsPageId;
    }

    /**
     * @param in Input.
     */
    public MetaPageUpdatePartitionDataRecord(DataInput in) throws IOException {
        super(in.readInt(), in.readLong());

        this.updateCntr = in.readLong();
        this.globalRmvId = in.readLong();
        this.partSize = in.readInt();
        this.cntrsPageId = in.readLong();
        this.state = in.readByte();
        this.allocatedIdxCandidate = in.readInt();
    }

    /**
     * @return Update counter.
     */
    public long updateCounter() {
        return updateCntr;
    }

    /**
     * @return Global remove ID.
     */
    public long globalRemoveId() {
        return globalRmvId;
    }

    /**
     * @return Partition size.
     */
    public int partitionSize() {
        return partSize;
    }

    /**
     * @return Partition size.
     */
    public long countersPageId() {
        return cntrsPageId;
    }

    /**
     * @return Partition state
     */
    public byte state() {
        return state;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.forPage(pageAddr);

        io.setUpdateCounter(pageAddr, updateCntr);
        io.setGlobalRemoveId(pageAddr, globalRmvId);
        io.setSize(pageAddr, partSize);
        io.setCountersPageId(pageAddr, cntrsPageId);
        io.setPartitionState(pageAddr, state);
        io.setCandidatePageCount(pageAddr, allocatedIdxCandidate);
    }

    /**
     *
     */
    public int allocatedIndexCandidate() {
        return allocatedIdxCandidate;
    }

    /**
     * @param buf Buffer.
     */
    public void toBytes(ByteBuffer buf) {
        buf.putInt(groupId());
        buf.putLong(pageId());

        buf.putLong(updateCounter());
        buf.putLong(globalRemoveId());
        buf.putInt(partitionSize());
        buf.putLong(countersPageId());
        buf.put(state());
        buf.putInt(allocatedIndexCandidate());
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PARTITION_META_PAGE_UPDATE_COUNTERS;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetaPageUpdatePartitionDataRecord.class, this, "partId", PageIdUtils.partId(pageId()), "super", super.toString());
    }
}
