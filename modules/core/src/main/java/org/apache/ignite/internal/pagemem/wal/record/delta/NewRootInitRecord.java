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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Initialize new root page.
 */
public class NewRootInitRecord<L> extends PageDeltaRecord {
    /** */
    private long newRootId;

    /** */
    private BPlusInnerIO<L> io;

    /** */
    private long leftChildId;

    /** */
    private byte[] rowBytes;

    /** */
    private long rightChildId;

    /**
     * @param grpId Cache group ID.
     * @param pageId  Page ID.
     * @param io IO.
     * @param leftChildId Left child ID.
     * @param rowBytes Row.
     * @param rightChildId Right child ID.
     */
    public NewRootInitRecord(
        int grpId,
        long pageId,
        long newRootId,
        BPlusInnerIO<L> io,
        long leftChildId,
        byte[] rowBytes,
        long rightChildId
    ) {
        super(grpId, pageId);

        assert io != null;

        this.newRootId = newRootId;
        this.io = io;
        this.leftChildId = leftChildId;
        this.rowBytes = rowBytes;
        this.rightChildId = rightChildId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        io.initNewRoot(pageAddr, newRootId, leftChildId, null, rowBytes, rightChildId, pageMem.realPageSize(groupId()),
            false);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_INIT_NEW_ROOT;
    }

    /**
     * @return IO.
     */
    public BPlusInnerIO<L> io() {
        return io;
    }

    /**
     * @return Root page ID.
     */
    public long rootId() {
        return newRootId;
    }

    /**
     * @return Left child ID.
     */
    public long leftId() {
        return leftChildId;
    }

    /**
     * @return Right child ID.
     */
    public long rightId() {
        return rightChildId;
    }

    /**
     * @return Row bytes.
     */
    public byte[] rowBytes() {
        return rowBytes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NewRootInitRecord.class, this, "super", super.toString());
    }
}
