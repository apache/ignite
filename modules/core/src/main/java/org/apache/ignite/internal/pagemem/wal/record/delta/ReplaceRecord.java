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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Replace.
 */
public class ReplaceRecord<L> extends PageDeltaRecord {
    /** */
    private BPlusIO<L> io;

    /** */
    private byte[] rowBytes;

    /** */
    private int idx;

    /**
     * @param grpId Cache group ID.
     * @param pageId  Page ID.
     * @param io IO.
     * @param rowBytes Row bytes.
     * @param idx Index.
     */
    public ReplaceRecord(int grpId, long pageId, BPlusIO<L> io, byte[] rowBytes, int idx) {
        super(grpId, pageId);

        this.io = io;
        this.rowBytes = rowBytes;
        this.idx = idx;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr)
        throws IgniteCheckedException {
        if (io.getCount(pageAddr) < idx)
            throw new DeltaApplicationException("Index is greater than count: " + idx);

        io.store(pageAddr, idx, null, rowBytes, false);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_PAGE_REPLACE;
    }

    /**
     * @return IO.
     */
    public BPlusIO<L> io() {
        return io;
    }

    /**
     * @return Index.
     */
    public int index() {
        return idx;
    }

    /**
     * @return Row bytes.
     */
    public byte[] rowBytes() {
        return rowBytes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ReplaceRecord.class, this, "super", super.toString());
    }
}
