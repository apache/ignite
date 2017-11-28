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
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Insert into inner or leaf page.
 */
public class InsertRecord<L> extends PageDeltaRecord {
    /** */
    private int idx;

    /** */
    private byte[] rowBytes;

    /** */
    @GridToStringExclude
    private long rightId;

    /** */
    private BPlusIO<L> io;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param io IO.
     * @param idx Index.
     * @param rowBytes Row bytes.
     * @param rightId Right ID.
     */
    public InsertRecord(
        int grpId,
        long pageId,
        BPlusIO<L> io,
        int idx,
        byte[] rowBytes,
        long rightId
    ) {
        super(grpId, pageId);

        this.io = io;
        this.idx = idx;
        this.rowBytes = rowBytes;
        this.rightId = rightId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        io.insert(pageAddr, idx, null, rowBytes, rightId, false);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_PAGE_INSERT;
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
     * @return Right ID.
     */
    public long rightId() {
        return rightId;
    }

    /**
     * @return Row bytes.
     */
    public byte[] rowBytes() {
        return rowBytes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(InsertRecord.class, this, "rightId", U.hexLong(rightId), "super", super.toString());
    }
}
