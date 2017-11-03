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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Inner replace on remove.
 */
public class InnerReplaceRecord<L> extends PageDeltaRecord {
    /** */
    private int dstIdx;

    /** */
    private long srcPageId;

    /** */
    private int srcIdx;

    /** */
    private long rmvId;

    /**
     * @param grpId Cache group ID.
     * @param pageId  Page ID.
     * @param dstIdx Destination index.
     * @param srcPageId Source page ID.
     * @param srcIdx Source index.
     */
    public InnerReplaceRecord(int grpId, long pageId, int dstIdx, long srcPageId, int srcIdx, long rmvId) {
        super(grpId, pageId);

        this.dstIdx = dstIdx;
        this.srcPageId = srcPageId;
        this.srcIdx = srcIdx;
        this.rmvId = rmvId;

        throw new IgniteException("Inner replace record should not be used directly (see GG-11640). " +
            "Clear the database directory and restart the node.");
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        throw new IgniteCheckedException("Inner replace record should not be logged.");
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_PAGE_INNER_REPLACE;
    }

    /**
     * @return Destination index.
     */
    public int destinationIndex() {
        return dstIdx;
    }

    /**
     * @return Source page ID.
     */
    public long sourcePageId() {
        return srcPageId;
    }

    /**
     * @return Source index.
     */
    public int sourceIndex() {
        return srcIdx;
    }

    /**
     * @return Remove ID.
     */
    public long removeId() {
        return rmvId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(InnerReplaceRecord.class, this, "super", super.toString());
    }
}
