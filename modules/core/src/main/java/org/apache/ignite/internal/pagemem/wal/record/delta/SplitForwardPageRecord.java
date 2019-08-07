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
 * Split forward page record.
 */
public class SplitForwardPageRecord extends PageDeltaRecord {
    /** */
    private long fwdId;

    /** */
    private int ioType;

    /** */
    private int ioVer;

    /** */
    private long srcPageId;

    /** */
    private int mid;

    /** */
    private int cnt;

    /**
     * @param grpId Cache group ID.
     * @param pageId Real forward page ID.
     * @param fwdId Virtual forward page ID.
     * @param ioType IO Type.
     * @param ioVer IO Version.
     * @param srcPageId ID of the page which is about to be splitted.
     * @param mid Bisection index.
     * @param cnt Initial elements count in the page being split.
     */
    public SplitForwardPageRecord(
        int grpId,
        long pageId,
        long fwdId,
        int ioType,
        int ioVer,
        long srcPageId,
        int mid,
        int cnt
    ) {
        super(grpId, pageId);

        this.fwdId = fwdId;

        this.ioType = ioType;
        this.ioVer = ioVer;

        this.srcPageId = srcPageId;
        this.mid = mid;
        this.cnt = cnt;

        throw new IgniteException("Split forward page record should not be used directly (see GG-11640). " +
            "Clear the database directory and restart the node.");
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        throw new IgniteCheckedException("Split forward page record should not be logged.");
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_FORWARD_PAGE_SPLIT;
    }

    /**
     *
     */
    public long forwardId() {
        return fwdId;
    }

    /**
     *
     */
    public int ioType() {
        return ioType;
    }

    /**
     *
     */
    public int ioVersion() {
        return ioVer;
    }

    /**
     *
     */
    public long sourcePageId() {
        return srcPageId;
    }

    /**
     *
     */
    public int middleIndex() {
        return mid;
    }

    /**
     *
     */
    public int count() {
        return cnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SplitForwardPageRecord.class, this, "super", super.toString());
    }
}
