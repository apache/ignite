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
 *
 */

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * IO for partition metadata pages.
 * Persistent partition contains it's own PendingTree.
 */
public class PagePartitionMetaIOV3 extends PagePartitionMetaIOV2 {
    /** Last reencrypted page index offset. */
    private static final int ENCRYPT_PAGE_IDX_OFF = END_OF_PARTITION_PAGE_META_V2;

    /** Total pages to be reencrypted offset. */
    private static final int ENCRYPT_PAGE_MAX_OFF = ENCRYPT_PAGE_IDX_OFF + 4;

    /**
     * @param ver Version.
     */
    public PagePartitionMetaIOV3(int ver) {
        super(ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setEncryptedPageIndex(pageAddr, 0);
        setEncryptedPageCount(pageAddr, 0);
    }

    /**
     * @param pageAddr Page address.
     * @return Index of the last reencrypted page.
     */
    public int getEncryptedPageIndex(long pageAddr) {
        return PageUtils.getInt(pageAddr, ENCRYPT_PAGE_IDX_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param pageIdx Index of the last reencrypted page.
     *
     * @return {@code true} if value has changed as a result of this method's invocation.
     */
    public boolean setEncryptedPageIndex(long pageAddr, int pageIdx) {
        if (getEncryptedPageIndex(pageAddr) == pageIdx)
            return false;

        PageUtils.putLong(pageAddr, ENCRYPT_PAGE_IDX_OFF, pageIdx);

        return true;
    }

    /**
     * @param pageAddr Page address.
     * @return Total pages to be reencrypted.
     */
    public int getEncryptedPageCount(long pageAddr) {
        return PageUtils.getInt(pageAddr, ENCRYPT_PAGE_MAX_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param pagesCnt Total pages to be reencrypted.
     *
     * @return {@code true} if value has changed as a result of this method's invocation.
     */
    public boolean setEncryptedPageCount(long pageAddr, int pagesCnt) {
        if (getEncryptedPageCount(pageAddr) == pagesCnt)
            return false;

        PageUtils.putInt(pageAddr, ENCRYPT_PAGE_MAX_OFF, pagesCnt);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long pageAddr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        byte state = getPartitionState(pageAddr);

        sb.a("PagePartitionMeta[\n\ttreeRoot=").a(getReuseListRoot(pageAddr));
        sb.a(",\n\tpendingTreeRoot=").a(getLastSuccessfulFullSnapshotId(pageAddr));
        sb.a(",\n\tlastSuccessfulFullSnapshotId=").a(getLastSuccessfulFullSnapshotId(pageAddr));
        sb.a(",\n\tlastSuccessfulSnapshotId=").a(getLastSuccessfulSnapshotId(pageAddr));
        sb.a(",\n\tnextSnapshotTag=").a(getNextSnapshotTag(pageAddr));
        sb.a(",\n\tlastSuccessfulSnapshotTag=").a(getLastSuccessfulSnapshotTag(pageAddr));
        sb.a(",\n\tlastAllocatedPageCount=").a(getLastAllocatedPageCount(pageAddr));
        sb.a(",\n\tcandidatePageCount=").a(getCandidatePageCount(pageAddr));
        sb.a(",\n\tencryptedPageIndex=").a(getEncryptedPageIndex(pageAddr));
        sb.a(",\n\tencryptedPageCount=").a(getEncryptedPageCount(pageAddr));
        sb.a(",\n\tsize=").a(getSize(pageAddr));
        sb.a(",\n\tupdateCounter=").a(getUpdateCounter(pageAddr));
        sb.a(",\n\tglobalRemoveId=").a(getGlobalRemoveId(pageAddr));
        sb.a(",\n\tpartitionState=").a(state).a("(").a(GridDhtPartitionState.fromOrdinal(state)).a(")");
        sb.a(",\n\tcountersPageId=").a(getCountersPageId(pageAddr));
        sb.a(",\n\tcntrUpdDataPageId=").a(getGapsLink(pageAddr));
        sb.a(",\n\tencryptedPageIndex=").a(getEncryptedPageIndex(pageAddr));
        sb.a(",\n\tencryptedPageCount=").a(getEncryptedPageCount(pageAddr));
        sb.a("\n]");
    }

    /**
     * Upgrade page to PagePartitionMetaIOV3.
     *
     * @param pageAddr Page address.
     */
    @Override public void upgradePage(long pageAddr) {
        assert PageIO.getType(pageAddr) == getType();
        assert PageIO.getVersion(pageAddr) < 3;

        PageIO.setVersion(pageAddr, getVersion());

        setEncryptedPageIndex(pageAddr, 0);
        setEncryptedPageCount(pageAddr, 0);
    }
}
