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

/** */
public class PagePartitionMetaIOV3 extends PagePartitionMetaIOV2 {
    /** */
    private static final int SIZE_OFF = PageMetaIO.END_OF_PAGE_META_V2;

    /** */
    private static final int UPDATE_CNTR_OFF = SIZE_OFF + 8;

    /** */
    private static final int GLOBAL_RMV_ID_OFF = UPDATE_CNTR_OFF + 8;

    /** */
    private static final int PARTITION_STATE_OFF = GLOBAL_RMV_ID_OFF + 8;

    /** */
    private static final int NEXT_PART_META_PAGE_OFF = PARTITION_STATE_OFF + 1;

    /** */
    private static final int PENDING_TREE_ROOT_OFF = NEXT_PART_META_PAGE_OFF + 8;

    /** */
    private static final int PART_META_REUSE_LIST_ROOT_OFF = PENDING_TREE_ROOT_OFF + 8;

    /** */
    private static final int GAPS_LINK = PART_META_REUSE_LIST_ROOT_OFF + 8;

    /** */
    private static final int END_OF_PARTITION_META_V3 = GAPS_LINK + 8;

    /**
     * @param ver Version.
     */
    public PagePartitionMetaIOV3(int ver) {
        super(ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setSize(pageAddr, 0);
        setUpdateCounter(pageAddr, 0);
        setGlobalRemoveId(pageAddr, 0);
        setPartitionState(pageAddr, (byte)-1);
        setCountersPageId(pageAddr, 0);
        setPendingTreeRoot(pageAddr, 0L);
        setPartitionMetaStoreReuseListRoot(pageAddr, 0L);
        setGapsLink(pageAddr, 0);
    }

    /**
     * @param pageAddr Page address.
     * @return Partition size.
     */
    @Override public long getSize(long pageAddr) {
        return PageUtils.getLong(pageAddr, SIZE_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param size Partition size.
     *
     * @return {@code true} if value has changed as a result of this method's invocation.
     */
    @Override public boolean setSize(long pageAddr, long size) {
        if (getSize(pageAddr) == size)
            return false;

        PageUtils.putLong(pageAddr, SIZE_OFF, size);

        return true;
    }

    /**
     * @param pageAddr Page address.
     * @return Partition update counter.
     */
    @Override public long getUpdateCounter(long pageAddr) {
        return PageUtils.getLong(pageAddr, UPDATE_CNTR_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param cntr Partition update counter.
     *
     * @return {@code true} if value has changed as a result of this method's invocation.
     */
    @Override public boolean setUpdateCounter(long pageAddr, long cntr) {
        if (getUpdateCounter(pageAddr) == cntr)
            return false;

        PageUtils.putLong(pageAddr, UPDATE_CNTR_OFF, cntr);

        return true;
    }

    /**
     * @param pageAddr Page address.
     * @return Global remove ID.
     */
    @Override public long getGlobalRemoveId(long pageAddr) {
        return PageUtils.getLong(pageAddr, GLOBAL_RMV_ID_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param rmvId Global remove ID.
     *
     * @return {@code true} if value has changed as a result of this method's invocation.
     */
    @Override public boolean setGlobalRemoveId(long pageAddr, long rmvId) {
        if (getGlobalRemoveId(pageAddr) == rmvId)
            return false;

        PageUtils.putLong(pageAddr, GLOBAL_RMV_ID_OFF, rmvId);

        return true;
    }

    /**
     * @param pageAddr Page address.
     */
    @Override public byte getPartitionState(long pageAddr) {
        return PageUtils.getByte(pageAddr, PARTITION_STATE_OFF);
    }

    /**
     * @param pageAddr Partition metadata page address.
     * @param state State.
     *
     * @return {@code true} if value has changed as a result of this method's invocation.
     */
    @Override public boolean setPartitionState(long pageAddr, byte state) {
        if (getPartitionState(pageAddr) == state)
            return false;

        PageUtils.putByte(pageAddr, PARTITION_STATE_OFF, state);

        return true;
    }

    /**
     * Returns partition counters page identifier, page with caches in cache group sizes.
     *
     * @param pageAddr Partition metadata page address.
     * @return Next meta partial page ID or {@code 0} if it does not exist.
     */
    @Override public long getCountersPageId(long pageAddr) {
        return PageUtils.getLong(pageAddr, NEXT_PART_META_PAGE_OFF);
    }

    /**
     * Sets new reference to partition counters page (logical cache sizes).
     *
     * @param pageAddr Partition metadata page address.
     * @param cntrsPageId New cache sizes page ID.
     */
    @Override public void setCountersPageId(long pageAddr, long cntrsPageId) {
        PageUtils.putLong(pageAddr, NEXT_PART_META_PAGE_OFF, cntrsPageId);
    }

    /** {@inheritDoc} */
    @Override public long getPendingTreeRoot(long pageAddr) {
        return PageUtils.getLong(pageAddr, PENDING_TREE_ROOT_OFF);
    }

    /** {@inheritDoc} */
    @Override public void setPendingTreeRoot(long pageAddr, long listRoot) {
        PageUtils.putLong(pageAddr, PENDING_TREE_ROOT_OFF, listRoot);
    }

    /**
     * @param pageAddr Page address.
     */
    @Override public long getPartitionMetaStoreReuseListRoot(long pageAddr) {
        return PageUtils.getLong(pageAddr, PART_META_REUSE_LIST_ROOT_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param listRoot List root.
     */
    @Override public void setPartitionMetaStoreReuseListRoot(long pageAddr, long listRoot) {
        PageUtils.putLong(pageAddr, PART_META_REUSE_LIST_ROOT_OFF, listRoot);
    }

    /**
     * @param pageAddr Page address.
     * @return Partition size.
     */
    @Override public long getGapsLink(long pageAddr) {
        return PageUtils.getLong(pageAddr, GAPS_LINK);
    }

    /**
     * @param pageAddr Page address.
     * @param link Link.
     *
     * @return {@code true} if value has changed as a result of this method's invocation.
     */
    @Override public boolean setGapsLink(long pageAddr, long link) {
        if (getGapsLink(pageAddr) == link)
            return false;

        PageUtils.putLong(pageAddr, GAPS_LINK, link);

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
        sb.a(",\n\tsize=").a(getSize(pageAddr));
        sb.a(",\n\tupdateCounter=").a(getUpdateCounter(pageAddr));
        sb.a(",\n\tglobalRemoveId=").a(getGlobalRemoveId(pageAddr));
        sb.a(",\n\tpartitionState=").a(state).a("(").a(GridDhtPartitionState.fromOrdinal(state)).a(")");
        sb.a(",\n\tcountersPageId=").a(getCountersPageId(pageAddr));
        sb.a(",\n\tcntrUpdDataPageId=").a(getGapsLink(pageAddr));
        sb.a("\n]");
    }

    /**
     * Upgrade page to PagePartitionMetaIOV2
     *
     * @param pageAddr Page address.
     */
    @Override public void upgradePage(long pageAddr) {
        assert PageIO.getType(pageAddr) == getType();
        assert PageIO.getVersion(pageAddr) < 3;

        if (PageIO.getVersion(pageAddr) < 2)
            super.upgradePage(pageAddr);

        PageIO.setVersion(pageAddr, getVersion());

        byte[] bytes = PageUtils.getBytes(pageAddr, END_OF_PAGE_META, END_OF_PARTITION_META_V3 - SIZE_OFF);

        PageUtils.putBytes(pageAddr, END_OF_PAGE_META_V2, bytes);

        setReserved(pageAddr, -1L);
    }
}
