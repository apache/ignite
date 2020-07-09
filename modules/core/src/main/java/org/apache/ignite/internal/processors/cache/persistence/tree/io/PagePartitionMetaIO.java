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
 */
public class PagePartitionMetaIO extends PageMetaIO {
    /** */
    private static final int SIZE_OFF = PageMetaIO.END_OF_PAGE_META;

    /** */
    private static final int UPDATE_CNTR_OFF = SIZE_OFF + 8;

    /** */
    private static final int GLOBAL_RMV_ID_OFF = UPDATE_CNTR_OFF + 8;

    /** */
    private static final int PARTITION_STATE_OFF = GLOBAL_RMV_ID_OFF + 8;

    /** */
    private static final int NEXT_PART_META_PAGE_OFF = PARTITION_STATE_OFF + 1;

    /** End of page partition meta. */
    static final int END_OF_PARTITION_PAGE_META = NEXT_PART_META_PAGE_OFF + 8;

    /** */
    public static final IOVersions<PagePartitionMetaIO> VERSIONS = new IOVersions<>(
        new PagePartitionMetaIO(1),
        new PagePartitionMetaIOV2(2)
    );

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setSize(pageAddr, 0);
        setUpdateCounter(pageAddr, 0);
        setGlobalRemoveId(pageAddr, 0);
        setPartitionState(pageAddr, (byte)-1);
        setCountersPageId(pageAddr, 0);
    }

    /**
     * @param ver Version.
     */
    public PagePartitionMetaIO(int ver) {
        super(T_PART_META, ver);
    }

    /**
     * @param pageAddr Page address.
     * @return Partition size.
     */
    public long getSize(long pageAddr) {
        return PageUtils.getLong(pageAddr, SIZE_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param size Partition size.
     *
     * @return {@code true} if value has changed as a result of this method's invocation.
     */
    public boolean setSize(long pageAddr, long size) {
        if (getSize(pageAddr) == size)
            return false;

        PageUtils.putLong(pageAddr, SIZE_OFF, size);

        return true;
    }

    /**
     * @param pageAddr Page address.
     * @return Partition update counter.
     */
    public long getUpdateCounter(long pageAddr) {
        return PageUtils.getLong(pageAddr, UPDATE_CNTR_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param cntr Partition update counter.
     *
     * @return {@code true} if value has changed as a result of this method's invocation.
     */
    public boolean setUpdateCounter(long pageAddr, long cntr) {
        if (getUpdateCounter(pageAddr) == cntr)
            return false;

        PageUtils.putLong(pageAddr, UPDATE_CNTR_OFF, cntr);

        return true;
    }

    /**
     * @param pageAddr Page address.
     * @return Global remove ID.
     */
    public long getGlobalRemoveId(long pageAddr) {
        return PageUtils.getLong(pageAddr, GLOBAL_RMV_ID_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param rmvId Global remove ID.
     *
     * @return {@code true} if value has changed as a result of this method's invocation.
     */
    public boolean setGlobalRemoveId(long pageAddr, long rmvId) {
        if (getGlobalRemoveId(pageAddr) == rmvId)
            return false;

        PageUtils.putLong(pageAddr, GLOBAL_RMV_ID_OFF, rmvId);

        return true;
    }

    /**
     * @param pageAddr Page address.
     */
    public byte getPartitionState(long pageAddr) {
        return PageUtils.getByte(pageAddr, PARTITION_STATE_OFF);
    }

    /**
     * @param pageAddr Partition metadata page address.
     * @param state State.
     *
     * @return {@code true} if value has changed as a result of this method's invocation.
     */
    public boolean setPartitionState(long pageAddr, byte state) {
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
    public long getCountersPageId(long pageAddr) {
        return PageUtils.getLong(pageAddr, NEXT_PART_META_PAGE_OFF);
    }

    /**
     * Sets new reference to partition counters page (logical cache sizes).
     *
     * @param pageAddr Partition metadata page address.
     * @param cntrsPageId New cache sizes page ID.
     */
    public void setCountersPageId(long pageAddr, long cntrsPageId) {
        PageUtils.putLong(pageAddr, NEXT_PART_META_PAGE_OFF, cntrsPageId);
    }

    /**
     * Returns partition pending tree root. Pending tree is used to tracking expiring entries.
     *
     * @param pageAddr Page address.
     * @return Pending Tree root page.
     */
    public long getPendingTreeRoot(long pageAddr) {
        throw new UnsupportedOperationException("Per partition pending tree is not supported by " +
            "this PagePartitionMetaIO version: ver=" + getVersion());
    }

    /**
     * Sets new partition pending tree root.
     *
     * @param pageAddr Page address.
     * @param treeRoot Pending Tree root
     */
    public void setPendingTreeRoot(long pageAddr, long treeRoot) {
        throw new UnsupportedOperationException("Per partition pending tree is not supported by " +
            "this PagePartitionMetaIO version: ver=" + getVersion());
    }

    /**
     * @param pageAddr Page address.
     */
    public long getPartitionMetaStoreReuseListRoot(long pageAddr) {
        throw new UnsupportedOperationException("Partition metastore is not supported by " +
            "this PagePartitionMetaIO version: ver=" + getVersion());
    }

    /**
     * @param pageAddr Page address.
     * @param listRoot List root.
     */
    public void setPartitionMetaStoreReuseListRoot(long pageAddr, long listRoot) {
        throw new UnsupportedOperationException("Partition metastore is not supported by " +
            "this PagePartitionMetaIO version: ver=" + getVersion());
    }

    /**
     * @param pageAddr Page address.
     */
    public long getGapsLink(long pageAddr) {
        throw new UnsupportedOperationException("Gaps link is not supported by " +
            "this PagePartitionMetaIO version: ver=" + getVersion());
    }

    /**
     * @param pageAddr Page address.
     * @param link Link.
     *
     * @return {@code true} if value has changed as a result of this method's invocation.
     */
    public boolean setGapsLink(long pageAddr, long link) {
        throw new UnsupportedOperationException("Gaps link is not supported by " +
            "this PagePartitionMetaIO version: ver=" + getVersion());
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long pageAddr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        super.printPage(pageAddr, pageSize, sb);

        byte state = getPartitionState(pageAddr);

        sb.a(",\nPagePartitionMeta[\n\tsize=").a(getSize(pageAddr))
            .a(",\n\tupdateCounter=").a(getUpdateCounter(pageAddr))
            .a(",\n\tglobalRemoveId=").a(getGlobalRemoveId(pageAddr))
            .a(",\n\tpartitionState=").a(state).a("(").a(GridDhtPartitionState.fromOrdinal(state)).a(")")
            .a(",\n\tcountersPageId=").a(getCountersPageId(pageAddr))
            .a("\n]");
    }
}
