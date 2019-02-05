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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class PageMetaIO extends PageIO {
    /** */
    private static final int TREE_ROOT_OFF = PageIO.COMMON_HEADER_END + 8;

    /** */
    private static final int REUSE_LIST_ROOT_OFF = TREE_ROOT_OFF + 8;

    /** Last successful snapshot id offset. */
    private static final int LAST_SUCCESSFUL_SNAPSHOT_ID_OFF = REUSE_LIST_ROOT_OFF + 8;

    /** Last successful full snapshot id offset. */
    private static final int LAST_SUCCESSFUL_FULL_SNAPSHOT_ID_OFF = LAST_SUCCESSFUL_SNAPSHOT_ID_OFF + 8;

    /** Next snapshot id offset. */
    private static final int NEXT_SNAPSHOT_TAG_OFF = LAST_SUCCESSFUL_FULL_SNAPSHOT_ID_OFF + 8;

    /** Last successful full snapshot tag offset. */
    private static final int LAST_SUCCESSFUL_FULL_SNAPSHOT_TAG_OFF = NEXT_SNAPSHOT_TAG_OFF + 8;

    /** Last allocated pages count offset. */
    private static final int LAST_PAGE_COUNT_OFF = LAST_SUCCESSFUL_FULL_SNAPSHOT_TAG_OFF + 8;

    /** Candidate allocated page count offset. */
    private static final int CANDIDATE_PAGE_COUNT_OFF = LAST_PAGE_COUNT_OFF + 4;

    /** End of page meta. */
    static final int END_OF_PAGE_META = CANDIDATE_PAGE_COUNT_OFF + 4;

    /** */
    public static final IOVersions<PageMetaIO> VERSIONS = new IOVersions<>(
        new PageMetaIO(1)
    );

    /**
     * @param ver Page format version.
     */
    public PageMetaIO(int ver) {
        super(PageIO.T_META, ver);
    }

    /**
     * @param type Type.
     * @param ver Version.
     */
    protected PageMetaIO(int type, int ver) {
        super(type, ver);
    }

    /** {@inheritDoc} */
    @Override public void initNewPage(long pageAddr, long pageId, int pageSize) {
        super.initNewPage(pageAddr, pageId, pageSize);

        setTreeRoot(pageAddr, 0);
        setReuseListRoot(pageAddr, 0);
        setLastSuccessfulFullSnapshotId(pageAddr, 0);
        setLastSuccessfulSnapshotId(pageAddr, 0);
        setNextSnapshotTag(pageAddr, 1);
        setLastSuccessfulSnapshotTag(pageAddr, 0);
        setLastAllocatedPageCount(pageAddr, 0);
        setCandidatePageCount(pageAddr, 0);
    }

    /**
     * @param pageAddr Page address.
     * @return Tree root page.
     */
    public long getTreeRoot(long pageAddr) {
        return PageUtils.getLong(pageAddr, TREE_ROOT_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param treeRoot Tree root
     */
    public void setTreeRoot(long pageAddr, long treeRoot) {
        PageUtils.putLong(pageAddr, TREE_ROOT_OFF, treeRoot);
    }

    /**
     * @param pageAddr Page address.
     * @return Reuse list root page.
     */
    public long getReuseListRoot(long pageAddr) {
        return PageUtils.getLong(pageAddr, REUSE_LIST_ROOT_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param pageId Root page ID.
     */
    public void setReuseListRoot(long pageAddr, long pageId) {
        PageUtils.putLong(pageAddr, REUSE_LIST_ROOT_OFF, pageId);
    }

    /**
     * @param pageAddr Page address.
     * @param lastSuccessfulSnapshotId Last successful snapshot id.
     * @deprecated Will be removed at 3.0. See IGNITE-11139.
     */
    @Deprecated
    public void setLastSuccessfulSnapshotId(long pageAddr, long lastSuccessfulSnapshotId) {
        PageUtils.putLong(pageAddr, LAST_SUCCESSFUL_SNAPSHOT_ID_OFF, lastSuccessfulSnapshotId);
    }

    /**
     * @param pageAddr Page address.
     * @deprecated Will be removed at 3.0. See IGNITE-11139.
     */
    @Deprecated
    public long getLastSuccessfulSnapshotId(long pageAddr) {
        return PageUtils.getLong(pageAddr, LAST_SUCCESSFUL_SNAPSHOT_ID_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param lastSuccessfulFullSnapshotId Last successful full snapshot id.
     * @deprecated Will be removed at 3.0. See IGNITE-11139.
     */
    @Deprecated
    public void setLastSuccessfulFullSnapshotId(long pageAddr, long lastSuccessfulFullSnapshotId) {
        PageUtils.putLong(pageAddr, LAST_SUCCESSFUL_FULL_SNAPSHOT_ID_OFF, lastSuccessfulFullSnapshotId);
    }

    /**
     * @param pageAddr Page address.
     * @deprecated Will be removed at 3.0. See IGNITE-11139.
     */
    @Deprecated
    public long getLastSuccessfulFullSnapshotId(long pageAddr) {
        return PageUtils.getLong(pageAddr, LAST_SUCCESSFUL_FULL_SNAPSHOT_ID_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param nextSnapshotTag Next snapshot tag.
     * @deprecated Will be removed at 3.0. See IGNITE-11139.
     */
    @Deprecated
    public void setNextSnapshotTag(long pageAddr, long nextSnapshotTag) {
        PageUtils.putLong(pageAddr, NEXT_SNAPSHOT_TAG_OFF, nextSnapshotTag);
    }

    /**
     * @param pageAddr Page address.
     * @deprecated Will be removed at 3.0. See IGNITE-11139.
     */
    @Deprecated
    public long getLastSuccessfulSnapshotTag(long pageAddr) {
        return PageUtils.getLong(pageAddr, LAST_SUCCESSFUL_FULL_SNAPSHOT_TAG_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param lastSuccessfulSnapshotTag Last successful snapshot tag.
     * @deprecated Will be removed at 3.0. See IGNITE-11139.
     */
    @Deprecated
    public void setLastSuccessfulSnapshotTag(long pageAddr, long lastSuccessfulSnapshotTag) {
        PageUtils.putLong(pageAddr, LAST_SUCCESSFUL_FULL_SNAPSHOT_TAG_OFF, lastSuccessfulSnapshotTag);
    }

    /**
     * @param pageAddr Page address.
     * @deprecated Will be removed at 3.0. See IGNITE-11139.
     */
    @Deprecated
    public long getNextSnapshotTag(long pageAddr) {
        return PageUtils.getLong(pageAddr, NEXT_SNAPSHOT_TAG_OFF);
    }

    /**
     * Sets last allocated pages count, used to save and observe previous allocated count
     *
     * @param pageAddr Meta Page address.
     * @param pageCnt Last allocated pages count to set
     */
    public void setLastAllocatedPageCount(final long pageAddr, final int pageCnt) {
        PageUtils.putInt(pageAddr, LAST_PAGE_COUNT_OFF, pageCnt);
    }

    /**
     * Gets last allocated pages count from given buffer
     *
     * @param buf Buffer to read data from.
     */
    public int getLastAllocatedPageCount(@NotNull final ByteBuffer buf) {
        return buf.getInt(LAST_PAGE_COUNT_OFF);
    }

    /**
     * Gets last allocated pages count by provided address
     *
     * @param pageAddr Meta page address.
     * @return Last allocated page count
     */
    public int getLastAllocatedPageCount(final long pageAddr) {
        return PageUtils.getInt(pageAddr, LAST_PAGE_COUNT_OFF);
    }

    /**
     * @param pageAddr Page address.
     * @param pageCnt Last page count.
     */
    public boolean setCandidatePageCount(long pageAddr, int pageCnt) {
        if (getCandidatePageCount(pageAddr) == pageCnt)
            return false;

        PageUtils.putInt(pageAddr, CANDIDATE_PAGE_COUNT_OFF, pageCnt);

        return true;
    }

    /**
     * @param pageAddr Page address.
     */
    public int getCandidatePageCount(long pageAddr) {
        return PageUtils.getInt(pageAddr, CANDIDATE_PAGE_COUNT_OFF);
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("PageMeta[\n\ttreeRoot=").a(getReuseListRoot(addr))
            .a(",\n\tlastSuccessfulFullSnapshotId=").a(getLastSuccessfulFullSnapshotId(addr))
            .a(",\n\tlastSuccessfulSnapshotId=").a(getLastSuccessfulSnapshotId(addr))
            .a(",\n\tnextSnapshotTag=").a(getNextSnapshotTag(addr))
            .a(",\n\tlastSuccessfulSnapshotTag=").a(getLastSuccessfulSnapshotTag(addr))
            .a(",\n\tlastAllocatedPageCount=").a(getLastAllocatedPageCount(addr))
            .a(",\n\tcandidatePageCount=").a(getCandidatePageCount(addr))
            .a("\n]");
    }
}
