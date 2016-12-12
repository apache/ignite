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

package org.apache.ignite.internal.processors.cache.database.tree.io;

import java.nio.ByteBuffer;
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

    /** Last allocated index offset. */
    private static final int LAST_ALLOCATED_INDEX_OFF = LAST_SUCCESSFUL_FULL_SNAPSHOT_TAG_OFF + 8;

    /** Candidate allocated index offset. */
    private static final int CANDIDATE_ALLOCATED_INDEX_OFF = LAST_ALLOCATED_INDEX_OFF + 4;

    /** End of page meta. */
    static final int END_OF_PAGE_META = CANDIDATE_ALLOCATED_INDEX_OFF + 4;

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
    @Override public void initNewPage(ByteBuffer buf, long pageId) {
        super.initNewPage(buf, pageId);

        setTreeRoot(buf, 0);
        setReuseListRoot(buf, 0);
        setLastSuccessfulFullSnapshotId(buf, 0);
        setLastSuccessfulSnapshotId(buf, 0);
        setNextSnapshotTag(buf, 1);
        setLastAllocatedIndex(buf, 0);
        setCandidateAllocatedIndex(buf, 0);
    }

    /**
     * @param buf Buffer.
     * @return Tree root page.
     */
    public long getTreeRoot(ByteBuffer buf) {
        return buf.getLong(TREE_ROOT_OFF);
    }

    /**
     * @param buf Buffer.
     * @param treeRoot Tree root
     */
    public void setTreeRoot(@NotNull ByteBuffer buf, long treeRoot) {
        buf.putLong(TREE_ROOT_OFF, treeRoot);
    }

    /**
     * @param buf Buffer.
     * @return Reuse list root page.
     */
    public long getReuseListRoot(ByteBuffer buf) {
        return buf.getLong(REUSE_LIST_ROOT_OFF);
    }

    /**
     * @param buf Buffer.
     * @param pageId Root page ID.
     */
    public void setReuseListRoot(@NotNull ByteBuffer buf, long pageId) {
        buf.putLong(REUSE_LIST_ROOT_OFF, pageId);
    }

    /**
     * @param buf Buffer.
     * @param lastSuccessfulSnapshotId Last successful snapshot id.
     */
    public void setLastSuccessfulSnapshotId(@NotNull ByteBuffer buf, long lastSuccessfulSnapshotId) {
        buf.putLong(LAST_SUCCESSFUL_SNAPSHOT_ID_OFF, lastSuccessfulSnapshotId);
    }

    /**
     * @param buf Buffer.
     */
    public long getLastSuccessfulSnapshotId(@NotNull ByteBuffer buf) {
        return buf.getLong(LAST_SUCCESSFUL_SNAPSHOT_ID_OFF);
    }

    /**
     * @param buf Buffer.
     * @param lastSuccessfulFullSnapshotId Last successful full snapshot id.
     */
    public void setLastSuccessfulFullSnapshotId(@NotNull ByteBuffer buf, long lastSuccessfulFullSnapshotId) {
        buf.putLong(LAST_SUCCESSFUL_FULL_SNAPSHOT_ID_OFF, lastSuccessfulFullSnapshotId);
    }

    /**
     * @param buf Buffer.
     */
    public long getLastSuccessfulFullSnapshotId(@NotNull ByteBuffer buf) {
        return buf.getLong(LAST_SUCCESSFUL_FULL_SNAPSHOT_ID_OFF);
    }

    /**
     * @param buf Buffer.
     * @param nextSnapshotId Next snapshot id.
     */
    public void setNextSnapshotTag(@NotNull ByteBuffer buf, long nextSnapshotId) {
        buf.putLong(NEXT_SNAPSHOT_TAG_OFF, nextSnapshotId);
    }

    /**
     * @param buf Buffer.
     */
    public long getLastSuccessfulSnapshotTag(@NotNull ByteBuffer buf) {
        return buf.getLong(LAST_SUCCESSFUL_FULL_SNAPSHOT_TAG_OFF);
    }

    /**
     * @param buf Buffer.
     * @param lastSuccessfulSnapshotTag Last successful snapshot tag.
     */
    public void setLastSuccessfulSnapshotTag(@NotNull ByteBuffer buf, long lastSuccessfulSnapshotTag) {
        buf.putLong(LAST_SUCCESSFUL_FULL_SNAPSHOT_TAG_OFF, lastSuccessfulSnapshotTag);
    }

    /**
     * @param buf Buffer.
     */
    public long getNextSnapshotTag(@NotNull ByteBuffer buf) {
        return buf.getLong(NEXT_SNAPSHOT_TAG_OFF);
    }

    /**
     * @param buf Buffer.
     * @param lastAllocatedIdx Last allocated index.
     */
    public void setLastAllocatedIndex(@NotNull ByteBuffer buf, int lastAllocatedIdx) {
        buf.putInt(LAST_ALLOCATED_INDEX_OFF, lastAllocatedIdx);
    }

    /**
     * @param buf Buffer.
     */
    public int getLastAllocatedIndex(@NotNull ByteBuffer buf) {
        return buf.getInt(LAST_ALLOCATED_INDEX_OFF);
    }

    /**
     * @param buf Buffer.
     * @param previousAllocatedIdx Last allocated index.
     */
    public void setCandidateAllocatedIndex(@NotNull ByteBuffer buf, int previousAllocatedIdx) {
        buf.putInt(CANDIDATE_ALLOCATED_INDEX_OFF, previousAllocatedIdx);
    }

    /**
     * @param buf Buffer.
     */
    public int getCandidateAllocatedIndex(@NotNull ByteBuffer buf) {
        return buf.getInt(CANDIDATE_ALLOCATED_INDEX_OFF);
    }
}
