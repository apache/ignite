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
    private static final int METASTORE_ROOT_OFF = PageIO.COMMON_HEADER_END;

    /** */
    private static final int REUSE_LIST_ROOT_OFF = METASTORE_ROOT_OFF + 8;

    /** Last successful backup id offset. */
    private static final int LAST_SUCCESSFUL_BACKUP_ID_OFF = REUSE_LIST_ROOT_OFF + 8;

    /** Last successful full backup id offset. */
    private static final int LAST_SUCCESSFUL_FULL_BACKUP_ID_OFF = LAST_SUCCESSFUL_BACKUP_ID_OFF + 8;

    /** Next backup id offset. */
    private static final int NEXT_BACKUP_ID_OFF = LAST_SUCCESSFUL_FULL_BACKUP_ID_OFF + 8;

    /** Last allocated index offset. */
    private static final int LAST_ALLOCATED_INDEX_OFF = NEXT_BACKUP_ID_OFF + 8;

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
        setLastSuccessfulFullBackupId(buf, 0);
        setLastSuccessfulBackupId(buf, 0);
        setNextBackupId(buf, 1);
        setLastAllocatedIndex(buf, 0);
        setCandidateAllocatedIndex(buf, 0);
    }

    /**
     * @param buf Buffer.
     * @return Meta store root page.
     */
    public long getTreeRoot(ByteBuffer buf) {
        return buf.getLong(METASTORE_ROOT_OFF);
    }

    /**
     * @param buf Buffer.
     * @param metastoreRoot metastore root
     */
    public void setTreeRoot(@NotNull ByteBuffer buf, long metastoreRoot) {
        buf.putLong(METASTORE_ROOT_OFF, metastoreRoot);
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
     * @param lastSuccessfulBackupId Last successful backup id.
     */
    public void setLastSuccessfulBackupId(@NotNull ByteBuffer buf, long lastSuccessfulBackupId) {
        buf.putLong(LAST_SUCCESSFUL_BACKUP_ID_OFF, lastSuccessfulBackupId);
    }

    /**
     * @param buf Buffer.
     */
    public long getLastSuccessfulBackupId(@NotNull ByteBuffer buf) {
        return buf.getLong(LAST_SUCCESSFUL_BACKUP_ID_OFF);
    }

    /**
     * @param buf Buffer.
     * @param lastSuccessfulFullBackupId Last successful full backup id.
     */
    public void setLastSuccessfulFullBackupId(@NotNull ByteBuffer buf, long lastSuccessfulFullBackupId) {
        buf.putLong(LAST_SUCCESSFUL_FULL_BACKUP_ID_OFF, lastSuccessfulFullBackupId);
    }

    /**
     * @param buf Buffer.
     */
    public long getLastSuccessfulFullBackupId(@NotNull ByteBuffer buf) {
        return buf.getLong(LAST_SUCCESSFUL_FULL_BACKUP_ID_OFF);
    }

    /**
     * @param buf Buffer.
     * @param nextBackupId Next backup id.
     */
    public void setNextBackupId(@NotNull ByteBuffer buf, long nextBackupId) {
        buf.putLong(NEXT_BACKUP_ID_OFF, nextBackupId);
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

    /**
     * @param buf Buffer.
     */
    public long getNextBackupId(@NotNull ByteBuffer buf) {
        return buf.getLong(NEXT_BACKUP_ID_OFF);
    }
}
