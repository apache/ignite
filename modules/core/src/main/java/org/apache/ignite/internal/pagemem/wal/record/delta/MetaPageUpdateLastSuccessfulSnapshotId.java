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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageMetaIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * @deprecated Will be removed at 3.0. See IGNITE-11139.
 */
@Deprecated
public class MetaPageUpdateLastSuccessfulSnapshotId extends PageDeltaRecord {
    /** */
    private final long lastSuccessfulSnapshotId;

    /** Last successful snapshot tag. */
    private final long lastSuccessfulSnapshotTag;

    /**
     * @param pageId Meta page ID.
     * @param snapshotTag
     */
    public MetaPageUpdateLastSuccessfulSnapshotId(int grpId, long pageId, long lastSuccessfulSnapshotId, long snapshotTag) {
        super(grpId, pageId);

        this.lastSuccessfulSnapshotId = lastSuccessfulSnapshotId;
        this.lastSuccessfulSnapshotTag = snapshotTag;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        PageMetaIO io = PageMetaIO.VERSIONS.forPage(pageAddr);

        io.setLastSuccessfulSnapshotId(pageAddr, lastSuccessfulSnapshotId);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID;
    }

    /**
     * @return lastSuccessfulSnapshotId
     */
    public long lastSuccessfulSnapshotId() {
        return lastSuccessfulSnapshotId;
    }

    /**
     * @return lastSuccessfulSnapshotTag
     */
    public long lastSuccessfulSnapshotTag() {
        return lastSuccessfulSnapshotTag;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetaPageUpdateLastSuccessfulSnapshotId.class, this, "super", super.toString());
    }
}

